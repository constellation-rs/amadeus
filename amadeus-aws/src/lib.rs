use amadeus_core::{
	dist_iter::{Consumer, DistributedIterator}, into_dist_iter::IteratorExt, util::ResultExpand
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use flate2::read::MultiGzDecoder;
use http::{Method, StatusCode};
use rusoto_core::Region;
use rusoto_s3::{S3Client, S3};
use serde::{Deserialize, Serialize};
use serde_closure::*;
use std::{
	convert::identity, error, fmt::{self, Display}, io::{self, BufRead, BufReader}, iter, net, ops::FnMut, sync::Arc, time::Duration, vec
};
use url::Url;

// https://docs.datadoghq.com/integrations/amazon_web_services/?tab=allpermissions#enable-logging-for-your-aws-service

type Closure<Env, Args, Output> =
	serde_closure::FnMut<Env, for<'r> fn(&'r mut Env, Args) -> Output>;

type CloudfrontInner = amadeus_core::dist_iter::Map<
	amadeus_core::dist_iter::FlatMap<
		amadeus_core::into_dist_iter::IterIter<vec::IntoIter<String>>,
		Closure<
			(String, Region),
			(String,),
			ResultExpand<
				iter::Map<
					iter::Filter<
						io::Lines<BufReader<MultiGzDecoder<Box<dyn io::Read + Send>>>>,
						serde_closure::FnMut<
							(),
							for<'r, 'a> fn(&'r mut (), (&'a Result<String, io::Error>,)) -> bool,
						>,
					>,
					Closure<(), (Result<String, io::Error>,), Result<CloudfrontRow, Error>>,
				>,
				Error,
			>,
		>,
	>,
	Closure<(), (Result<Result<CloudfrontRow, Error>, Error>,), Result<CloudfrontRow, Error>>,
>;

pub struct Cloudfront {
	i: CloudfrontInner,
}
impl Cloudfront {
	pub fn new(region: Region, bucket: &str, prefix: &str) -> Result<Self, Error> {
		let (bucket, prefix) = (bucket.to_owned(), prefix.to_owned());
		let s3client = S3Client::new(region.clone());

		let (mut first, mut continuation_token) = (true, None);
		let objects: Result<Vec<String>, _> = iter::from_fn(|| {
			if !first && continuation_token.is_none() {
				return None;
			}
			first = false;
			Some(ResultExpand(
				s3client
					.list_objects_v2(rusoto_s3::ListObjectsV2Request {
						bucket: bucket.clone(),
						prefix: Some(prefix.clone()),
						continuation_token: continuation_token.take(),
						..rusoto_s3::ListObjectsV2Request::default()
					})
					.sync()
					.map(|res| {
						continuation_token = res.next_continuation_token;
						res.contents
							.unwrap_or_default()
							.into_iter()
							.map(|object: rusoto_s3::Object| object.key.unwrap())
					}),
			))
		})
		.flatten()
		.collect();

		let i = objects?
			.into_iter()
			.dist()
			.flat_map(FnMut!([bucket, region] move |key:String| {
				let s3client = S3Client::new(region.clone());
				ResultExpand(
					s3client
						.get_object(rusoto_s3::GetObjectRequest {
							bucket: bucket.clone(),
							key: key,
							..rusoto_s3::GetObjectRequest::default()
						})
						.sync()
						.map_err(Error::from)
						.map(|res| {
							let body = res.body.unwrap().into_blocking_read();
							BufReader::new(MultiGzDecoder::new(Box::new(body) as Box<dyn io::Read + Send>))
								.lines()
								.filter(FnMut!(|x:&Result<String,io::Error>| {
									if let Ok(x) = x {
										x.chars().filter(|x| !x.is_whitespace()).nth(0) != Some('#')
									} else {
										true
									}
								}))
								.map(FnMut!(|x:Result<String,io::Error>| {
									if let Ok(x) = x {
										let mut values = x.split('\t');
										let (
											date,
											time,
											x_edge_location,
											sc_bytes,
											c_ip,
											cs_method,
											cs_host,
											cs_uri_stem,
											sc_status,
											cs_referer,
											cs_user_agent,
											cs_uri_query,
											cs_cookie,
											x_edge_result_type,
											x_edge_request_id,
											x_host_header,
											cs_protocol,
											cs_bytes,
											time_taken,
											x_forwarded_for,
											ssl_protocol,
											ssl_cipher,
											x_edge_response_result_type,
											cs_protocol_version,
											fle_status,
											fle_encrypted_fields,
										) = (
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
											values.next().unwrap(),
										);
										assert_eq!(values.next(), None);
										let time = Utc.from_utc_datetime(&NaiveDateTime::new(
											NaiveDate::parse_from_str(&date, "%Y-%m-%d").unwrap(),
											NaiveTime::parse_from_str(&time, "%H:%M:%S").unwrap(),
										));
										let status = if sc_status != "000" {
											Some(StatusCode::from_bytes(sc_status.as_bytes()).unwrap())
										} else {
											None
										};
										#[allow(clippy::cast_sign_loss,clippy::cast_possible_truncation)]
										let time_taken = Duration::from_millis(
											(time_taken.parse::<f64>().unwrap() * 1000.0).round()
												as u64,
										);
										Ok(CloudfrontRow {
											time,
											edge_location: x_edge_location.to_owned(),
											response_bytes: sc_bytes.parse().unwrap(),
											remote_ip: c_ip.parse().unwrap(),
											method: cs_method.parse().unwrap(),
											host: cs_host.to_owned(),
											url: Url::parse(&format!(
												"{}://{}{}{}{}",
												cs_protocol,
												x_host_header,
												cs_uri_stem,
												if cs_uri_query == "-" { "" } else { "?" },
												if cs_uri_query == "-" {
													""
												} else {
													&cs_uri_query
												}
											))
											.unwrap(),
											status,
											user_agent: if cs_user_agent != "-" {
												Some(cs_user_agent.to_owned())
											} else {
												None
											},
											referer: if cs_referer != "-" {
												Some(cs_referer.to_owned())
											} else {
												None
											},
											cookie: if cs_cookie != "-" {
												Some(cs_cookie.to_owned())
											} else {
												None
											},
											result_type: x_edge_result_type.to_owned(),
											request_id: x_edge_request_id.to_owned(),
											request_bytes: cs_bytes.parse().unwrap(),
											time_taken,
											forwarded_for: if x_forwarded_for != "-" {
												Some(x_forwarded_for.to_owned())
											} else {
												None
											},
											ssl_protocol_cipher: if let ("-", "-") = (&*ssl_protocol, &*ssl_cipher) {
												None
											} else {
												Some((ssl_protocol.to_owned(), ssl_cipher.to_owned()))
											},
											response_result_type: x_edge_response_result_type
												.to_owned(),
											http_version: cs_protocol_version.to_owned(),
											fle_status: if fle_status != "-" {
												Some(fle_status.to_owned())
											} else {
												None
											},
											fle_encrypted_fields: if fle_encrypted_fields != "-" {
												Some(fle_encrypted_fields.to_owned())
											} else {
												None
											},
										})
									} else {
										Err(Error::from(x.err().unwrap()))
									}
								}))
						}),
				)
			}))
			.map(FnMut!(
				|x: Result<Result<CloudfrontRow, _>, _>| x.and_then(identity)
			));
		Ok(Self { i })
	}
}

impl DistributedIterator for Cloudfront {
	type Item = Result<CloudfrontRow, Error>;
	type Task = CloudfrontConsumer;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| CloudfrontConsumer { task })
	}
}

#[derive(Serialize, Deserialize)]
pub struct CloudfrontConsumer {
	task: <CloudfrontInner as DistributedIterator>::Task,
}

impl Consumer for CloudfrontConsumer {
	type Item = Result<CloudfrontRow, Error>;

	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		self.task.run(i)
	}
}

#[derive(Debug)]
#[allow(clippy::pub_enum_variant_names)]
pub enum Error {
	NoSuchBucket(String),
	NoSuchKey(String),
	HttpDispatch(rusoto_core::request::HttpDispatchError),
	Credentials(rusoto_core::CredentialsError),
	Validation(String),
	ParseError(String),
	Unknown(rusoto_core::request::BufferedHttpResponse),
	Io(Arc<io::Error>),
}
impl Clone for Error {
	fn clone(&self) -> Self {
		match self {
			Self::NoSuchBucket(err) => Self::NoSuchBucket(err.clone()),
			Self::NoSuchKey(err) => Self::NoSuchKey(err.clone()),
			Self::HttpDispatch(err) => Self::HttpDispatch(err.clone()),
			Self::Credentials(rusoto_core::CredentialsError { message }) => {
				Self::Credentials(rusoto_core::CredentialsError {
					message: message.clone(),
				})
			}
			Self::Validation(err) => Self::Validation(err.clone()),
			Self::ParseError(err) => Self::ParseError(err.clone()),
			Self::Unknown(rusoto_core::request::BufferedHttpResponse {
				status,
				body,
				headers,
			}) => Self::Unknown(rusoto_core::request::BufferedHttpResponse {
				status: *status,
				body: body.clone(),
				headers: headers.clone(),
			}),
			Self::Io(err) => Self::Io(err.clone()),
		}
	}
}
impl error::Error for Error {}
impl Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::NoSuchBucket(err) => err.fmt(f),
			Self::NoSuchKey(err) => err.fmt(f),
			Self::HttpDispatch(err) => err.fmt(f),
			Self::Credentials(err) => err.fmt(f),
			Self::Validation(err) => err.fmt(f),
			Self::ParseError(err) => err.fmt(f),
			Self::Unknown(err) => fmt::Debug::fmt(err, f),
			Self::Io(err) => err.fmt(f),
		}
	}
}
impl From<io::Error> for Error {
	fn from(err: io::Error) -> Self {
		Self::Io(Arc::new(err))
	}
}
impl<E> From<rusoto_core::RusotoError<E>> for Error
where
	Error: From<E>,
{
	fn from(err: rusoto_core::RusotoError<E>) -> Self {
		match err {
			rusoto_core::RusotoError::Service(e) => e.into(),
			rusoto_core::RusotoError::HttpDispatch(http_dispatch_error) => {
				Self::HttpDispatch(http_dispatch_error)
			}
			rusoto_core::RusotoError::Credentials(credentials_error) => {
				Self::Credentials(credentials_error)
			}
			rusoto_core::RusotoError::Validation(string) => Self::Validation(string),
			rusoto_core::RusotoError::ParseError(string) => Self::ParseError(string),
			rusoto_core::RusotoError::Unknown(buffered_http_response) => {
				Self::Unknown(buffered_http_response)
			}
		}
	}
}
impl From<rusoto_s3::ListObjectsV2Error> for Error {
	fn from(err: rusoto_s3::ListObjectsV2Error) -> Self {
		match err {
			rusoto_s3::ListObjectsV2Error::NoSuchBucket(err) => Self::NoSuchBucket(err),
		}
	}
}
impl From<rusoto_s3::GetObjectError> for Error {
	fn from(err: rusoto_s3::GetObjectError) -> Self {
		match err {
			rusoto_s3::GetObjectError::NoSuchKey(err) => Self::NoSuchKey(err),
		}
	}
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct CloudfrontRow {
	pub time: DateTime<Utc>,
	pub edge_location: String,
	pub response_bytes: u64,
	pub remote_ip: net::IpAddr,
	#[serde(with = "http_serde")]
	pub method: Method,
	pub host: String,
	pub url: Url,
	#[serde(with = "http_serde")]
	pub status: Option<StatusCode>,
	pub user_agent: Option<String>,
	pub referer: Option<String>,
	pub cookie: Option<String>,
	pub result_type: String,
	pub request_id: String,
	pub request_bytes: u64,
	pub time_taken: Duration,
	pub forwarded_for: Option<String>,
	pub ssl_protocol_cipher: Option<(String, String)>,
	pub response_result_type: String,
	pub http_version: String,
	pub fle_status: Option<String>,
	pub fle_encrypted_fields: Option<String>,
}

mod http_serde {
	use http::{Method, StatusCode};
	use serde::{Deserialize, Deserializer, Serialize, Serializer};
	use std::error::Error;

	pub struct Serde<T>(T);

	impl Serialize for Serde<&Method> {
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			self.0.as_str().serialize(serializer)
		}
	}
	impl Serialize for Serde<&Option<StatusCode>> {
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			self.0.map(|x| x.as_u16()).serialize(serializer)
		}
	}
	impl<'de> Deserialize<'de> for Serde<Method> {
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			String::deserialize(deserializer)
				.and_then(|x| {
					x.parse::<Method>()
						.map_err(|err| serde::de::Error::custom(err.description()))
				})
				.map(Self)
		}
	}
	impl<'de> Deserialize<'de> for Serde<Option<StatusCode>> {
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			Option::<u16>::deserialize(deserializer)
				.and_then(|x| {
					x.map(|x| {
						StatusCode::from_u16(x)
							.map_err(|err| serde::de::Error::custom(err.description()))
					})
					.transpose()
				})
				.map(Self)
		}
	}

	pub fn serialize<T, S>(t: &T, serializer: S) -> Result<S::Ok, S::Error>
	where
		for<'a> Serde<&'a T>: Serialize,
		S: Serializer,
	{
		Serde(t).serialize(serializer)
	}
	pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
	where
		Serde<T>: Deserialize<'de>,
		D: Deserializer<'de>,
	{
		Serde::<T>::deserialize(deserializer).map(|x| x.0)
	}
}
