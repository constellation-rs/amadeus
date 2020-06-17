use async_compression::futures::bufread::GzipDecoder;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use futures::{future, io::BufReader, AsyncBufReadExt, FutureExt, StreamExt, TryStreamExt};
use http::{Method, StatusCode};
use rusoto_s3::{GetObjectRequest, Object, S3Client, S3};
use serde::{Deserialize, Serialize};
use serde_closure::*;
use std::{
	convert::identity, io::{self}, time::Duration
};

use amadeus_core::{
	into_par_stream::IntoDistributedStream, par_stream::{DistributedStream, ParallelStream}, util::{DistParStream, ResultExpandIter}, Source
};
use amadeus_types::{DateTime, IpAddr, Url};

use super::{list, retry, AwsCredentials, AwsError, AwsRegion, Ref, RUSOTO_DISPATCHER};

pub struct Cloudfront {
	region: AwsRegion,
	bucket: String,
	objects: Vec<String>,
	credentials: AwsCredentials,
}
impl Cloudfront {
	pub async fn new(region: AwsRegion, bucket: &str, prefix: &str) -> Result<Self, AwsError> {
		Self::new_with(region, bucket, prefix, AwsCredentials::Environment).await
	}
	pub async fn new_with(
		region: AwsRegion, bucket: &str, prefix: &str, credentials: AwsCredentials,
	) -> Result<Self, AwsError> {
		let (bucket, prefix) = (bucket.to_owned(), prefix.to_owned());
		let client = S3Client::new_with(
			Ref(&*RUSOTO_DISPATCHER),
			credentials.clone(),
			region.clone(),
		);

		let objects = list(&client, &bucket, &prefix)
			.await?
			.into_iter()
			.map(|object: Object| object.key.unwrap())
			.collect();

		Ok(Self {
			region,
			bucket,
			objects,
			credentials,
		})
	}
}
impl Source for Cloudfront {
	type Item = CloudfrontRow;
	type Error = AwsError;

	#[cfg(not(feature = "doc"))]
	type ParStream = impl ParallelStream<Item = Result<Self::Item, Self::Error>>;
	#[cfg(feature = "doc")]
	type ParStream = amadeus_core::util::ImplParallelStream<Result<Self::Item, Self::Error>>;
	#[cfg(not(feature = "doc"))]
	type DistStream = impl DistributedStream<Item = Result<Self::Item, Self::Error>>;
	#[cfg(feature = "doc")]
	type DistStream = amadeus_core::util::ImplDistributedStream<Result<Self::Item, Self::Error>>;

	fn par_stream(self) -> Self::ParStream {
		DistParStream::new(self.dist_stream())
	}
	#[allow(clippy::let_and_return)]
	fn dist_stream(self) -> Self::DistStream {
		let Self {
			bucket,
			region,
			objects,
			credentials,
		} = self;
		let ret = objects
			.into_dist_stream()
			.flat_map(FnMut!(move |key: String| {
				let (credentials, region, bucket) =
					(credentials.clone(), region.clone(), bucket.clone());
				async move {
					let client = S3Client::new_with(
						Ref(once_cell::sync::Lazy::force(&RUSOTO_DISPATCHER)),
						credentials,
						region,
					);
					let rows = retry(|| {
						client.get_object(GetObjectRequest {
							bucket: bucket.clone(),
							key: key.clone(),
							..GetObjectRequest::default()
						})
					})
					.await
					.map_err(AwsError::from)
					.map(|res| {
						let body = BufReader::new(TryStreamExt::into_async_read(res.body.unwrap()));
						let mut body = GzipDecoder::new(body); // Content-Encoding isn't set, so decode manually
						body.multiple_members(true);
						BufReader::new(body)
							.lines()
							.filter(|x: &Result<String, io::Error>| {
								future::ready(if let Ok(x) = x {
									x.chars().find(|x| !x.is_whitespace()) != Some('#')
								} else {
									true
								})
							})
							.then(|x: Result<String, io::Error>| async {
								if let Ok(x) = x {
									Ok(CloudfrontRow::from_line(&x))
								} else {
									Err(AwsError::from(x.err().unwrap()))
								}
							})
					});
					ResultExpandIter::new(rows)
				}
				.flatten_stream()
			}))
			.map(FnMut!(
				|x: Result<Result<CloudfrontRow, _>, _>| x.and_then(self::identity)
			));
		#[cfg(feature = "doc")]
		let ret = amadeus_core::util::ImplDistributedStream::new(ret);
		ret
	}
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct CloudfrontRow {
	pub time: DateTime,
	pub edge_location: String,
	pub response_bytes: u64,
	pub remote_ip: IpAddr,
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
impl CloudfrontRow {
	#[inline(always)]
	fn from_line(line: &str) -> Self {
		let mut values = line.split('\t');
		let date = values.next().unwrap();
		let time = values.next().unwrap();
		let x_edge_location = values.next().unwrap();
		let sc_bytes = values.next().unwrap();
		let c_ip = values.next().unwrap();
		let cs_method = values.next().unwrap();
		let cs_host = values.next().unwrap();
		let cs_uri_stem = values.next().unwrap();
		let sc_status = values.next().unwrap();
		let cs_referer = values.next().unwrap();
		let cs_user_agent = values.next().unwrap();
		let cs_uri_query = values.next().unwrap();
		let cs_cookie = values.next().unwrap();
		let x_edge_result_type = values.next().unwrap();
		let x_edge_request_id = values.next().unwrap();
		let x_host_header = values.next().unwrap();
		let cs_protocol = values.next().unwrap();
		let cs_bytes = values.next().unwrap();
		let time_taken = values.next().unwrap();
		let x_forwarded_for = values.next().unwrap();
		let ssl_protocol = values.next().unwrap();
		let ssl_cipher = values.next().unwrap();
		let x_edge_response_result_type = values.next().unwrap();
		let cs_protocol_version = values.next().unwrap();
		let fle_status = values.next().unwrap();
		let fle_encrypted_fields = values.next().unwrap();
		assert_eq!(values.next(), None);
		let time = DateTime::from_chrono(&Utc.from_utc_datetime(&NaiveDateTime::new(
			NaiveDate::parse_from_str(&date, "%Y-%m-%d").unwrap(),
			NaiveTime::parse_from_str(&time, "%H:%M:%S").unwrap(),
		)));
		let status = if sc_status != "000" {
			Some(StatusCode::from_bytes(sc_status.as_bytes()).unwrap())
		} else {
			None
		};
		#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
		let time_taken =
			Duration::from_millis((time_taken.parse::<f64>().unwrap() * 1000.0).round() as u64);
		CloudfrontRow {
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
			response_result_type: x_edge_response_result_type.to_owned(),
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
		}
	}
}

mod http_serde {
	use http::{Method, StatusCode};
	use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
						.map_err(|err| serde::de::Error::custom(err.to_string()))
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
							.map_err(|err| serde::de::Error::custom(err.to_string()))
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
