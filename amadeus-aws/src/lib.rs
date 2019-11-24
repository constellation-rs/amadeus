#![doc(html_root_url = "https://docs.rs/amadeus-aws/0.1.4")]
#![feature(type_alias_impl_trait)]

mod cloudfront;
mod file;

use futures::future::FutureExt;
use once_cell::sync::Lazy;
use rusoto_core::{
	credential::StaticProvider, request::{DispatchSignedRequest, HttpClient}, signature::SignedRequest, DefaultCredentialsProvider, ProvideAwsCredentials, RusotoError
};
use rusoto_s3::{GetObjectError, ListObjectsV2Error, ListObjectsV2Request, Object, S3Client, S3};
use serde::{Deserialize, Serialize};
use std::{
	cell::RefCell, error, fmt::{self, Display}, future::Future, io, iter, mem::transmute, ops::FnMut, time::Duration
};
use tokio::runtime::Runtime;

use amadeus_core::util::{IoError, ResultExpand};

#[doc(inline)]
pub use cloudfront::{Cloudfront, CloudfrontRow};
#[doc(inline)]
pub use file::{S3Directory, S3File};
#[doc(inline)]
pub use rusoto_core::Region as AwsRegion;

// https://docs.datadoghq.com/integrations/amazon_web_services/?tab=allpermissions#enable-logging-for-your-aws-service

static RUNTIME: Lazy<Runtime> =
	Lazy::new(|| Runtime::new().expect("failed to create tokio runtime"));
static RUSOTO_DISPATCHER: Lazy<HttpClient> =
	Lazy::new(|| HttpClient::new().expect("failed to create request dispatcher"));
static RUSOTO_CREDENTIALS_PROVIDER: Lazy<DefaultCredentialsProvider> =
	Lazy::new(|| DefaultCredentialsProvider::new().expect("failed to create credentials provider"));

thread_local! {
	static REENTRANCY_CHECK: RefCell<()> = RefCell::new(());
}

fn block_on_01<F>(future: F) -> Result<F::Item, F::Error>
where
	F: futures_01::future::Future + Send,
	F::Item: Send,
	F::Error: Send,
{
	use futures_01::{future::Future, sync::oneshot};
	REENTRANCY_CHECK.with(|reentrancy_check| {
		let _reentrancy_check = reentrancy_check.borrow_mut();
		// unsafe is used here to magic away tokio's 'static constraints
		struct Receiver<T>(Option<T>);
		unsafe impl<T> Sync for Receiver<T> {}
		let mut i = Receiver(None);
		let mut e = Receiver(None);
		let mut future = future.map(|i_| i.0 = Some(i_)).map_err(|e_| e.0 = Some(e_));
		let future: &mut (dyn Future<Item = (), Error = ()> + Send) = &mut future;
		let future: &mut (dyn Future<Item = (), Error = ()> + Send) = unsafe { transmute(future) };
		oneshot::spawn(future, &RUNTIME.executor())
			.wait()
			.map(|()| i.0.take().unwrap())
			.map_err(|()| e.0.take().unwrap())
	})
}
fn block_on<F>(future: F) -> F::Output
where
	F: Future + Send,
	F::Output: Send,
{
	block_on_01(futures::compat::Compat::new(
		Box::pin(future).map(Ok::<_, ()>),
	))
	.unwrap()
}

fn retry<F, FU, S>(f: F) -> impl futures_01::future::Future<Item = FU::Item, Error = FU::Error>
where
	F: FnMut() -> FU,
	FU: futures_01::future::Future<Error = RusotoError<S>>,
{
	use futures_01::future::Future;
	tokio_retry::RetryIf::spawn(
		tokio_retry::strategy::ExponentialBackoff::from_millis(10),
		f,
		|err: &RusotoError<_>| {
			if let RusotoError::HttpDispatch(_) = *err {
				true
			} else {
				false
			}
		},
	)
	.map_err(|err| match err {
		tokio_retry::Error::OperationError(err) => err,
		_ => panic!(),
	})
}

fn list(
	client: &S3Client, bucket: &str, prefix: &str,
) -> Result<Vec<Object>, RusotoError<ListObjectsV2Error>> {
	let (mut first, mut continuation_token) = (true, None);
	let objects: Result<Vec<Object>, _> = iter::from_fn(|| {
		if !first && continuation_token.is_none() {
			return None;
		}
		first = false;
		Some(ResultExpand(
			block_on_01(retry(|| {
				client.list_objects_v2(ListObjectsV2Request {
					bucket: bucket.to_owned(),
					prefix: Some(prefix.to_owned()),
					continuation_token: continuation_token.take(),
					..ListObjectsV2Request::default()
				})
			}))
			.map(|res| {
				continuation_token = res.next_continuation_token;
				res.contents.unwrap_or_default().into_iter()
			}),
		))
	})
	.flatten()
	.collect();
	objects
}

struct Ref<T: 'static>(&'static T);
impl<T: 'static> Copy for Ref<T> {}
impl<T: 'static> Clone for Ref<T> {
	fn clone(&self) -> Self {
		Ref(self.0)
	}
}
impl<D> DispatchSignedRequest for Ref<D>
where
	D: DispatchSignedRequest,
{
	type Future = D::Future;

	fn dispatch(&self, request: SignedRequest, timeout: Option<Duration>) -> Self::Future {
		D::dispatch(self.0, request, timeout)
	}
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub enum AwsCredentials {
	Anonymous,
	AccessKey { id: String, secret: String },
	Environment,
}
impl Default for AwsCredentials {
	fn default() -> Self {
		AwsCredentials::Environment
	}
}
impl ProvideAwsCredentials for AwsCredentials {
	type Future = futures_01::future::Either<
		<StaticProvider as ProvideAwsCredentials>::Future,
		<DefaultCredentialsProvider as ProvideAwsCredentials>::Future,
	>;

	fn credentials(&self) -> Self::Future {
		match self {
			AwsCredentials::Anonymous => futures_01::future::Either::A(
				StaticProvider::from(rusoto_core::credential::AwsCredentials::default())
					.credentials(),
			),
			AwsCredentials::AccessKey { id, secret } => futures_01::future::Either::A(
				StaticProvider::new(id.clone(), secret.clone(), None, None).credentials(),
			),
			AwsCredentials::Environment => {
				futures_01::future::Either::B(RUSOTO_CREDENTIALS_PROVIDER.credentials())
			}
		}
	}
}

#[derive(Debug)]
#[allow(clippy::pub_enum_variant_names)]
pub enum AwsError {
	NoSuchBucket(String),
	NoSuchKey(String),
	HttpDispatch(rusoto_core::request::HttpDispatchError),
	Credentials(rusoto_core::CredentialsError),
	Validation(String),
	ParseError(String),
	Unknown(rusoto_core::request::BufferedHttpResponse),
	Io(IoError),
}
impl Clone for AwsError {
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
impl PartialEq for AwsError {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::NoSuchBucket(a), Self::NoSuchBucket(b)) => a == b,
			(Self::NoSuchKey(a), Self::NoSuchKey(b)) => a == b,
			(Self::HttpDispatch(a), Self::HttpDispatch(b)) => a == b,
			(Self::Credentials(a), Self::Credentials(b)) => a == b,
			(Self::Validation(a), Self::Validation(b)) => a == b,
			(Self::ParseError(a), Self::ParseError(b)) => a == b,
			(Self::Unknown(a), Self::Unknown(b)) => a == b,
			(Self::Io(a), Self::Io(b)) => a == b,
			_ => false,
		}
	}
}
impl error::Error for AwsError {}
impl Display for AwsError {
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
impl From<io::Error> for AwsError {
	fn from(err: io::Error) -> Self {
		Self::Io(err.into())
	}
}
impl<E> From<RusotoError<E>> for AwsError
where
	AwsError: From<E>,
{
	fn from(err: RusotoError<E>) -> Self {
		match err {
			RusotoError::Service(err) => err.into(),
			RusotoError::HttpDispatch(err) => Self::HttpDispatch(err),
			RusotoError::Credentials(err) => Self::Credentials(err),
			RusotoError::Validation(err) => Self::Validation(err),
			RusotoError::ParseError(err) => Self::ParseError(err),
			RusotoError::Unknown(err) => Self::Unknown(err),
		}
	}
}
impl From<ListObjectsV2Error> for AwsError {
	fn from(err: ListObjectsV2Error) -> Self {
		match err {
			ListObjectsV2Error::NoSuchBucket(err) => Self::NoSuchBucket(err),
		}
	}
}
impl From<GetObjectError> for AwsError {
	fn from(err: GetObjectError) -> Self {
		match err {
			GetObjectError::NoSuchKey(err) => Self::NoSuchKey(err),
		}
	}
}
