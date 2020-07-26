//! Harmonious distributed data processing & analysis in Rust.
//!
//! <p style="font-family: 'Fira Sans',sans-serif;padding:0.3em 0"><strong>
//! <a href="https://crates.io/crates/amadeus">📦&nbsp;&nbsp;Crates.io</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://github.com/constellation-rs/amadeus">📑&nbsp;&nbsp;GitHub</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://constellation.zulipchat.com/#narrow/stream/213231-amadeus">💬&nbsp;&nbsp;Chat</a>
//! </strong></p>
//!
//! This is a support crate of [Amadeus](https://github.com/constellation-rs/amadeus) and is not intended to be used directly. These types are re-exposed in [`amadeus::source`](https://docs.rs/amadeus/0.3/amadeus/source/index.html).

#![doc(html_root_url = "https://docs.rs/amadeus-aws/0.3.6")]
#![cfg_attr(nightly, feature(type_alias_impl_trait))]
#![warn(
	// missing_copy_implementations,
	// missing_debug_implementations,
	// missing_docs,
	trivial_numeric_casts,
	unused_import_braces,
	unused_qualifications,
	unused_results,
	unreachable_pub,
	clippy::pedantic,
)]
#![allow(
	clippy::module_name_repetitions,
	clippy::if_not_else,
	clippy::too_many_lines,
	clippy::must_use_candidate,
	clippy::type_repetition_in_bounds,
	clippy::filter_map,
	clippy::missing_errors_doc
)]
#![deny(unsafe_code)]

mod cloudfront;
mod file;

use async_trait::async_trait;
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use once_cell::sync::Lazy;
use rusoto_core::{
	credential::StaticProvider, request::{DispatchSignedRequest, DispatchSignedRequestFuture, HttpClient}, signature::SignedRequest, RusotoError
};
use rusoto_credential::{CredentialsError, DefaultCredentialsProvider, ProvideAwsCredentials};
use rusoto_s3::{GetObjectError, ListObjectsV2Error, ListObjectsV2Request, Object, S3Client, S3};
use serde::{Deserialize, Serialize};
use std::{
	error, fmt::{self, Display}, future::Future, io, ops::FnMut, time::Duration
};

use amadeus_core::util::{IoError, ResultExpand};

#[doc(inline)]
pub use cloudfront::{Cloudfront, CloudfrontRow};
#[doc(inline)]
pub use file::{S3Directory, S3File};
#[doc(inline)]
pub use rusoto_core::Region as AwsRegion;

// https://docs.datadoghq.com/integrations/amazon_web_services/?tab=allpermissions#enable-logging-for-your-aws-service

static RUSOTO_DISPATCHER: Lazy<HttpClient> =
	Lazy::new(|| HttpClient::new().expect("failed to create request dispatcher"));
static RUSOTO_CREDENTIALS_PROVIDER: Lazy<DefaultCredentialsProvider> =
	Lazy::new(|| DefaultCredentialsProvider::new().expect("failed to create credentials provider"));

fn retry<F, FU, T, S>(f: F) -> impl Future<Output = Result<T, RusotoError<S>>>
where
	F: FnMut() -> FU + Unpin,
	FU: Future<Output = Result<T, RusotoError<S>>>,
{
	futures_retry::FutureRetry::new(f, |err| match err {
		RusotoError::HttpDispatch(_) => {
			futures_retry::RetryPolicy::WaitRetry(std::time::Duration::from_millis(10))
		}
		RusotoError::Unknown(response) if response.status.is_server_error() => {
			futures_retry::RetryPolicy::WaitRetry(std::time::Duration::from_millis(10))
		}
		e => futures_retry::RetryPolicy::ForwardError(e),
	})
	.map_ok(|(x, _)| x)
	.map_err(|(x, _)| x)
}

async fn list(
	client: &S3Client, bucket: &str, prefix: &str,
) -> Result<Vec<Object>, RusotoError<ListObjectsV2Error>> {
	let (first, continuation_token) = (true, None);
	let objects: Result<Vec<Object>, _> = stream::unfold(
		(first, continuation_token),
		|(mut first, mut continuation_token)| async move {
			if !first && continuation_token.is_none() {
				return None;
			}
			first = false;
			Some((
				stream::iter(ResultExpand(
					retry(|| {
						client.list_objects_v2(ListObjectsV2Request {
							bucket: bucket.to_owned(),
							prefix: Some(prefix.to_owned()),
							continuation_token: continuation_token.take(),
							..ListObjectsV2Request::default()
						})
					})
					.await
					.map(|res| {
						continuation_token = res.next_continuation_token;
						res.contents.unwrap_or_default().into_iter()
					}),
				)),
				(first, continuation_token),
			))
		},
	)
	.flatten()
	.try_collect()
	.await;
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
	fn dispatch(
		&self, request: SignedRequest, timeout: Option<Duration>,
	) -> DispatchSignedRequestFuture {
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
#[async_trait]
impl ProvideAwsCredentials for AwsCredentials {
	async fn credentials(&self) -> Result<rusoto_credential::AwsCredentials, CredentialsError> {
		match self {
			AwsCredentials::Anonymous => {
				StaticProvider::from(rusoto_core::credential::AwsCredentials::default())
					.credentials()
					.await
			}

			AwsCredentials::AccessKey { id, secret } => {
				StaticProvider::new(id.clone(), secret.clone(), None, None)
					.credentials()
					.await
			}

			AwsCredentials::Environment => RUSOTO_CREDENTIALS_PROVIDER.credentials().await,
		}
	}
}

#[derive(Debug)]
#[allow(clippy::pub_enum_variant_names)]
pub enum AwsError {
	NoSuchBucket(String),
	NoSuchKey(String),
	HttpDispatch(rusoto_core::request::HttpDispatchError),
	Credentials(CredentialsError),
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
			Self::Credentials(CredentialsError { message }) => {
				Self::Credentials(CredentialsError {
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
			(Self::NoSuchBucket(a), Self::NoSuchBucket(b))
			| (Self::NoSuchKey(a), Self::NoSuchKey(b))
			| (Self::Validation(a), Self::Validation(b))
			| (Self::ParseError(a), Self::ParseError(b)) => a == b,
			(Self::HttpDispatch(a), Self::HttpDispatch(b)) => a == b,
			(Self::Credentials(a), Self::Credentials(b)) => a == b,
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
			Self::NoSuchBucket(err)
			| Self::NoSuchKey(err)
			| Self::Validation(err)
			| Self::ParseError(err) => err.fmt(f),
			Self::HttpDispatch(err) => err.fmt(f),
			Self::Credentials(err) => err.fmt(f),
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
	E: Into<AwsError>,
{
	fn from(err: RusotoError<E>) -> Self {
		match err {
			RusotoError::Service(err) => err.into(),
			RusotoError::HttpDispatch(err) => Self::HttpDispatch(err),
			RusotoError::Credentials(err) => Self::Credentials(err),
			RusotoError::Validation(err) => Self::Validation(err),
			RusotoError::ParseError(err) => Self::ParseError(err),
			RusotoError::Unknown(err) => Self::Unknown(err),
			RusotoError::Blocking => unreachable!(),
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
