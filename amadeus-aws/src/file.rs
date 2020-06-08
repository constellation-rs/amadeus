use async_trait::async_trait;
use rusoto_core::RusotoError;
use rusoto_s3::{GetObjectRequest, HeadObjectRequest, S3Client, S3};
use serde::{Deserialize, Serialize};
use std::{
	convert::{TryFrom, TryInto}, future::Future, pin::Pin
};
use tokio::io::AsyncReadExt;

use amadeus_core::{
	file::{Directory, File, Page, Partition, PathBuf}, util::IoError
};

use super::{retry, AwsCredentials, AwsError, AwsRegion, Ref, RUSOTO_DISPATCHER};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct S3Directory {
	region: AwsRegion,
	bucket: String,
	prefix: String,
	credentials: AwsCredentials,
}
impl S3Directory {
	pub fn new(region: AwsRegion, bucket: &str, prefix: &str) -> Self {
		Self::new_with(region, bucket, prefix, AwsCredentials::Environment)
	}
	pub fn new_with(
		region: AwsRegion, bucket: &str, prefix: &str, credentials: AwsCredentials,
	) -> Self {
		let (bucket, prefix) = (bucket.to_owned(), prefix.to_owned());
		Self {
			region,
			bucket,
			prefix,
			credentials,
		}
	}
}
#[async_trait(?Send)]
impl Directory for S3Directory {
	async fn partitions_filter<F>(
		self, mut f: F,
	) -> Result<Vec<<Self as File>::Partition>, <Self as File>::Error>
	where
		F: FnMut(&PathBuf) -> bool,
	{
		let Self {
			region,
			bucket,
			prefix,
			credentials,
		} = self;
		let client = S3Client::new_with(
			Ref(&*RUSOTO_DISPATCHER),
			credentials.clone(),
			region.clone(),
		);
		let objects = super::list(&client, &bucket, &prefix).await?;

		let mut current_path = PathBuf::new();
		let mut skip = false;
		let mut last_key: Option<String> = None;
		objects
			.into_iter()
			.filter(|object| {
				let key = object.key.as_ref().unwrap();
				assert!(key.starts_with(&prefix));
				let key = &key[prefix.len()..];
				assert!(last_key.is_none() || **last_key.as_ref().unwrap() < *key, "S3 API not returning objects in \"UTF-8 character encoding in lexicographical order\" as their docs specify");
				last_key = Some(key.to_owned());
				let mut path = key.split('/').collect::<Vec<&str>>();
				let file_name = path.pop().unwrap();
				skip = skip
					&& path.len() >= current_path.depth()
					&& path
						.iter()
						.take(current_path.depth())
						.copied()
						.eq(current_path.iter());
				if skip {
					return false;
				}
				while current_path.depth() > path.len()
					|| (current_path.depth() > 0
						&& current_path.last().unwrap() != path[current_path.depth() - 1])
				{
					current_path.pop().unwrap();
				}
				while path.len() > current_path.depth() {
					current_path.push(path[current_path.depth()]);
					if !f(&current_path) {
						skip = true;
						return false;
					}
				}
				current_path.set_file_name(Some(file_name));
				let ret = f(&current_path);
				current_path.set_file_name::<Vec<u8>>(None);
				ret
			})
			.map(|object| {
				Ok(S3Partition {
					region: region.clone(),
					bucket: bucket.clone(),
					key: object.key.unwrap(),
					len: object.size.unwrap().try_into().unwrap(),
					credentials: credentials.clone()
				})
			})
			.collect()
	}
}

#[async_trait(?Send)]
impl File for S3Directory {
	type Partition = S3Partition;
	type Error = AwsError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		self.partitions_filter(|_| true).await
	}
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct S3File {
	region: AwsRegion,
	bucket: String,
	key: String,
	credentials: AwsCredentials,
}
impl S3File {
	pub fn new(region: AwsRegion, bucket: &str, key: &str) -> Self {
		Self::new_with(region, bucket, key, AwsCredentials::Environment)
	}
	pub fn new_with(
		region: AwsRegion, bucket: &str, key: &str, credentials: AwsCredentials,
	) -> Self {
		let (bucket, key) = (bucket.to_owned(), key.to_owned());
		Self {
			region,
			bucket,
			key,
			credentials,
		}
	}
}
#[async_trait(?Send)]
impl File for S3File {
	type Partition = S3File;
	type Error = AwsError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		Ok(vec![self])
	}
}
#[async_trait(?Send)]
impl Partition for S3File {
	type Page = S3Page;
	type Error = IoError;

	async fn pages(self) -> Result<Vec<Self::Page>, Self::Error> {
		Ok(vec![
			S3Page::new(self.region, self.bucket, self.key, self.credentials).await,
		])
	}
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct S3Partition {
	region: AwsRegion,
	bucket: String,
	key: String,
	len: u64,
	credentials: AwsCredentials,
}
#[async_trait(?Send)]
impl Partition for S3Partition {
	type Page = S3Page;
	type Error = IoError;

	async fn pages(self) -> Result<Vec<Self::Page>, Self::Error> {
		let client = S3Client::new_with(Ref(&*RUSOTO_DISPATCHER), self.credentials, self.region);
		let (bucket, key, len) = (self.bucket, self.key, self.len);
		Ok(vec![S3Page {
			client,
			bucket,
			key,
			len,
		}])
	}
}

pub struct S3Page {
	client: S3Client,
	bucket: String,
	key: String,
	len: u64,
}
impl S3Page {
	async fn new(
		region: AwsRegion, bucket: String, key: String, credentials: AwsCredentials,
	) -> Self {
		let client = S3Client::new_with(Ref(&*RUSOTO_DISPATCHER), credentials, region);
		let object = retry(|| {
			client.head_object(HeadObjectRequest {
				bucket: bucket.clone(),
				key: key.clone(),
				..HeadObjectRequest::default()
			})
		})
		.await
		.unwrap();
		let len = object.content_length.unwrap().try_into().unwrap();
		S3Page {
			client,
			bucket,
			key,
			len,
		}
	}
}
impl Page for S3Page {
	type Error = IoError;

	fn len(&self) -> u64 {
		self.len
	}
	fn set_len(&self, _len: u64) -> Result<(), Self::Error> {
		unimplemented!()
	}
	fn read<'a>(
		&'a self, offset: u64, mut buf: &'a mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
		Box::pin(async move {
			let len: u64 = buf.len().try_into().unwrap();
			let mut pos = 0u64;
			let mut errors: usize = 0;
			let end = offset + len - 1;
			while buf.len() > 0 {
				let start = offset + pos;
				assert_eq!(start, end + 1 - u64::try_from(buf.len()).unwrap()); // TODO
				let res = self.client.get_object(GetObjectRequest {
					bucket: self.bucket.clone(),
					key: self.key.clone(),
					range: Some(format!("bytes={}-{}", start, end)),
					..GetObjectRequest::default()
				});
				let res = res.await;
				match res {
					Err(RusotoError::HttpDispatch(_)) if errors < 10 => {
						errors += 1;
						continue;
					}
					Err(RusotoError::Unknown(response))
						if response.status.is_server_error() && errors < 10 =>
					{
						errors += 1;
						continue;
					}
					_ => (),
				}
				let mut read = res.unwrap().body.unwrap().into_async_read();
				while buf.len() > 0 {
					let res = read.read_buf(&mut buf).await;
					match res {
						Ok(0) | Err(_) => break,
						Ok(n) => pos += u64::try_from(n).unwrap(),
					}
				}
			}
			Ok(())
		})
	}
	fn write<'a>(
		&'a self, _offset: u64, _buf: &'a [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
		unimplemented!()
	}
}
