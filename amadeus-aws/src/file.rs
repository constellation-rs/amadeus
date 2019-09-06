use futures::future::FutureExt;
use rusoto_s3::{GetObjectRequest, HeadObjectRequest, S3Client, S3};
use serde::{Deserialize, Serialize};
use std::{
	convert::{TryFrom, TryInto}, future::Future, io, pin::Pin
};
use tokio::io::AsyncRead;

use amadeus_core::{
	file::{Directory, File, Page, Partition, PathBuf}, util::IoError
};

fn block_on<F>(future: F) -> F::Output
where
	F: Future,
{
	tokio::runtime::current_thread::Runtime::new()
		.unwrap()
		.block_on(futures::compat::Compat::new(
			Box::pin(future).map(Ok::<_, ()>),
		))
		.unwrap()
}

pub type Region = rusoto_core::Region;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct S3Directory {
	region: Region,
	bucket: String,
	prefix: String,
}
impl S3Directory {
	pub fn new(region: Region, bucket: &str, prefix: &str) -> Self {
		let (bucket, prefix) = (bucket.to_owned(), prefix.to_owned());
		Self {
			region,
			bucket,
			prefix,
		}
	}
}
impl Directory for S3Directory {
	fn partitions_filter<F>(self, mut f: F) -> Result<Vec<Self::Partition>, Self::Error>
	where
		F: FnMut(&PathBuf) -> bool,
	{
		let Self {
			region,
			bucket,
			prefix,
		} = self;
		let client = S3Client::new(region.clone());
		let objects = super::list(&client, &bucket, &prefix)?;

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
				})
			})
			.collect()
	}
}

impl File for S3Directory {
	type Partition = S3Partition;
	type Error = super::Error;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		self.partitions_filter(|_| true)
	}
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct S3File {
	region: Region,
	bucket: String,
	key: String,
}
impl S3File {
	pub fn new(region: Region, bucket: &str, key: &str) -> Self {
		let (bucket, key) = (bucket.to_owned(), key.to_owned());
		Self {
			region,
			bucket,
			key,
		}
	}
}
impl File for S3File {
	type Partition = S3File;
	type Error = super::Error;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		Ok(vec![self])
	}
}
impl Partition for S3File {
	type Page = S3Page;
	type Error = IoError;

	fn pages(self) -> Result<Vec<Self::Page>, Self::Error> {
		Ok(vec![S3Page::new(self.region, self.bucket, self.key)])
	}
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct S3Partition {
	region: Region,
	bucket: String,
	key: String,
	len: u64,
}
impl Partition for S3Partition {
	type Page = S3Page;
	type Error = IoError;

	fn pages(self) -> Result<Vec<Self::Page>, Self::Error> {
		let client = S3Client::new(self.region);
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
	fn new(region: Region, bucket: String, key: String) -> Self {
		let client = S3Client::new(region);
		let object = client
			.head_object(HeadObjectRequest {
				bucket: bucket.clone(),
				key: key.clone(),
				..HeadObjectRequest::default()
			})
			.sync()
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

	fn block_on<F>(future: F) -> F::Output
	where
		F: Future,
	{
		block_on(future)
	}
	fn len(&self) -> u64 {
		self.len
	}
	fn set_len(&self, _len: u64) -> Result<(), Self::Error> {
		unimplemented!()
	}
	fn read<'a>(
		&'a self, offset: u64, buf: &'a mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
		Box::pin(async move {
			let (start, end) = (offset, offset + u64::try_from(buf.len()).unwrap() - 1);
			let res =
				futures::compat::Compat01As03::new(self.client.get_object(GetObjectRequest {
					bucket: self.bucket.clone(),
					key: self.key.clone(),
					range: Some(format!("bytes={}-{}", start, end)),
					..GetObjectRequest::default()
				}))
				.await
				.unwrap();
			let len: u64 = buf.len().try_into().unwrap();
			let mut cursor = io::Cursor::new(buf);
			let mut read = res.body.unwrap().into_async_read();
			while len - cursor.position() > 0 {
				let _: usize =
					futures::compat::Compat01As03::new(tokio::prelude::future::poll_fn(|| {
						read.read_buf(&mut cursor)
					}))
					.await
					.unwrap();
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
