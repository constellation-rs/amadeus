#![allow(clippy::needless_lifetimes)]

use async_trait::async_trait;
use futures::{future, future::LocalBoxFuture, stream, FutureExt, StreamExt, TryStreamExt};
use std::{
	ffi::{OsStr, OsString}, fs, future::Future, io, path::{Path, PathBuf}, sync::Arc
};
use walkdir::WalkDir;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
#[cfg(target_arch = "wasm32")]
use {
	futures::lock::Mutex, js_sys::{ArrayBuffer, Uint8Array}, std::convert::TryFrom, wasm_bindgen::{JsCast, JsValue}, wasm_bindgen_futures::JsFuture, web_sys::{Blob, Response}
};
#[cfg(not(target_arch = "wasm32"))]
use {
	std::io::{Seek, SeekFrom}, tokio::task::spawn_blocking
};

use super::{Directory, File, Page, Partition};
#[cfg(target_arch = "wasm32")]
use crate::util::{f64_to_u64, u64_to_f64};
use crate::util::{IoError, ResultExpand};

#[async_trait(?Send)]
impl<F> File for Vec<F>
where
	F: File,
{
	type Partition = F::Partition;
	type Error = F::Error;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		stream::iter(self.into_iter())
			.flat_map(|file| {
				async { stream::iter(ResultExpand(file.partitions().await)) }.flatten_stream()
			})
			.try_collect()
			.await
	}
}
#[async_trait(?Send)]
impl<F> File for &[F]
where
	F: File + Clone,
{
	type Partition = F::Partition;
	type Error = F::Error;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		stream::iter(self.iter().cloned())
			.flat_map(|file| {
				async { stream::iter(ResultExpand(file.partitions().await)) }.flatten_stream()
			})
			.try_collect()
			.await
	}
}
#[async_trait(?Send)]
impl File for PathBuf {
	type Partition = Self;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		Ok(vec![self])
	}
}
#[async_trait(?Send)]
impl Directory for PathBuf {
	async fn partitions_filter<F>(
		self, f: F,
	) -> Result<Vec<<Self as File>::Partition>, <Self as File>::Error>
	where
		F: FnMut(&super::PathBuf) -> bool,
	{
		(*self).partitions_filter(f).await
	}
}
#[async_trait(?Send)]
impl Partition for PathBuf {
	type Page = LocalFile;
	type Error = IoError;

	async fn pages(self) -> Result<Vec<Self::Page>, Self::Error> {
		Ok(vec![LocalFile::open(self)?])
	}
}
#[async_trait(?Send)]
impl Directory for &Path {
	async fn partitions_filter<F>(
		self, mut f: F,
	) -> Result<Vec<<Self as File>::Partition>, <Self as File>::Error>
	where
		F: FnMut(&super::PathBuf) -> bool,
	{
		WalkDir::new(self)
			.follow_links(true)
			.sort_by(|a, b| a.file_name().cmp(b.file_name()))
			.into_iter()
			.filter_entry(|e| {
				let is_dir = e.file_type().is_dir();
				let path = e.path();
				if path == self {
					return true;
				}
				let mut path = path.strip_prefix(self).unwrap();
				let mut path_buf = super::PathBuf::new();
				let mut file_name = None;
				if !is_dir {
					file_name = Some(path.file_name().unwrap());
					path = path.parent().unwrap();
				}
				for component in path {
					path_buf.push(component);
				}
				path_buf.set_file_name(file_name);
				f(&path_buf)
			})
			.filter_map(|e| match e {
				Ok(ref e) if e.file_type().is_dir() => None,
				Ok(e) => Some(Ok(e.into_path())),
				Err(e) => Some(Err(if e.io_error().is_some() {
					e.into_io_error().unwrap()
				} else {
					io::Error::new(io::ErrorKind::Other, e)
				}
				.into())),
			})
			.collect()
	}
}
#[async_trait(?Send)]
impl File for &Path {
	type Partition = PathBuf;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into()).await
	}
}
#[async_trait(?Send)]
impl File for String {
	type Partition = PathBuf;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into()).await
	}
}
#[async_trait(?Send)]
impl File for &str {
	type Partition = PathBuf;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into()).await
	}
}
#[async_trait(?Send)]
impl File for OsString {
	type Partition = PathBuf;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into()).await
	}
}
#[async_trait(?Send)]
impl File for &OsStr {
	type Partition = PathBuf;
	type Error = IoError;

	async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		PathBuf::partitions(self.into()).await
	}
}
// impl File for fs::File {
// 	type Partition = Self;
// 	type Error = IoError;

// 	fn partitions(self) -> Result<Vec<Self::Partition>,Self::Error> {
// 		Ok(vec![self])
// 	}
// }
// impl Partition for fs::File {
// 	type Page = LocalFile;
// 	type Error = IoError;

// 	fn pages(self) -> Result<Vec<Self::Page>,Self::Error> {
// 		Ok(vec![LocalFile::from_file(self)?])
// 	}
// }

// To support converting back to fs::File:
// https://github.com/vasi/positioned-io/blob/a03c792f5b6f99cb4f72e146befdfc8b1f6e1d28/src/raf.rs

#[cfg(target_arch = "wasm32")]
enum FutureOrOutput<Fut: Future> {
	Future(Fut),
	Output(Fut::Output),
}
#[cfg(target_arch = "wasm32")]
impl<Fut: Future> FutureOrOutput<Fut> {
	fn output(&mut self) -> Option<&mut Fut::Output> {
		if let FutureOrOutput::Output(output) = self {
			Some(output)
		} else {
			None
		}
	}
}

#[cfg(not(target_arch = "wasm32"))]
struct LocalFileInner {
	file: fs::File,
}
#[cfg(target_arch = "wasm32")]
struct LocalFileInner {
	file: Mutex<FutureOrOutput<LocalBoxFuture<'static, Blob>>>,
}
pub struct LocalFile {
	inner: Arc<LocalFileInner>,
}
impl LocalFile {
	/// [Opens](https://doc.rust-lang.org/std/fs/struct.File.html#method.open)
	/// a file for random access.
	pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
		#[cfg(not(target_arch = "wasm32"))]
		{
			Self::from_file(fs::File::open(path)?)
		}
		#[cfg(target_arch = "wasm32")]
		{
			let path = path.as_ref().to_string_lossy().into_owned();
			let file = Mutex::new(FutureOrOutput::Future(
				async move {
					let window = web_sys::window().unwrap();
					let resp_value = JsFuture::from(window.fetch_with_str(&path)).await.unwrap();
					let resp: Response = resp_value.dyn_into().unwrap();
					let blob: JsValue = JsFuture::from(resp.blob().unwrap()).await.unwrap();
					let blob: Blob = blob.dyn_into().unwrap();
					blob
				}
				.boxed_local(),
			));
			let inner = Arc::new(LocalFileInner { file });
			Ok(Self { inner })
		}
	}

	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}

	#[cfg(not(target_arch = "wasm32"))]
	fn len(&self) -> LocalBoxFuture<'static, Result<u64, <Self as Page>::Error>> {
		let self_ = self.inner.clone();
		future::lazy(move |_| (&self_.file).seek(SeekFrom::End(0)).map_err(Into::into))
			.boxed_local()
	}
	#[cfg(target_arch = "wasm32")]
	fn len(&self) -> LocalBoxFuture<'static, Result<u64, <Self as Page>::Error>> {
		let self_ = self.inner.clone();
		async move {
			let mut file = self_.file.lock().await;
			if let FutureOrOutput::Future(fut) = &mut *file {
				*file = FutureOrOutput::Output(fut.await);
			}
			let blob = file.output().unwrap();
			let size = blob.size();
			Ok(f64_to_u64(size))
		}
		.boxed_local()
	}

	#[cfg(not(target_arch = "wasm32"))]
	fn from_file(file: fs::File) -> io::Result<Self> {
		let inner = Arc::new(LocalFileInner { file });
		Ok(Self { inner })
	}
	#[cfg(target_arch = "wasm32")]
	fn from_file(_file: fs::File) -> io::Result<Self> {
		unimplemented!()
	}
}

impl From<fs::File> for LocalFile {
	fn from(file: fs::File) -> Self {
		Self::from_file(file).unwrap()
	}
}

// read_at/write_at for tokio::File https://github.com/tokio-rs/tokio/issues/1529

#[cfg(unix)]
impl LocalFile {
	#[inline]
	fn read_at<'a>(
		&self, pos: u64, buf: &'a mut [u8],
	) -> impl Future<Output = io::Result<usize>> + 'a {
		let self_ = self.inner.clone();
		let len = buf.len();
		spawn_blocking(move || {
			let mut buf = vec![0; len];
			FileExt::read_at(&self_.file, &mut buf, pos).map(|len| {
				buf.truncate(len);
				buf
			})
		})
		.map(move |vec| {
			vec.unwrap().map(move |vec| {
				buf[..vec.len()].copy_from_slice(&vec);
				vec.len()
			})
		})
	}
	#[inline]
	fn write_at<'a>(
		&self, pos: u64, buf: &'a [u8],
	) -> impl Future<Output = io::Result<usize>> + 'a {
		let self_ = self.inner.clone();
		let buf = buf.to_owned();
		spawn_blocking(move || FileExt::write_at(&self_.file, &buf, pos)).map(Result::unwrap)
	}
}

#[cfg(windows)]
impl LocalFile {
	#[inline]
	fn read_at<'a>(
		&self, pos: u64, buf: &'a mut [u8],
	) -> impl Future<Output = io::Result<usize>> + 'a {
		let self_ = self.inner.clone();
		let len = buf.len();
		spawn_blocking(move || {
			let mut buf = vec![0; len];
			FileExt::seek_read(&self_.file, &mut buf, pos).map(|len| {
				buf.truncate(len);
				buf
			})
		})
		.map(move |vec| {
			vec.unwrap().map(move |vec| {
				buf[..vec.len()].copy_from_slice(&vec);
				vec.len()
			})
		})
	}
	#[inline]
	fn write_at<'a>(
		&self, pos: u64, buf: &'a [u8],
	) -> impl Future<Output = io::Result<usize>> + 'a {
		let self_ = self.inner.clone();
		let buf = buf.to_owned();
		spawn_blocking(move || FileExt::seek_write(&self_.file, &buf, pos)).map(Result::unwrap)
	}
}

#[cfg(target_arch = "wasm32")]
impl LocalFile {
	fn read_at<'a>(
		&self, pos: u64, buf: &'a mut [u8],
	) -> impl Future<Output = io::Result<usize>> + 'a {
		let self_ = self.inner.clone();
		async move {
			let mut file = self_.file.lock().await;
			if let FutureOrOutput::Future(fut) = &mut *file {
				*file = FutureOrOutput::Output(fut.await);
			}
			let blob = file.output().unwrap();
			let end = pos + u64::try_from(buf.len()).unwrap();
			let slice: Blob = blob
				.slice_with_f64_and_f64(u64_to_f64(pos), u64_to_f64(end))
				.unwrap();
			drop(file);
			// TODO: only do workaround when necessary
			let array_buffer = if false {
				slice.array_buffer()
			} else {
				// workaround for lack of Blob::array_buffer
				Response::new_with_opt_blob(Some(&slice))
					.unwrap()
					.array_buffer()
					.unwrap()
			};
			drop(slice);
			let array_buffer: JsValue = JsFuture::from(array_buffer).await.unwrap();
			let array_buffer: ArrayBuffer = array_buffer.dyn_into().unwrap();
			let buf_: Uint8Array = Uint8Array::new(&array_buffer);
			drop(array_buffer);
			let len = usize::try_from(buf_.length()).unwrap();
			buf_.copy_to(&mut buf[..len]);
			Ok(len)
		}
		.boxed_local()
	}
	#[inline]
	fn write_at<'a>(
		&self, _pos: u64, _buf: &'a [u8],
	) -> impl Future<Output = io::Result<usize>> + 'a {
		let _self = self;
		future::lazy(|_| unimplemented!()).boxed_local()
	}
}

impl Page for LocalFile {
	type Error = IoError;

	fn len(&self) -> LocalBoxFuture<'static, Result<u64, Self::Error>> {
		self.len()
	}
	fn read(
		&self, mut offset: u64, len: usize,
	) -> LocalBoxFuture<'static, Result<Box<[u8]>, Self::Error>> {
		let self_ = self.clone();
		Box::pin(async move {
			let mut buf_ = vec![0; len];
			let mut buf = &mut *buf_;
			while !buf.is_empty() {
				match self_.read_at(offset, buf).await {
					Ok(0) => break,
					Ok(n) => {
						let tmp = buf;
						buf = &mut tmp[n..];
						offset += n as u64;
					}
					Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
					Err(e) => return Err(e.into()),
				}
			}
			let len = len - buf.len();
			buf_.truncate(len);
			Ok(buf_.into_boxed_slice())
		})
	}
	fn write(
		&self, mut offset: u64, buf: Box<[u8]>,
	) -> LocalBoxFuture<'static, Result<(), Self::Error>> {
		let self_ = self.clone();
		Box::pin(async move {
			let mut buf = &*buf;
			while !buf.is_empty() {
				match self_.write_at(offset, buf).await {
					Ok(0) => {
						return Err(io::Error::new(
							io::ErrorKind::WriteZero,
							"failed to write whole buffer",
						)
						.into())
					}
					Ok(n) => {
						buf = &buf[n..];
						offset += n as u64
					}
					Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
					Err(e) => return Err(e.into()),
				}
			}
			Ok(())
		})
	}
}
