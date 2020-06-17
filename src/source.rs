#![allow(clippy::unsafe_derive_deserialize)]

use ::serde::{Deserialize, Serialize};
use derive_new::new;
use futures::pin_mut;
use pin_project::pin_project;
use std::{
	marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use crate::{
	dist_sink::{DistributedSink, ParallelSink}, dist_stream::{DistributedStream, ParallelStream, StreamTask, StreamTaskAsync}
};

#[cfg(feature = "aws")]
#[doc(inline)]
pub use amadeus_aws::Cloudfront;
#[cfg(feature = "aws")]
pub mod aws {
	pub use crate::data::CloudfrontRow;
	#[doc(inline)]
	pub use amadeus_aws::{AwsCredentials, AwsError, AwsRegion, S3Directory, S3File};
}
#[cfg(feature = "commoncrawl")]
#[doc(inline)]
pub use amadeus_commoncrawl::{self as commoncrawl, CommonCrawl};
#[cfg(feature = "parquet")]
#[doc(inline)]
pub use amadeus_parquet::{self as parquet, Parquet, ParquetDirectory};
#[cfg(feature = "postgres")]
#[doc(inline)]
pub use amadeus_postgres::{self as postgres, Postgres};
#[cfg(feature = "amadeus-serde")]
#[doc(inline)]
pub use amadeus_serde::{self as serde, Csv, Json};

pub trait Source {
	type Item: crate::data::Data;
	type Error: std::error::Error;

	type ParStream: ParallelStream<Item = Result<Self::Item, Self::Error>>;
	type DistStream: DistributedStream<Item = Result<Self::Item, Self::Error>>;

	fn par_stream(self) -> Self::ParStream;
	fn dist_stream(self) -> Self::DistStream;
}

pub trait Destination {
	type Item: crate::data::Data;
	type Error: std::error::Error;

	type ParSink: ParallelSink<Self::Item, Output = Result<(), Self::Error>>;
	type DistSink: DistributedSink<Self::Item, Output = Result<(), Self::Error>>;

	fn par_sink(self) -> Self::ParSink;
	fn dist_sink(self) -> Self::DistSink;
}

#[cfg(feature = "amadeus-serde")]
impl<File, Row> Source for Json<File, Row>
where
	File: amadeus_core::file::File,
	Row: super::data::Data,
{
	type Item = <Self as amadeus_core::Source>::Item;
	type Error = <Self as amadeus_core::Source>::Error;

	type ParStream = <Self as amadeus_core::Source>::ParStream;
	type DistStream = <Self as amadeus_core::Source>::DistStream;

	fn par_stream(self) -> Self::ParStream {
		<Self as amadeus_core::Source>::par_stream(self)
	}
	fn dist_stream(self) -> Self::DistStream {
		<Self as amadeus_core::Source>::dist_stream(self)
	}
}
#[cfg(feature = "amadeus-serde")]
impl<File, Row> Source for Csv<File, Row>
where
	File: amadeus_core::file::File,
	Row: super::data::Data,
{
	type Item = <Self as amadeus_core::Source>::Item;
	type Error = <Self as amadeus_core::Source>::Error;

	type ParStream = <Self as amadeus_core::Source>::ParStream;
	type DistStream = <Self as amadeus_core::Source>::DistStream;

	fn par_stream(self) -> Self::ParStream {
		<Self as amadeus_core::Source>::par_stream(self)
	}
	fn dist_stream(self) -> Self::DistStream {
		<Self as amadeus_core::Source>::dist_stream(self)
	}
}
#[cfg(feature = "parquet")]
impl<File, Row> Source for Parquet<File, Row>
where
	File: amadeus_core::file::File,
	Row: super::data::Data,
{
	type Item = <Self as amadeus_core::Source>::Item;
	type Error = <Self as amadeus_core::Source>::Error;

	type ParStream = <Self as amadeus_core::Source>::ParStream;
	type DistStream = <Self as amadeus_core::Source>::DistStream;

	fn par_stream(self) -> Self::ParStream {
		<Self as amadeus_core::Source>::par_stream(self)
	}
	fn dist_stream(self) -> Self::DistStream {
		<Self as amadeus_core::Source>::dist_stream(self)
	}
}
#[cfg(feature = "postgres")]
impl<Row> Source for Postgres<Row>
where
	Row: super::data::Data,
{
	type Item = <Self as amadeus_core::Source>::Item;
	type Error = <Self as amadeus_core::Source>::Error;

	type ParStream = <Self as amadeus_core::Source>::ParStream;
	type DistStream = <Self as amadeus_core::Source>::DistStream;

	fn par_stream(self) -> Self::ParStream {
		<Self as amadeus_core::Source>::par_stream(self)
	}
	fn dist_stream(self) -> Self::DistStream {
		<Self as amadeus_core::Source>::dist_stream(self)
	}
}
#[cfg(feature = "aws")]
impl Source for Cloudfront {
	type Item = crate::data::CloudfrontRow;
	type Error = <Self as amadeus_core::Source>::Error;

	type ParStream = IntoStream<<Self as amadeus_core::Source>::ParStream, Self::Item>;
	type DistStream = IntoStream<<Self as amadeus_core::Source>::DistStream, Self::Item>;

	fn par_stream(self) -> Self::ParStream {
		IntoStream::new(<Self as amadeus_core::Source>::par_stream(self))
	}
	fn dist_stream(self) -> Self::DistStream {
		IntoStream::new(<Self as amadeus_core::Source>::dist_stream(self))
	}
}
#[cfg(feature = "commoncrawl")]
impl Source for CommonCrawl {
	type Item = amadeus_types::Webpage<'static>;
	type Error = <Self as amadeus_core::Source>::Error;

	type ParStream = IntoStream<<Self as amadeus_core::Source>::ParStream, Self::Item>;
	type DistStream = IntoStream<<Self as amadeus_core::Source>::DistStream, Self::Item>;

	fn par_stream(self) -> Self::ParStream {
		IntoStream::new(<Self as amadeus_core::Source>::par_stream(self))
	}
	fn dist_stream(self) -> Self::DistStream {
		IntoStream::new(<Self as amadeus_core::Source>::dist_stream(self))
	}
}

#[derive(new)]
pub struct IntoStream<I, U>(I, PhantomData<fn() -> U>);
impl<I, T, E, U> ParallelStream for IntoStream<I, U>
where
	I: ParallelStream<Item = Result<T, E>>,
	T: Into<U>,
	U: 'static,
{
	type Item = Result<U, E>;
	type Task = IntoTask<I::Task, U>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.0.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.0.next_task().map(|task| IntoTask {
			task,
			marker: PhantomData,
		})
	}
}
impl<I, T, E, U> DistributedStream for IntoStream<I, U>
where
	I: DistributedStream<Item = Result<T, E>>,
	T: Into<U>,
	U: 'static,
{
	type Item = Result<U, E>;
	type Task = IntoTask<I::Task, U>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.0.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.0.next_task().map(|task| IntoTask {
			task,
			marker: PhantomData,
		})
	}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct IntoTask<I, U> {
	#[pin]
	task: I,
	#[serde(skip)]
	marker: PhantomData<fn() -> U>,
}
impl<I, T, E, U> StreamTask for IntoTask<I, U>
where
	I: StreamTask<Item = Result<T, E>>,
	T: Into<U>,
{
	type Item = Result<U, E>;
	type Async = IntoTask<I::Async, U>;
	fn into_async(self) -> Self::Async {
		IntoTask {
			task: self.task.into_async(),
			marker: self.marker,
		}
	}
}
impl<I, T, E, U> StreamTaskAsync for IntoTask<I, U>
where
	I: StreamTaskAsync<Item = Result<T, E>>,
	T: Into<U>,
{
	type Item = Result<U, E>;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context,
		sink: Pin<&mut impl amadeus_core::sink::Sink<Item = Self::Item>>,
	) -> Poll<()> {
		let sink =
			amadeus_core::sink::SinkMap::new(sink, |item: Result<_, _>| item.map(Into::into));
		pin_mut!(sink);
		self.project().task.poll_run(cx, sink)
	}
}
impl<I, T, E, U> Iterator for IntoStream<I, U>
where
	I: Iterator<Item = Result<T, E>>,
	T: Into<U>,
{
	type Item = Result<U, E>;

	fn next(&mut self) -> Option<Self::Item> {
		self.0.next().map(|x| x.map(Into::into))
	}
}
