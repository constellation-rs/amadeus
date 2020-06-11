#![allow(clippy::unsafe_derive_deserialize)]

use ::serde::{Deserialize, Serialize};
use futures::pin_mut;
use pin_project::pin_project;
use std::{
	pin::Pin, task::{Context, Poll}
};

use crate::{
	dist_pipe::DistributedPipe, dist_sink::DistributedSink, dist_stream::{DistributedStream, StreamTask, StreamTaskAsync}
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

	type DistStream: DistributedStream<Item = Result<Self::Item, Self::Error>>;

	fn dist_stream(self) -> Self::DistStream;
}

pub trait Destination<I>
where
	I: DistributedPipe<Self::Item>,
{
	type Item: crate::data::Data;
	type Error: std::error::Error;

	type DistSink: DistributedSink<I, Self::Item, Result<(), Self::Error>>;
}

#[cfg(feature = "amadeus-serde")]
impl<File, Row> Source for Json<File, Row>
where
	File: amadeus_core::file::File,
	Row: super::data::Data,
{
	type Item = <Self as amadeus_core::Source>::Item;
	type Error = <Self as amadeus_core::Source>::Error;

	type DistStream = <Self as amadeus_core::Source>::DistStream;

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

	type DistStream = <Self as amadeus_core::Source>::DistStream;

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

	type DistStream = <Self as amadeus_core::Source>::DistStream;

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

	type DistStream = <Self as amadeus_core::Source>::DistStream;

	fn dist_stream(self) -> Self::DistStream {
		<Self as amadeus_core::Source>::dist_stream(self)
	}
}
#[cfg(feature = "aws")]
impl Source for Cloudfront {
	type Item = crate::data::CloudfrontRow;
	type Error = <Self as amadeus_core::Source>::Error;

	type DistStream = IntoIter<<Self as amadeus_core::Source>::DistStream, Self::Item>;

	fn dist_stream(self) -> Self::DistStream {
		IntoIter(
			<Self as amadeus_core::Source>::dist_stream(self),
			PhantomData,
		)
	}
}
#[cfg(feature = "commoncrawl")]
impl Source for CommonCrawl {
	type Item = amadeus_types::Webpage<'static>;
	type Error = <Self as amadeus_core::Source>::Error;

	type DistStream = IntoIter<<Self as amadeus_core::Source>::DistStream, Self::Item>;

	fn dist_stream(self) -> Self::DistStream {
		IntoIter(
			<Self as amadeus_core::Source>::dist_stream(self),
			PhantomData,
		)
	}
}
use std::marker::PhantomData;
pub struct IntoIter<I, U>(I, PhantomData<fn() -> U>);
impl<I, T, E, U> DistributedStream for IntoIter<I, U>
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
		sink: Pin<&mut impl amadeus_core::sink::Sink<Self::Item>>,
	) -> Poll<()> {
		let sink =
			amadeus_core::sink::SinkMap::new(sink, |item: Result<_, _>| item.map(Into::into));
		pin_mut!(sink);
		self.project().task.poll_run(cx, sink)
	}
}
impl<I, T, E, U> Iterator for IntoIter<I, U>
where
	I: Iterator<Item = Result<T, E>>,
	T: Into<U>,
{
	type Item = Result<U, E>;

	fn next(&mut self) -> Option<Self::Item> {
		self.0.next().map(|x| x.map(Into::into))
	}
}
