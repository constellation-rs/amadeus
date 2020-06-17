use derive_new::new;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	any::type_name, error, fmt, io, marker::PhantomData, pin::Pin, sync::Arc, task::{Context, Poll}
};

use crate::dist_stream::{DistributedStream, ParallelStream};
#[cfg(feature = "doc")]
use crate::{
	dist_stream::{DistributedStream, StreamTask, StreamTaskAsync}, sink::Sink
};

pub struct ResultExpand<T, E>(pub Result<T, E>);
impl<T, E> IntoIterator for ResultExpand<T, E>
where
	T: IntoIterator,
{
	type Item = Result<T::Item, E>;
	type IntoIter = ResultExpandIter<T::IntoIter, E>;
	fn into_iter(self) -> Self::IntoIter {
		ResultExpandIter::new(self.0.map(IntoIterator::into_iter))
	}
}
#[pin_project(project=ResultExpandIterProj)]
pub enum ResultExpandIter<T, E> {
	Ok(#[pin] T),
	Err(Option<E>),
}
impl<T, E> ResultExpandIter<T, E> {
	pub fn new(t: Result<T, E>) -> Self {
		match t {
			Ok(t) => Self::Ok(t),
			Err(e) => Self::Err(Some(e)),
		}
	}
}
impl<T, E> Iterator for ResultExpandIter<T, E>
where
	T: Iterator,
{
	type Item = Result<T::Item, E>;
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			Self::Ok(t) => t.next().map(Ok),
			Self::Err(e) => e.take().map(Err),
		}
	}
}
impl<T, E> Stream for ResultExpandIter<T, E>
where
	T: Stream,
{
	type Item = Result<T::Item, E>;
	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		let ret = match self.project() {
			ResultExpandIterProj::Ok(t) => ready!(t.poll_next(cx)).map(Ok),
			ResultExpandIterProj::Err(e) => e.take().map(Err),
		};
		Poll::Ready(ret)
	}
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IoError(#[serde(with = "crate::misc_serde")] Arc<io::Error>);
impl PartialEq for IoError {
	fn eq(&self, other: &Self) -> bool {
		self.0.to_string() == other.0.to_string()
	}
}
impl error::Error for IoError {}
impl fmt::Display for IoError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		fmt::Display::fmt(&self.0, f)
	}
}
impl fmt::Debug for IoError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		fmt::Debug::fmt(&self.0, f)
	}
}
impl From<io::Error> for IoError {
	fn from(err: io::Error) -> Self {
		Self(Arc::new(err))
	}
}
impl From<IoError> for io::Error {
	fn from(err: IoError) -> Self {
		Arc::try_unwrap(err.0).unwrap()
	}
}

#[derive(new)]
#[repr(transparent)]
pub struct DistParStream<S>(S);
impl<S> ParallelStream for DistParStream<S>
where
	S: DistributedStream,
{
	type Item = S::Item;
	type Task = S::Task;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.0.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.0.next_task()
	}
}

impl_par_dist_rename! {
	#[cfg(feature = "doc")]
	#[doc(hidden)]
	pub struct ImplParallelStream<T>(PhantomData<fn() -> T>);
	#[cfg(feature = "doc")]
	impl<T> ImplParallelStream<T> {
		pub fn new<U>(_drop: U) -> Self
		where
			U: ParallelStream<Item = T>,
		{
			Self(PhantomData)
		}
	}
	#[cfg(feature = "doc")]
	impl<T: 'static> ParallelStream for ImplParallelStream<T> {
		type Item = T;
		type Task = ImplTask<T>;

		fn size_hint(&self) -> (usize, Option<usize>) {
			unreachable!()
		}
		fn next_task(&mut self) -> Option<Self::Task> {
			unreachable!()
		}
	}
}

#[cfg(feature = "doc")]
#[doc(hidden)]
#[derive(Serialize, Deserialize)]
pub struct ImplTask<T>(PhantomData<fn() -> T>);
#[cfg(feature = "doc")]
impl<T> StreamTask for ImplTask<T>
where
	T: 'static,
{
	type Item = T;
	type Async = ImplTask<T>;

	fn into_async(self) -> Self::Async {
		self
	}
}
#[cfg(feature = "doc")]
impl<T: 'static> StreamTaskAsync for ImplTask<T> {
	type Item = T;

	fn poll_run(
		self: Pin<&mut Self>, _cx: &mut Context, _sink: Pin<&mut impl Sink<Item = Self::Item>>,
	) -> Poll<()> {
		unreachable!()
	}
}

pub fn type_coerce<A, B>(a: A) -> B {
	try_type_coerce(a)
		.unwrap_or_else(|| panic!("can't coerce {} to {}", type_name::<A>(), type_name::<B>()))
}
pub fn try_type_coerce<A, B>(a: A) -> Option<B> {
	trait Eq<B> {
		fn eq(self) -> Option<B>;
	}

	struct Foo<A, B>(A, PhantomData<fn() -> B>);

	impl<A, B> Eq<B> for Foo<A, B> {
		default fn eq(self) -> Option<B> {
			None
		}
	}
	impl<A> Eq<A> for Foo<A, A> {
		fn eq(self) -> Option<A> {
			Some(self.0)
		}
	}

	Foo::<A, B>(a, PhantomData).eq()
}

#[repr(transparent)]
pub struct Debug<T: ?Sized>(pub T);
trait DebugDuck {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>;
}
impl<T: ?Sized> DebugDuck for T {
	default fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
		write!(f, "{}", std::any::type_name::<Self>())
	}
}
impl<T: ?Sized> DebugDuck for T
where
	T: std::fmt::Debug,
{
	fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
		<T as std::fmt::Debug>::fmt(self, f)
	}
}
impl<T: ?Sized> std::fmt::Debug for Debug<T> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
		<T as DebugDuck>::fmt(&self.0, f)
	}
}
