use serde::{Deserialize, Serialize};
use std::{any::type_name, error, fmt, io, marker::PhantomData, sync::Arc};

#[cfg(feature = "doc")]
use crate::{
	dist_iter::{Consumer, ConsumerAsync, DistributedIterator}, sink::Sink
};
#[cfg(feature = "doc")]
use std::{
	pin::Pin, task::{Context, Poll}
};

pub struct ResultExpand<T, E>(pub Result<T, E>);
impl<T, E> IntoIterator for ResultExpand<T, E>
where
	T: IntoIterator,
{
	type Item = Result<T::Item, E>;
	type IntoIter = ResultExpandIter<T::IntoIter, E>;
	fn into_iter(self) -> Self::IntoIter {
		ResultExpandIter(self.0.map(IntoIterator::into_iter).map_err(Some))
	}
}
pub struct ResultExpandIter<T, E>(Result<T, Option<E>>);
impl<T, E> Iterator for ResultExpandIter<T, E>
where
	T: Iterator,
{
	type Item = Result<T::Item, E>;
	fn next(&mut self) -> Option<Self::Item> {
		transpose(self.0.as_mut().map(Iterator::next).map_err(Option::take))
	}
}
fn transpose<T, E>(result: Result<Option<T>, Option<E>>) -> Option<Result<T, E>> {
	match result {
		Ok(Some(x)) => Some(Ok(x)),
		Err(Some(e)) => Some(Err(e)),
		Ok(None) | Err(None) => None,
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

#[cfg(feature = "doc")]
#[doc(hidden)]
pub struct ImplDistributedIterator<T>(PhantomData<fn(T)>);
#[cfg(feature = "doc")]
impl<T> ImplDistributedIterator<T> {
	pub fn new<U>(_drop: U) -> Self
	where
		U: DistributedIterator<Item = T>,
	{
		Self(PhantomData)
	}
}
#[cfg(feature = "doc")]
impl<T: 'static> DistributedIterator for ImplDistributedIterator<T> {
	type Item = T;
	type Task = ImplConsumer<T>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		unreachable!()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		unreachable!()
	}
}

#[cfg(feature = "doc")]
#[doc(hidden)]
#[derive(Serialize, Deserialize)]
pub struct ImplConsumer<T>(PhantomData<fn(T)>);
#[cfg(feature = "doc")]
impl<T> Consumer for ImplConsumer<T>
where
	T: 'static,
{
	type Item = T;
	type Async = ImplConsumer<T>;

	fn into_async(self) -> Self::Async {
		self
	}
}
#[cfg(feature = "doc")]
impl<T: 'static> ConsumerAsync for ImplConsumer<T> {
	type Item = T;

	fn poll_run(
		self: Pin<&mut Self>, _cx: &mut Context, _sink: Pin<&mut impl Sink<Self::Item>>,
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

	struct Foo<A, B>(A, PhantomData<fn(B)>);

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
