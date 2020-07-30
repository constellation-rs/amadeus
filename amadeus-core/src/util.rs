use derive_new::new;
use futures::{ready, Stream};
use pin_project::pin_project;
use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};
use std::{
	any::{Any, TypeId}, error, fmt, hash::{Hash, Hasher}, io, marker::PhantomData, pin::Pin, sync::Arc, task::{Context, Poll}
};

use crate::par_stream::{DistributedStream, ParallelStream};

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
#[pin_project(project = ResultExpandIterProj)]
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

#[pin_project]
#[derive(new)]
#[repr(transparent)]
pub struct DistParStream<S>(#[pin] S);
impl<S> ParallelStream for DistParStream<S>
where
	S: DistributedStream,
{
	type Item = S::Item;
	type Task = S::Task;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.0.size_hint()
	}
	fn next_task(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Task>> {
		self.project().0.next_task(cx)
	}
}

// This is a dumb hack to avoid triggering https://github.com/rust-lang/rust/issues/48214 in amadeus-derive: see https://github.com/taiki-e/pin-project/issues/102#issuecomment-540472282
#[doc(hidden)]
#[repr(transparent)]
pub struct Wrapper<'a, T: ?Sized>(PhantomData<&'a ()>, T);
impl<'a, T: ?Sized> Wrapper<'a, T> {
	pub fn new(t: T) -> Self
	where
		T: Sized,
	{
		Self(PhantomData, t)
	}
	pub fn into_inner(self) -> T
	where
		T: Sized,
	{
		self.1
	}
}
impl<'a, T: ?Sized> Hash for Wrapper<'a, T>
where
	T: Hash,
{
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.1.hash(state)
	}
}
impl<'a, T: ?Sized> Serialize for Wrapper<'a, T>
where
	T: Serialize,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.1.serialize(serializer)
	}
}
impl<'a, 'de, T: ?Sized> Deserialize<'de> for Wrapper<'a, T>
where
	T: Deserialize<'de>,
{
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		T::deserialize(deserializer).map(Wrapper::new)
	}
}
impl<'a, T: ?Sized> fmt::Debug for Wrapper<'a, T>
where
	T: fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		fmt::Debug::fmt(&self.1, f)
	}
}

/// As unsafe as regular `core::mem::transmute`, but asserts size at runtime.
///
/// # Safety
///
/// Not.
#[allow(unsafe_code)]
#[inline(always)]
pub unsafe fn transmute<A, B>(a: A) -> B {
	use std::mem;
	assert_eq!(
		(mem::size_of::<A>(), mem::align_of::<A>()),
		(mem::size_of::<B>(), mem::align_of::<B>())
	);
	let ret = mem::transmute_copy(&a);
	mem::forget(a);
	ret
}

#[allow(unsafe_code)]
#[inline(always)]
pub fn type_coerce<A, B>(a: A) -> Option<B>
where
	A: 'static,
	B: 'static,
{
	if type_eq::<A, B>() {
		Some(unsafe { transmute(a) })
	} else {
		None
	}
}
#[inline(always)]
pub fn type_coerce_ref<A, B>(a: &A) -> Option<&B>
where
	A: 'static,
	B: 'static,
{
	<dyn Any>::downcast_ref(a)
}
#[inline(always)]
pub fn type_coerce_mut<A, B>(a: &mut A) -> Option<&mut B>
where
	A: 'static,
	B: 'static,
{
	<dyn Any>::downcast_mut(a)
}

#[inline(always)]
pub fn type_eq<A: ?Sized, B: ?Sized>() -> bool
where
	A: 'static,
	B: 'static,
{
	TypeId::of::<A>() == TypeId::of::<B>()
}
