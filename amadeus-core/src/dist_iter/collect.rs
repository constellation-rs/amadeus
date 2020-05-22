use futures::{pin_mut, ready, Stream, StreamExt};
use pin_project::{pin_project, project};
use serde::{Deserialize, Serialize};
use std::{
	collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque}, hash::{BuildHasher, Hash}, marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, ReducerA, ReducerAsync
};
use crate::pool::ProcessSend;

#[must_use]
pub struct Collect<I, A> {
	i: I,
	marker: PhantomData<fn() -> A>,
}
impl<I, A> Collect<I, A> {
	pub(crate) fn new(i: I) -> Self {
		Self {
			i,
			marker: PhantomData,
		}
	}
}

impl<I: DistributedIteratorMulti<Source>, Source, T: FromDistributedIterator<I::Item>>
	DistributedReducer<I, Source, T> for Collect<I, T>
{
	type ReduceAFactory = T::ReduceAFactory;
	type ReduceA = T::ReduceA;
	type ReduceB = T::ReduceB;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		let (a, b) = T::reducers();
		(self.i, a, b)
	}
}

pub trait FromDistributedIterator<T>: Sized {
	type ReduceAFactory: ReduceFactory<Reducer = Self::ReduceA>;
	type ReduceA: ReducerA<Item = T> + ProcessSend;
	type ReduceB: Reducer<Item = <Self::ReduceA as Reducer>::Output, Output = Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB);
	// 	fn from_dist_iter<I>(dist_iter: I, pool: &Pool) -> Self where T: Serialize + DeserializeOwned + Send + 'static, I: IntoDistributedIterator<Item = T>, <<I as IntoDistributedIterator>::Iter as DistributedIterator>::Task: Serialize + DeserializeOwned + Send + 'static;
}

pub struct DefaultReduceFactory<T>(PhantomData<fn(T)>);
impl<T> Default for DefaultReduceFactory<T> {
	fn default() -> Self {
		Self(PhantomData)
	}
}
impl<T: Default + Reducer> ReduceFactory for DefaultReduceFactory<T> {
	type Reducer = T;
	fn make(&self) -> Self::Reducer {
		T::default()
	}
}

pub trait Push<A> {
	fn push(&mut self, item: A);
}
// impl<A, T: Extend<A>> Push<A> for T {
// 	default fn push(&mut self, item: A) {
// 		self.extend(iter::once(item));
// 	}
// }
impl<T> Push<T> for Vec<T> {
	#[inline]
	fn push(&mut self, item: T) {
		self.push(item);
	}
}
impl<T> Push<T> for LinkedList<T> {
	#[inline]
	fn push(&mut self, item: T) {
		self.push_back(item);
	}
}
impl<T, S> Push<T> for HashSet<T, S>
where
	T: Eq + Hash,
	S: BuildHasher,
{
	#[inline]
	fn push(&mut self, item: T) {
		let _ = self.insert(item);
	}
}
impl<K, V, S> Push<(K, V)> for HashMap<K, V, S>
where
	K: Eq + Hash,
	S: BuildHasher,
{
	#[inline]
	fn push(&mut self, item: (K, V)) {
		let _ = self.insert(item.0, item.1);
	}
}
impl<T> Push<T> for BTreeSet<T>
where
	T: Ord,
{
	#[inline]
	fn push(&mut self, item: T) {
		let _ = self.insert(item);
	}
}
impl<K, V> Push<(K, V)> for BTreeMap<K, V>
where
	K: Ord,
{
	#[inline]
	fn push(&mut self, item: (K, V)) {
		let _ = self.insert(item.0, item.1);
	}
}
impl Push<char> for String {
	#[inline]
	fn push(&mut self, item: char) {
		self.push(item);
	}
}
impl Push<Self> for String {
	#[inline]
	fn push(&mut self, item: Self) {
		self.push_str(&item);
	}
}
impl Push<Self> for () {
	#[inline]
	fn push(&mut self, _item: Self) {}
}

#[pin_project]
#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "T: Serialize"),
	bound(deserialize = "T: Deserialize<'de>")
)]
pub struct PushReducer<A, T = A>(Option<T>, PhantomData<fn(A)>);
impl<A, T> PushReducer<A, T> {
	pub(crate) fn new(t: T) -> Self {
		Self(Some(t), PhantomData)
	}
}
impl<A, T> Default for PushReducer<A, T>
where
	T: Default,
{
	fn default() -> Self {
		Self(Some(T::default()), PhantomData)
	}
}
impl<A, T: Push<A>> Reducer for PushReducer<A, T> {
	type Item = A;
	type Output = T;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A, T: Push<A>> ReducerAsync for PushReducer<A, T> {
	type Item = A;
	type Output = T;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_.0.as_mut().unwrap().push(item);
		}
		Poll::Ready(())
	}
	fn poll_output(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(self.0.take().unwrap())
	}
}
impl<A, T: Push<A>> ReducerA for PushReducer<A, T>
where
	A: 'static,
	T: ProcessSend,
{
	type Output = T;
}

#[pin_project]
pub struct ExtendReducer<A, T = A>(Option<T>, PhantomData<fn(A)>);
impl<A, T> Default for ExtendReducer<A, T>
where
	T: Default,
{
	fn default() -> Self {
		Self(Some(T::default()), PhantomData)
	}
}
impl<A: IntoIterator<Item = B>, T: Extend<B>, B> Reducer for ExtendReducer<A, T> {
	type Item = A;
	type Output = T;
	type Async = Self;

	fn into_async(self) -> Self::Async {
		self
	}
}
impl<A: IntoIterator<Item = B>, T: Extend<B>, B> ReducerAsync for ExtendReducer<A, T> {
	type Item = A;
	type Output = T;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_.0.as_mut().unwrap().extend(item);
		}
		Poll::Ready(())
	}
	fn poll_output(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(self.0.take().unwrap())
	}
}

#[pin_project]
pub struct IntoReducer<R, T>(#[pin] R, PhantomData<fn(T)>);
impl<R, T> Default for IntoReducer<R, T>
where
	R: Default,
{
	fn default() -> Self {
		Self(R::default(), PhantomData)
	}
}
impl<R: Reducer, T> Reducer for IntoReducer<R, T>
where
	R::Output: Into<T>,
{
	type Item = R::Item;
	type Output = T;
	type Async = IntoReducer<R::Async, T>;

	fn into_async(self) -> Self::Async {
		IntoReducer(self.0.into_async(), PhantomData)
	}
}
impl<R: ReducerAsync, T> ReducerAsync for IntoReducer<R, T>
where
	R::Output: Into<T>,
{
	type Item = R::Item;
	type Output = T;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let stream = stream.map(Into::into);
		pin_mut!(stream);
		self.project().0.poll_forward(cx, stream)
	}
	fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		self.project().0.poll_output(cx).map(Into::into)
	}
}

pub struct OptionReduceFactory<RF: ReduceFactory>(RF);
impl<RF: ReduceFactory> ReduceFactory for OptionReduceFactory<RF> {
	type Reducer = OptionReducer<RF::Reducer>;

	fn make(&self) -> Self::Reducer {
		OptionReducer(Some(self.0.make()))
	}
}
#[pin_project]
#[derive(Serialize, Deserialize)]
pub struct OptionReducer<R>(#[pin] Option<R>);
impl<R> Default for OptionReducer<R>
where
	R: Default,
{
	fn default() -> Self {
		Self(Some(R::default()))
	}
}
impl<R: Reducer> Reducer for OptionReducer<R> {
	type Item = Option<R::Item>;
	type Output = Option<R::Output>;
	type Async = OptionReducer<R::Async>;

	fn into_async(self) -> Self::Async {
		OptionReducer(Some(self.0.unwrap().into_async()))
	}
}
impl<R: ReducerAsync> ReducerAsync for OptionReducer<R> {
	type Item = Option<R::Item>;
	type Output = Option<R::Output>;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, _cx: &mut Context, _stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		todo!()
		// match (&mut self.0, item.is_some()) {
		// 	(&mut Some(ref mut a), true) => {
		// 		return a.push(item.unwrap());
		// 	}
		// 	(self_, _) => *self_ = None,
		// }
		// self.0.is_some()
	}
	fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		self.project()
			.0
			.as_pin_mut()
			.map(|r| r.poll_output(cx).map(Some))
			.unwrap_or(Poll::Ready(None))
	}
}
impl<R: Reducer> ReducerA for OptionReducer<R>
where
	R: ProcessSend,
	R::Output: ProcessSend,
{
	type Output = Option<R::Output>;
}

pub struct ResultReduceFactory<RF: ReduceFactory, E>(RF, PhantomData<fn(E)>);
impl<RF: ReduceFactory, E> ReduceFactory for ResultReduceFactory<RF, E> {
	type Reducer = ResultReducer<RF::Reducer, E>;

	fn make(&self) -> Self::Reducer {
		ResultReducer(self.0.make(), PhantomData)
	}
}

#[derive(Serialize, Deserialize)]
pub struct ResultReducer<R, E>(R, PhantomData<E>);
impl<R, E> Default for ResultReducer<R, E>
where
	R: Default,
{
	fn default() -> Self {
		Self(R::default(), PhantomData)
	}
}
impl<R: Reducer, E> Reducer for ResultReducer<R, E> {
	type Item = Result<R::Item, E>;
	type Output = Result<R::Output, E>;
	type Async = ResultReducerAsync<R::Async, E>;

	fn into_async(self) -> Self::Async {
		ResultReducerAsync::Ok(self.0.into_async())
	}
}
#[pin_project]
pub enum ResultReducerAsync<R, E> {
	Ok(#[pin] R),
	Err(Option<E>),
}
impl<R: ReducerAsync, E> ReducerAsync for ResultReducerAsync<R, E> {
	type Item = Result<R::Item, E>;
	type Output = Result<R::Output, E>;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, _cx: &mut Context, _stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		todo!()
		// match (&mut self.0, item.is_ok()) {
		// 	(&mut Ok(ref mut a), true) => {
		// 		return a.push(item.ok().unwrap());
		// 	}
		// 	(self_, false) => *self_ = Err(item.err().unwrap()),
		// 	_ => (),
		// }
		// self.0.is_ok()
	}
	#[project]
	fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		#[project]
		match self.project() {
			ResultReducerAsync::Ok(a) => a.poll_output(cx).map(Ok),
			ResultReducerAsync::Err(b) => Poll::Ready(Err(b.take().unwrap())),
		}
	}
}
impl<R: Reducer, E> ReducerA for ResultReducer<R, E>
where
	R: ProcessSend,
	R::Output: ProcessSend,
	E: ProcessSend,
{
	type Output = Result<R::Output, E>;
}

impl<T> FromDistributedIterator<T> for Vec<T>
where
	T: ProcessSend,
{
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<T, Self>;
	type ReduceB = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<T> FromDistributedIterator<T> for VecDeque<T>
where
	T: ProcessSend,
{
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<T, Vec<T>>;
	type ReduceB = IntoReducer<ExtendReducer<Vec<T>>, Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<T: Ord> FromDistributedIterator<T> for BinaryHeap<T>
where
	T: ProcessSend,
{
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<T, Vec<T>>;
	type ReduceB = IntoReducer<ExtendReducer<Vec<T>>, Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<T> FromDistributedIterator<T> for LinkedList<T>
where
	T: ProcessSend,
{
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<T, Self>;
	type ReduceB = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<T, S> FromDistributedIterator<T> for HashSet<T, S>
where
	T: Eq + Hash + ProcessSend,
	S: BuildHasher + Default + Send + 'static,
{
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<T, Self>;
	type ReduceB = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<K, V, S> FromDistributedIterator<(K, V)> for HashMap<K, V, S>
where
	K: Eq + Hash + ProcessSend,
	V: ProcessSend,
	S: BuildHasher + Default + Send + 'static,
{
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<(K, V), Self>;
	type ReduceB = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<T> FromDistributedIterator<T> for BTreeSet<T>
where
	T: Ord + ProcessSend,
{
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<T, Self>;
	type ReduceB = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<K, V> FromDistributedIterator<(K, V)> for BTreeMap<K, V>
where
	K: Ord + ProcessSend,
	V: ProcessSend,
{
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<(K, V), Self>;
	type ReduceB = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl FromDistributedIterator<char> for String {
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<char, Self>;
	type ReduceB = PushReducer<Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl FromDistributedIterator<Self> for String {
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<Self>;
	type ReduceB = PushReducer<Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl FromDistributedIterator<()> for () {
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<Self>;
	type ReduceB = PushReducer<Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<T, C: FromDistributedIterator<T>> FromDistributedIterator<Option<T>> for Option<C> {
	type ReduceAFactory = OptionReduceFactory<C::ReduceAFactory>;
	type ReduceA = OptionReducer<C::ReduceA>;
	type ReduceB = OptionReducer<C::ReduceB>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		let (a, b) = C::reducers();
		(OptionReduceFactory(a), OptionReducer(Some(b)))
	}
}

impl<T, C: FromDistributedIterator<T>, E> FromDistributedIterator<Result<T, E>> for Result<C, E>
where
	E: ProcessSend,
{
	type ReduceAFactory = ResultReduceFactory<C::ReduceAFactory, E>;
	type ReduceA = ResultReducer<C::ReduceA, E>;
	type ReduceB = ResultReducer<C::ReduceB, E>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		let (a, b) = C::reducers();
		(
			ResultReduceFactory(a, PhantomData),
			ResultReducer(b, PhantomData),
		)
	}
}
