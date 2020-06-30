use derive_new::new;
use educe::Educe;
use futures::{pin_mut, ready, Stream, StreamExt};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque}, hash::{BuildHasher, Hash}, iter, marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::pool::ProcessSend;

#[derive(new)]
#[must_use]
pub struct Collect<I, A> {
	i: I,
	marker: PhantomData<fn() -> A>,
}

impl<I: ParallelPipe<Source>, Source, T: FromParallelStream<I::Item>> ParallelSink<Source>
	for Collect<I, T>
{
	type Output = T;
	type Pipe = I;
	type ReduceA = T::ReduceA;
	type ReduceC = T::ReduceC;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		let (a, b) = T::reducers();
		(self.i, a, b)
	}
}
impl<I: DistributedPipe<Source>, Source, T: FromDistributedStream<I::Item>> DistributedSink<Source>
	for Collect<I, T>
{
	type Output = T;
	type Pipe = I;
	type ReduceA = T::ReduceA;
	type ReduceB = T::ReduceB;
	type ReduceC = T::ReduceC;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		let (a, b, c) = T::reducers();
		(self.i, a, b, c)
	}
}

pub trait FromParallelStream<T>: Sized {
	type ReduceA: ReducerSend<Item = T> + Clone + Send;
	type ReduceC: Reducer<Item = <Self::ReduceA as Reducer>::Output, Output = Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC);
}

pub trait FromDistributedStream<T>: Sized {
	type ReduceA: ReducerSend<Item = T> + Clone + ProcessSend;
	type ReduceB: ReducerProcessSend<Item = <Self::ReduceA as Reducer>::Output>
		+ Clone
		+ ProcessSend;
	type ReduceC: Reducer<Item = <Self::ReduceB as Reducer>::Output, Output = Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC);
	// 	fn from_dist_stream<I>(dist_stream: I, pool: &Pool) -> Self where T: Serialize + DeserializeOwned + Send + 'static, I: IntoDistributedStream<Item = T>, <<I as IntoDistributedStream>::Iter as DistributedStream>::Task: Serialize + DeserializeOwned + Send + 'static;
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone, Default)]
#[serde(bound = "")]
pub struct PushReducer<A, T = A>(PhantomData<fn() -> (T, A)>);

impl<A, T: Default + Extend<A>> Reducer for PushReducer<A, T> {
	type Item = A;
	type Output = T;
	type Async = PushReducerAsync<A, T>;

	fn into_async(self) -> Self::Async {
		PushReducerAsync(Some(Default::default()), PhantomData)
	}
}
impl<A, T: Default + Extend<A>> ReducerProcessSend for PushReducer<A, T>
where
	T: ProcessSend + 'static,
{
	type Output = T;
}
impl<A, T: Default + Extend<A>> ReducerSend for PushReducer<A, T>
where
	T: Send + 'static,
{
	type Output = T;
}

#[pin_project]
pub struct PushReducerAsync<A, T = A>(Option<T>, PhantomData<fn() -> A>);
impl<A, T: Extend<A>> ReducerAsync for PushReducerAsync<A, T> {
	type Item = A;
	type Output = T;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		mut stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		let self_ = self.project();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_.0.as_mut().unwrap().extend(iter::once(item));
		}
		Poll::Ready(())
	}
	fn poll_output(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
		Poll::Ready(self.0.take().unwrap())
	}
}

#[derive(Educe, Serialize, Deserialize)]
#[educe(Clone, Default)]
#[serde(bound = "")]
pub struct ExtendReducer<A, T = A>(PhantomData<fn() -> (T, A)>);
impl<A: IntoIterator<Item = B>, T: Default + Extend<B>, B> Reducer for ExtendReducer<A, T> {
	type Item = A;
	type Output = T;
	type Async = ExtendReducerAsync<A, T>;

	fn into_async(self) -> Self::Async {
		ExtendReducerAsync(Some(T::default()), PhantomData)
	}
}
impl<A: IntoIterator<Item = B>, T: Default + Extend<B>, B> ReducerProcessSend
	for ExtendReducer<A, T>
where
	T: ProcessSend + 'static,
{
	type Output = T;
}
impl<A: IntoIterator<Item = B>, T: Default + Extend<B>, B> ReducerSend for ExtendReducer<A, T>
where
	T: Send + 'static,
{
	type Output = T;
}

#[pin_project]
pub struct ExtendReducerAsync<A, T = A>(Option<T>, PhantomData<fn() -> A>);
impl<A: IntoIterator<Item = B>, T: Extend<B>, B> ReducerAsync for ExtendReducerAsync<A, T> {
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

#[derive(Educe, Serialize, Deserialize)]
#[educe(Clone(bound = "R: Clone"))] //, Default(bound = "R: Default"))]
#[serde(
	bound(serialize = "R: Serialize"),
	bound(deserialize = "R: Deserialize<'de>")
)]
pub struct IntoReducer<R, T>(R, PhantomData<fn() -> T>);
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
	type Async = IntoReducerAsync<R::Async, T>;

	fn into_async(self) -> Self::Async {
		IntoReducerAsync(self.0.into_async(), PhantomData)
	}
}

#[pin_project]
pub struct IntoReducerAsync<R, T>(#[pin] R, PhantomData<fn() -> T>);

impl<R: ReducerAsync, T> ReducerAsync for IntoReducerAsync<R, T>
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

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct OptionReducer<R>(R);
impl<R: Reducer> Reducer for OptionReducer<R> {
	type Item = Option<R::Item>;
	type Output = Option<R::Output>;
	type Async = OptionReducerAsync<R::Async>;

	fn into_async(self) -> Self::Async {
		OptionReducerAsync(Some(self.0.into_async()))
	}
}
impl<R: Reducer> ReducerProcessSend for OptionReducer<R>
where
	R::Output: ProcessSend + 'static,
{
	type Output = Option<R::Output>;
}
impl<R: Reducer> ReducerSend for OptionReducer<R>
where
	R::Output: Send + 'static,
{
	type Output = Option<R::Output>;
}

#[pin_project]
pub struct OptionReducerAsync<R>(#[pin] Option<R>);

impl<R: ReducerAsync> ReducerAsync for OptionReducerAsync<R> {
	type Item = Option<R::Item>;
	type Output = Option<R::Output>;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, _cx: &mut Context, _stream: Pin<&mut impl Stream<Item = Self::Item>>,
	) -> Poll<()> {
		todo!("Tracking at https://github.com/constellation-rs/amadeus/projects/3#card-40276549");
		// let self_ = self.project();
		// match (self_.0, item.is_some()) {
		// 	(&mut Some(ref mut a), true) => {
		// 		return a.push(item.unwrap());
		// 	}
		// 	(self_, _) => *self_ = None,
		// }
		// self_.0.is_some()
	}
	fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		self.project()
			.0
			.as_pin_mut()
			.map(|r| r.poll_output(cx).map(Some))
			.unwrap_or(Poll::Ready(None))
	}
}

#[derive(Educe, Serialize, Deserialize)]
#[educe(Clone(bound = "R: Clone"))] // , Default(bound = "R: Default"))]
#[serde(
	bound(serialize = "R: Serialize"),
	bound(deserialize = "R: Deserialize<'de>")
)]
pub struct ResultReducer<R, E>(R, PhantomData<fn() -> E>);
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
impl<R: Reducer, E> ReducerProcessSend for ResultReducer<R, E>
where
	R::Output: ProcessSend + 'static,
	E: ProcessSend + 'static,
{
	type Output = Result<R::Output, E>;
}
impl<R: Reducer, E> ReducerSend for ResultReducer<R, E>
where
	R::Output: Send + 'static,
	E: Send + 'static,
{
	type Output = Result<R::Output, E>;
}

#[pin_project(project = ResultReducerAsyncProj)]
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
		todo!("Tracking at https://github.com/constellation-rs/amadeus/projects/3#card-40276549");
		// let self_ = self.project();
		// match (self_.0, item.is_ok()) {
		// 	(&mut Ok(ref mut a), true) => {
		// 		return a.push(item.ok().unwrap());
		// 	}
		// 	(self_, false) => *self_ = Err(item.err().unwrap()),
		// 	_ => (),
		// }
		// self_.0.is_ok()
	}
	fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		match self.project() {
			ResultReducerAsyncProj::Ok(a) => a.poll_output(cx).map(Ok),
			ResultReducerAsyncProj::Err(b) => Poll::Ready(Err(b.take().unwrap())),
		}
	}
}

impl<T> FromParallelStream<T> for Vec<T>
where
	T: Send + 'static,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T> FromParallelStream<T> for VecDeque<T>
where
	T: Send + 'static,
{
	type ReduceA = PushReducer<T, Vec<T>>;
	type ReduceC = IntoReducer<ExtendReducer<Vec<T>>, Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T: Ord> FromParallelStream<T> for BinaryHeap<T>
where
	T: Send + 'static,
{
	type ReduceA = PushReducer<T, Vec<T>>;
	type ReduceC = IntoReducer<ExtendReducer<Vec<T>>, Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T> FromParallelStream<T> for LinkedList<T>
where
	T: Send + 'static,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T, S> FromParallelStream<T> for HashSet<T, S>
where
	T: Eq + Hash + Send + 'static,
	S: BuildHasher + Default + Send + 'static,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<K, V, S> FromParallelStream<(K, V)> for HashMap<K, V, S>
where
	K: Eq + Hash + Send + 'static,
	V: Send + 'static,
	S: BuildHasher + Default + Send + 'static,
{
	type ReduceA = PushReducer<(K, V), Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T> FromParallelStream<T> for BTreeSet<T>
where
	T: Ord + Send + 'static,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<K, V> FromParallelStream<(K, V)> for BTreeMap<K, V>
where
	K: Ord + Send + 'static,
	V: Send + 'static,
{
	type ReduceA = PushReducer<(K, V), Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl FromParallelStream<char> for String {
	type ReduceA = PushReducer<char, Self>;
	type ReduceC = PushReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl FromParallelStream<Self> for String {
	type ReduceA = PushReducer<Self>;
	type ReduceC = PushReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl FromParallelStream<()> for () {
	type ReduceA = PushReducer<Self>;
	type ReduceC = PushReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T, C: FromParallelStream<T>> FromParallelStream<Option<T>> for Option<C> {
	type ReduceA = OptionReducer<C::ReduceA>;
	type ReduceC = OptionReducer<C::ReduceC>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		let (a, c) = C::reducers();
		(OptionReducer(a), OptionReducer(c))
	}
}

impl<T, C: FromParallelStream<T>, E> FromParallelStream<Result<T, E>> for Result<C, E>
where
	E: Send + 'static,
{
	type ReduceA = ResultReducer<C::ReduceA, E>;
	type ReduceC = ResultReducer<C::ReduceC, E>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		let (a, c) = C::reducers();
		(ResultReducer(a, PhantomData), ResultReducer(c, PhantomData))
	}
}

impl<T> FromDistributedStream<T> for Vec<T>
where
	T: ProcessSend + 'static,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceB = ExtendReducer<Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		Default::default()
	}
}

impl<T> FromDistributedStream<T> for VecDeque<T>
where
	T: ProcessSend + 'static,
{
	type ReduceA = PushReducer<T, Vec<T>>;
	type ReduceB = ExtendReducer<Vec<T>>;
	type ReduceC = IntoReducer<ExtendReducer<Vec<T>>, Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		Default::default()
	}
}

impl<T: Ord> FromDistributedStream<T> for BinaryHeap<T>
where
	T: ProcessSend + 'static,
{
	type ReduceA = PushReducer<T, Vec<T>>;
	type ReduceB = ExtendReducer<Vec<T>>;
	type ReduceC = IntoReducer<ExtendReducer<Vec<T>>, Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		Default::default()
	}
}

impl<T> FromDistributedStream<T> for LinkedList<T>
where
	T: ProcessSend + 'static,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceB = ExtendReducer<Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		Default::default()
	}
}

impl<T, S> FromDistributedStream<T> for HashSet<T, S>
where
	T: Eq + Hash + ProcessSend + 'static,
	S: BuildHasher + Default + Send + 'static,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceB = ExtendReducer<Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		Default::default()
	}
}

impl<K, V, S> FromDistributedStream<(K, V)> for HashMap<K, V, S>
where
	K: Eq + Hash + ProcessSend + 'static,
	V: ProcessSend + 'static,
	S: BuildHasher + Default + Send + 'static,
{
	type ReduceA = PushReducer<(K, V), Self>;
	type ReduceB = ExtendReducer<Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		Default::default()
	}
}

impl<T> FromDistributedStream<T> for BTreeSet<T>
where
	T: Ord + ProcessSend + 'static,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceB = ExtendReducer<Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		Default::default()
	}
}

impl<K, V> FromDistributedStream<(K, V)> for BTreeMap<K, V>
where
	K: Ord + ProcessSend + 'static,
	V: ProcessSend + 'static,
{
	type ReduceA = PushReducer<(K, V), Self>;
	type ReduceB = ExtendReducer<Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		Default::default()
	}
}

impl FromDistributedStream<char> for String {
	type ReduceA = PushReducer<char, Self>;
	type ReduceB = PushReducer<Self>;
	type ReduceC = PushReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		Default::default()
	}
}

impl FromDistributedStream<Self> for String {
	type ReduceA = PushReducer<Self>;
	type ReduceB = PushReducer<Self>;
	type ReduceC = PushReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		Default::default()
	}
}

impl FromDistributedStream<()> for () {
	type ReduceA = PushReducer<Self>;
	type ReduceB = PushReducer<Self>;
	type ReduceC = PushReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		Default::default()
	}
}

impl<T, C: FromDistributedStream<T>> FromDistributedStream<Option<T>> for Option<C> {
	type ReduceA = OptionReducer<C::ReduceA>;
	type ReduceB = OptionReducer<C::ReduceB>;
	type ReduceC = OptionReducer<C::ReduceC>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		let (a, b, c) = C::reducers();
		(OptionReducer(a), OptionReducer(b), OptionReducer(c))
	}
}

impl<T, C: FromDistributedStream<T>, E> FromDistributedStream<Result<T, E>> for Result<C, E>
where
	E: ProcessSend + 'static,
{
	type ReduceA = ResultReducer<C::ReduceA, E>;
	type ReduceB = ResultReducer<C::ReduceB, E>;
	type ReduceC = ResultReducer<C::ReduceC, E>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		let (a, b, c) = C::reducers();
		(
			ResultReducer(a, PhantomData),
			ResultReducer(b, PhantomData),
			ResultReducer(c, PhantomData),
		)
	}
}
