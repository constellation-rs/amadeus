use derive_new::new;
use educe::Educe;
use futures::{pin_mut, ready, Stream, StreamExt};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque}, future::Future, hash::{BuildHasher, Hash}, iter, marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::{pipe::Sink, pool::ProcessSend};

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
	type ReduceA: ReducerSend<T> + Clone + Send;
	type ReduceC: Reducer<<Self::ReduceA as ReducerSend<T>>::Output, Output = Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC);
}

pub trait FromDistributedStream<T>: Sized {
	type ReduceA: ReducerSend<T> + Clone + ProcessSend;
	type ReduceB: ReducerProcessSend<<Self::ReduceA as ReducerSend<T>>::Output>
		+ Clone
		+ ProcessSend;
	type ReduceC: Reducer<
		<Self::ReduceB as ReducerProcessSend<<Self::ReduceA as ReducerSend<T>>::Output>>::Output,
		Output = Self,
	>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC);
	// 	fn from_dist_stream<I>(dist_stream: I, pool: &Pool) -> Self where T: Serialize + DeserializeOwned + Send + 'static, I: IntoDistributedStream<Item = T>, <<I as IntoDistributedStream>::Iter as DistributedStream>::Task: Serialize + DeserializeOwned + Send + 'static;
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone, Default)]
#[serde(bound = "")]
pub struct PushReducer<A, T = A>(PhantomData<fn() -> (T, A)>);

impl<A, T: Default + Extend<A>> Reducer<A> for PushReducer<A, T> {
	type Output = T;
	type Async = PushReducerAsync<A, T>;

	fn into_async(self) -> Self::Async {
		PushReducerAsync(Some(Default::default()), PhantomData)
	}
}
impl<A, T: Default + Extend<A>> ReducerProcessSend<A> for PushReducer<A, T>
where
	T: ProcessSend + 'static,
{
	type Output = T;
}
impl<A, T: Default + Extend<A>> ReducerSend<A> for PushReducer<A, T>
where
	T: Send + 'static,
{
	type Output = T;
}

#[pin_project]
pub struct PushReducerAsync<A, T = A>(Option<T>, PhantomData<fn() -> A>);
impl<A, T: Extend<A>> Sink<A> for PushReducerAsync<A, T> {
	#[inline]
	fn poll_pipe(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = A>>,
	) -> Poll<()> {
		let self_ = self.project();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_.0.as_mut().unwrap().extend(iter::once(item));
		}
		Poll::Ready(())
	}
}
impl<A, T: Extend<A>> ReducerAsync<A> for PushReducerAsync<A, T> {
	type Output = T;

	fn output<'a>(mut self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
		Box::pin(async move { self.0.take().unwrap() })
	}
}

#[derive(Educe, Serialize, Deserialize)]
#[educe(Clone, Default)]
#[serde(bound = "")]
pub struct ExtendReducer<A, T = A>(PhantomData<fn() -> (T, A)>);
impl<A: IntoIterator<Item = B>, T: Default + Extend<B>, B> Reducer<A> for ExtendReducer<A, T> {
	type Output = T;
	type Async = ExtendReducerAsync<A, T>;

	fn into_async(self) -> Self::Async {
		ExtendReducerAsync(Some(T::default()), PhantomData)
	}
}
impl<A: IntoIterator<Item = B>, T: Default + Extend<B>, B> ReducerProcessSend<A>
	for ExtendReducer<A, T>
where
	T: ProcessSend + 'static,
{
	type Output = T;
}
impl<A: IntoIterator<Item = B>, T: Default + Extend<B>, B> ReducerSend<A> for ExtendReducer<A, T>
where
	T: Send + 'static,
{
	type Output = T;
}

#[pin_project]
pub struct ExtendReducerAsync<A, T = A>(Option<T>, PhantomData<fn() -> A>);
impl<A: IntoIterator<Item = B>, T: Extend<B>, B> Sink<A> for ExtendReducerAsync<A, T> {
	#[inline]
	fn poll_pipe(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = A>>,
	) -> Poll<()> {
		let self_ = self.project();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_.0.as_mut().unwrap().extend(item);
		}
		Poll::Ready(())
	}
}
impl<A: IntoIterator<Item = B>, T: Extend<B>, B> ReducerAsync<A> for ExtendReducerAsync<A, T> {
	type Output = T;

	fn output<'a>(mut self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
		Box::pin(async move { self.0.take().unwrap() })
	}
}

#[derive(Educe, Serialize, Deserialize)]
#[educe(Clone(bound = "R: Clone"), Default(bound = "R: Default"))]
#[serde(
	bound(serialize = "R: Serialize"),
	bound(deserialize = "R: Deserialize<'de>")
)]
pub struct IntoReducer<R, T>(R, PhantomData<fn() -> T>);

impl<R: Reducer<Item>, T, Item> Reducer<Item> for IntoReducer<R, T>
where
	R::Output: Into<T>,
{
	type Output = T;
	type Async = IntoReducerAsync<R::Async, T>;

	fn into_async(self) -> Self::Async {
		IntoReducerAsync(self.0.into_async(), PhantomData)
	}
}

#[pin_project]
pub struct IntoReducerAsync<R, T>(#[pin] R, PhantomData<fn() -> T>);

impl<R: ReducerAsync<Item>, T, Item> Sink<Item> for IntoReducerAsync<R, T>
where
	R::Output: Into<T>,
{
	#[inline]
	fn poll_pipe(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<()> {
		let stream = stream.map(Into::into);
		pin_mut!(stream);
		self.project().0.poll_pipe(cx, stream)
	}
}
impl<R: ReducerAsync<Item>, T, Item> ReducerAsync<Item> for IntoReducerAsync<R, T>
where
	R::Output: Into<T>,
{
	type Output = T;

	fn output<'a>(self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
		Box::pin(async move { self.project().0.output().await.into() })
	}
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct OptionReducer<R>(R);
impl<R: Reducer<Item>, Item> Reducer<Option<Item>> for OptionReducer<R> {
	type Output = Option<R::Output>;
	type Async = OptionReducerAsync<R::Async>;

	fn into_async(self) -> Self::Async {
		OptionReducerAsync(Some(self.0.into_async()))
	}
}
impl<R: Reducer<Item>, Item> ReducerProcessSend<Option<Item>> for OptionReducer<R>
where
	R::Output: ProcessSend + 'static,
{
	type Output = Option<R::Output>;
}
impl<R: Reducer<Item>, Item> ReducerSend<Option<Item>> for OptionReducer<R>
where
	R::Output: Send + 'static,
{
	type Output = Option<R::Output>;
}

#[pin_project]
pub struct OptionReducerAsync<R>(#[pin] Option<R>);

impl<R: ReducerAsync<Item>, Item> Sink<Option<Item>> for OptionReducerAsync<R> {
	#[inline]
	fn poll_pipe(
		self: Pin<&mut Self>, _cx: &mut Context,
		_stream: Pin<&mut impl Stream<Item = Option<Item>>>,
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
}
impl<R: ReducerAsync<Item>, Item> ReducerAsync<Option<Item>> for OptionReducerAsync<R> {
	type Output = Option<R::Output>;

	fn output<'a>(self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
		Box::pin(async move { Some(self.project().0.as_pin_mut()?.output().await) })
	}
}

#[derive(Educe, Serialize, Deserialize)]
#[educe(Clone(bound = "R: Clone"), Default(bound = "R: Default"))]
#[serde(
	bound(serialize = "R: Serialize"),
	bound(deserialize = "R: Deserialize<'de>")
)]
pub struct ResultReducer<R, E>(R, PhantomData<fn() -> E>);

impl<R: Reducer<Item>, E, Item> Reducer<Result<Item, E>> for ResultReducer<R, E> {
	type Output = Result<R::Output, E>;
	type Async = ResultReducerAsync<R::Async, E>;

	fn into_async(self) -> Self::Async {
		ResultReducerAsync::Ok(self.0.into_async())
	}
}
impl<R: Reducer<Item>, E, Item> ReducerProcessSend<Result<Item, E>> for ResultReducer<R, E>
where
	R::Output: ProcessSend + 'static,
	E: ProcessSend + 'static,
{
	type Output = Result<R::Output, E>;
}
impl<R: Reducer<Item>, E, Item> ReducerSend<Result<Item, E>> for ResultReducer<R, E>
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
impl<R: ReducerAsync<Item>, E, Item> Sink<Result<Item, E>> for ResultReducerAsync<R, E> {
	#[inline]
	fn poll_pipe(
		self: Pin<&mut Self>, _cx: &mut Context,
		_stream: Pin<&mut impl Stream<Item = Result<Item, E>>>,
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
}
impl<R: ReducerAsync<Item>, E, Item> ReducerAsync<Result<Item, E>> for ResultReducerAsync<R, E> {
	type Output = Result<R::Output, E>;

	fn output<'a>(self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
		Box::pin(async move {
			match self.project() {
				ResultReducerAsyncProj::Ok(a) => Ok(a.output().await),
				ResultReducerAsyncProj::Err(b) => Err(b.take().unwrap()),
			}
		})
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
