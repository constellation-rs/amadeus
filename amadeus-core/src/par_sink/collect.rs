use derive_new::new;
use educe::Educe;
use futures::{pin_mut, ready, Stream, StreamExt};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque}, hash::{BuildHasher, Hash}, iter, marker::PhantomData, pin::Pin, task::{Context, Poll}
};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, Reducer, ReducerProcessSend, ReducerSend
};
use crate::{pipe::Sink, pool::ProcessSend};

#[derive(new)]
#[must_use]
pub struct Collect<P, A> {
	pipe: P,
	marker: PhantomData<fn() -> A>,
}

impl<P: ParallelPipe<Item>, Item, T: FromParallelStream<P::Output>> ParallelSink<Item>
	for Collect<P, T>
{
	type Done = T;
	type Pipe = P;
	type ReduceA = T::ReduceA;
	type ReduceC = T::ReduceC;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
		let (a, b) = T::reducers();
		(self.pipe, a, b)
	}
}
impl<P: DistributedPipe<Item>, Item, T: FromDistributedStream<P::Output>> DistributedSink<Item>
	for Collect<P, T>
{
	type Done = T;
	type Pipe = P;
	type ReduceA = T::ReduceA;
	type ReduceB = T::ReduceB;
	type ReduceC = T::ReduceC;

	fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
		let (a, b, c) = T::reducers();
		(self.pipe, a, b, c)
	}
}

pub trait FromParallelStream<T>: Sized {
	type ReduceA: ReducerSend<T> + Clone + Send;
	type ReduceC: Reducer<<Self::ReduceA as ReducerSend<T>>::Done, Done = Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC);
}

pub trait FromDistributedStream<T>: Sized {
	type ReduceA: ReducerSend<T> + Clone + ProcessSend;
	type ReduceB: ReducerProcessSend<<Self::ReduceA as ReducerSend<T>>::Done> + Clone + ProcessSend;
	type ReduceC: Reducer<
		<Self::ReduceB as ReducerProcessSend<<Self::ReduceA as ReducerSend<T>>::Done>>::Done,
		Done = Self,
	>;

	fn reducers() -> (Self::ReduceA, Self::ReduceB, Self::ReduceC);
	// 	fn from_dist_stream<P>(dist_stream: P, pool: &Pool) -> Self where T: Serialize + DeserializeOwned + Send, P: IntoDistributedStream<Item = T>, <<P as IntoDistributedStream>::Iter as DistributedStream>::Task: Serialize + DeserializeOwned + Send;
}

#[derive(Educe, Serialize, Deserialize, new)]
#[educe(Clone, Default)]
#[serde(bound = "")]
pub struct PushReducer<Item, T = Item>(PhantomData<fn() -> (T, Item)>);

impl<Item, T: Default + Extend<Item>> Reducer<Item> for PushReducer<Item, T> {
	type Done = T;
	type Async = PushReducerAsync<Item, T>;

	fn into_async(self) -> Self::Async {
		PushReducerAsync(Some(Default::default()), PhantomData)
	}
}
impl<Item, T: Default + Extend<Item>> ReducerProcessSend<Item> for PushReducer<Item, T>
where
	T: ProcessSend,
{
	type Done = T;
}
impl<Item, T: Default + Extend<Item>> ReducerSend<Item> for PushReducer<Item, T>
where
	T: Send,
{
	type Done = T;
}

#[pin_project]
pub struct PushReducerAsync<Item, T = Item>(Option<T>, PhantomData<fn() -> Item>);
impl<Item, T: Extend<Item>> Sink<Item> for PushReducerAsync<Item, T> {
	type Done = T;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<Self::Done> {
		let self_ = self.project();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_.0.as_mut().unwrap().extend(iter::once(item));
		}
		Poll::Ready(self_.0.take().unwrap())
	}
}

#[derive(Educe, Serialize, Deserialize)]
#[educe(Clone, Default)]
#[serde(bound = "")]
pub struct ExtendReducer<Item, T = Item>(PhantomData<fn() -> (T, Item)>);
impl<Item: IntoIterator<Item = B>, T: Default + Extend<B>, B> Reducer<Item>
	for ExtendReducer<Item, T>
{
	type Done = T;
	type Async = ExtendReducerAsync<Item, T>;

	fn into_async(self) -> Self::Async {
		ExtendReducerAsync(Some(T::default()), PhantomData)
	}
}
impl<Item: IntoIterator<Item = B>, T: Default + Extend<B>, B> ReducerProcessSend<Item>
	for ExtendReducer<Item, T>
where
	T: ProcessSend,
{
	type Done = T;
}
impl<Item: IntoIterator<Item = B>, T: Default + Extend<B>, B> ReducerSend<Item>
	for ExtendReducer<Item, T>
where
	T: Send,
{
	type Done = T;
}

#[pin_project]
pub struct ExtendReducerAsync<Item, T = Item>(Option<T>, PhantomData<fn() -> Item>);
impl<Item: IntoIterator<Item = B>, T: Extend<B>, B> Sink<Item> for ExtendReducerAsync<Item, T> {
	type Done = T;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<Self::Done> {
		let self_ = self.project();
		while let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
			self_.0.as_mut().unwrap().extend(item);
		}
		Poll::Ready(self_.0.take().unwrap())
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
	R::Done: Into<T>,
{
	type Done = T;
	type Async = IntoReducerAsync<R::Async, T>;

	fn into_async(self) -> Self::Async {
		IntoReducerAsync(self.0.into_async(), PhantomData)
	}
}

#[pin_project]
pub struct IntoReducerAsync<R, T>(#[pin] R, PhantomData<fn() -> T>);

impl<R: Sink<Item>, T, Item> Sink<Item> for IntoReducerAsync<R, T>
where
	R::Done: Into<T>,
{
	type Done = T;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
	) -> Poll<Self::Done> {
		let stream = stream.map(Into::into);
		pin_mut!(stream);
		self.project().0.poll_forward(cx, stream).map(Into::into)
	}
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct OptionReducer<R>(R);
impl<R: Reducer<Item>, Item> Reducer<Option<Item>> for OptionReducer<R> {
	type Done = Option<R::Done>;
	type Async = OptionReducerAsync<R::Async>;

	fn into_async(self) -> Self::Async {
		OptionReducerAsync(Some(self.0.into_async()))
	}
}
impl<R: Reducer<Item>, Item> ReducerProcessSend<Option<Item>> for OptionReducer<R>
where
	R::Done: ProcessSend,
{
	type Done = Option<R::Done>;
}
impl<R: Reducer<Item>, Item> ReducerSend<Option<Item>> for OptionReducer<R>
where
	R::Done: Send,
{
	type Done = Option<R::Done>;
}

#[pin_project]
pub struct OptionReducerAsync<R>(#[pin] Option<R>);

impl<R: Sink<Item>, Item> Sink<Option<Item>> for OptionReducerAsync<R> {
	type Done = Option<R::Done>;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Option<Item>>>,
	) -> Poll<Self::Done> {
		let stream = stream.map(|x|x.expect("Not yet implemented: Tracking at https://github.com/constellation-rs/amadeus/projects/3#card-40276549"));
		pin_mut!(stream);
		match self.project().0.as_pin_mut() {
			Some(a) => a.poll_forward(cx, stream).map(Some),
			None => Poll::Ready(None),
		}
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
	type Done = Result<R::Done, E>;
	type Async = ResultReducerAsync<R::Async, E>;

	fn into_async(self) -> Self::Async {
		ResultReducerAsync::Ok(self.0.into_async())
	}
}
impl<R: Reducer<Item>, E, Item> ReducerProcessSend<Result<Item, E>> for ResultReducer<R, E>
where
	R::Done: ProcessSend,
	E: ProcessSend,
{
	type Done = Result<R::Done, E>;
}
impl<R: Reducer<Item>, E, Item> ReducerSend<Result<Item, E>> for ResultReducer<R, E>
where
	R::Done: Send,
	E: Send,
{
	type Done = Result<R::Done, E>;
}

#[pin_project(project = ResultReducerAsyncProj)]
pub enum ResultReducerAsync<R, E> {
	Ok(#[pin] R),
	Err(Option<E>),
}
impl<R: Sink<Item>, E, Item> Sink<Result<Item, E>> for ResultReducerAsync<R, E> {
	type Done = Result<R::Done, E>;

	#[inline]
	fn poll_forward(
		self: Pin<&mut Self>, cx: &mut Context,
		stream: Pin<&mut impl Stream<Item = Result<Item, E>>>,
	) -> Poll<Self::Done> {
		let stream = stream.map(|x|x.ok().expect("Not yet implemented: Tracking at https://github.com/constellation-rs/amadeus/projects/3#card-40276549"));
		pin_mut!(stream);
		match self.project() {
			ResultReducerAsyncProj::Ok(a) => a.poll_forward(cx, stream).map(Ok),
			ResultReducerAsyncProj::Err(b) => Poll::Ready(Err(b.take().unwrap())),
		}
	}
}

impl<T> FromParallelStream<T> for Vec<T>
where
	T: Send,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T> FromParallelStream<T> for VecDeque<T>
where
	T: Send,
{
	type ReduceA = PushReducer<T, Vec<T>>;
	type ReduceC = IntoReducer<ExtendReducer<Vec<T>>, Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T: Ord> FromParallelStream<T> for BinaryHeap<T>
where
	T: Send,
{
	type ReduceA = PushReducer<T, Vec<T>>;
	type ReduceC = IntoReducer<ExtendReducer<Vec<T>>, Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T> FromParallelStream<T> for LinkedList<T>
where
	T: Send,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T, S> FromParallelStream<T> for HashSet<T, S>
where
	T: Eq + Hash + Send,
	S: BuildHasher + Default + Send,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<K, V, S> FromParallelStream<(K, V)> for HashMap<K, V, S>
where
	K: Eq + Hash + Send,
	V: Send,
	S: BuildHasher + Default + Send,
{
	type ReduceA = PushReducer<(K, V), Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T> FromParallelStream<T> for BTreeSet<T>
where
	T: Ord + Send,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<K, V> FromParallelStream<(K, V)> for BTreeMap<K, V>
where
	K: Ord + Send,
	V: Send,
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
	E: Send,
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
	T: ProcessSend,
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
	T: ProcessSend,
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
	T: ProcessSend,
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
	T: ProcessSend,
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
	T: Eq + Hash + ProcessSend,
	S: BuildHasher + Default + Send,
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
	K: Eq + Hash + ProcessSend,
	V: ProcessSend,
	S: BuildHasher + Default + Send,
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
	T: Ord + ProcessSend,
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
	K: Ord + ProcessSend,
	V: ProcessSend,
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
	E: ProcessSend,
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
