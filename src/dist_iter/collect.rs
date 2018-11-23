use super::{DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer};
use serde::{de::Deserialize, ser::Serialize};
use std::{
	collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque}, hash::{BuildHasher, Hash}, marker
};

#[must_use]
pub struct Collect<I, A> {
	i: I,
	b: marker::PhantomData<fn() -> A>,
}
impl<I, A> Collect<I, A> {
	pub(super) fn new(i: I) -> Self {
		Self {
			i,
			b: marker::PhantomData,
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
	type ReduceA: Reducer<Item = T>;
	type ReduceB: Reducer<Item = <Self::ReduceA as Reducer>::Output, Output = Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB);
	// 	fn from_dist_iter<I>(dist_iter: I, pool: &Pool) -> Self where T: Serialize + DeserializeOwned + Send + 'static, I: IntoDistributedIterator<Item = T>, <<I as IntoDistributedIterator>::Iter as DistributedIterator>::Task: Serialize + DeserializeOwned + Send + 'static;
}

pub struct DefaultReduceFactory<T>(marker::PhantomData<fn(T)>);
impl<T> Default for DefaultReduceFactory<T> {
	fn default() -> Self {
		DefaultReduceFactory(marker::PhantomData)
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

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "T: Serialize"),
	bound(deserialize = "T: Deserialize<'de>")
)]
pub struct PushReducer<A, T = A>(pub(super) T, pub(super) marker::PhantomData<fn(A)>);
impl<A, T> Default for PushReducer<A, T>
where
	T: Default,
{
	fn default() -> Self {
		PushReducer(T::default(), marker::PhantomData)
	}
}
impl<A, T: Push<A>> Reducer for PushReducer<A, T> {
	type Item = A;
	type Output = T;

	#[inline]
	fn push(&mut self, item: Self::Item) {
		self.0.push(item)
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}

pub struct ExtendReducer<A, T = A>(T, marker::PhantomData<fn(A)>);
impl<A, T> Default for ExtendReducer<A, T>
where
	T: Default,
{
	fn default() -> Self {
		ExtendReducer(T::default(), marker::PhantomData)
	}
}
impl<A: IntoIterator<Item = B>, T: Extend<B>, B> Reducer for ExtendReducer<A, T> {
	type Item = A;
	type Output = T;

	#[inline]
	fn push(&mut self, item: Self::Item) {
		self.0.extend(item)
	}
	fn ret(self) -> Self::Output {
		self.0
	}
}

pub struct IntoReducer<R: Reducer, T>(R, marker::PhantomData<fn(T)>)
where
	R::Output: Into<T>;
impl<R: Reducer, T> Default for IntoReducer<R, T>
where
	R: Default,
	R::Output: Into<T>,
{
	fn default() -> Self {
		IntoReducer(R::default(), marker::PhantomData)
	}
}
impl<R: Reducer, T> Reducer for IntoReducer<R, T>
where
	R::Output: Into<T>,
{
	type Item = R::Item;
	type Output = T;

	#[inline]
	fn push(&mut self, item: Self::Item) {
		self.0.push(item)
	}
	fn ret(self) -> Self::Output {
		self.0.ret().into()
	}
}

pub struct OptionReduceFactory<RF: ReduceFactory>(RF);
impl<RF: ReduceFactory> ReduceFactory for OptionReduceFactory<RF> {
	type Reducer = OptionReducer<RF::Reducer>;

	fn make(&self) -> Self::Reducer {
		OptionReducer(Some(self.0.make()))
	}
}
#[derive(Serialize, Deserialize)]
pub struct OptionReducer<R: Reducer>(Option<R>);
impl<R: Reducer> Default for OptionReducer<R>
where
	R: Default,
{
	fn default() -> Self {
		OptionReducer(Some(R::default()))
	}
}
impl<R: Reducer> Reducer for OptionReducer<R> {
	type Item = Option<R::Item>;
	type Output = Option<R::Output>;

	#[inline]
	fn push(&mut self, item: Self::Item) {
		match (&mut self.0, item.is_some()) {
			(&mut Some(ref mut a), true) => {
				a.push(item.unwrap());
			}
			(self_, _) => *self_ = None,
		}
	}
	fn ret(self) -> Self::Output {
		self.0.map(Reducer::ret)
	}
}

pub struct ResultReduceFactory<RF: ReduceFactory, E>(RF, marker::PhantomData<fn(E)>);
impl<RF: ReduceFactory, E> ReduceFactory for ResultReduceFactory<RF, E> {
	type Reducer = ResultReducer<RF::Reducer, E>;

	fn make(&self) -> Self::Reducer {
		ResultReducer(Ok(self.0.make()))
	}
}
#[derive(Serialize, Deserialize)]
pub struct ResultReducer<R: Reducer, E>(Result<R, E>);
impl<R: Reducer, E> Default for ResultReducer<R, E>
where
	R: Default,
{
	fn default() -> Self {
		ResultReducer(Ok(R::default()))
	}
}
impl<R: Reducer, E> Reducer for ResultReducer<R, E> {
	type Item = Result<R::Item, E>;
	type Output = Result<R::Output, E>;

	#[inline]
	fn push(&mut self, item: Self::Item) {
		match (&mut self.0, item.is_ok()) {
			(&mut Ok(ref mut a), true) => {
				a.push(item.ok().unwrap());
			}
			(self_, false) => *self_ = Err(item.err().unwrap()),
			_ => (),
		}
	}
	fn ret(self) -> Self::Output {
		self.0.map(Reducer::ret)
	}
}

impl<T> FromDistributedIterator<T> for Vec<T> {
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<T, Self>;
	type ReduceB = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<T> FromDistributedIterator<T> for VecDeque<T> {
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<T, Vec<T>>;
	type ReduceB = IntoReducer<ExtendReducer<Vec<T>>, Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<T: Ord> FromDistributedIterator<T> for BinaryHeap<T> {
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<T, Vec<T>>;
	type ReduceB = IntoReducer<ExtendReducer<Vec<T>>, Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<T> FromDistributedIterator<T> for LinkedList<T> {
	type ReduceAFactory = DefaultReduceFactory<Self::ReduceA>;
	type ReduceA = PushReducer<T, Self>;
	type ReduceB = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		Default::default()
	}
}

impl<T, S> FromDistributedIterator<T> for HashSet<T, S>
where
	T: Eq + Hash,
	S: BuildHasher + Default,
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
	K: Eq + Hash,
	S: BuildHasher + Default,
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
	T: Ord,
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
	K: Ord,
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

impl<T, C: FromDistributedIterator<T>, E> FromDistributedIterator<Result<T, E>> for Result<C, E> {
	type ReduceAFactory = ResultReduceFactory<C::ReduceAFactory, E>;
	type ReduceA = ResultReducer<C::ReduceA, E>;
	type ReduceB = ResultReducer<C::ReduceB, E>;

	fn reducers() -> (Self::ReduceAFactory, Self::ReduceB) {
		let (a, b) = C::reducers();
		(
			ResultReduceFactory(a, marker::PhantomData),
			ResultReducer(Ok(b)),
		)
	}
}
