use super::{DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, ReducerA};
use either::Either;
use replace_with::replace_with_or_abort;
use serde::{de::Deserialize, ser::Serialize};
use std::marker::PhantomData;

#[must_use]
pub struct Fold<I, ID, F, B> {
	i: I,
	identity: ID,
	op: F,
	marker: PhantomData<fn() -> B>,
}
impl<I, ID, F, B> Fold<I, ID, F, B> {
	pub(super) fn new(i: I, identity: ID, op: F) -> Self {
		Self {
			i,
			identity,
			op,
			marker: PhantomData,
		}
	}
}

impl<I: DistributedIteratorMulti<Source>, Source, ID, F, B> DistributedReducer<I, Source, B>
	for Fold<I, ID, F, B>
where
	ID: FnMut() -> B + Clone + Serialize + for<'de> Deserialize<'de> + 'static,
	F: FnMut(B, Either<I::Item, B>) -> B + Clone + Serialize + for<'de> Deserialize<'de> + 'static,
	B: Serialize + for<'de> Deserialize<'de> + Send + 'static,
	I::Item: 'static,
{
	type ReduceAFactory = FoldReducerFactory<I::Item, ID, F, B>;
	type ReduceA = FoldReducerA<I::Item, ID, F, B>;
	type ReduceB = FoldReducerB<I::Item, ID, F, B>;

	fn reducers(self) -> (I, Self::ReduceAFactory, Self::ReduceB) {
		(
			self.i,
			FoldReducerFactory(self.identity.clone(), self.op.clone(), PhantomData),
			FoldReducerB(Either::Left(self.identity), self.op, PhantomData),
		)
	}
}

pub struct FoldReducerFactory<A, ID, F, B>(ID, F, PhantomData<fn(A, B)>);

impl<A, ID, F, B> ReduceFactory for FoldReducerFactory<A, ID, F, B>
where
	ID: FnMut() -> B + Clone,
	F: FnMut(B, Either<A, B>) -> B + Clone,
{
	type Reducer = FoldReducerA<A, ID, F, B>;
	fn make(&self) -> Self::Reducer {
		FoldReducerA(Either::Left(self.0.clone()), self.1.clone(), PhantomData)
	}
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "ID: Serialize, B: Serialize, F: Serialize"),
	bound(deserialize = "ID: Deserialize<'de>, B: Deserialize<'de>, F: Deserialize<'de>")
)]
pub struct FoldReducerA<A, ID, F, B>(Either<ID, B>, F, PhantomData<fn(A)>);

impl<A, ID, F, B> Reducer for FoldReducerA<A, ID, F, B>
where
	ID: FnMut() -> B + Clone,
	F: FnMut(B, Either<A, B>) -> B + Clone,
{
	type Item = A;
	type Output = B;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		replace_with_or_abort(&mut self.0, |self_0| {
			Either::Right(self_0.map_left(|mut identity| identity()).into_inner())
		});
		let self_1 = &mut self.1;
		replace_with_or_abort(&mut self.0, |self_0| {
			Either::Right((self_1)(self_0.right().unwrap(), Either::Left(item)))
		});
		true
	}
	fn ret(self) -> Self::Output {
		self.0.map_left(|mut identity| identity()).into_inner()
	}
}
impl<A, ID, F, B> ReducerA for FoldReducerA<A, ID, F, B>
where
	A: 'static,
	ID: FnMut() -> B + Clone + Serialize + for<'de> Deserialize<'de> + 'static,
	F: FnMut(B, Either<A, B>) -> B + Clone + Serialize + for<'de> Deserialize<'de> + 'static,
	B: Serialize + for<'de> Deserialize<'de> + Send + 'static,
{
	type Output = B;
}

#[derive(Serialize, Deserialize)]
#[serde(
	bound(serialize = "ID: Serialize, B: Serialize, F: Serialize"),
	bound(deserialize = "ID: Deserialize<'de>, B: Deserialize<'de>, F: Deserialize<'de>")
)]
pub struct FoldReducerB<A, ID, F, B>(Either<ID, B>, F, PhantomData<fn(A)>);

impl<A, ID, F, B> Clone for FoldReducerB<A, ID, F, B>
where
	ID: Clone,
	F: Clone,
{
	fn clone(&self) -> Self {
		FoldReducerB(
			Either::Left(self.0.as_ref().left().unwrap().clone()),
			self.1.clone(),
			PhantomData,
		)
	}
}

impl<A, ID, F, B> Reducer for FoldReducerB<A, ID, F, B>
where
	ID: FnMut() -> B + Clone,
	F: FnMut(B, Either<A, B>) -> B + Clone,
{
	type Item = B;
	type Output = B;

	#[inline(always)]
	fn push(&mut self, item: Self::Item) -> bool {
		replace_with_or_abort(&mut self.0, |self_0| {
			Either::Right(self_0.map_left(|mut identity| identity()).into_inner())
		});
		let self_1 = &mut self.1;
		replace_with_or_abort(&mut self.0, |self_0| {
			Either::Right((self_1)(self_0.right().unwrap(), Either::Right(item)))
		});
		true
	}
	fn ret(self) -> Self::Output {
		self.0.map_left(|mut identity| identity()).into_inner()
	}
}
