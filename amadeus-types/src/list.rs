#![allow(clippy::len_without_is_empty)]

use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};
use std::{
	cmp::Ordering, fmt::{self, Debug}, hash::{Hash, Hasher}, iter::FromIterator, mem::ManuallyDrop, ops::{Deref, DerefMut}, panic::{RefUnwindSafe, UnwindSafe}
};

use super::{util::IteratorExt, AmadeusOrd, Data};
use amadeus_core::{
	par_sink::{ExtendReducer, FromDistributedStream, FromParallelStream, PushReducer}, pool::ProcessSend
};

pub struct List<T: Data> {
	vec: T::Vec,
}
impl<T: Data> List<T> {
	#[inline(always)]
	pub fn new() -> Self {
		Self {
			vec: ListVec::new(),
		}
	}
	#[inline(always)]
	pub fn new_with(type_: T::DynamicType) -> Self {
		let vec = T::new_vec(type_);
		Self { vec }
	}
	#[inline(always)]
	pub fn from_(vec: T::Vec) -> Self {
		Self { vec }
	}
	#[inline(always)]
	pub fn push(&mut self, t: T) {
		self.vec.push(t)
	}
	#[inline(always)]
	pub fn len(&self) -> usize {
		self.vec.len()
	}
	#[inline(always)]
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
	#[inline(always)]
	pub fn into_boxed_slice(self) -> Box<[T]> {
		self.vec.into_vec().into_boxed_slice()
	}
	// #[inline(always)]
	// pub fn iter(&self) -> <&Self as IntoIterator>::IntoIter {
	// 	self.into_iter()
	// }
	#[inline(always)]
	pub fn map<F, U: Data>(self, f: F) -> List<U>
	where
		F: FnMut(T) -> U,
	{
		// TODO
		self.into_iter().map(f).collect()
	}
	#[inline(always)]
	pub fn try_map<F, U: Data, E>(self, f: F) -> Result<List<U>, E>
	where
		F: FnMut(T) -> Result<U, E>,
	{
		// TODO
		self.into_iter().map(f).collect()
	}
}
impl<T: Data> Default for List<T> {
	#[inline(always)]
	fn default() -> Self {
		Self::new()
	}
}
impl<T: Data> From<Vec<T>> for List<T> {
	#[inline(always)]
	fn from(vec: Vec<T>) -> Self {
		let vec = ListVec::from_vec(vec);
		Self { vec }
	}
}
impl<T: Data> From<List<T>> for Vec<T> {
	#[inline(always)]
	fn from(list: List<T>) -> Self {
		list.vec.into_vec()
	}
}
impl<T: Data> From<Box<[T]>> for List<T> {
	#[inline(always)]
	fn from(s: Box<[T]>) -> Self {
		s.into_vec().into()
	}
}
impl<T: Data> IntoIterator for List<T> {
	type Item = T;
	type IntoIter = <<T as Data>::Vec as ListVec<T>>::IntoIter;

	#[inline(always)]
	fn into_iter(self) -> Self::IntoIter {
		<T as Data>::Vec::into_iter_a(self.vec)
	}
}
// impl<'a, T: Data> IntoIterator for &'a List<T> {
// 	type Item = &'a T;
// 	type IntoIter = Box<dyn Iterator<Item = &'a T> + 'a>;

// 	#[inline(always)]
// 	fn into_iter(self) -> Self::IntoIter {
// 		<T as Data>::Vec::iter_a(&self.vec)
// 	}
// }
impl<T: Data> FromIterator<T> for List<T> {
	#[inline(always)]
	fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
		Vec::from_iter(iter).into()
	}
}
impl<T: Data> Extend<T> for List<T> {
	#[inline(always)]
	fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
		for el in iter {
			self.push(el);
		}
	}
}
impl<T: Data> Clone for List<T>
where
	T: Clone,
{
	#[inline(always)]
	fn clone(&self) -> Self {
		Self {
			vec: self.vec.clone_a(),
		}
	}
}
impl<T: Data, U: Data> PartialEq<List<U>> for List<T>
where
	T: PartialEq<U>,
{
	#[inline(always)]
	fn eq(&self, other: &List<U>) -> bool {
		self.clone()
			.into_iter()
			.eq_by_(other.clone().into_iter(), |a, b| a.eq(&b))
	}
}
impl<T: Data> Eq for List<T> where T: Eq {}
impl<T: Data, U: Data> PartialOrd<List<U>> for List<T>
where
	T: PartialOrd<U>,
{
	#[inline(always)]
	fn partial_cmp(&self, other: &List<U>) -> Option<Ordering> {
		self.clone()
			.into_iter()
			.partial_cmp_by_(other.clone().into_iter(), |a, b| a.partial_cmp(&b))
	}
}
impl<T: Data> Ord for List<T>
where
	T: Ord,
{
	#[inline(always)]
	fn cmp(&self, other: &Self) -> Ordering {
		self.clone()
			.into_iter()
			.cmp_by_(other.clone().into_iter(), |a, b| a.cmp(&b))
	}
}
impl<T: Data> AmadeusOrd for List<T>
where
	T: AmadeusOrd,
{
	#[inline(always)]
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		self.clone()
			.into_iter()
			.cmp_by_(other.clone().into_iter(), |a, b| a.amadeus_cmp(&b))
	}
}
impl<T: Data> Hash for List<T>
where
	T: Hash,
{
	#[inline(always)]
	fn hash<H>(&self, state: &mut H)
	where
		H: Hasher,
	{
		self.vec.hash_a(state)
	}
}
impl<T: Data> Serialize for List<T>
where
	T: Serialize,
{
	#[inline(always)]
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.vec.serialize_a(serializer)
	}
}
impl<'de, T: Data> Deserialize<'de> for List<T>
where
	T: Deserialize<'de>,
{
	#[inline(always)]
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		<<T as Data>::Vec>::deserialize_a(deserializer).map(|vec| Self { vec })
	}
}
impl<T: Data> Debug for List<T>
where
	T: Debug,
{
	fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
		self.vec.fmt_a(fmt)
	}
}
impl<T: Data> UnwindSafe for List<T> where T: UnwindSafe {}
impl<T: Data> RefUnwindSafe for List<T> where T: RefUnwindSafe {}
impl<T: Data> Unpin for List<T> where T: Unpin {}
// Implementers of ListVec must be Send/Sync if T is Send/Sync!
#[allow(unsafe_code)]
unsafe impl<T: Data> Send for List<T> where T: Send {}
#[allow(unsafe_code)]
unsafe impl<T: Data> Sync for List<T> where T: Sync {}

impl Deref for List<u8> {
	type Target = [u8];

	#[inline(always)]
	fn deref(&self) -> &Self::Target {
		&*self.vec
	}
}
impl DerefMut for List<u8> {
	#[inline(always)]
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut *self.vec
	}
}

#[derive(PartialEq, Eq, PartialOrd)]
pub struct ValueRef<'a, T>(ManuallyDrop<T>, &'a ());
impl<'a, T> Deref for ValueRef<'a, T> {
	type Target = T;

	#[inline(always)]
	fn deref(&self) -> &Self::Target {
		&*self.0
	}
}

#[doc(hidden)]
pub trait ListVec<T>
where
	T: Data,
{
	type IntoIter: Iterator<Item = T>;

	fn new() -> Self;
	fn push(&mut self, t: T);
	fn pop(&mut self) -> Option<T>;
	fn len(&self) -> usize;
	fn from_vec(vec: Vec<T>) -> Self;
	fn into_vec(self) -> Vec<T>;
	fn into_iter_a(self) -> Self::IntoIter;
	// fn iter_a<'a>(&'a self) -> Box<dyn Iterator<Item = &'a T> + 'a>;
	fn clone_a(&self) -> Self
	where
		T: Clone;
	fn hash_a<H>(&self, state: &mut H)
	where
		H: Hasher,
		T: Hash;
	fn serialize_a<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
		T: Serialize;
	fn deserialize_a<'de, D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
		T: Deserialize<'de>,
		Self: Sized;
	fn fmt_a(&self, fmt: &mut fmt::Formatter) -> fmt::Result
	where
		T: Debug;
}
impl<T: Data> ListVec<T> for Vec<T> {
	type IntoIter = std::vec::IntoIter<T>;

	#[inline(always)]
	fn new() -> Self {
		Self::new()
	}
	#[inline(always)]
	fn push(&mut self, t: T) {
		self.push(t)
	}
	#[inline(always)]
	fn pop(&mut self) -> Option<T> {
		self.pop()
	}
	#[inline(always)]
	fn len(&self) -> usize {
		self.len()
	}
	#[inline(always)]
	fn from_vec(vec: Vec<T>) -> Self {
		vec
	}
	#[inline(always)]
	fn into_vec(self) -> Vec<T> {
		self
	}
	#[inline(always)]
	fn into_iter_a(self) -> Self::IntoIter {
		self.into_iter()
	}
	// #[inline(always)]
	// fn iter_a<'a>(&'a self) -> Box<dyn Iterator<Item = &'a T> + 'a> {
	// 	Box::new(self.iter())
	// }
	#[inline(always)]
	fn clone_a(&self) -> Self
	where
		T: Clone,
	{
		self.clone()
	}
	#[inline(always)]
	fn hash_a<H>(&self, state: &mut H)
	where
		H: Hasher,
		T: Hash,
	{
		self.hash(state)
	}
	#[inline(always)]
	fn serialize_a<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
		T: Serialize,
	{
		self.serialize(serializer)
	}
	#[inline(always)]
	fn deserialize_a<'de, D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
		T: Deserialize<'de>,
		Self: Sized,
	{
		Self::deserialize(deserializer)
	}
	fn fmt_a(&self, fmt: &mut fmt::Formatter) -> fmt::Result
	where
		T: Debug,
	{
		self.fmt(fmt)
	}
}
// #[doc(hidden)]
// pub trait ListVecRef<'a, T: 'a> {
// 	type IntoIter: Iterator<Item = &'a T>;

// 	fn into_iter(self) -> Self::IntoIter;
// }
// impl<'a, T: 'a> ListVecRef<'a, T> for &'a Vec<T> {
// 	type IntoIter = std::slice::Iter<'a, T>;

// 	#[inline(always)]
// 	fn into_iter(self) -> Self::IntoIter {
// 		IntoIterator::into_iter(self)
// 	}
// }

impl<T: Data> FromParallelStream<T> for List<T>
where
	T: Send,
{
	type ReduceA = PushReducer<T, Self>;
	type ReduceC = ExtendReducer<Self>;

	fn reducers() -> (Self::ReduceA, Self::ReduceC) {
		Default::default()
	}
}

impl<T: Data> FromDistributedStream<T> for List<T>
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

#[cfg(test)]
mod test {
	use super::*;
	use crate::Value;

	#[test]
	fn test() {
		let mut list: List<u8> = List::new();
		list.push(0_u8);
		list.push(1_u8);
		println!("{:#?}", list);
		let mut list: List<Value> = List::new();
		list.push(Value::U8(0));
		list.push(Value::U8(1));
		println!("{:#?}", list);
		let mut list: List<Value> = List::new_with(crate::value::ValueType::U8);
		list.push(Value::U8(0));
		list.push(Value::U8(1));
		println!("{:#?}", list);
		let mut list2: List<List<Value>> = List::new();
		list2.push(list.clone());
		list2.push(list);
		println!("{:#?}", list2);
		// let mut list2: List<List<Value>> = List::new(type_coerce(Vec::<List<Value>>::new()));//List::<Value>::new(ValueVec::U8(List::new(type_coerce(Vec::<u8>::new()))))));
		// list2.push(list.clone());
		// list2.push(list.clone());
		// println!("{:#?}", list);
	}
}
