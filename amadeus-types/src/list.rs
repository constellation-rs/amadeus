use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};
use std::{
	cmp::Ordering, fmt::{self, Debug}, hash::{Hash, Hasher}, iter::FromIterator, mem::ManuallyDrop, ops::{Deref, DerefMut}, panic::{RefUnwindSafe, UnwindSafe}, ptr
};

use super::{AmadeusOrd, Value};
use amadeus_core::util::type_coerce;

macro_rules! duck {
	( $trait:ident $duck_trait:ident<$($l:lifetime),*;$($p:ident),*> { $(fn $fn:ident $duck_fn:ident<$($t:ident),*>($($args:tt)*) -> $ret:ty where ($($where:tt)*) [$($arg_names:ident),*]; )* } ) => {
		trait $duck_trait<$($l,)*$($p,)*> {
			$(
				fn $duck_fn<$($t,)*>($($args)*) -> $ret where $($where)*;
			)*
		}
		impl<$($l,)*$($p,)* T: ?Sized> $duck_trait<$($l,)*$($p,)*> for T {
			$(
				#[inline(always)]
				default fn $duck_fn<$($t,)*>($($args)*) -> $ret where $($where)* {
					let _ = ($($arg_names,)*);
					unreachable!()
				}
			)*
		}
		impl<$($l,)*$($p,)* T: ?Sized> $duck_trait<$($l,)*$($p,)*> for T
		where
			T: $trait<$($l,)*$($p,)*>,
		{
			$(
				#[inline(always)]
				fn $duck_fn<$($t,)*>($($args)*) -> $ret where $($where)* {
					$trait::$fn($($arg_names,)*)
				}
			)*
		}
	}
}

duck! {
	Clone CloneDuck<;> {
		fn clone clone_duck<>(&self) -> Self where (Self: Sized) [self];
	}
}
// duck! {
// 	PartialEq PartialEqDuck<;Rhs> {
// 		fn eq eq_duck<>(&self, other: &Rhs) -> bool where () [self, other];
// 	}
// }
// duck! {
// 	PartialOrd PartialOrdDuck<;Rhs> {
// 		fn partial_cmp partial_cmp_duck<>(&self, other: &Rhs) -> Option<Ordering> where () [self, other];
// 	}
// }
// duck! {
// 	Ord OrdDuck<;> {
// 		fn cmp cmp_duck<>(&self, other: &Self) -> Ordering where () [self, other];
// 	}
// }
// duck! {
// 	AmadeusOrd AmadeusOrdDuck<;> {
// 		fn amadeus_cmp amadeus_cmp_duck<>(&self, other: &Self) -> Ordering where () [self, other];
// 	}
// }
duck! {
	Hash HashDuck<;> {
		fn hash hash_duck<H>(&self, state: &mut H) -> () where (H: Hasher) [self, state];
	}
}
duck! {
	Serialize SerializeDuck<;> {
		fn serialize serialize_duck<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where (S: Serializer) [self, serializer];
	}
}
duck! {
	Deserialize DeserializeDuck<'de;> {
		fn deserialize deserialize_duck<D>(deserializer: D) -> Result<Self, D::Error> where (D: Deserializer<'de>, Self: Sized) [deserializer];
	}
}
duck! {
	Debug DebugDuck<;> {
		fn fmt fmt_duck<>(&self, fmt: &mut fmt::Formatter) -> fmt::Result where () [self, fmt];
	}
}

pub struct List<T> {
	vec: <T as ListItem>::Vec,
}
impl<T> List<T> {
	#[inline(always)]
	pub fn new() -> Self {
		Self {
			vec: ListVec::new(),
		}
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
	#[inline(always)]
	pub fn iter(&self) -> <&'_ Self as IntoIterator>::IntoIter {
		self.into_iter()
	}
}
impl<T> Default for List<T> {
	#[inline(always)]
	fn default() -> Self {
		Self::new()
	}
}
impl List<Value> {
	#[doc(hidden)]
	#[inline(always)]
	pub fn new_with(type_: ValueType) -> Self {
		let vec = match type_ {
			ValueType::U8 => ValueVec::U8(ListVec::new()),
			ValueType::U16 => ValueVec::U16(ListVec::new()),
			ValueType::List => ValueVec::List(ListVec::new()),
		};
		Self { vec }
	}
}
impl<T> From<Vec<T>> for List<T> {
	#[inline(always)]
	fn from(vec: Vec<T>) -> Self {
		let vec = ListVec::from_vec(vec);
		Self { vec }
	}
}
impl From<Vec<u8>> for List<Value> {
	#[inline(always)]
	fn from(vec: Vec<u8>) -> Self {
		let vec = ValueVec::U8(vec);
		Self { vec }
	}
}
impl<T> From<List<T>> for Vec<T> {
	#[inline(always)]
	fn from(list: List<T>) -> Self {
		list.vec.into_vec()
	}
}
impl<T> From<Box<[T]>> for List<T> {
	#[inline(always)]
	fn from(s: Box<[T]>) -> Self {
		s.into_vec().into()
	}
}
impl<T> IntoIterator for List<T> {
	type Item = T;
	type IntoIter = <<T as ListItem>::Vec as ListVec<T>>::IntoIter;

	#[inline(always)]
	fn into_iter(self) -> Self::IntoIter {
		<T as ListItem>::Vec::into_iter(self.vec)
	}
}
impl<'a, T> IntoIterator for &'a List<T> {
	// type Item = <<&'a <T as ListItem>::Vec as ListVecRef<'a,T>>::IntoIter as Iterator>::Item;
	// type IntoIter = <&'a <T as ListItem>::Vec as ListVecRef<'a,T>>::IntoIter;
	type Item = ValueRef<'a, T>;
	type IntoIter = std::vec::IntoIter<ValueRef<'a, T>>;

	#[inline(always)]
	fn into_iter(self) -> Self::IntoIter {
		self.vec.iter()
	}
}
impl<T> FromIterator<T> for List<T> {
	#[inline(always)]
	fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
		Vec::from_iter(iter).into()
	}
}
impl FromIterator<u8> for List<Value> {
	#[inline(always)]
	fn from_iter<I: IntoIterator<Item = u8>>(iter: I) -> Self {
		Vec::from_iter(iter).into()
	}
}
impl<T> Extend<T> for List<T> {
	#[inline(always)]
	fn extend<I: IntoIterator<Item = T>>(&mut self, _iter: I) {
		unimplemented!()
	}
}
impl Extend<u8> for List<Value> {
	#[inline(always)]
	fn extend<I: IntoIterator<Item = u8>>(&mut self, _iter: I) {
		unimplemented!()
	}
}
impl<T> Clone for List<T>
where
	T: Clone,
{
	#[inline(always)]
	fn clone(&self) -> Self {
		Self {
			vec: self.vec.clone_duck(),
		}
	}
}
impl<T, U> PartialEq<List<U>> for List<T>
where
	T: PartialEq<U>,
{
	#[inline(always)]
	fn eq(&self, other: &List<U>) -> bool {
		self.iter().eq_by(other.iter(), |a, b| a.eq(&b))
	}
}
impl<T> Eq for List<T> where T: Eq {}
impl<T, U> PartialOrd<List<U>> for List<T>
where
	T: PartialOrd<U>,
{
	#[inline(always)]
	fn partial_cmp(&self, other: &List<U>) -> Option<Ordering> {
		self.iter()
			.partial_cmp_by(other.iter(), |a, b| a.partial_cmp(&b))
	}
}
impl<T> Ord for List<T>
where
	T: Ord,
{
	#[inline(always)]
	fn cmp(&self, other: &Self) -> Ordering {
		self.iter().cmp_by(other.iter(), |a, b| a.cmp(&b))
	}
}
impl<T> AmadeusOrd for List<T>
where
	T: AmadeusOrd,
{
	#[inline(always)]
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		self.iter().cmp_by(other.iter(), |a, b| a.amadeus_cmp(&b))
	}
}
impl<T> Hash for List<T>
where
	T: Hash,
{
	#[inline(always)]
	fn hash<H>(&self, state: &mut H)
	where
		H: Hasher,
	{
		self.vec.hash_duck(state)
	}
}
impl<T> Serialize for List<T>
where
	T: Serialize,
{
	#[inline(always)]
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.vec.serialize_duck(serializer)
	}
}
impl<'de, T> Deserialize<'de> for List<T>
where
	T: Deserialize<'de>,
{
	#[inline(always)]
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		DeserializeDuck::deserialize_duck(deserializer)
	}
}
impl<T> Debug for List<T>
where
	T: Debug,
{
	#[inline(always)]
	fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
		self.vec.fmt_duck(fmt)
	}
}
impl<T> UnwindSafe for List<T> where T: UnwindSafe {}
impl<T> RefUnwindSafe for List<T> where T: RefUnwindSafe {}
impl<T> Unpin for List<T> where T: Unpin {}
unsafe impl<T> Send for List<T> where T: Send {}
unsafe impl<T> Sync for List<T> where T: Sync {}

impl Deref for List<u8> {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		&*type_coerce::<_, &Vec<u8>>(&self.vec)
	}
}
impl DerefMut for List<u8> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut *type_coerce::<_, &mut Vec<u8>>(&mut self.vec)
	}
}

#[doc(hidden)]
pub trait ListItem: Sized {
	type Vec: ListVec<Self>; // + ListVecRefHack<Self>;
}
impl<T> ListItem for T {
	default type Vec = Vec<T>;
}

// trait ListVecRefHack<T> where for<'a> &'a Self: ListVecRef<'a,T> {
// }
// impl<T,U> ListVecRefHack<U> for T where for<'a> &'a Self: ListVecRef<'a,T> {}

#[doc(hidden)]
pub trait ListVec<T> {
	//where for<'a> &'a Self: ListVecRef<'a,T> {
	type IntoIter: Iterator<Item = T>;

	fn new() -> Self;
	fn push(&mut self, t: T);
	fn len(&self) -> usize;
	fn from_vec(vec: Vec<T>) -> Self;
	fn into_vec(self) -> Vec<T>;
	fn into_iter(self) -> Self::IntoIter;
	fn iter(&self) -> std::vec::IntoIter<ValueRef<'_, T>>;
}
impl<T> ListVec<T> for Vec<T> {
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
	fn into_iter(self) -> Self::IntoIter {
		IntoIterator::into_iter(self)
	}
	#[inline(always)]
	fn iter(&self) -> std::vec::IntoIter<ValueRef<'_, T>> {
		IntoIterator::into_iter(
			<[T]>::iter(self)
				.map(|next| ValueRef(ManuallyDrop::new(unsafe { ptr::read(next) }), &()))
				.collect::<Vec<_>>(),
		)
	}
}
#[doc(hidden)]
pub trait ListVecRef<'a, T: 'a> {
	type IntoIter: Iterator<Item = &'a T>;

	fn into_iter(self) -> Self::IntoIter;
}
impl<'a, T: 'a> ListVecRef<'a, T> for &'a Vec<T> {
	type IntoIter = std::slice::Iter<'a, T>;

	#[inline(always)]
	fn into_iter(self) -> Self::IntoIter {
		IntoIterator::into_iter(self)
	}
}

// #[derive(Clone, Debug)]
// enum Value {
// 	U8(u8),
// 	U16(u16),
// 	List(Box<List<Value>>),
// }
// impl Value {
// 	fn u8(self) -> u8 {
// 		if let Self::U8(ret) = self {
// 			ret
// 		} else {
// 			panic!()
// 		}
// 	}
// 	fn u16(self) -> u16 {
// 		if let Self::U16(ret) = self {
// 			ret
// 		} else {
// 			panic!()
// 		}
// 	}
// 	fn list(self) -> List<Value> {
// 		if let Self::List(ret) = self {
// 			*ret
// 		} else {
// 			panic!()
// 		}
// 	}
// }
#[doc(hidden)]
#[derive(Clone, Debug)]
pub enum ValueType {
	U8,
	U16,
	List,
}
#[doc(hidden)]
#[derive(Clone, Hash, Serialize, Deserialize)]
pub enum ValueVec {
	U8(Vec<u8>),
	U16(Vec<u16>),
	List(Vec<List<Value>>),
	Value(Vec<Value>),
}
impl Debug for ValueVec {
	fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::U8(self_) => fmt.debug_tuple("U8").field(self_).finish(),
			Self::U16(self_) => fmt.debug_tuple("U16").field(self_).finish(),
			Self::List(self_) => fmt.debug_tuple("List").field(self_).finish(),
			Self::Value(self_) => self_.fmt(fmt),
		}
	}
}
impl ListItem for Value {
	type Vec = ValueVec;
}
impl ListVec<Value> for ValueVec {
	type IntoIter = std::vec::IntoIter<Value>;

	#[inline(always)]
	fn new() -> Self {
		Self::Value(vec![])
	}
	#[inline(always)]
	fn push(&mut self, t: Value) {
		match self {
			ValueVec::U8(list) => list.push(t.into_u8().unwrap()),
			ValueVec::U16(list) => list.push(t.into_u16().unwrap()),
			ValueVec::List(list) => list.push(t.into_list().unwrap()),
			ValueVec::Value(list) => list.push(t),
		}
	}
	#[inline(always)]
	fn len(&self) -> usize {
		match self {
			ValueVec::U8(list) => list.len(),
			ValueVec::U16(list) => list.len(),
			ValueVec::List(list) => list.len(),
			ValueVec::Value(list) => list.len(),
		}
	}
	#[inline(always)]
	fn from_vec(vec: Vec<Value>) -> Self {
		Self::Value(vec)
	}
	#[inline(always)]
	fn into_vec(self) -> Vec<Value> {
		match self {
			Self::U8(vec) => IntoIterator::into_iter(vec).map(Into::into).collect(),
			Self::U16(vec) => IntoIterator::into_iter(vec).map(Into::into).collect(),
			Self::List(vec) => IntoIterator::into_iter(vec).map(Into::into).collect(),
			Self::Value(vec) => vec,
		}
	}
	#[inline(always)]
	fn into_iter(self) -> Self::IntoIter {
		// TODO
		IntoIterator::into_iter(self.into_vec())
	}
	#[inline(always)]
	fn iter(&self) -> std::vec::IntoIter<ValueRef<'_, Value>> {
		match self {
			Self::U8(vec) => IntoIterator::into_iter(Iter(<[_]>::iter(vec)).collect::<Vec<_>>()),
			Self::U16(vec) => IntoIterator::into_iter(Iter(<[_]>::iter(vec)).collect::<Vec<_>>()),
			Self::List(vec) => IntoIterator::into_iter(Iter(<[_]>::iter(vec)).collect::<Vec<_>>()),
			Self::Value(vec) => IntoIterator::into_iter(Iter(<[_]>::iter(vec)).collect::<Vec<_>>()),
		}
	}
}
struct Iter<'a, T>(std::slice::Iter<'a, T>);
impl<'a> Iterator for Iter<'a, u8> {
	type Item = ValueRef<'a, Value>;
	#[inline(always)]
	fn next(&mut self) -> Option<Self::Item> {
		self.0
			.next()
			.map(|next| ValueRef(ManuallyDrop::new(Value::U8(*next)), &()))
	}
}
impl<'a> Iterator for Iter<'a, u16> {
	type Item = ValueRef<'a, Value>;
	#[inline(always)]
	fn next(&mut self) -> Option<Self::Item> {
		self.0
			.next()
			.map(|next| ValueRef(ManuallyDrop::new(Value::U16(*next)), &()))
	}
}
impl<'a> Iterator for Iter<'a, List<Value>> {
	type Item = ValueRef<'a, Value>;
	#[inline(always)]
	fn next(&mut self) -> Option<Self::Item> {
		self.0.next().map(|next| {
			ValueRef(
				ManuallyDrop::new(Value::List(Box::new(unsafe { ptr::read(next) }))),
				&(),
			)
		})
	}
}
impl<'a> Iterator for Iter<'a, Value> {
	type Item = ValueRef<'a, Value>;
	#[inline(always)]
	fn next(&mut self) -> Option<Self::Item> {
		self.0
			.next()
			.map(|next| ValueRef(ManuallyDrop::new(unsafe { ptr::read(next) }), &()))
	}
}

#[derive(PartialEq, Eq, PartialOrd)]
pub struct ValueRef<'a, T>(ManuallyDrop<T>, &'a ());
impl<'a, T> Deref for ValueRef<'a, T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		&*self.0
	}
}
// impl<'a,T> Drop for ValueRef<'a,T> {
// 	fn drop(&mut self) {
// 		if let Some(self_) = try_type_coerce::<&mut ValueRef<'a,T>,&mut ValueRef<'a,Value>>(self) {
// 			match unsafe { ManuallyDrop::take(&mut self_.0) } {
// 				Value::Bool(x) => assert_copy(x),
// 				Value::U8(x) => assert_copy(x),
// 				Value::I8(x) => assert_copy(x),
// 				Value::U16(x) => assert_copy(x),
// 				Value::I16(x) => assert_copy(x),
// 				Value::U32(x) => assert_copy(x),
// 				Value::I32(x) => assert_copy(x),
// 				Value::U64(x) => assert_copy(x),
// 				Value::I64(x) => assert_copy(x),
// 				Value::F32(x) => assert_copy(x),
// 				Value::F64(x) => assert_copy(x),
// 				Value::Date(x) => assert_copy(x),
// 				Value::DateWithoutTimezone(x) => assert_copy(x),
// 				Value::Time(x) => assert_copy(x),
// 				Value::TimeWithoutTimezone(x) => assert_copy(x),
// 				Value::DateTime(x) => assert_copy(x),
// 				Value::DateTimeWithoutTimezone(x) => assert_copy(x),
// 				Value::Timezone(x) => assert_copy(x),
// 				Value::Decimal(x) => forget(x),
// 				Value::Bson(x) => forget(x),
// 				Value::String(x) => forget(x),
// 				Value::Json(x) => forget(x),
// 				Value::Enum(x) => forget(x),
// 				Value::Url(x) => forget(x),
// 				Value::Webpage(x) => forget(x),
// 				Value::IpAddr(x) => assert_copy(x),
// 				Value::List(x) => forget(*x),
// 				Value::Map(x) => forget(x),
// 				Value::Group(x) => forget(x),
// 				Value::Option(x) => forget(x),
// 			}
// 		}
// 	}
// }

// fn assert_copy<T: Copy>(_t: T) {}

impl<'a, T> Serialize for ValueRef<'a, T>
where
	T: Serialize,
{
	#[inline(always)]
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		(**self).serialize(serializer)
	}
}
impl<'a, T> AmadeusOrd for ValueRef<'a, T>
where
	T: AmadeusOrd,
{
	fn amadeus_cmp(&self, _other: &Self) -> Ordering {
		unimplemented!()
	}
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn test() {
		let mut list: List<u8> = List::new();
		list.push(0u8);
		list.push(1u8);
		println!("{:#?}", list);
		let mut list: List<Value> = List::new();
		list.push(Value::U8(0));
		list.push(Value::U8(1));
		println!("{:#?}", list);
		let mut list: List<Value> = List::new_with(ValueType::U8);
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
