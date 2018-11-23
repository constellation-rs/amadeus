use super::{IntoDistributedIterator, IterIter};
use std::{
	collections::{
		binary_heap, btree_map, btree_set, hash_map, hash_set, linked_list, vec_deque, BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque
	}, hash::{BuildHasher, Hash}, iter, option, result, slice, str, vec
};

pub struct TupleCloned<I: Iterator>(I);
impl<'a, 'b, I: Iterator<Item = (&'a A, &'b B)>, A: Clone + 'a, B: Clone + 'b> Iterator
	for TupleCloned<I>
{
	type Item = (A, B);
	fn next(&mut self) -> Option<Self::Item> {
		self.0.next().map(|(a, b)| (a.clone(), b.clone()))
	}
}

impl<T> IntoDistributedIterator for Vec<T> {
	type Iter = IterIter<vec::IntoIter<T>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedIterator for &'a Vec<T> {
	type Iter = IterIter<iter::Cloned<slice::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<T> IntoDistributedIterator for VecDeque<T> {
	type Iter = IterIter<vec_deque::IntoIter<T>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedIterator for &'a VecDeque<T> {
	type Iter = IterIter<iter::Cloned<vec_deque::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<T: Ord> IntoDistributedIterator for BinaryHeap<T> {
	type Iter = IterIter<binary_heap::IntoIter<T>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Ord + Clone> IntoDistributedIterator for &'a BinaryHeap<T> {
	type Iter = IterIter<iter::Cloned<binary_heap::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<T> IntoDistributedIterator for LinkedList<T> {
	type Iter = IterIter<linked_list::IntoIter<T>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedIterator for &'a LinkedList<T> {
	type Iter = IterIter<iter::Cloned<linked_list::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<T, S> IntoDistributedIterator for HashSet<T, S>
where
	T: Eq + Hash,
	S: BuildHasher + Default,
{
	type Iter = IterIter<hash_set::IntoIter<T>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone, S> IntoDistributedIterator for &'a HashSet<T, S>
where
	T: Eq + Hash,
	S: BuildHasher + Default,
{
	type Iter = IterIter<iter::Cloned<hash_set::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<K, V, S> IntoDistributedIterator for HashMap<K, V, S>
where
	K: Eq + Hash,
	S: BuildHasher + Default,
{
	type Iter = IterIter<hash_map::IntoIter<K, V>>;
	type Item = (K, V);

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, K: Clone, V: Clone, S> IntoDistributedIterator for &'a HashMap<K, V, S>
where
	K: Eq + Hash,
	S: BuildHasher + Default,
{
	type Iter = IterIter<TupleCloned<hash_map::Iter<'a, K, V>>>;
	type Item = (K, V);

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(TupleCloned(self.iter()))
	}
}

impl<T> IntoDistributedIterator for BTreeSet<T> {
	type Iter = IterIter<btree_set::IntoIter<T>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedIterator for &'a BTreeSet<T> {
	type Iter = IterIter<iter::Cloned<btree_set::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<K, V> IntoDistributedIterator for BTreeMap<K, V> {
	type Iter = IterIter<btree_map::IntoIter<K, V>>;
	type Item = (K, V);

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, K: Clone, V: Clone> IntoDistributedIterator for &'a BTreeMap<K, V> {
	type Iter = IterIter<TupleCloned<btree_map::Iter<'a, K, V>>>;
	type Item = (K, V);

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(TupleCloned(self.iter()))
	}
}

impl IntoDistributedIterator for String {
	type Iter = IterIter<IntoChars>;
	type Item = char;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(IntoChars::new(self))
	}
}
impl<'a> IntoDistributedIterator for &'a String {
	type Iter = IterIter<str::Chars<'a>>;
	type Item = char;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.chars())
	}
}

impl<T> IntoDistributedIterator for Option<T> {
	type Iter = IterIter<option::IntoIter<T>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedIterator for &'a Option<T> {
	type Iter = IterIter<iter::Cloned<option::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<T, E> IntoDistributedIterator for Result<T, E> {
	type Iter = IterIter<result::IntoIter<T>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone, E> IntoDistributedIterator for &'a Result<T, E> {
	type Iter = IterIter<iter::Cloned<result::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_iter(self) -> Self::Iter
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

mod string {
	// Until there is an into_chars() in stdlib
	use std::{mem, str};

	pub struct IntoChars(String, str::Chars<'static>);
	impl IntoChars {
		pub fn new(s: String) -> Self {
			let x = unsafe { mem::transmute::<&str, &str>(&*s) }.chars();
			IntoChars(s, x)
		}
	}
	impl Iterator for IntoChars {
		type Item = char;

		fn next(&mut self) -> Option<char> {
			self.1.next()
		}
	}
}
use self::string::IntoChars;
