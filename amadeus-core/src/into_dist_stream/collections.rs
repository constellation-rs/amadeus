use owned_chars::{OwnedChars as IntoChars, OwnedCharsExt};
use std::{
	collections::{
		binary_heap, btree_map, btree_set, hash_map, hash_set, linked_list, vec_deque, BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque
	}, hash::{BuildHasher, Hash}, iter, option, result, slice, str, vec
};

use super::{IntoDistributedStream, IterIter};
use crate::pool::ProcessSend;

pub struct TupleCloned<I: Iterator>(I);
impl<'a, 'b, I: Iterator<Item = (&'a A, &'b B)>, A: Clone + 'a, B: Clone + 'b> Iterator
	for TupleCloned<I>
{
	type Item = (A, B);
	fn next(&mut self) -> Option<Self::Item> {
		self.0.next().map(|(a, b)| (a.clone(), b.clone()))
	}
}

impl<T> IntoDistributedStream for Vec<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<vec::IntoIter<T>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedStream for &'a Vec<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<iter::Cloned<slice::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<T> IntoDistributedStream for VecDeque<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<vec_deque::IntoIter<T>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedStream for &'a VecDeque<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<iter::Cloned<vec_deque::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<T: Ord> IntoDistributedStream for BinaryHeap<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<binary_heap::IntoIter<T>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Ord + Clone> IntoDistributedStream for &'a BinaryHeap<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<iter::Cloned<binary_heap::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<T> IntoDistributedStream for LinkedList<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<linked_list::IntoIter<T>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedStream for &'a LinkedList<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<iter::Cloned<linked_list::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<T, S> IntoDistributedStream for HashSet<T, S>
where
	T: Eq + Hash + ProcessSend,
	S: BuildHasher + Default,
{
	type DistStream = IterIter<hash_set::IntoIter<T>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone, S> IntoDistributedStream for &'a HashSet<T, S>
where
	T: Eq + Hash + ProcessSend,
	S: BuildHasher + Default,
{
	type DistStream = IterIter<iter::Cloned<hash_set::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<K, V, S> IntoDistributedStream for HashMap<K, V, S>
where
	K: Eq + Hash + ProcessSend,
	V: ProcessSend,
	S: BuildHasher + Default,
{
	type DistStream = IterIter<hash_map::IntoIter<K, V>>;
	type Item = (K, V);

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, K: Clone, V: Clone, S> IntoDistributedStream for &'a HashMap<K, V, S>
where
	K: Eq + Hash + ProcessSend,
	V: ProcessSend,
	S: BuildHasher + Default,
{
	type DistStream = IterIter<TupleCloned<hash_map::Iter<'a, K, V>>>;
	type Item = (K, V);

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(TupleCloned(self.iter()))
	}
}

impl<T> IntoDistributedStream for BTreeSet<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<btree_set::IntoIter<T>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedStream for &'a BTreeSet<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<iter::Cloned<btree_set::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<K, V> IntoDistributedStream for BTreeMap<K, V>
where
	K: ProcessSend,
	V: ProcessSend,
{
	type DistStream = IterIter<btree_map::IntoIter<K, V>>;
	type Item = (K, V);

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, K: Clone, V: Clone> IntoDistributedStream for &'a BTreeMap<K, V>
where
	K: ProcessSend,
	V: ProcessSend,
{
	type DistStream = IterIter<TupleCloned<btree_map::Iter<'a, K, V>>>;
	type Item = (K, V);

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(TupleCloned(self.iter()))
	}
}

impl IntoDistributedStream for String {
	type DistStream = IterIter<IntoChars>;
	type Item = char;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.into_chars())
	}
}
impl<'a> IntoDistributedStream for &'a String {
	type DistStream = IterIter<str::Chars<'a>>;
	type Item = char;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.chars())
	}
}

impl<T> IntoDistributedStream for Option<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<option::IntoIter<T>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone> IntoDistributedStream for &'a Option<T>
where
	T: ProcessSend,
{
	type DistStream = IterIter<iter::Cloned<option::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}

impl<T, E> IntoDistributedStream for Result<T, E>
where
	T: ProcessSend,
{
	type DistStream = IterIter<result::IntoIter<T>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.into_iter())
	}
}
impl<'a, T: Clone, E> IntoDistributedStream for &'a Result<T, E>
where
	T: ProcessSend,
{
	type DistStream = IterIter<iter::Cloned<result::Iter<'a, T>>>;
	type Item = T;

	fn into_dist_stream(self) -> Self::DistStream
	where
		Self: Sized,
	{
		IterIter(self.iter().cloned())
	}
}
