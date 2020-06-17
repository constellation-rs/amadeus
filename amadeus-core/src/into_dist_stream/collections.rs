use owned_chars::{OwnedChars as IntoChars, OwnedCharsExt};
use std::{
	collections::{
		binary_heap, btree_map, btree_set, hash_map, hash_set, linked_list, vec_deque, BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque
	}, hash::{BuildHasher, Hash}, iter, option, result, slice, str, vec
};

use super::{IntoDistributedStream, IntoParallelStream, IterDistStream, IterParStream};
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

impl_par_dist_rename! {
	impl<T> IntoParallelStream for Vec<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<vec::IntoIter<T>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}
	impl<'a, T: Clone> IntoParallelStream for &'a Vec<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<iter::Cloned<slice::Iter<'a, T>>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.iter().cloned())
		}
	}

	impl<T> IntoParallelStream for VecDeque<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<vec_deque::IntoIter<T>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}
	impl<'a, T: Clone> IntoParallelStream for &'a VecDeque<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<iter::Cloned<vec_deque::Iter<'a, T>>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.iter().cloned())
		}
	}

	impl<T: Ord> IntoParallelStream for BinaryHeap<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<binary_heap::IntoIter<T>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}
	impl<'a, T: Ord + Clone> IntoParallelStream for &'a BinaryHeap<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<iter::Cloned<binary_heap::Iter<'a, T>>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.iter().cloned())
		}
	}

	impl<T> IntoParallelStream for LinkedList<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<linked_list::IntoIter<T>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}
	impl<'a, T: Clone> IntoParallelStream for &'a LinkedList<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<iter::Cloned<linked_list::Iter<'a, T>>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.iter().cloned())
		}
	}

	impl<T, S> IntoParallelStream for HashSet<T, S>
	where
		T: Eq + Hash + ProcessSend,
		S: BuildHasher + Default,
	{
		type ParStream = IterParStream<hash_set::IntoIter<T>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}
	impl<'a, T: Clone, S> IntoParallelStream for &'a HashSet<T, S>
	where
		T: Eq + Hash + ProcessSend,
		S: BuildHasher + Default,
	{
		type ParStream = IterParStream<iter::Cloned<hash_set::Iter<'a, T>>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.iter().cloned())
		}
	}

	impl<K, V, S> IntoParallelStream for HashMap<K, V, S>
	where
		K: Eq + Hash + ProcessSend,
		V: ProcessSend,
		S: BuildHasher + Default,
	{
		type ParStream = IterParStream<hash_map::IntoIter<K, V>>;
		type Item = (K, V);

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}
	impl<'a, K: Clone, V: Clone, S> IntoParallelStream for &'a HashMap<K, V, S>
	where
		K: Eq + Hash + ProcessSend,
		V: ProcessSend,
		S: BuildHasher + Default,
	{
		type ParStream = IterParStream<TupleCloned<hash_map::Iter<'a, K, V>>>;
		type Item = (K, V);

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(TupleCloned(self.iter()))
		}
	}

	impl<T> IntoParallelStream for BTreeSet<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<btree_set::IntoIter<T>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}
	impl<'a, T: Clone> IntoParallelStream for &'a BTreeSet<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<iter::Cloned<btree_set::Iter<'a, T>>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.iter().cloned())
		}
	}

	impl<K, V> IntoParallelStream for BTreeMap<K, V>
	where
		K: ProcessSend,
		V: ProcessSend,
	{
		type ParStream = IterParStream<btree_map::IntoIter<K, V>>;
		type Item = (K, V);

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}
	impl<'a, K: Clone, V: Clone> IntoParallelStream for &'a BTreeMap<K, V>
	where
		K: ProcessSend,
		V: ProcessSend,
	{
		type ParStream = IterParStream<TupleCloned<btree_map::Iter<'a, K, V>>>;
		type Item = (K, V);

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(TupleCloned(self.iter()))
		}
	}

	impl IntoParallelStream for String {
		type ParStream = IterParStream<IntoChars>;
		type Item = char;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_chars())
		}
	}
	impl<'a> IntoParallelStream for &'a String {
		type ParStream = IterParStream<str::Chars<'a>>;
		type Item = char;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.chars())
		}
	}

	impl<T> IntoParallelStream for Option<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<option::IntoIter<T>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}
	impl<'a, T: Clone> IntoParallelStream for &'a Option<T>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<iter::Cloned<option::Iter<'a, T>>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.iter().cloned())
		}
	}

	impl<T, E> IntoParallelStream for Result<T, E>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<result::IntoIter<T>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}
	impl<'a, T: Clone, E> IntoParallelStream for &'a Result<T, E>
	where
		T: ProcessSend,
	{
		type ParStream = IterParStream<iter::Cloned<result::Iter<'a, T>>>;
		type Item = T;

		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.iter().cloned())
		}
	}
}
