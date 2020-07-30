use owned_chars::{OwnedChars as IntoChars, OwnedCharsExt};
use std::{
	collections::{
		binary_heap, btree_map, btree_set, hash_map, hash_set, linked_list, vec_deque, BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque
	}, hash::{BuildHasher, Hash}, option, result, slice, vec
};

use super::{IntoDistributedStream, IntoParallelStream, IterDistStream, IterParStream};
use crate::pool::ProcessSend;

impl<'a, T> IntoParallelStream for &'a Vec<T>
where
	T: Sync,
{
	type ParStream = IterParStream<slice::Iter<'a, T>>;
	type Item = &'a T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter())
	}
}
impl<'a, T> IntoParallelStream for &'a mut Vec<T>
where
	T: Send,
{
	type ParStream = IterParStream<slice::IterMut<'a, T>>;
	type Item = &'a mut T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter_mut())
	}
}

impl<'a, T> IntoParallelStream for &'a VecDeque<T>
where
	T: Sync,
{
	type ParStream = IterParStream<vec_deque::Iter<'a, T>>;
	type Item = &'a T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter())
	}
}
impl<'a, T> IntoParallelStream for &'a mut VecDeque<T>
where
	T: Send,
{
	type ParStream = IterParStream<vec_deque::IterMut<'a, T>>;
	type Item = &'a mut T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter_mut())
	}
}

impl<'a, T: Ord> IntoParallelStream for &'a BinaryHeap<T>
where
	T: Sync,
{
	type ParStream = IterParStream<binary_heap::Iter<'a, T>>;
	type Item = &'a T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter())
	}
}

impl<'a, T> IntoParallelStream for &'a LinkedList<T>
where
	T: Sync,
{
	type ParStream = IterParStream<linked_list::Iter<'a, T>>;
	type Item = &'a T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter())
	}
}
impl<'a, T> IntoParallelStream for &'a mut LinkedList<T>
where
	T: Send,
{
	type ParStream = IterParStream<linked_list::IterMut<'a, T>>;
	type Item = &'a mut T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter_mut())
	}
}

impl<'a, T, S> IntoParallelStream for &'a HashSet<T, S>
where
	T: Eq + Hash + Sync,
	S: BuildHasher + Default,
{
	type ParStream = IterParStream<hash_set::Iter<'a, T>>;
	type Item = &'a T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter())
	}
}

impl<'a, K, V, S> IntoParallelStream for &'a HashMap<K, V, S>
where
	K: Eq + Hash + Sync,
	V: Sync,
	S: BuildHasher + Default,
{
	type ParStream = IterParStream<hash_map::Iter<'a, K, V>>;
	type Item = (&'a K, &'a V);

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter())
	}
}
impl<'a, K, V, S> IntoParallelStream for &'a mut HashMap<K, V, S>
where
	K: Eq + Hash + Sync,
	V: Send,
	S: BuildHasher + Default,
{
	type ParStream = IterParStream<hash_map::IterMut<'a, K, V>>;
	type Item = (&'a K, &'a mut V);

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter_mut())
	}
}

impl<'a, T> IntoParallelStream for &'a BTreeSet<T>
where
	T: Sync,
{
	type ParStream = IterParStream<btree_set::Iter<'a, T>>;
	type Item = &'a T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter())
	}
}

impl<'a, K, V> IntoParallelStream for &'a BTreeMap<K, V>
where
	K: Sync,
	V: Sync,
{
	type ParStream = IterParStream<btree_map::Iter<'a, K, V>>;
	type Item = (&'a K, &'a V);

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter())
	}
}
impl<'a, K, V> IntoParallelStream for &'a mut BTreeMap<K, V>
where
	K: Sync,
	V: Send,
{
	type ParStream = IterParStream<btree_map::IterMut<'a, K, V>>;
	type Item = (&'a K, &'a mut V);

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter_mut())
	}
}

impl<'a, T> IntoParallelStream for &'a Option<T>
where
	T: Sync,
{
	type ParStream = IterParStream<option::Iter<'a, T>>;
	type Item = &'a T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter())
	}
}
impl<'a, T> IntoParallelStream for &'a mut Option<T>
where
	T: Send,
{
	type ParStream = IterParStream<option::IterMut<'a, T>>;
	type Item = &'a mut T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter_mut())
	}
}

impl<'a, T, E> IntoParallelStream for &'a Result<T, E>
where
	T: Sync,
{
	type ParStream = IterParStream<result::Iter<'a, T>>;
	type Item = &'a T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter())
	}
}
impl<'a, T, E> IntoParallelStream for &'a mut Result<T, E>
where
	T: Send,
{
	type ParStream = IterParStream<result::IterMut<'a, T>>;
	type Item = &'a mut T;

	#[inline(always)]
	fn into_par_stream(self) -> Self::ParStream
	where
		Self: Sized,
	{
		IterParStream(self.iter_mut())
	}
}

impl_par_dist_rename! {
	impl<T> IntoParallelStream for Vec<T>
	where
		T: Send,
	{
		type ParStream = IterParStream<vec::IntoIter<T>>;
		type Item = T;

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}

	impl<T> IntoParallelStream for VecDeque<T>
	where
		T: Send,
	{
		type ParStream = IterParStream<vec_deque::IntoIter<T>>;
		type Item = T;

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}

	impl<T: Ord> IntoParallelStream for BinaryHeap<T>
	where
		T: Send,
	{
		type ParStream = IterParStream<binary_heap::IntoIter<T>>;
		type Item = T;

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}

	impl<T> IntoParallelStream for LinkedList<T>
	where
		T: Send,
	{
		type ParStream = IterParStream<linked_list::IntoIter<T>>;
		type Item = T;

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}

	impl<T, S> IntoParallelStream for HashSet<T, S>
	where
		T: Eq + Hash + Send,
		S: BuildHasher + Default,
	{
		type ParStream = IterParStream<hash_set::IntoIter<T>>;
		type Item = T;

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}

	impl<K, V, S> IntoParallelStream for HashMap<K, V, S>
	where
		K: Eq + Hash + Send,
		V: Send,
		S: BuildHasher + Default,
	{
		type ParStream = IterParStream<hash_map::IntoIter<K, V>>;
		type Item = (K, V);

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}

	impl<T> IntoParallelStream for BTreeSet<T>
	where
		T: Send,
	{
		type ParStream = IterParStream<btree_set::IntoIter<T>>;
		type Item = T;

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}

	impl<K, V> IntoParallelStream for BTreeMap<K, V>
	where
		K: Send,
		V: Send,
	{
		type ParStream = IterParStream<btree_map::IntoIter<K, V>>;
		type Item = (K, V);

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}

	impl IntoParallelStream for String {
		type ParStream = IterParStream<IntoChars>;
		type Item = char;

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_chars())
		}
	}

	impl<T> IntoParallelStream for Option<T>
	where
		T: Send,
	{
		type ParStream = IterParStream<option::IntoIter<T>>;
		type Item = T;

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}

	impl<T, E> IntoParallelStream for Result<T, E>
	where
		T: Send,
	{
		type ParStream = IterParStream<result::IntoIter<T>>;
		type Item = T;

		#[inline(always)]
		fn into_par_stream(self) -> Self::ParStream
		where
			Self: Sized,
		{
			IterParStream(self.into_iter())
		}
	}
}
