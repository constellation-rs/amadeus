use ordered_linked_list::{OrderedLinkedList, OrderedLinkedListIndex, OrderedLinkedListIter};
use std::{
	cmp, collections::{hash_map::Entry, HashMap}, fmt::{self, Debug}, hash::Hash, iter, ops
};
use twox_hash::RandomXxHashBuilder;

#[derive(Clone, Serialize, Deserialize)]
struct Node<T, C>(T, C);
impl<T, C: Ord> Ord for Node<T, C> {
	#[inline(always)]
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		self.1.cmp(&other.1)
	}
}
impl<T, C: PartialOrd> PartialOrd for Node<T, C> {
	#[inline(always)]
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		self.1.partial_cmp(&other.1)
	}
}
impl<T, C: PartialEq> PartialEq for Node<T, C> {
	#[inline(always)]
	fn eq(&self, other: &Self) -> bool {
		self.1.eq(&other.1)
	}
}
impl<T, C: Eq> Eq for Node<T, C> {}

/// This data structure tracks the `n` most frequently seen elements in a stream, using only `O(n)` space. It is approximate.
///
/// The formulation used is described [here](https://stackoverflow.com/questions/37851407/on-heavy-hitters-with-o1-epsilon-space/37851947#37851947).
///
/// ```text
/// while an item i from the input stream arrives:
///     if H[i] exists
///         increment the value associated with H[i]
///     elsif number of items in H < k
///         put H[i] into map with value of base + 1
///     elseif there exists an entry j with a value of base
///         remove j and put H[i] into map with value of base + 1
///     else
///         increment base
/// endwhile
/// ```
///
/// See [*A Simple Algorithm for Finding Frequent Elements in Streams and Bags*](http://www.cs.umd.edu/%7Esamir/498/karp.pdf) for more information.
#[derive(Clone, Serialize, Deserialize)]
pub struct MostFrequent<T: Hash + Eq + Clone> {
	map: HashMap<T, OrderedLinkedListIndex<'static>, RandomXxHashBuilder>,
	list: OrderedLinkedList<Node<T, usize>>,
	base: usize,
}
impl<T: Hash + Eq + Clone> MostFrequent<T> {
	/// Create an empty `MostFrequent` data structure with the specified capacity.
	pub fn new(n: usize) -> Self {
		Self {
			map: HashMap::with_capacity_and_hasher(n, RandomXxHashBuilder::default()),
			list: OrderedLinkedList::new(n),
			base: 0,
		}
	}
	fn assert(&self) {
		if !cfg!(feature = "assert") {
			return;
		}
		for (k, &v) in &self.map {
			assert!(&self.list[v].0 == k);
		}
		let mut cur = self.list[self.list.head().unwrap()].1;
		for &Node(_, count) in self.list.iter() {
			assert!(cur >= count);
			cur = count;
		}
		assert!(cur >= self.base);
	}
	/// The `n` most frequent elements we have capacity to track.
	pub fn capacity(&self) -> usize {
		self.list.capacity()
	}
	/// "Visit" an element.
	pub fn push(&mut self, item: T) {
		let entry = self.map.entry(item.clone());
		match entry {
			Entry::Occupied(entry) => {
				let offset = *entry.get();
				self.list
					.mutate(offset, |Node(t, count)| Node(t, count + 1));
			}
			Entry::Vacant(entry) => {
				if self.list.len() < self.list.capacity() {
					let new = self.list.push_back(Node(item, self.base + 1));
					let new = unsafe { new.staticify() };
					let _ = entry.insert(new);
				} else if self.list[self.list.tail().unwrap()].1 == self.base {
					let old = self.list.pop_back().0;
					let new = self.list.push_back(Node(item, self.base + 1));
					let new = unsafe { new.staticify() };
					let _ = entry.insert(new);
					let _ = self.map.remove(&old).unwrap();
				} else {
					self.base += 1;
				}
			}
		}
		self.assert();
	}
	/// An iterator visiting all elements and their counts in descending order of frequency. The iterator element type is (&'a T, usize).
	pub fn iter(&self) -> MostFrequentIter<'_, T> {
		MostFrequentIter {
			list_iter: self.list.iter(),
		}
	}
}
impl<'a, T: Hash + Eq + Clone + Debug> Debug for MostFrequent<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_list().entries(self.iter()).finish()
	}
}

/// An iterator over the entries and counts in a [`MostFrequent`] datastructure.
///
/// This struct is created by the [`iter`](MostFrequent::iter()) method on [`MostFrequent`]. See its documentation for more.
pub struct MostFrequentIter<'a, T: Hash + Eq + Clone + 'a> {
	list_iter: OrderedLinkedListIter<'a, Node<T, usize>>,
}
impl<'a, T: Hash + Eq + Clone> Clone for MostFrequentIter<'a, T> {
	fn clone(&self) -> Self {
		Self {
			list_iter: self.list_iter.clone(),
		}
	}
}
impl<'a, T: Hash + Eq + Clone> Iterator for MostFrequentIter<'a, T> {
	type Item = (&'a T, usize);
	fn next(&mut self) -> Option<(&'a T, usize)> {
		self.list_iter.next().map(|x| (&x.0, x.1))
	}
}
impl<'a, T: Hash + Eq + Clone + Debug> Debug for MostFrequentIter<'a, T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_list().entries(self.clone()).finish()
	}
}
// impl<T: Hash + Eq + Clone> iter::Sum for MostFrequent<T> {
// 	fn sum<I>(iter: I) -> Self
// 	where
// 		I: Iterator<Item = Self>,
// 	{
// 		let mut scores = HashMap::new();
// 		for top in iter {
// 			for (url, count) in top.iter() {
// 				*scores.entry(url.clone()).or_insert(0) += count;
// 			}
// 		}
// 		let mut top = Self::new(10_000);
// 		for (url, count) in scores {
// 			for _ in 0..count {
// 				top.push(url.clone());
// 			}
// 		}
// 		top
// 	}
// }

impl<T: Hash + Eq + Clone> iter::Sum for MostFrequent<T> {
	fn sum<I>(iter: I) -> Self
	where
		I: Iterator<Item = Self>,
	{
		let mut total = Self::new(0); // TODO
		for sample in iter {
			total += sample;
		}
		total
	}
}
impl<T: Hash + Eq + Clone> ops::Add for MostFrequent<T> {
	type Output = Self;

	fn add(mut self, other: Self) -> Self {
		self += other;
		self
	}
}
impl<T: Hash + Eq + Clone> ops::AddAssign for MostFrequent<T> {
	fn add_assign(&mut self, other: Self) {
		if self.capacity() > 0 {
			// TODO
			assert_eq!(self.capacity(), other.capacity());
			let mut scores = HashMap::new();
			for (url, count) in self.iter() {
				*scores.entry(url.clone()).or_insert(0) += count;
			}
			for (url, count) in other.iter() {
				*scores.entry(url.clone()).or_insert(0) += count;
			}
			let mut top = Self::new(self.capacity());
			for (url, count) in scores {
				for _ in 0..count {
					top.push(url.clone()); // TODO
				}
			}
			*self = top;
		// unimplemented!()
		// assert_eq!(self.reservoir.capacity(), other.reservoir.capacity());
		// let mut new = Vec::with_capacity(self.reservoir.capacity());
		// let (m, n) = (self.i, other.i);
		// let mut rng = ::rand::thread_rng(); // TODO
		// for _ in 0..new.capacity() {
		// 	if rng.gen_range(0, m + n) < m {
		// 		new.push(self.reservoir.pop().unwrap());
		// 	} else {
		// 		new.push(other.reservoir.pop().unwrap());
		// 	}
		// }
		// self.reservoir = new;
		// self.i += other.i;
		} else {
			*self = other;
		}
	}
}
