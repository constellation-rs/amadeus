use serde::{Deserialize, Serialize};
use std::{
	cmp, collections::{hash_map::Entry, HashMap}, fmt::{self, Debug}, hash::Hash, iter, ops
};
use twox_hash::RandomXxHashBuilder;

use crate::{
	count_min::CountMinSketch, ordered_linked_list::{OrderedLinkedList, OrderedLinkedListIndex, OrderedLinkedListIter}, traits::{Intersect, New, UnionAssign}, IntersectPlusUnionIsPlus
};

/// This probabilistic data structure tracks the `n` top keys given a stream of `(key,value)` tuples, ordered by the sum of the values for each key (the "aggregated value"). It uses only `O(n)` space.
///
/// Its implementation is two parts:
///
/// * a doubly linked hashmap, mapping the top `n` keys to their aggregated values, and ordered by their aggregated values. This is used to keep a more precise track of the aggregated value of the top `n` keys, and reduce collisions in the count-min sketch.
/// * a [count-min sketch](https://en.wikipedia.org/wiki/Count–min_sketch) to track all of the keys outside the top `n`. This data structure is also known as a [counting Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter#Counting_filters). It uses *conservative updating* for increased accuracy.
///
/// The algorithm is as follows:
///
/// ```text
/// while a key and value from the input stream arrive:
///     if H[key] exists
///         increment aggregated value associated with H[key]
///     elsif number of items in H < k
///         put H[key] into map with its associated value
///     else
///         add C[key] into the count-min sketch with its associated value
///         if aggregated value associated with C[key] is > the lowest aggregated value in H
///             move the lowest key and value from H into C
///             move C[key] and value from C into H
/// endwhile
/// ```
///
/// See [*An Improved Data Stream Summary: The Count-Min Sketch and its Applications*](http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf) and [*New Directions in Traffic Measurement and Accounting*](http://pages.cs.wisc.edu/~suman/courses/740/papers/estan03tocs.pdf) for background on the count-min sketch with conservative updating.
#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(
	serialize = "A: Hash + Eq + Serialize, C: Serialize, <C as New>::Config: Serialize",
	deserialize = "A: Hash + Eq + Deserialize<'de>, C: Deserialize<'de>, <C as New>::Config: Deserialize<'de>"
))]
pub struct Top<A, C: New> {
	map: HashMap<A, OrderedLinkedListIndex<'static>, RandomXxHashBuilder>,
	list: OrderedLinkedList<Node<A, C>>,
	count_min: CountMinSketch<A, C>,
	config: <C as New>::Config,
}
impl<A: Hash + Eq + Clone, C: Ord + New + for<'a> UnionAssign<&'a C> + Intersect> Top<A, C> {
	/// Create an empty `Top` data structure with the specified `n` capacity.
	pub fn new(n: usize, probability: f64, tolerance: f64, config: <C as New>::Config) -> Self {
		Self {
			map: HashMap::with_capacity_and_hasher(n, RandomXxHashBuilder::default()),
			list: OrderedLinkedList::new(n),
			count_min: CountMinSketch::new(probability, tolerance, config.clone()),
			config,
		}
	}
	fn assert(&self) {
		if !cfg!(feature = "assert") {
			return;
		}
		for (k, &v) in &self.map {
			assert!(&self.list[v].0 == k);
		}
		let mut cur = &self.list[self.list.head().unwrap()].1;
		for &Node(_, ref count) in self.list.iter() {
			assert!(cur >= count);
			cur = count;
		}
	}
	/// The `n` most frequent elements we have capacity to track.
	pub fn capacity(&self) -> usize {
		self.list.capacity()
	}
	/// "Visit" an element.
	pub fn push<V: ?Sized>(&mut self, item: A, value: &V)
	where
		C: for<'a> ops::AddAssign<&'a V> + IntersectPlusUnionIsPlus,
	{
		match self.map.entry(item.clone()) {
			Entry::Occupied(entry) => {
				let offset = *entry.get();
				self.list.mutate(offset, |Node(t, mut count)| {
					count += value;
					Node(t, count)
				});
			}
			Entry::Vacant(entry) => {
				if self.list.len() < self.list.capacity() {
					let mut x = C::new(&self.config);
					x += value;
					let new = self.list.push_back(Node(item, x));
					let new = unsafe { new.staticify() };
					let _ = entry.insert(new);
				} else {
					let score = self.count_min.push(&item, value);
					if score > self.list[self.list.tail().unwrap()].1 {
						let old = self.list.pop_back();
						let new = self.list.push_back(Node(item, score));
						let new = unsafe { new.staticify() };
						let _ = entry.insert(new);
						let _ = self.map.remove(&old.0).unwrap();
						self.count_min.union_assign(&old.0, &old.1);
					}
				}
			}
		}
		self.assert();
	}
	/// Clears the `Top` data structure, as if it was new.
	pub fn clear(&mut self) {
		self.map.clear();
		self.list.clear();
		self.count_min.clear();
	}
	/// An iterator visiting all elements and their counts in descending order of frequency. The iterator element type is (&'a A, usize).
	pub fn iter(&self) -> TopIter<'_, A, C> {
		TopIter {
			list_iter: self.list.iter(),
		}
	}
}
impl<
		A: Hash + Eq + Clone + Debug,
		C: Ord + New + Clone + for<'a> UnionAssign<&'a C> + Intersect + Debug,
	> Debug for Top<A, C>
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_list().entries(self.iter()).finish()
	}
}

/// An iterator over the entries and counts in a [`Top`] datastructure.
///
/// This struct is created by the [`iter`](Top::iter()) method on [`Top`]. See its documentation for more.
pub struct TopIter<'a, A: Hash + Eq + Clone + 'a, C: Ord + 'a> {
	list_iter: OrderedLinkedListIter<'a, Node<A, C>>,
}
impl<'a, A: Hash + Eq + Clone, C: Ord + 'a> Clone for TopIter<'a, A, C> {
	fn clone(&self) -> Self {
		Self {
			list_iter: self.list_iter.clone(),
		}
	}
}
impl<'a, A: Hash + Eq + Clone, C: Ord + 'a> Iterator for TopIter<'a, A, C> {
	type Item = (&'a A, &'a C);
	fn next(&mut self) -> Option<(&'a A, &'a C)> {
		self.list_iter.next().map(|x| (&x.0, &x.1))
	}
}
impl<'a, A: Hash + Eq + Clone + Debug, C: Ord + Debug + 'a> Debug for TopIter<'a, A, C> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_list().entries(self.clone()).finish()
	}
}

impl<
		A: Hash + Eq + Clone,
		C: Ord
			+ New
			+ Clone
			+ for<'a> ops::AddAssign<&'a C>
			+ for<'a> UnionAssign<&'a C>
			+ Intersect
			+ IntersectPlusUnionIsPlus,
	> iter::Sum<Top<A, C>> for Option<Top<A, C>>
{
	fn sum<I>(mut iter: I) -> Self
	where
		I: Iterator<Item = Top<A, C>>,
	{
		let mut total = iter.next()?;
		for sample in iter {
			total += sample;
		}
		Some(total)
	}
}
impl<
		A: Hash + Eq + Clone,
		C: Ord
			+ New
			+ Clone
			+ for<'a> ops::AddAssign<&'a C>
			+ for<'a> UnionAssign<&'a C>
			+ Intersect
			+ IntersectPlusUnionIsPlus,
	> ops::Add for Top<A, C>
{
	type Output = Self;
	fn add(mut self, other: Self) -> Self {
		self += other;
		self
	}
}
impl<
		A: Hash + Eq + Clone,
		C: Ord
			+ New
			+ Clone
			+ for<'a> ops::AddAssign<&'a C>
			+ for<'a> UnionAssign<&'a C>
			+ Intersect
			+ IntersectPlusUnionIsPlus,
	> ops::AddAssign for Top<A, C>
{
	fn add_assign(&mut self, other: Self) {
		assert_eq!(self.capacity(), other.capacity());

		let mut scores = HashMap::<_, C>::new();
		for (url, count) in self.iter() {
			*scores
				.entry(url.clone())
				.or_insert_with(|| C::new(&self.config)) += count;
		}
		for (url, count) in other.iter() {
			*scores
				.entry(url.clone())
				.or_insert_with(|| C::new(&self.config)) += count;
		}
		let mut top = self.clone();
		top.clear();
		for (url, count) in scores {
			top.push(url.clone(), &count);
		}
		*self = top;
	}
}

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

#[cfg(test)]
mod test {
	use super::*;
	use crate::{distinct::HyperLogLog, traits::IntersectPlusUnionIsPlus};
	use rand::{self, Rng, SeedableRng};
	use std::time;

	#[test]
	#[cfg_attr(miri, ignore)]
	fn abc() {
		let mut rng =
			rand::rngs::SmallRng::from_seed([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
		let mut top = Top::<String, usize>::new(100, 0.99, 2.0 / 1000.0, ());
		let mut x = HashMap::new();
		for _ in 0..10_000 {
			let (a, b) = (rng.gen_range(0, 2) == 0, rng.gen_range(0, 2) == 0);
			let c = rng.gen_range(0, 50);
			let record = match (a, b) {
				(true, _) => format!("a{}", c),
				(false, true) => format!("b{}", c),
				(false, false) => format!("c{}", c),
			};
			top.push(record.clone(), &1);
			*x.entry(record).or_insert(0) += 1;
		}
		println!("{:#?}", top);
		let mut x = x.into_iter().collect::<Vec<_>>();
		x.sort_by_key(|x| cmp::Reverse(x.1));
		println!("{:#?}", x);
	}

	#[derive(Serialize, Deserialize)]
	#[serde(bound = "")]
	struct Hll<V>(HyperLogLog<V>);
	impl<V: Hash> Ord for Hll<V> {
		#[inline(always)]
		fn cmp(&self, other: &Self) -> cmp::Ordering {
			self.0.len().partial_cmp(&other.0.len()).unwrap()
		}
	}
	impl<V: Hash> PartialOrd for Hll<V> {
		#[inline(always)]
		fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
			self.0.len().partial_cmp(&other.0.len())
		}
	}
	impl<V: Hash> PartialEq for Hll<V> {
		#[inline(always)]
		fn eq(&self, other: &Self) -> bool {
			self.0.len().eq(&other.0.len())
		}
	}
	impl<V: Hash> Eq for Hll<V> {}
	impl<V: Hash> Clone for Hll<V> {
		fn clone(&self) -> Self {
			Self(self.0.clone())
		}
	}
	impl<V: Hash> New for Hll<V> {
		type Config = f64;
		fn new(config: &Self::Config) -> Self {
			Self(New::new(config))
		}
	}
	impl<V: Hash> Intersect for Hll<V> {
		fn intersect<'a>(iter: impl Iterator<Item = &'a Self>) -> Option<Self>
		where
			Self: Sized + 'a,
		{
			Intersect::intersect(iter.map(|x| &x.0)).map(Self)
		}
	}
	impl<'a, V: Hash> UnionAssign<&'a Hll<V>> for Hll<V> {
		fn union_assign(&mut self, rhs: &'a Self) {
			self.0.union_assign(&rhs.0)
		}
	}
	impl<'a, V: Hash> ops::AddAssign<&'a V> for Hll<V> {
		fn add_assign(&mut self, rhs: &'a V) {
			self.0.add_assign(rhs)
		}
	}
	impl<V: Hash> Debug for Hll<V> {
		fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
			self.0.fmt(fmt)
		}
	}
	impl<V> IntersectPlusUnionIsPlus for Hll<V> {
		const VAL: bool = <HyperLogLog<V> as IntersectPlusUnionIsPlus>::VAL;
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn top_hll() {
		let mut rng =
			rand::rngs::SmallRng::from_seed([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
		let mut top = Top::<String, Hll<String>>::new(1000, 0.99, 2.0 / 1000.0, 0.00408);
		// let mut x = HashMap::new();
		for _ in 0..5_000 {
			let (a, b) = (rng.gen_range(0, 2) == 0, rng.gen_range(0, 2) == 0);
			let c = rng.gen_range(0, 800);
			let record = match (a, b) {
				(true, _) => (format!("a{}", c), format!("{}", rng.gen_range(0, 500))),
				(false, true) => (format!("b{}", c), format!("{}", rng.gen_range(0, 200))),
				(false, false) => (format!("c{}", c), format!("{}", rng.gen_range(0, 200))),
			};
			// *x.entry(record.0)
			// 	.or_insert(HashMap::new())
			// 	.entry(record.1)
			// 	.or_insert(0) += 1;
			top.push(record.0, &record.1);
		}
		println!("{:#?}", top);
		// let mut x = x.into_iter().collect::<Vec<_>>();
		// x.sort_by_key(|x|cmp::Reverse(x.1));
		// println!("{:#?}", x);
	}

	#[ignore] // takes too long on CI
	#[test]
	fn many() {
		let start = time::Instant::now();

		let mut rng =
			rand::rngs::SmallRng::from_seed([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
		let mut top = Top::<String, Hll<String>>::new(1000, 0.99, 2.0 / 1000.0, 0.05);
		// let mut x = HashMap::new();
		for _ in 0..5_000_000 {
			let (a, b) = (rng.gen_range(0, 2) == 0, rng.gen_range(0, 2) == 0);
			let c = rng.gen_range(0, 800);
			let record = match (a, b) {
				(true, _) => (format!("a{}", c), format!("{}", rng.gen_range(0, 500))),
				(false, true) => (format!("b{}", c), format!("{}", rng.gen_range(0, 200))),
				(false, false) => (format!("c{}", c), format!("{}", rng.gen_range(0, 200))),
			};
			// *x.entry(record.0)
			// 	.or_insert(HashMap::new())
			// 	.entry(record.1)
			// 	.or_insert(0) += 1;
			top.push(record.0, &record.1);
		}

		println!("{:?}", start.elapsed());
		// println!("{:#?}", top);
		// let mut x = x.into_iter().collect::<Vec<_>>();
		// x.sort_by_key(|x|cmp::Reverse(x.1));
		// println!("{:#?}", x);
	}
}

// mod merge {
// 	// https://stackoverflow.com/questions/23039130/rust-implementing-merge-sorted-iterator/32020190#32020190
// 	use std::{cmp::Ordering, iter::Peekable};

// 	pub struct Merge<L, R>
// 	where
// 		L: Iterator<Item = R::Item>,
// 		R: Iterator,
// 	{
// 		left: Peekable<L>,
// 		right: Peekable<R>,
// 	}
// 	impl<L, R> Merge<L, R>
// 	where
// 		L: Iterator<Item = R::Item>,
// 		R: Iterator,
// 	{
// 		pub fn new(left: L, right: R) -> Self {
// 			Merge {
// 				left: left.peekable(),
// 				right: right.peekable(),
// 			}
// 		}
// 	}

// 	impl<L, R> Iterator for Merge<L, R>
// 	where
// 		L: Iterator<Item = R::Item>,
// 		R: Iterator,
// 		L::Item: Ord,
// 	{
// 		type Item = L::Item;

// 		fn next(&mut self) -> Option<L::Item> {
// 			let which = match (self.left.peek(), self.right.peek()) {
// 				(Some(l), Some(r)) => Some(l.cmp(r)),
// 				(Some(_), None) => Some(Ordering::Less),
// 				(None, Some(_)) => Some(Ordering::Greater),
// 				(None, None) => None,
// 			};

// 			match which {
// 				Some(Ordering::Less) => self.left.next(),
// 				Some(Ordering::Equal) => self.left.next(),
// 				Some(Ordering::Greater) => self.right.next(),
// 				None => None,
// 			}
// 		}
// 	}
// }
