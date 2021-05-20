#![allow(missing_docs)] // due to FnNamed

use serde::{Deserialize, Serialize};
use serde_closure::{traits, FnNamed};
use std::{
	cmp::Ordering, fmt::{self, Debug}, iter, ops
};

FnNamed! {
	pub type NeverEqual<F, T> = |self, f: F|a=> &T, b=> &T| -> Ordering where ; where F: (for<'a> traits::Fn<(&'a T, &'a T), Output = Ordering>) {
		match (self.f).call((a, b)) {
			Ordering::Equal => Ordering::Less,
			ord => ord
		}
	}
}

/// This data structure tracks the `n` top values given a stream. It uses only `O(n)` space.
#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(
	serialize = "T: Serialize, F: Serialize + for<'a> traits::Fn<(&'a T, &'a T), Output = Ordering>",
	deserialize = "T: Deserialize<'de>, F: Deserialize<'de> + for<'a> traits::Fn<(&'a T, &'a T), Output = Ordering>"
))]
pub struct Sort<T, F> {
	top: BTreeSet<T, NeverEqual<F, T>>,
	n: usize,
}
impl<T, F> Sort<T, F> {
	/// Create an empty `Sort` data structure with the specified `n` capacity.
	pub fn new(cmp: F, n: usize) -> Self {
		Self {
			top: BTreeSet::with_cmp(NeverEqual::new(cmp)),
			n,
		}
	}

	/// The `n` top elements we have capacity to track.
	pub fn capacity(&self) -> usize {
		self.n
	}

	/// The number of elements currently held.
	pub fn len(&self) -> usize {
		self.top.len()
	}

	/// If `.len() == 0`
	pub fn is_empty(&self) -> bool {
		self.top.is_empty()
	}

	/// Clears the `Sort` data structure, as if it was new.
	pub fn clear(&mut self) {
		self.top.clear();
	}

	/// An iterator visiting all elements in ascending order. The iterator element type is `&'_ T`.
	pub fn iter(&self) -> std::collections::btree_set::Iter<'_, T> {
		self.top.iter()
	}
}
#[cfg_attr(not(nightly), serde_closure::desugar)]
impl<T, F> Sort<T, F>
where
	F: traits::Fn(&T, &T) -> Ordering,
{
	/// "Visit" an element.
	pub fn push(&mut self, item: T) {
		let mut at_capacity = false;
		if self.top.len() < self.n || {
			at_capacity = true;
			!matches!(self.top.partial_cmp(&item), Some(Ordering::Less))
		} {
			let x = self.top.insert(item);
			assert!(x);
			if at_capacity {
				let _ = self.top.pop_last().unwrap();
			}
		}
	}
}
impl<T, F> IntoIterator for Sort<T, F> {
	type Item = T;
	type IntoIter = std::collections::btree_set::IntoIter<T>;

	fn into_iter(self) -> Self::IntoIter {
		self.top.into_iter()
	}
}
#[cfg_attr(not(nightly), serde_closure::desugar)]
impl<T, F> iter::Sum<Sort<T, F>> for Option<Sort<T, F>>
where
	F: traits::Fn(&T, &T) -> Ordering,
{
	fn sum<I>(mut iter: I) -> Self
	where
		I: Iterator<Item = Sort<T, F>>,
	{
		let mut total = iter.next()?;
		for sample in iter {
			total += sample;
		}
		Some(total)
	}
}
#[cfg_attr(not(nightly), serde_closure::desugar)]
impl<T, F> ops::Add for Sort<T, F>
where
	F: traits::Fn(&T, &T) -> Ordering,
{
	type Output = Self;

	fn add(mut self, other: Self) -> Self {
		self += other;
		self
	}
}
#[cfg_attr(not(nightly), serde_closure::desugar)]
impl<T, F> ops::AddAssign for Sort<T, F>
where
	F: traits::Fn(&T, &T) -> Ordering,
{
	fn add_assign(&mut self, other: Self) {
		assert_eq!(self.n, other.n);
		for t in other.top {
			self.push(t);
		}
	}
}
impl<T, F> Debug for Sort<T, F>
where
	T: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_list().entries(self.iter()).finish()
	}
}

use btree_set::BTreeSet;
mod btree_set {
	use serde::{Deserialize, Deserializer, Serialize, Serializer};
	use serde_closure::traits;
	use std::{
		borrow::Borrow, cmp::Ordering, collections::btree_set, marker::PhantomData, mem::{self, ManuallyDrop, MaybeUninit}
	};

	#[derive(Clone, Serialize, Deserialize)]
	#[serde(bound(
		serialize = "T: Serialize, F: Serialize + for<'a> traits::FnMut<(&'a T, &'a T), Output = Ordering>",
		deserialize = "T: Deserialize<'de>, F: Deserialize<'de> + for<'a> traits::FnMut<(&'a T, &'a T), Output = Ordering>"
	))]
	pub struct BTreeSet<T, F> {
		set: std::collections::BTreeSet<Node<T, F>>,
		cmp: F,
	}
	impl<T, F> BTreeSet<T, F> {
		// pub fn new() -> BTreeSet<T, Cmp> {
		//     Self::with_cmp(Cmp)
		// }
		pub fn with_cmp(cmp: F) -> Self {
			// Sound due to repr(transparent)
			let set = unsafe {
				mem::transmute::<
					btree_set::BTreeSet<TrivialOrd<Node<T, F>>>,
					btree_set::BTreeSet<Node<T, F>>,
				>(btree_set::BTreeSet::new())
			};
			Self { set, cmp }
		}
		pub fn cmp(&self) -> &F {
			&self.cmp
		}
		pub fn cmp_mut(&mut self) -> &mut F {
			&mut self.cmp
		}
		pub fn clear(&mut self) {
			self.trivial_ord_mut().clear();
		}
		pub fn iter(&self) -> btree_set::Iter<'_, T> {
			// Sound due to repr(transparent)
			unsafe { mem::transmute(self.set.iter()) }
		}
		pub fn len(&self) -> usize {
			self.set.len()
		}
		pub fn is_empty(&self) -> bool {
			self.set.is_empty()
		}
		pub fn pop_last(&mut self) -> Option<T> {
			#[cfg(nightly)]
			return self.trivial_ord_mut().pop_last().map(|value| value.0.t);
			#[cfg(not(nightly))]
			todo!();
		}
		fn trivial_ord_mut(&mut self) -> &mut std::collections::BTreeSet<TrivialOrd<Node<T, F>>> {
			let set: *mut std::collections::BTreeSet<Node<T, F>> = &mut self.set;
			let set: *mut std::collections::BTreeSet<TrivialOrd<Node<T, F>>> = set.cast();
			// Sound due to repr(transparent)
			unsafe { &mut *set }
		}
	}
	impl<T, F> BTreeSet<T, F>
	where
		F: for<'a> traits::FnMut<(&'a T, &'a T), Output = Ordering>,
	{
		pub fn insert(&mut self, value: T) -> bool {
			self.set.insert(Node::new(value, &self.cmp))
		}
		pub fn remove(&mut self, value: &T) -> Option<T> {
			let value: *const T = value;
			let value: *const TrivialOrd<T> = value.cast();
			let value = unsafe { &*value };
			self.set.take(value).map(|node| node.t)
		}
	}
	impl<T, F> IntoIterator for BTreeSet<T, F> {
		type Item = T;
		type IntoIter = btree_set::IntoIter<T>;

		fn into_iter(self) -> Self::IntoIter {
			// Sound due to repr(transparent)
			unsafe { mem::transmute(self.set.into_iter()) }
		}
	}
	#[cfg_attr(not(nightly), serde_closure::desugar)]
	impl<T, F> PartialEq<T> for BTreeSet<T, F>
	where
		F: traits::Fn(&T, &T) -> Ordering,
	{
		fn eq(&self, other: &T) -> bool {
			matches!(
				self.cmp.call((self.iter().next().unwrap(), other)),
				Ordering::Equal
			) && matches!(
				self.cmp.call((self.iter().last().unwrap(), other)),
				Ordering::Equal
			)
		}
	}
	#[cfg_attr(not(nightly), serde_closure::desugar)]
	impl<T, F> PartialOrd<T> for BTreeSet<T, F>
	where
		F: traits::Fn(&T, &T) -> Ordering,
	{
		fn partial_cmp(&self, other: &T) -> Option<Ordering> {
			match (
				self.cmp.call((self.iter().next().unwrap(), other)),
				self.cmp.call((self.iter().last().unwrap(), other)),
			) {
				(Ordering::Less, Ordering::Less) => Some(Ordering::Less),
				(Ordering::Equal, Ordering::Equal) => Some(Ordering::Equal),
				(Ordering::Greater, Ordering::Greater) => Some(Ordering::Greater),
				_ => None,
			}
		}
	}

	#[repr(transparent)]
	struct TrivialOrd<T: ?Sized>(T);
	impl<T: ?Sized> PartialEq for TrivialOrd<T> {
		fn eq(&self, _other: &Self) -> bool {
			unreachable!()
		}
	}
	impl<T: ?Sized> Eq for TrivialOrd<T> {}
	impl<T: ?Sized> PartialOrd for TrivialOrd<T> {
		fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
			unreachable!()
		}
	}
	impl<T: ?Sized> Ord for TrivialOrd<T> {
		fn cmp(&self, _other: &Self) -> Ordering {
			unreachable!()
		}
	}

	#[repr(transparent)]
	struct Node<T, F: ?Sized> {
		t: T,
		marker: PhantomData<fn() -> F>,
	}
	impl<T, F: ?Sized> Node<T, F> {
		fn new(t: T, f: &F) -> Self {
			if mem::size_of_val(f) != 0 {
				panic!("Closures with nonzero size not supported");
			}
			Self {
				t,
				marker: PhantomData,
			}
		}
	}
	impl<T, F: ?Sized> Borrow<T> for Node<T, F> {
		fn borrow(&self) -> &T {
			&self.t
		}
	}
	impl<T, F: ?Sized> Borrow<TrivialOrd<T>> for Node<T, F> {
		fn borrow(&self) -> &TrivialOrd<T> {
			let self_: *const T = &self.t;
			let self_: *const TrivialOrd<T> = self_.cast();
			unsafe { &*self_ }
		}
	}
	impl<T, F> PartialEq for Node<T, F>
	where
		F: for<'a> traits::FnMut<(&'a T, &'a T), Output = Ordering>,
	{
		fn eq(&self, other: &Self) -> bool {
			matches!(self.cmp(other), Ordering::Equal)
		}
	}
	impl<T, F> Eq for Node<T, F> where F: for<'a> traits::FnMut<(&'a T, &'a T), Output = Ordering> {}
	impl<T, F> PartialOrd for Node<T, F>
	where
		F: for<'a> traits::FnMut<(&'a T, &'a T), Output = Ordering>,
	{
		fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
			Some(self.cmp(other))
		}
	}
	impl<T, F> Ord for Node<T, F>
	where
		F: for<'a> traits::FnMut<(&'a T, &'a T), Output = Ordering>,
	{
		fn cmp(&self, other: &Self) -> Ordering {
			// This is safe as an F has already been materialized (so we know it isn't
			// uninhabited) and its size is zero. Related:
			// https://internals.rust-lang.org/t/is-synthesizing-zero-sized-values-safe/11506
			#[allow(clippy::uninit_assumed_init)]
			let mut cmp: ManuallyDrop<F> = unsafe { MaybeUninit::uninit().assume_init() };
			cmp.call_mut((&self.t, &other.t))
		}
	}

	impl<T, F: ?Sized> Clone for Node<T, F>
	where
		T: Clone,
	{
		fn clone(&self) -> Self {
			Self {
				t: self.t.clone(),
				marker: PhantomData,
			}
		}
	}
	impl<T, F: ?Sized> Serialize for Node<T, F>
	where
		T: Serialize,
	{
		fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where
			S: Serializer,
		{
			self.t.serialize(serializer)
		}
	}
	impl<'de, T, F: ?Sized> Deserialize<'de> for Node<T, F>
	where
		T: Deserialize<'de>,
	{
		fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
		where
			D: Deserializer<'de>,
		{
			T::deserialize(deserializer).map(|t| Self {
				t,
				marker: PhantomData,
			})
		}
	}
}
