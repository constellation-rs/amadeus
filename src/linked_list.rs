use std::{iter, marker, ops};

#[derive(Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct LinkedListIndex<'a>(usize, marker::PhantomData<&'a ()>);
impl<'a> LinkedListIndex<'a> {
	#[inline(always)]
	pub unsafe fn staticify(self) -> LinkedListIndex<'static> {
		LinkedListIndex(self.0, marker::PhantomData)
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct LinkedList<T> {
	vec: Box<[(usize, usize, Option<T>)]>,
	head: usize,
	tail: usize,
	free: usize,
	len: usize,
}
impl<T> LinkedList<T> {
	pub fn new(cap: usize) -> Self {
		let vec = if cap >= 2 {
			iter::once((usize::max_value(), 1, None))
				.chain((1..cap - 1).map(|i| (i - 1, i + 1, None)))
				.chain(iter::once((cap - 2, usize::max_value(), None)))
				.collect::<Vec<_>>()
		} else {
			(0..cap)
				.map(|_| (usize::max_value(), usize::max_value(), None))
				.collect::<Vec<_>>()
		}
		.into_boxed_slice();
		let ret = Self {
			vec,
			head: usize::max_value(),
			tail: usize::max_value(),
			free: 0,
			len: 0,
		};
		ret.assert();
		ret
	}
	fn assert(&self) {
		if !cfg!(feature = "assert") {
			return;
		}
		let mut len = 0;
		{
			let mut cur = self.head;
			while cur != usize::max_value() {
				assert!(self.vec[cur].2.is_some());
				let next = self.vec[cur].1;
				if next != usize::max_value() {
					assert_eq!(self.vec[next].0, cur);
				} else {
					assert_eq!(self.tail, cur);
				}
				cur = next;
				len += 1;
			}
		}
		assert_eq!(self.len, len);
		let mut len_free = 0;
		{
			let mut cur = self.free;
			while cur != usize::max_value() {
				assert!(!self.vec[cur].2.is_some());
				let next = self.vec[cur].1;
				if next != usize::max_value() {
					assert_eq!(self.vec[next].0, cur);
				}
				cur = next;
				len_free += 1;
			}
		}
		assert_eq!(len + len_free, self.vec.len());
	}
	#[inline(always)]
	fn alloc(&mut self) -> usize {
		let free = self.free;
		assert_ne!(free, usize::max_value());
		self.free = self.vec[free].1;
		if self.free != usize::max_value() {
			self.vec[self.free].0 = usize::max_value();
		}
		free
	}
	#[inline(always)]
	fn free(&mut self, idx: usize) {
		if self.free != usize::max_value() {
			self.vec[self.free].0 = idx;
		}
		self.vec[idx].0 = usize::max_value();
		self.vec[idx].1 = self.free;
		self.free = idx;
	}
	#[inline(always)]
	pub fn head(&self) -> Option<LinkedListIndex> {
		if self.head != usize::max_value() {
			Some(LinkedListIndex(self.head, marker::PhantomData))
		} else {
			None
		}
	}
	#[inline(always)]
	pub fn tail(&self) -> Option<LinkedListIndex> {
		if self.tail != usize::max_value() {
			Some(LinkedListIndex(self.tail, marker::PhantomData))
		} else {
			None
		}
	}
	#[inline(always)]
	pub fn len(&self) -> usize {
		self.len
	}
	#[inline(always)]
	pub fn capacity(&self) -> usize {
		self.vec.len()
	}
	pub fn push_back(&mut self, t: T) -> LinkedListIndex {
		let idx = self.alloc();
		self.vec[idx] = (self.tail, usize::max_value(), Some(t));
		if self.tail != usize::max_value() {
			self.vec[self.tail].1 = idx;
		} else {
			self.head = idx;
		}
		self.tail = idx;
		self.len += 1;
		self.assert();
		LinkedListIndex(idx, marker::PhantomData)
	}
	pub fn push_front(&mut self, t: T) -> LinkedListIndex {
		let idx = self.alloc();
		self.vec[idx] = (usize::max_value(), self.head, Some(t));
		if self.head != usize::max_value() {
			self.vec[self.head].0 = idx;
		} else {
			self.tail = idx;
		}
		self.head = idx;
		self.len += 1;
		self.assert();
		LinkedListIndex(idx, marker::PhantomData)
	}
	pub fn pop_back(&mut self) -> T {
		let idx = self.tail;
		let ret = self.vec[idx].2.take().unwrap();
		self.tail = self.vec[idx].0;
		self.vec[self.tail].1 = usize::max_value();
		self.free(idx);
		self.len -= 1;
		self.assert();
		ret
	}
	pub fn pop_front(&mut self) -> T {
		let idx = self.head;
		let ret = self.vec[idx].2.take().unwrap();
		self.head = self.vec[idx].1;
		if self.head != usize::max_value() {
			self.vec[self.head].0 = usize::max_value();
		}
		self.free(idx);
		self.len -= 1;
		self.assert();
		ret
	}
	pub fn insert_after(&mut self, index: LinkedListIndex, t: T) -> LinkedListIndex {
		let idx = self.alloc();
		let next = self.vec[index.0].1;
		self.vec[idx] = (index.0, next, Some(t));
		self.vec[index.0].1 = idx;
		if next != usize::max_value() {
			self.vec[next].0 = idx;
		} else {
			self.tail = idx;
		}
		self.len += 1;
		self.assert();
		LinkedListIndex(idx, marker::PhantomData)
	}
	pub fn insert_before(&mut self, index: LinkedListIndex, t: T) -> LinkedListIndex {
		let idx = self.alloc();
		let prev = self.vec[index.0].0;
		self.vec[idx] = (prev, index.0, Some(t));
		self.vec[index.0].0 = idx;
		if prev != usize::max_value() {
			self.vec[prev].1 = idx;
		} else {
			self.head = idx;
		}
		self.len += 1;
		self.assert();
		LinkedListIndex(idx, marker::PhantomData)
	}
	pub fn remove(&mut self, index: LinkedListIndex) -> T {
		let prev = self.vec[index.0].0;
		let next = self.vec[index.0].1;
		if prev != usize::max_value() {
			self.vec[prev].1 = next;
		} else {
			self.head = next;
		}
		if next != usize::max_value() {
			self.vec[next].0 = prev;
		} else {
			self.tail = prev;
		}
		let ret = self.vec[index.0].2.take().unwrap();
		self.free(index.0);
		self.len -= 1;
		self.assert();
		ret
	}
	pub fn move_after(&mut self, from: LinkedListIndex, to: LinkedListIndex) {
		assert_ne!(from.0, to.0);
		let prev = self.vec[from.0].0;
		let next = self.vec[from.0].1;
		if prev != usize::max_value() {
			self.vec[prev].1 = next;
		} else {
			self.head = next;
		}
		if next != usize::max_value() {
			self.vec[next].0 = prev;
		} else {
			self.tail = prev;
		}

		let next2 = self.vec[to.0].1;
		self.vec[from.0].0 = to.0;
		self.vec[from.0].1 = next2;
		self.vec[to.0].1 = from.0;
		if next2 != usize::max_value() {
			self.vec[next2].0 = from.0;
		} else {
			self.tail = from.0;
		}
		self.assert();
	}
	pub fn move_before(&mut self, from: LinkedListIndex, to: LinkedListIndex) {
		assert_ne!(from.0, to.0);
		let prev = self.vec[from.0].0;
		let next = self.vec[from.0].1;
		if prev != usize::max_value() {
			self.vec[prev].1 = next;
		} else {
			self.head = next;
		}
		if next != usize::max_value() {
			self.vec[next].0 = prev;
		} else {
			self.tail = prev;
		}

		let prev2 = self.vec[to.0].0;
		self.vec[from.0].0 = prev2;
		self.vec[from.0].1 = to.0;
		self.vec[to.0].0 = from.0;
		if prev2 != usize::max_value() {
			self.vec[prev2].1 = from.0;
		} else {
			self.head = from.0;
		}
		self.assert();
	}
	#[inline(always)]
	pub fn increment(&self, index: &mut LinkedListIndex) {
		index.0 = self.vec[index.0].1;
		assert_ne!(index.0, usize::max_value());
	}
	#[inline(always)]
	pub fn decrement(&self, index: &mut LinkedListIndex) {
		index.0 = self.vec[index.0].0;
		assert_ne!(index.0, usize::max_value());
	}
	pub fn clear(&mut self) {
		while self.len() != 0 {
			let _ = self.pop_back();
		}
	}
}
impl<'a, T> ops::Index<LinkedListIndex<'a>> for LinkedList<T> {
	type Output = T;
	#[inline(always)]
	fn index(&self, index: LinkedListIndex) -> &T {
		self.vec[index.0].2.as_ref().unwrap()
	}
}
impl<'a, T> ops::IndexMut<LinkedListIndex<'a>> for LinkedList<T> {
	#[inline(always)]
	fn index_mut(&mut self, index: LinkedListIndex) -> &mut T {
		self.vec[index.0].2.as_mut().unwrap()
	}
}

pub struct LinkedListIter<'a, T: 'a> {
	linked_list: &'a LinkedList<T>,
	index: Option<LinkedListIndex<'a>>,
}
impl<'a, T> Iterator for LinkedListIter<'a, T> {
	type Item = &'a T;
	fn next(&mut self) -> Option<&'a T> {
		if let Some(index) = self.index {
			if index != self.linked_list.tail().unwrap() {
				self.linked_list.increment(self.index.as_mut().unwrap());
			} else {
				self.index = None;
			}
			Some(&self.linked_list[index])
		} else {
			None
		}
	}
}
