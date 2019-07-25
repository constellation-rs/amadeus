use crate::linked_list::{LinkedList, LinkedListIndex};
use serde::{Deserialize, Serialize};
use std::{ops, ptr};

#[derive(Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct OrderedLinkedListIndex<'a>(LinkedListIndex<'a>);
impl<'a> OrderedLinkedListIndex<'a> {
	#[inline(always)]
	pub unsafe fn staticify(self) -> OrderedLinkedListIndex<'static> {
		OrderedLinkedListIndex(self.0.staticify())
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OrderedLinkedList<T>(LinkedList<T>);
impl<T: Ord> OrderedLinkedList<T> {
	pub fn new(cap: usize) -> Self {
		Self(LinkedList::new(cap))
	}
	fn assert(&self) {
		if !cfg!(feature = "assert") {
			return;
		}
		if self.0.len() <= 1 {
			return;
		}
		let mut idx = self.0.head().unwrap();
		let mut cur = &self.0[idx];
		let mut count = 0;
		loop {
			let x = &self.0[idx];
			assert!(cur >= x);
			cur = x;
			count += 1;
			if idx == self.0.tail().unwrap() {
				break;
			}
			self.0.increment(&mut idx);
		}
		assert_eq!(count, self.0.len());
	}
	#[inline(always)]
	pub fn head(&self) -> Option<OrderedLinkedListIndex> {
		self.0.head().map(OrderedLinkedListIndex)
	}
	#[inline(always)]
	pub fn tail(&self) -> Option<OrderedLinkedListIndex> {
		self.0.tail().map(OrderedLinkedListIndex)
	}
	#[inline(always)]
	pub fn len(&self) -> usize {
		self.0.len()
	}
	#[inline(always)]
	pub fn capacity(&self) -> usize {
		self.0.capacity()
	}
	pub fn push_back(&mut self, t: T) -> OrderedLinkedListIndex {
		if self.0.len() == 0 {
			return OrderedLinkedListIndex(self.0.push_back(t));
		}
		let mut idx = unsafe { self.0.tail().unwrap().staticify() };
		while self.0[idx] < t {
			if idx == self.0.head().unwrap() {
				let ret =
					OrderedLinkedListIndex(unsafe { self.0.insert_before(idx, t).staticify() });
				self.assert();
				return ret;
			}
			self.0.decrement(&mut idx);
		}
		let ret = OrderedLinkedListIndex(unsafe { self.0.insert_after(idx, t).staticify() });
		self.assert();
		ret
	}
	pub fn push_front(&mut self, t: T) -> OrderedLinkedListIndex {
		if self.0.len() == 0 {
			return OrderedLinkedListIndex(self.0.push_front(t));
		}
		let mut idx = unsafe { self.0.head().unwrap().staticify() };
		while self.0[idx] > t {
			if idx == self.0.tail().unwrap() {
				let ret =
					OrderedLinkedListIndex(unsafe { self.0.insert_after(idx, t).staticify() });
				self.assert();
				return ret;
			}
			self.0.increment(&mut idx);
		}
		let ret = OrderedLinkedListIndex(unsafe { self.0.insert_before(idx, t).staticify() });
		self.assert();
		ret
	}
	pub fn mutate(&mut self, index: OrderedLinkedListIndex, f: impl FnOnce(T) -> T) {
		let idx = index.0;
		let x = &mut self.0[idx];
		unsafe { ptr::write(x, f(ptr::read(x))) };
		{
			let val = &self.0[idx];
			let mut prev = idx;
			if prev != self.0.head().unwrap() {
				self.0.decrement(&mut prev);
			}
			if val > &self.0[prev] {
				while val > &self.0[prev] {
					if prev == self.0.head().unwrap() {
						self.0.move_before(idx, prev);
						self.assert();
						return;
					}
					self.0.decrement(&mut prev);
				}
				self.0.move_after(idx, prev);
			}
		}
		{
			let val = &self.0[idx];
			let mut next = idx;
			if next != self.0.tail().unwrap() {
				self.0.increment(&mut next);
			}
			if val < &self.0[next] {
				while val < &self.0[next] {
					if next == self.0.tail().unwrap() {
						self.0.move_after(idx, next);
						self.assert();
						return;
					}
					self.0.increment(&mut next);
				}
				self.0.move_before(idx, next);
			}
		}
		self.assert();
	}
	#[inline(always)]
	pub fn pop_back(&mut self) -> T {
		self.0.pop_back()
	}
	#[inline(always)]
	pub fn pop_front(&mut self) -> T {
		self.0.pop_front()
	}
	pub fn insert_after(
		&mut self, _index: OrderedLinkedListIndex, _t: T,
	) -> OrderedLinkedListIndex {
		unimplemented!()
	}
	pub fn insert_before(
		&mut self, _index: OrderedLinkedListIndex, _t: T,
	) -> OrderedLinkedListIndex {
		unimplemented!()
	}
	#[inline(always)]
	pub fn remove(&mut self, index: OrderedLinkedListIndex) -> T {
		self.0.remove(index.0)
	}
	pub fn move_after(&mut self, _from: OrderedLinkedListIndex, _to: OrderedLinkedListIndex) {
		unimplemented!()
	}
	pub fn move_before(&mut self, _from: OrderedLinkedListIndex, _to: OrderedLinkedListIndex) {
		unimplemented!()
	}
	#[inline(always)]
	pub fn increment(&self, index: &mut OrderedLinkedListIndex) {
		self.0.increment(&mut index.0)
	}
	#[inline(always)]
	pub fn decrement(&self, index: &mut OrderedLinkedListIndex) {
		self.0.decrement(&mut index.0)
	}
	pub fn clear(&mut self) {
		self.0.clear()
	}
	pub fn iter(&self) -> OrderedLinkedListIter<'_, T> {
		OrderedLinkedListIter {
			linked_list: self,
			index: self.head(),
		}
	}
}
impl<'a, T: Ord> ops::Index<OrderedLinkedListIndex<'a>> for OrderedLinkedList<T> {
	type Output = T;
	#[inline(always)]
	fn index(&self, index: OrderedLinkedListIndex) -> &T {
		&self.0[index.0]
	}
}

pub struct OrderedLinkedListIter<'a, T: Ord + 'a> {
	linked_list: &'a OrderedLinkedList<T>,
	index: Option<OrderedLinkedListIndex<'a>>,
}
impl<'a, T: Ord> Iterator for OrderedLinkedListIter<'a, T> {
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
impl<'a, T: Ord> Clone for OrderedLinkedListIter<'a, T> {
	fn clone(&self) -> Self {
		Self {
			linked_list: self.linked_list,
			index: self.index,
		}
	}
}
