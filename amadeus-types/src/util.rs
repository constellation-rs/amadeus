// from https://github.com/rust-lang/rust/blob/0cd7ff7ddfb75a38dca81ad3e76b1e984129e939/src/libcore/iter/traits/iterator.rs

use std::cmp::Ordering;

pub(crate) trait IteratorExt: Iterator {
	fn cmp_by_<I, F>(mut self, other: I, mut cmp: F) -> Ordering
	where
		Self: Sized,
		I: IntoIterator,
		F: FnMut(Self::Item, I::Item) -> Ordering,
	{
		let mut other = other.into_iter();

		loop {
			let x = match self.next() {
				None => {
					if other.next().is_none() {
						return Ordering::Equal;
					} else {
						return Ordering::Less;
					}
				}
				Some(val) => val,
			};

			let y = match other.next() {
				None => return Ordering::Greater,
				Some(val) => val,
			};

			match cmp(x, y) {
				Ordering::Equal => (),
				non_eq => return non_eq,
			}
		}
	}
	fn partial_cmp_by_<I, F>(mut self, other: I, mut partial_cmp: F) -> Option<Ordering>
	where
		Self: Sized,
		I: IntoIterator,
		F: FnMut(Self::Item, I::Item) -> Option<Ordering>,
	{
		let mut other = other.into_iter();

		loop {
			let x = match self.next() {
				None => {
					if other.next().is_none() {
						return Some(Ordering::Equal);
					} else {
						return Some(Ordering::Less);
					}
				}
				Some(val) => val,
			};

			let y = match other.next() {
				None => return Some(Ordering::Greater),
				Some(val) => val,
			};

			match partial_cmp(x, y) {
				Some(Ordering::Equal) => (),
				non_eq => return non_eq,
			}
		}
	}
	fn eq_by_<I, F>(mut self, other: I, mut eq: F) -> bool
	where
		Self: Sized,
		I: IntoIterator,
		F: FnMut(Self::Item, I::Item) -> bool,
	{
		let mut other = other.into_iter();

		loop {
			let x = match self.next() {
				None => return other.next().is_none(),
				Some(val) => val,
			};

			let y = match other.next() {
				None => return false,
				Some(val) => val,
			};

			if !eq(x, y) {
				return false;
			}
		}
	}
}

impl<I: ?Sized> IteratorExt for I where I: Iterator {}
