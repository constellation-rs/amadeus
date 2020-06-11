use std::{cmp::Ordering, collections::HashMap, hash::BuildHasher};

pub trait AmadeusOrd {
	fn amadeus_cmp(&self, other: &Self) -> Ordering;
}

macro_rules! ord {
	($($t:ty)*) => {$(
		impl AmadeusOrd for $t {
			fn amadeus_cmp(&self, other: &Self) -> Ordering {
				Ord::cmp(self, other)
			}
		}
	)*}
}
ord!(bool u8 i8 u16 i16 u32 i32 u64 i64 String);

impl AmadeusOrd for f32 {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(
			&ordered_float::OrderedFloat(*self),
			&ordered_float::OrderedFloat(*other),
		)
	}
}
impl AmadeusOrd for f64 {
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		Ord::cmp(
			&ordered_float::OrderedFloat(*self),
			&ordered_float::OrderedFloat(*other),
		)
	}
}

impl<T> AmadeusOrd for &T
where
	T: AmadeusOrd + ?Sized,
{
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		(**self).amadeus_cmp(&**other)
	}
}

impl<T> AmadeusOrd for Box<T>
where
	T: AmadeusOrd,
{
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		(**self).amadeus_cmp(&**other)
	}
}

/// Sort `None` as larger than any non-`None` value
impl<T> AmadeusOrd for Option<T>
where
	T: AmadeusOrd,
{
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(Some(a), Some(b)) => a.amadeus_cmp(b),
			(None, None) => Ordering::Equal,
			(None, Some(_)) => Ordering::Greater,
			(Some(_), None) => Ordering::Less,
		}
	}
}

impl<T> AmadeusOrd for Vec<T>
where
	T: AmadeusOrd,
{
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		for i in 0..self.len().min(other.len()) {
			match self[i].amadeus_cmp(&other[i]) {
				Ordering::Equal => (),
				res => return res,
			}
		}
		self.len().cmp(&other.len())
	}
}

impl<K, V, S> AmadeusOrd for HashMap<K, V, S>
where
	K: AmadeusOrd,
	V: AmadeusOrd,
	S: BuildHasher,
{
	fn amadeus_cmp(&self, other: &Self) -> Ordering {
		let mut keys: Vec<(&K, &V, bool)> = self
			.iter()
			.map(|(k, v)| (k, v, false))
			.chain(other.iter().map(|(k, v)| (k, v, true)))
			.collect();
		keys.sort_by(|(a, _, _), (b, _, _)| a.amadeus_cmp(b));
		let mut keys = &*keys;
		while keys.len() >= 2 {
			let ((a_k, a_v, a_r), (b_k, b_v, b_r)) = (keys[0], keys[1]);
			if !a_r && b_r {
				match a_k.amadeus_cmp(b_k) {
					Ordering::Equal => (),
					res => return res,
				}
				match a_v.amadeus_cmp(b_v) {
					Ordering::Equal => (),
					res => return res,
				}
				keys = &keys[2..];
			} else if !a_r {
				return Ordering::Greater;
			} else {
				return Ordering::Less;
			}
		}
		if keys.len() == 1 {
			if !keys[0].2 {
				Ordering::Greater
			} else {
				Ordering::Less
			}
		} else {
			Ordering::Equal
		}
	}
}

macro_rules! ord {
	($($i:tt)*) => {$(
		impl<T> AmadeusOrd for [T; $i]
		where
			T: AmadeusOrd
		{
			fn amadeus_cmp(&self, other: &Self) -> Ordering {
				for (a,b) in self.iter().zip(other.iter()) {
					match a.amadeus_cmp(b) {
						Ordering::Equal => (),
						res => return res,
					}
				}
				Ordering::Equal
			}
		}
	)*};
}
array!(ord);

macro_rules! ord {
	($len:tt $($t:ident $i:tt)*) => {
		impl<$($t,)*> AmadeusOrd for ($($t,)*) where $($t: AmadeusOrd,)* {
			#[allow(unused_variables)]
			fn amadeus_cmp(&self, other: &Self) -> Ordering {
				Ordering::Equal
					$(.then_with(|| self.$i.amadeus_cmp(&other.$i)))*
			}
		}
	};
}
tuple!(ord);
