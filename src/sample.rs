use rand::{self, Rng, SeedableRng};
use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};
use std::{convert::TryFrom, fmt, iter, ops, vec};

/// Given population and sample sizes, returns true if this element is in the sample. Without replacement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SampleTotal {
	total: usize,
	samples: usize,
	picked: usize,
	i: usize,
}
impl SampleTotal {
	/// Create a `SampleTotal` that will provide a sample of size `samples` of a population of size `total`.
	pub fn new(total: usize, samples: usize) -> Self {
		assert!(total >= samples);
		Self {
			total,
			samples,
			picked: 0,
			i: 0,
		}
	}

	/// Returns whether or not to this value is in the sample
	pub fn sample<R: Rng>(&mut self, rng: &mut R) -> bool {
		let sample = rng.gen_range(0, self.total - self.i) < (self.samples - self.picked);
		self.i += 1;
		if sample {
			self.picked += 1;
		}
		sample
	}
}
impl Drop for SampleTotal {
	fn drop(&mut self) {
		assert_eq!(self.picked, self.samples);
	}
}

#[derive(Clone)]
struct FixedCapVec<T>(Vec<T>);
impl<T> FixedCapVec<T> {
	fn new(cap: usize) -> Self {
		let self_ = Self(Vec::with_capacity(cap));
		assert_eq!(self_.capacity(), cap);
		self_
	}
	fn len(&self) -> usize {
		self.0.len()
	}
	fn capacity(&self) -> usize {
		self.0.capacity()
	}
	fn push(&mut self, t: T) {
		assert!(self.len() < self.capacity());
		let cap = self.capacity();
		self.0.push(t);
		assert_eq!(self.capacity(), cap);
	}
	fn pop(&mut self) -> Option<T> {
		let cap = self.capacity();
		let ret = self.0.pop();
		assert_eq!(self.capacity(), cap);
		ret
	}
	fn into_iter(self) -> std::vec::IntoIter<T> {
		self.0.into_iter()
	}
}
impl<T, Idx> std::ops::Index<Idx> for FixedCapVec<T>
where
	Idx: std::slice::SliceIndex<[T]>,
{
	type Output = <Vec<T> as std::ops::Index<Idx>>::Output;
	fn index(&self, index: Idx) -> &Self::Output {
		std::ops::Index::index(&self.0, index)
	}
}
impl<T, Idx> std::ops::IndexMut<Idx> for FixedCapVec<T>
where
	Idx: std::slice::SliceIndex<[T]>,
{
	fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
		std::ops::IndexMut::index_mut(&mut self.0, index)
	}
}
impl<T> fmt::Debug for FixedCapVec<T>
where
	T: fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		self.0.fmt(f)
	}
}
impl<T> Serialize for FixedCapVec<T>
where
	T: Serialize,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		<(usize, &Vec<T>)>::serialize(&(self.0.capacity(), &self.0), serializer)
	}
}
impl<'de, T> Deserialize<'de> for FixedCapVec<T>
where
	T: Deserialize<'de>,
{
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		<(usize, Vec<T>)>::deserialize(deserializer).map(|(cap, mut vec)| {
			vec.reserve_exact(cap - vec.len());
			assert_eq!(vec.capacity(), cap);
			Self(vec)
		})
	}
}

/// [Reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling). Without replacement, and the returned order is unstable.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SampleUnstable<T> {
	reservoir: FixedCapVec<T>,
	i: usize,
}
impl<T> SampleUnstable<T> {
	/// Create a `SampleUnstable` that will provide a sample of size `samples`.
	pub fn new(samples: usize) -> Self {
		Self {
			reservoir: FixedCapVec::new(samples),
			i: 0,
		}
	}

	/// "Visit" this element
	pub fn push<R: Rng>(&mut self, t: T, rng: &mut R) {
		// TODO: https://dl.acm.org/citation.cfm?id=198435
		if self.reservoir.len() < self.reservoir.capacity() {
			self.reservoir.push(t);
		} else {
			let idx = rng.gen_range(0, self.i);
			if idx < self.reservoir.capacity() {
				self.reservoir[idx] = t;
			}
		}
		self.i += 1;
	}
}
impl<T> IntoIterator for SampleUnstable<T> {
	type Item = T;
	type IntoIter = vec::IntoIter<T>;

	fn into_iter(self) -> vec::IntoIter<T> {
		self.reservoir.into_iter()
	}
}
impl<T> iter::Sum for SampleUnstable<T> {
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
impl<T> ops::Add for SampleUnstable<T> {
	type Output = Self;

	fn add(mut self, other: Self) -> Self {
		self += other;
		self
	}
}
impl<T> ops::AddAssign for SampleUnstable<T> {
	fn add_assign(&mut self, mut other: Self) {
		if self.reservoir.capacity() > 0 {
			// TODO
			assert_eq!(self.reservoir.capacity(), other.reservoir.capacity());
			let mut new = FixedCapVec::new(self.reservoir.capacity());
			let (m, n) = (self.i, other.i);
			let mut rng = rand::prng::XorShiftRng::from_seed([
				u8::try_from(m & 0xff).unwrap(),
				u8::try_from(n & 0xff).unwrap(),
				u8::try_from(self.reservoir.capacity() & 0xff).unwrap(),
				3,
				4,
				5,
				6,
				7,
				8,
				9,
				10,
				11,
				12,
				13,
				14,
				15,
			]); // TODO
			for _ in 0..new.capacity() {
				if rng.gen_range(0, m + n) < m {
					new.push(self.reservoir.pop().unwrap());
				} else {
					new.push(other.reservoir.pop().unwrap());
				}
			}
			self.reservoir = new;
			self.i += other.i;
		} else {
			*self = other;
		}
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use rand;
	use std::collections::HashMap;

	#[test]
	fn sample_without_replacement() {
		let total = 6;
		let samples = 2;

		let mut hash = HashMap::new();
		for _ in 0..1_000_000 {
			let mut res = Vec::with_capacity(samples);
			let mut x = SampleTotal::new(total, samples);
			for i in 0..total {
				if x.sample(&mut rand::thread_rng()) {
					res.push(i);
				}
			}
			*hash.entry(res).or_insert(0) += 1;
		}
		println!("{:#?}", hash);
	}

	#[test]
	fn sample_unstable() {
		let total = 6;
		let samples = 2;

		let mut hash = HashMap::new();
		for _ in 0..1_000_000 {
			let mut x = SampleUnstable::new(samples);
			for i in 0..total {
				x.push(i, &mut rand::thread_rng());
			}
			*hash.entry(x.into_iter().collect::<Vec<_>>()).or_insert(0) += 1;
		}
		println!("{:#?}", hash);
	}
}
