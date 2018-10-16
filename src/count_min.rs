// This file includes source code from https://github.com/jedisct1/rust-count-min-sketch/blob/088274e22a3decc986dec928c92cc90a709a0274/src/lib.rs under the following MIT License:

// Copyright (c) 2016 Frank Denis

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use serde::{de::Deserialize, ser::Serialize};
use std::{
	borrow::Borrow, cmp::max, fmt, hash::{Hash, Hasher}, marker::PhantomData, ops
};
use traits::{Intersect, IntersectPlusUnionIsPlus, New, UnionAssign};
use twox_hash::XxHash;

/// An implementation of a [count-min sketch](https://en.wikipedia.org/wiki/Count–min_sketch) data structure with *conservative updating* for increased accuracy.
///
/// This data structure is also known as a [counting Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter#Counting_filters).
///
/// See [*An Improved Data Stream Summary: The Count-Min Sketch and its Applications*](http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf) and [*New Directions in Traffic Measurement and Accounting*](http://pages.cs.wisc.edu/~suman/courses/740/papers/estan03tocs.pdf) for background on the count-min sketch with conservative updating.
#[derive(Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, <C as New>::Config: Serialize",
	deserialize = "C: Deserialize<'de>, <C as New>::Config: Deserialize<'de>"
))]
pub struct CountMinSketch<K: ?Sized, C: New> {
	counters: Vec<Vec<C>>,
	offsets: Vec<usize>, // to avoid malloc/free each push
	mask: usize,
	k_num: usize,
	config: <C as New>::Config,
	marker: PhantomData<fn(K)>,
}

impl<K: ?Sized, C> CountMinSketch<K, C>
where
	K: Hash,
	C: New + for<'a> UnionAssign<&'a C> + Intersect,
{
	/// Create an empty `CountMinSketch` data structure with the specified error tolerance.
	pub fn new(probability: f64, tolerance: f64, config: C::Config) -> Self {
		let width = Self::optimal_width(tolerance);
		let k_num = Self::optimal_k_num(probability);
		let counters: Vec<Vec<C>> = (0..k_num)
			.map(|_| (0..width).map(|_| C::new(&config)).collect())
			.collect();
		let offsets = vec![0; k_num];
		Self {
			counters,
			offsets,
			mask: Self::mask(width),
			k_num,
			config,
			marker: PhantomData,
		}
	}

	/// "Visit" an element.
	pub fn push<Q: ?Sized, V: ?Sized>(&mut self, key: &Q, value: &V) -> C
	where
		Q: Hash,
		K: Borrow<Q>,
		C: for<'a> ops::AddAssign<&'a V>,
	{
		if !<C as IntersectPlusUnionIsPlus>::VAL {
			let offsets = self.offsets(key);
			self.offsets
				.iter_mut()
				.zip(offsets)
				.for_each(|(offset, offset_new)| {
					*offset = offset_new;
				});
			let mut lowest = C::intersect(
				self.offsets
					.iter()
					.enumerate()
					.map(|(k_i, &offset)| &self.counters[k_i][offset]),
			)
			.unwrap();
			lowest += value;
			self.counters
				.iter_mut()
				.zip(self.offsets.iter())
				.for_each(|(counters, &offset)| {
					counters[offset].union_assign(&lowest);
				});
			lowest
		} else {
			let offsets = self.offsets(key);
			C::intersect(
				self.counters
					.iter_mut()
					.zip(offsets)
					.map(|(counters, offset)| {
						counters[offset] += value;
						&counters[offset]
					}),
			)
			.unwrap()
		}
	}

	/// Union the aggregated value for `key` with `value`.
	pub fn union_assign<Q: ?Sized>(&mut self, key: &Q, value: &C)
	where
		Q: Hash,
		K: Borrow<Q>,
	{
		let offsets = self.offsets(key);
		self.counters
			.iter_mut()
			.zip(offsets)
			.for_each(|(counters, offset)| {
				counters[offset].union_assign(value);
			})
	}

	/// Retrieve an estimate of the aggregated value for `key`.
	pub fn get<Q: ?Sized>(&self, key: &Q) -> C
	where
		Q: Hash,
		K: Borrow<Q>,
	{
		C::intersect(
			self.counters
				.iter()
				.zip(self.offsets(key))
				.map(|(counters, offset)| &counters[offset]),
		)
		.unwrap()
	}

	// pub fn estimate_memory(
	// 	probability: f64, tolerance: f64,
	// ) -> Result<usize, &'static str> {
	// 	let width = Self::optimal_width(tolerance);
	// 	let k_num = Self::optimal_k_num(probability);
	// 	Ok(width * mem::size_of::<C>() * k_num)
	// }

	/// Clears the `CountMinSketch` data structure, as if it was new.
	pub fn clear(&mut self) {
		let config = &self.config;
		self.counters
			.iter_mut()
			.flat_map(|x| x.iter_mut())
			.for_each(|counter| {
				*counter = C::new(config);
			})
	}

	fn optimal_width(tolerance: f64) -> usize {
		let e = tolerance;
		let width = (2.0 / e).round() as usize;
		max(2, width)
			.checked_next_power_of_two()
			.expect("Width would be way too large")
	}

	fn mask(width: usize) -> usize {
		assert!(width > 1);
		assert_eq!(width & (width - 1), 0);
		width - 1
	}

	fn optimal_k_num(probability: f64) -> usize {
		max(1, ((1.0 - probability).ln() / 0.5_f64.ln()) as usize)
	}

	fn offsets<Q: ?Sized>(&self, key: &Q) -> impl Iterator<Item = usize>
	where
		Q: Hash,
		K: Borrow<Q>,
	{
		// if k_i < 2 {
		//     let sip = &mut self.hashers[k_i as usize].clone();
		//     key.hash(sip);
		//     let hash = sip.finish();
		//     hashes[k_i as usize] = hash;
		//     hash as usize & self.mask
		// } else {
		//     hashes[0]
		//         .wrapping_add((k_i as u64).wrapping_mul(hashes[1]) %
		//                       0xffffffffffffffc5) as usize & self.mask
		// }
		let mask = self.mask;
		hashes(key).map(move |hash| hash as usize & mask)
	}
}

fn hashes<Q: ?Sized>(key: &Q) -> impl Iterator<Item = u64>
where
	Q: Hash,
{
	#[allow(missing_copy_implementations, missing_debug_implementations)]
	struct X(XxHash);
	impl Iterator for X {
		type Item = u64;
		fn next(&mut self) -> Option<Self::Item> {
			let ret = self.0.finish();
			self.0.write(&[123]);
			Some(ret)
		}
	}
	let mut hasher = XxHash::default();
	key.hash(&mut hasher);
	X(hasher)
}

impl<K: ?Sized, C: New + Clone> Clone for CountMinSketch<K, C> {
	fn clone(&self) -> Self {
		Self {
			counters: self.counters.clone(),
			offsets: vec![0; self.offsets.len()],
			mask: self.mask,
			k_num: self.k_num,
			config: self.config.clone(),
			marker: PhantomData,
		}
	}
}
impl<K: ?Sized, C: New> fmt::Debug for CountMinSketch<K, C> {
	fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
		fmt.debug_struct("CountMinSketch")
            // .field("counters", &self.counters)
            .finish()
	}
}

#[cfg(test)]
mod tests {
	type CountMinSketch8<K> = super::CountMinSketch<K, u8>;
	type CountMinSketch16<K> = super::CountMinSketch<K, u16>;
	type CountMinSketch64<K> = super::CountMinSketch<K, u64>;

	#[ignore] // release mode stops panic
	#[test]
	#[should_panic]
	fn test_overflow() {
		let mut cms = CountMinSketch8::<&str>::new(0.95, 10.0 / 100.0, ());
		for _ in 0..300 {
			let _ = cms.push("key", &1);
		}
		// assert_eq!(cms.get("key"), &u8::max_value());
	}

	#[test]
	fn test_increment() {
		let mut cms = CountMinSketch16::<&str>::new(0.95, 10.0 / 100.0, ());
		for _ in 0..300 {
			let _ = cms.push("key", &1);
		}
		assert_eq!(cms.get("key"), 300);
	}

	#[test]
	fn test_increment_multi() {
		let mut cms = CountMinSketch64::<u64>::new(0.99, 2.0 / 100.0, ());
		for i in 0..1_000_000 {
			let _ = cms.push(&(i % 100), &1);
		}
		for key in 0..100 {
			assert!(cms.get(&key) >= 9_000);
		}
		// cms.reset();
		// for key in 0..100 {
		//     assert!(cms.get(&key) < 11_000);
		// }
	}
}
