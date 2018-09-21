// This file includes source code from https://github.com/jedisct1/rust-hyperloglog/blob/36d73a2c0a324f4122d32febdb19dd4a815147f0/src/hyperloglog/lib.rs under the following BSD 2-Clause "Simplified" License:
//
// Copyright (c) 2013-2016, Frank Denis
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//   Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
//
//   Redistributions in binary form must reproduce the above copyright notice, this
//   list of conditions and the following disclaimer in the documentation and/or
//   other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
// ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// This file includes source code from https://github.com/codahale/sketchy/blob/09e9ede8ac27e6fd37d5c5f53ac9b7776c37bc19/src/hyperloglog.rs under the following Apache License 2.0:
//
// Copyright (c) 2015-2017 Coda Hale
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// https://github.com/twitter/algebird/blob/5fdb079447271a5fe0f1fba068e5f86591ccde36/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala
// https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD countApproxDistinct

// use bytecount;
use std::{
	cmp::{self, Ordering}, fmt, hash::{Hash, Hasher}, marker::PhantomData, ops
};
use traits::{Intersect, IntersectPlusUnionIsPlus, New, UnionAssign};
use twox_hash::XxHash;
// use faster::{self, IntoSIMDRefIterator,SIMDIterator};
use packed_simd::{self, FromBits};
use std::{convert::identity, ops::Range};

mod consts;
use self::consts::*;

/// An implementation of the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) data structure with *bias correction*.
///
/// See [*HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm*](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) and [*HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm*](https://ai.google/research/pubs/pub40671) for background on HyperLogLog with bias correction.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct HyperLogLog<V: ?Sized> {
	alpha: f64,
	zero: usize,
	sum: f64,
	p: u8,
	m: Box<[u8]>,
	marker: PhantomData<fn(V)>,
}

impl<V: ?Sized> HyperLogLog<V>
where
	V: Hash,
{
	/// Create an empty `HyperLogLog` data structure with the specified error tolerance.
	pub fn new(error_rate: f64) -> Self {
		assert!(0.0 < error_rate && error_rate < 1.0);
		let p = (f64::log2(1.04 / error_rate) * 2.0).ceil() as u8;
		assert!(0 < p && p < 64);
		let alpha = Self::get_alpha(p);
		// let max_rho = 64;//64 - self.p + 1;
		Self {
			alpha,
			zero: 1 << p,
			sum: (1 << p) as f64, //(1u128 << max_rho) * (1 << p),
			p,
			m: vec![0; 1 << p].into_boxed_slice(),
			marker: PhantomData,
		}
	}

	/// Create an empty `HyperLogLog` data structure, copying the error tolerance from `hll`.
	pub fn new_from(hll: &Self) -> Self {
		Self {
			alpha: hll.alpha,
			zero: hll.m.len(),
			sum: hll.m.len() as f64,
			p: hll.p,
			m: vec![0; hll.m.len()].into_boxed_slice(),
			marker: PhantomData,
		}
	}

	/// "Visit" an element.
	#[inline]
	pub fn push(&mut self, value: &V) {
		let mut hasher = XxHash::default();
		value.hash(&mut hasher);
		let x = hasher.finish();
		let j = x & (self.m.len() as u64 - 1);
		let w = x >> self.p;
		let rho = Self::get_rho(w, 64 - self.p);
		let mjr = &mut self.m[j as usize];
		let old = *mjr;
		let new = cmp::max(old, rho);
		self.zero -= if old == 0 { 1 } else { 0 };

		// let max_rho = 64;//64 - self.p + 1;
		// self.sum -= 1u128 << (max_rho-old);
		// self.sum += 1u128 << (max_rho-new);

		// see pow_bithack()
		self.sum -= f64::from_bits(u64::max_value().wrapping_sub(old as u64) << 54 >> 2)
			- f64::from_bits(u64::max_value().wrapping_sub(new as u64) << 54 >> 2);

		*mjr = new;
	}

	/// Retrieve an estimate of the carginality of the stream.
	pub fn len(&self) -> f64 {
		let v = self.zero;
		// assert_eq!(bytecount::count(&self.m, 0), self.zero);
		if v > 0 {
			let h = self.m.len() as f64 * (self.m.len() as f64 / v as f64).ln();
			if h <= Self::get_threshold(self.p - 4) {
				return h;
			}
		}
		self.ep()
	}

	/// Returns true if the cardinality estimate is 0
	pub fn is_empty(&self) -> bool {
		// self.len() == 0.0
		self.m.iter().all(|&x| x == 0)
	}

	/// Merge another HyperLogLog data structure into `self`.
	///
	/// This is the same as an HLL approximating cardinality of the union of two multisets.
	pub fn union(&mut self, src: &Self) {
		assert_eq!(src.alpha, self.alpha);
		assert_eq!(src.p, self.p);
		assert_eq!(src.m.len(), self.m.len());
		// let mut zero = self.zero;
		// src.m
		// 	.iter()
		// 	.zip(self.m.iter_mut())
		// 	.for_each(|(src_mir, mir)| {
		// 		if *mir == 0 && *src_mir != 0 {
		// 			zero -= 1;
		// 		}
		// 		*mir = cmp::max(*mir, *src_mir);
		// 	});
		// self.zero = zero;
		use packed_simd::*;
		assert_eq!(self.m.len() % u8s::lanes(), 0);
		assert_eq!(u8s::lanes(), f32s::lanes() * 4);
		assert_eq!(f32s::lanes(), u32s::lanes());
		assert_eq!(u8sq::lanes(), u32s::lanes());
		let mut zero = u8s_sad_out::splat(0);
		let mut sum = f32s::splat(0.0);
		for i in (0..self.m.len()).step_by(u8s::lanes()) {
			unsafe {
				let self_m = u8s::from_slice_unaligned_unchecked(self.m.get_unchecked(i..));
				let src_m = u8s::from_slice_unaligned_unchecked(src.m.get_unchecked(i..));
				let res = self_m.max(src_m);
				res.write_to_slice_unaligned_unchecked(self.m.get_unchecked_mut(i..));
				let count: u8s = u8s::splat(0) - u8s::from_bits(res.eq(u8s::splat(0)));
				let count2 = Sad::<u8s>::sad(count, u8s::splat(0));
				zero += count2;
				for j in 0..4 {
					let x = u8sq::from_slice_unaligned_unchecked(
						self.m.get_unchecked(i + j * u8sq::lanes()..),
					);
					let x: u32s = x.cast();
					let x: f32s = ((u32s::splat(u32::max_value()) - x) << 25 >> 2).into_bits();
					sum += x;
				}
			}
		}
		self.zero = zero.wrapping_sum() as usize;
		// assert_eq!(self.zero, self.m.iter().filter(|&&x| x == 0).count());// bytecount::count(&self.m, 0);
		// self.zero = self.m.iter().filter(|&&x| x == 0).count(); //bytecount::count(&self.m, 0);
		self.sum = sum.sum() as f64;
		// let max_rho = 64;//64 - self.p + 1;
		// let sum2 = self
		// 	.m
		// 	.iter()
		// 	.map(|&x| {
		// 		1u128 << (max_rho-x)
		// 	}).sum::<u128>() as f64 / (1u128 << max_rho) as f64;
		// if self.sum != sum2 {
		// 	println!("{} {}", self.sum, sum2);
		// }
		// https://github.com/AdamNiederer/faster/issues/37
		// (src.m.simd_iter(faster::u8s(0)),self.m.simd_iter_mut(faster::u8s(0))).zip()
		// 00011111 00011111 00011111 00011111 00011111 00011111 00011111 00011111
	}

	/// Intersect another HyperLogLog data structure into `self`.
	///
	/// Note: This is different to an HLL approximating cardinality of the intersection of two multisets.
	pub fn intersect(&mut self, src: &Self) {
		assert_eq!(src.alpha, self.alpha);
		assert_eq!(src.p, self.p);
		assert_eq!(src.m.len(), self.m.len());
		// let mut zero = self.zero;
		// src.m
		// 	.iter()
		// 	.zip(self.m.iter_mut())
		// 	.for_each(|(src_mir, mir)| {
		// 		if *mir != 0 && *src_mir == 0 {
		// 			zero += 1;
		// 		}
		// 		*mir = cmp::min(*mir, *src_mir);
		// 	});
		// self.zero = zero;
		use packed_simd::*;
		assert_eq!(self.m.len() % u8s::lanes(), 0);
		assert_eq!(u8s::lanes(), f32s::lanes() * 4);
		assert_eq!(f32s::lanes(), u32s::lanes());
		assert_eq!(u8sq::lanes(), u32s::lanes());
		// is_x86_feature_detected
		let mut zero = u8s_sad_out::splat(0);
		let mut sum = f32s::splat(0.0);
		for i in (0..self.m.len()).step_by(u8s::lanes()) {
			unsafe {
				let self_m = u8s::from_slice_unaligned_unchecked(self.m.get_unchecked(i..));
				let src_m = u8s::from_slice_unaligned_unchecked(src.m.get_unchecked(i..));
				let res = self_m.min(src_m);
				res.write_to_slice_unaligned_unchecked(self.m.get_unchecked_mut(i..));
				let count: u8s = u8s::splat(0) - u8s::from_bits(res.eq(u8s::splat(0)));
				let count2 = Sad::<u8s>::sad(count, u8s::splat(0));
				zero += count2;
				for j in 0..4 {
					let x = u8sq::from_slice_unaligned_unchecked(
						self.m.get_unchecked(i + j * u8sq::lanes()..),
					);
					let x: u32s = x.cast();
					let x: f32s = ((u32s::splat(u32::max_value()) - x) << 25 >> 2).into_bits();
					sum += x;
				}
				// f32::from_bits(u32::max_value().wrapping_sub(x as u32) << 25 >> 2)
				// f64::from_bits(u64::max_value().wrapping_sub(x as u64) << 54 >> 2)
			}
		}
		self.zero = zero.wrapping_sum() as usize;
		// assert_eq!(self.zero, self.m.iter().filter(|&&x| x == 0).count());// bytecount::count(&self.m, 0);
		self.sum = sum.sum() as f64;
		// let max_rho = 64;//64 - self.p + 1;
		// let sum2 = self
		// 	.m
		// 	.iter()
		// 	.map(|&x| {
		// 		1u128 << (max_rho-x)
		// 	}).sum::<u128>() as f64 / (1u128 << max_rho) as f64;
		// if self.sum != sum2 {
		// 	println!("{} {}", self.sum, sum2);
		// }
	}

	/// Clears the `HyperLogLog` data structure, as if it was new.
	pub fn clear(&mut self) {
		self.zero = self.m.len();
		self.m.iter_mut().for_each(|x| {
			*x = 0;
		});
	}

	fn get_threshold(p: u8) -> f64 {
		TRESHOLD_DATA[p as usize]
	}

	fn get_alpha(p: u8) -> f64 {
		assert!(4 <= p && p <= 16);
		match p {
			4 => 0.673,
			5 => 0.697,
			6 => 0.709,
			_ => 0.7213 / (1.0 + 1.079 / (1_u64 << p) as f64),
		}
	}

	fn get_rho(w: u64, max_width: u8) -> u8 {
		let rho = max_width - (64 - w.leading_zeros() as u8) + 1;
		assert!(0 < rho && rho < 65);
		rho
	}

	fn estimate_bias(e: f64, p: u8) -> f64 {
		let bias_vector = BIAS_DATA[(p - 4) as usize];
		let neighbors = Self::get_nearest_neighbors(e, RAW_ESTIMATE_DATA[(p - 4) as usize]);
		assert_eq!(neighbors.len(), 6);
		bias_vector[neighbors].into_iter().sum::<f64>() / 6.0_f64
	}

	fn get_nearest_neighbors(e: f64, estimate_vector: &[f64]) -> Range<usize> {
		// TODO binary search
		// let mut r: Vec<(f64, usize)> = estimate_vector
		// 	.into_iter()
		// 	.enumerate()
		// 	.map(|(i, estimate)| {
		// 		let dr = e - estimate;
		// 		(dr * dr, i)
		// 	}).collect();
		// r.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
		// r.truncate(6);
		// r.sort_by_key(|a| a.1);

		let index = estimate_vector
			.binary_search_by(|a| a.partial_cmp(&e).unwrap_or(Ordering::Equal))
			.unwrap_or_else(identity);
		let mut min = if index > 6 { index - 6 } else { 0 };
		let mut max = cmp::min(index + 6, estimate_vector.len());
		// let mut b: Vec<(f64, usize)> = estimate_vector[min..max].into_iter().enumerate().map(|(i, estimate)| {
		// 		let dr = e - estimate;
		// 		(dr * dr, min+i)
		// 	}).collect();
		// b.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
		// b.truncate(6);

		while max - min != 6 {
			let (min_val, max_val) = unsafe {
				(
					*estimate_vector.get_unchecked(min),
					*estimate_vector.get_unchecked(max - 1),
				)
			};
			// assert!(min_val <= e && e <= max_val);
			if 2.0 * e - min_val > max_val {
				min += 1;
			} else {
				max -= 1;
			}
		}

		// assert_eq!(r[0].1, min);
		// assert_eq!(r[5].1, max - 1);

		min..max
		// let mut c: Vec<(f64, usize)> = estimate_vector[min..max].into_iter().enumerate().map(|(i, estimate)| {
		// 		let dr = e - estimate;
		// 		(dr * dr, min+i)
		// 	}).collect();
		// c.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

		// assert_eq!(b, c);
		// b.into_iter().map(|(_, b)| b)
	}

	fn ep(&self) -> f64 {
		// let sum = self
		// 	.m
		// 	.iter()
		// 	.map(|&x| {
		// 		// see pow_bithack()
		// 		f64::from_bits(u64::max_value().wrapping_sub(x as u64) << 54 >> 2)
		// 	}).sum::<f64>();
		// let max_rho = 64;//64 - self.p + 1;
		// let sum2 = self
		// 	.m
		// 	.iter()
		// 	.map(|&x| {
		// 		1u128 << (max_rho-x)
		// 	}).sum::<u128>();
		// assert_eq!(sum2, self.sum);
		// let sum = self.sum as f64 / (1u128 << max_rho) as f64;
		// if (sum - self.sum).abs() > ::std::f64::EPSILON {
		// 	println!("{} {}", sum, self.sum);
		// }
		// https://github.com/AdamNiederer/faster/issues/59
		// let sum = m.simd_iter(faster::u8s(0)).simd_map(|v| {
		// 	(u64::max_value() - v) << 54 >> 2
		// }).simd_reduce(faster::f64s(0.0), |acc, v| acc + v).sum();
		let e = self.alpha * (self.m.len() * self.m.len()) as f64 / self.sum;
		if e <= (5 * self.m.len()) as f64 {
			e - Self::estimate_bias(e, self.p)
		} else {
			e
		}
	}
}

impl<V: ?Sized> Clone for HyperLogLog<V> {
	fn clone(&self) -> Self {
		Self {
			alpha: self.alpha,
			zero: self.zero,
			sum: self.sum,
			p: self.p,
			m: self.m.clone(),
			marker: PhantomData,
		}
	}
}
impl<V: ?Sized> fmt::Debug for HyperLogLog<V>
where
	V: Hash,
{
	fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
		fmt.debug_struct("HyperLogLog")
			.field("len", &self.len())
			.finish()
	}
}
impl<V: ?Sized> New for HyperLogLog<V>
where
	V: Hash,
{
	type Config = f64;
	fn new(config: &Self::Config) -> Self {
		Self::new(*config)
	}
}
impl<V: ?Sized> Intersect for HyperLogLog<V>
where
	V: Hash,
{
	fn intersect<'a>(mut iter: impl Iterator<Item = &'a Self>) -> Option<Self>
	where
		Self: Sized + 'a,
	{
		let mut ret = iter.next()?.clone();
		iter.for_each(|x| {
			ret.intersect(x);
		});
		Some(ret)
	}
}
impl<'a, V: ?Sized> UnionAssign<&'a HyperLogLog<V>> for HyperLogLog<V>
where
	V: Hash,
{
	fn union_assign(&mut self, rhs: &'a Self) {
		self.union(rhs)
	}
}
impl<'a, V: ?Sized> ops::AddAssign<&'a V> for HyperLogLog<V>
where
	V: Hash,
{
	fn add_assign(&mut self, rhs: &'a V) {
		self.push(rhs)
	}
}
impl<V: ?Sized> IntersectPlusUnionIsPlus for HyperLogLog<V> {
	const VAL: bool = true;
}

#[cfg(target_feature = "avx512bw")] // TODO
mod simd_types {
	use super::*;
	pub type u8s = packed_simd::u8x64;
	pub type u8s_sad_out = packed_simd::u64x8;
	pub type f32s = packed_simd::f32x16;
	pub type u32s = packed_simd::u32x16;
	pub type u8sq = packed_simd::u8x16;
}
#[cfg(target_feature = "avx2")]
mod simd_types {
	use super::*;
	pub type u8s = packed_simd::u8x32;
	pub type u8s_sad_out = packed_simd::u64x4;
	pub type f32s = packed_simd::f32x8;
	pub type u32s = packed_simd::u32x8;
	pub type u8sq = packed_simd::u8x8;
}
#[cfg(all(not(target_feature = "avx2"), target_feature = "sse2"))]
mod simd_types {
	use super::*;
	pub type u8s = packed_simd::u8x16;
	pub type u8s_sad_out = packed_simd::u64x2;
	pub type f32s = packed_simd::f32x4;
	pub type u32s = packed_simd::u32x4;
	pub type u8sq = packed_simd::u8x4;
}
#[cfg(all(not(target_feature = "avx2"), not(target_feature = "sse2")))]
mod simd_types {
	use super::*;
	pub type u8s = packed_simd::u8x8;
	pub type u8s_sad_out = u64;
	pub type f32s = packed_simd::f32x2;
	pub type u32s = packed_simd::u32x2;
	pub type u8sq = packed_simd::u8x2;
}
use self::simd_types::*;

struct Sad<X>(PhantomData<fn(X)>);
// TODO
// #[cfg(target_feature = "avx512bw")]
// impl Sad<packed_simd::u8x64> {
// 	#[inline]
// 	#[target_feature(enable = "avx512bw")]
// 	unsafe fn sad(a: packed_simd::u8x64, b: packed_simd::u8x64) -> packed_simd::u64x8 {
// 		use std::{arch::x86_64::_mm512_sad_epu8, mem::transmute};
// 		packed_simd::Simd(transmute(_mm512_sad_epu8(transmute(a.0), transmute(b.0))))
// 	}
// }
#[cfg(target_feature = "avx2")]
impl Sad<packed_simd::u8x32> {
	#[inline]
	#[target_feature(enable = "avx2")]
	unsafe fn sad(a: packed_simd::u8x32, b: packed_simd::u8x32) -> packed_simd::u64x4 {
		use std::{arch::x86_64::_mm256_sad_epu8, mem::transmute};
		packed_simd::Simd(transmute(_mm256_sad_epu8(transmute(a.0), transmute(b.0))))
	}
}
#[cfg(target_feature = "sse2")]
impl Sad<packed_simd::u8x16> {
	#[inline]
	#[target_feature(enable = "sse2")]
	unsafe fn sad(a: packed_simd::u8x16, b: packed_simd::u8x16) -> packed_simd::u64x2 {
		use std::{arch::x86_64::_mm_sad_epu8, mem::transmute};
		packed_simd::Simd(transmute(_mm_sad_epu8(transmute(a.0), transmute(b.0))))
	}
}
#[cfg(target_feature = "sse,mmx")]
impl Sad<packed_simd::u8x8> {
	#[inline]
	#[target_feature(enable = "sse,mmx")]
	unsafe fn sad(a: packed_simd::u8x8, b: packed_simd::u8x8) -> u64 {
		use std::{arch::x86_64::_mm_sad_pu8, mem::transmute};
		transmute(_mm_sad_pu8(transmute(a.0), transmute(b.0)))
	}
}
#[cfg(not(target_feature = "sse,mmx"))]
impl Sad<packed_simd::u8x8> {
	#[inline(always)]
	unsafe fn sad(a: packed_simd::u8x8, b: packed_simd::u8x8) -> u64 {
		assert_eq!(b, packed_simd::u8x8::splat(0));
		(0..8).map(|i| a.extract(i) as u64).sum()
	}
}

#[cfg(test)]
mod test {
	use super::HyperLogLog;
	use std::f64;

	#[test]
	fn pow_bithack() {
		// build the float from x, manipulating it to be the mantissa we want.
		// no portability issues in theory https://doc.rust-lang.org/stable/std/primitive.f64.html#method.from_bits
		for x in 0_u8..65 {
			let a = 2.0_f64.powi(-(x as i32));
			let b = f64::from_bits(u64::max_value().wrapping_sub(x as u64) << 54 >> 2);
			assert_eq!(a, b);
		}
	}

	#[test]
	fn hyperloglog_test_simple() {
		let mut hll = HyperLogLog::new(0.00408);
		let keys = ["test1", "test2", "test3", "test2", "test2", "test2"];
		for k in &keys {
			hll.push(k);
		}
		assert!((hll.len().round() - 3.0).abs() < f64::EPSILON);
		assert!(!hll.is_empty());
		hll.clear();
		assert!(hll.is_empty());
		assert!(hll.len() == 0.0);
	}

	#[test]
	fn hyperloglog_test_merge() {
		let mut hll = HyperLogLog::new(0.00408);
		let keys = ["test1", "test2", "test3", "test2", "test2", "test2"];
		for k in &keys {
			hll.push(k);
		}
		assert!((hll.len().round() - 3.0).abs() < f64::EPSILON);

		let mut hll2 = HyperLogLog::new_from(&hll);
		let keys2 = ["test3", "test4", "test4", "test4", "test4", "test1"];
		for k in &keys2 {
			hll2.push(k);
		}
		assert!((hll2.len().round() - 3.0).abs() < f64::EPSILON);

		hll.union(&hll2);
		assert!((hll.len().round() - 4.0).abs() < f64::EPSILON);
	}

	#[test]
	fn push() {
		let actual = 100_000.0;
		let p = 0.05;
		let mut hll = HyperLogLog::new(p);
		for i in 0..actual as usize {
			hll.push(&i);
		}

		// assert_eq!(111013.12482663046, hll.len());

		assert!(hll.len() > (actual - (actual * p * 3.0)));
		assert!(hll.len() < (actual + (actual * p * 3.0)));
	}
}
