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
// is_x86_feature_detected ?

#[cfg(feature = "protobuf")]
use crate::proto_util;
#[cfg(feature = "protobuf")]
use protobuf::CodedOutputStream;

use serde::{Deserialize, Serialize};
use std::{
	cmp::{self, Ordering}, convert::{identity, TryFrom}, fmt, hash::{Hash, Hasher}, marker::PhantomData, ops::{self, Range}
};
use twox_hash::XxHash;

use super::{f64_to_u8, u64_to_f64, usize_to_f64};
use crate::traits::{Intersect, IntersectPlusUnionIsPlus, New, UnionAssign};

mod consts;
use self::consts::{BIAS_DATA, RAW_ESTIMATE_DATA, TRESHOLD_DATA};

/// Like [`HyperLogLog`] but implements `Ord` and `Eq` by using the estimate of the cardinality.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct HyperLogLogMagnitude<V>(HyperLogLog<V>);
impl<V: Hash> Ord for HyperLogLogMagnitude<V> {
	#[inline(always)]
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.len().partial_cmp(&other.0.len()).unwrap()
	}
}
impl<V: Hash> PartialOrd for HyperLogLogMagnitude<V> {
	#[inline(always)]
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.0.len().partial_cmp(&other.0.len())
	}
}
impl<V: Hash> PartialEq for HyperLogLogMagnitude<V> {
	#[inline(always)]
	fn eq(&self, other: &Self) -> bool {
		self.0.len().eq(&other.0.len())
	}
}
impl<V: Hash> Eq for HyperLogLogMagnitude<V> {}
impl<V: Hash> Clone for HyperLogLogMagnitude<V> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}
impl<V: Hash> New for HyperLogLogMagnitude<V> {
	type Config = f64;
	fn new(config: &Self::Config) -> Self {
		Self(New::new(config))
	}
}
impl<V: Hash> Intersect for HyperLogLogMagnitude<V> {
	fn intersect<'a>(iter: impl Iterator<Item = &'a Self>) -> Option<Self>
	where
		Self: Sized + 'a,
	{
		Intersect::intersect(iter.map(|x| &x.0)).map(Self)
	}
}
impl<'a, V: Hash> UnionAssign<&'a HyperLogLogMagnitude<V>> for HyperLogLogMagnitude<V> {
	fn union_assign(&mut self, rhs: &'a Self) {
		self.0.union_assign(&rhs.0)
	}
}
impl<'a, V: Hash> ops::AddAssign<&'a V> for HyperLogLogMagnitude<V> {
	fn add_assign(&mut self, rhs: &'a V) {
		self.0.add_assign(rhs)
	}
}
impl<'a, V: Hash> ops::AddAssign<&'a Self> for HyperLogLogMagnitude<V> {
	fn add_assign(&mut self, rhs: &'a Self) {
		self.0.add_assign(&rhs.0)
	}
}
impl<V: Hash> fmt::Debug for HyperLogLogMagnitude<V> {
	fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
		self.0.fmt(fmt)
	}
}
impl<V> IntersectPlusUnionIsPlus for HyperLogLogMagnitude<V> {
	const VAL: bool = <HyperLogLog<V> as IntersectPlusUnionIsPlus>::VAL;
}

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
		let p = f64_to_u8((f64::log2(1.04 / error_rate) * 2.0).ceil());
		assert!(0 < p && p < 64);
		let alpha = Self::get_alpha(p);
		Self {
			alpha,
			zero: 1 << p,
			sum: f64::from(1 << p),
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
			sum: usize_to_f64(hll.m.len()),
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
		let mjr = &mut self.m[usize::try_from(j).unwrap()];
		let old = *mjr;
		let new = cmp::max(old, rho);
		self.zero -= if old == 0 { 1 } else { 0 };

		// see pow_bithack()
		self.sum -= f64::from_bits(u64::max_value().wrapping_sub(u64::from(old)) << 54 >> 2)
			- f64::from_bits(u64::max_value().wrapping_sub(u64::from(new)) << 54 >> 2);

		*mjr = new;
	}

	/// Retrieve an estimate of the carginality of the stream.
	pub fn len(&self) -> f64 {
		let v = self.zero;
		if v > 0 {
			let h =
				usize_to_f64(self.m.len()) * (usize_to_f64(self.m.len()) / usize_to_f64(v)).ln();
			if h <= Self::get_threshold(self.p - 4) {
				return h;
			}
		}
		self.ep()
	}

	/// Returns true if empty.
	pub fn is_empty(&self) -> bool {
		self.zero == self.m.len()
	}

	/// Merge another HyperLogLog data structure into `self`.
	///
	/// This is the same as an HLL approximating cardinality of the union of two multisets.
	pub fn union(&mut self, src: &Self) {
		assert_eq!(src.alpha, self.alpha);
		assert_eq!(src.p, self.p);
		assert_eq!(src.m.len(), self.m.len());
		#[cfg(all(
			feature = "packed_simd",
			any(target_arch = "x86", target_arch = "x86_64")
		))]
		{
			assert_eq!(self.m.len() % u8s::lanes(), 0); // TODO: high error rate can trigger this
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
			self.zero = usize::try_from(zero.wrapping_sum()).unwrap();
			self.sum = f64::from(sum.sum());
			// https://github.com/AdamNiederer/faster/issues/37
			// (src.m.simd_iter(faster::u8s(0)),self.m.simd_iter_mut(faster::u8s(0))).zip()
		}
		#[cfg(not(all(
			feature = "packed_simd",
			any(target_arch = "x86", target_arch = "x86_64")
		)))]
		{
			let mut zero = 0;
			let mut sum = 0.0;
			for (to, from) in self.m.iter_mut().zip(src.m.iter()) {
				*to = (*to).max(*from);
				zero += if *to == 0 { 1 } else { 0 };
				sum += f64::from_bits(u64::max_value().wrapping_sub(u64::from(*to)) << 54 >> 2);
			}
			self.zero = zero;
			self.sum = sum;
		}
	}

	/// Intersect another HyperLogLog data structure into `self`.
	///
	/// Note: This is different to an HLL approximating cardinality of the intersection of two multisets.
	pub fn intersect(&mut self, src: &Self) {
		assert_eq!(src.alpha, self.alpha);
		assert_eq!(src.p, self.p);
		assert_eq!(src.m.len(), self.m.len());
		#[cfg(all(
			feature = "packed_simd",
			any(target_arch = "x86", target_arch = "x86_64")
		))]
		{
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
				}
			}
			self.zero = usize::try_from(zero.wrapping_sum()).unwrap();
			self.sum = f64::from(sum.sum());
		}
		#[cfg(not(all(
			feature = "packed_simd",
			any(target_arch = "x86", target_arch = "x86_64")
		)))]
		{
			let mut zero = 0;
			let mut sum = 0.0;
			for (to, from) in self.m.iter_mut().zip(src.m.iter()) {
				*to = (*to).min(*from);
				zero += if *to == 0 { 1 } else { 0 };
				sum += f64::from_bits(u64::max_value().wrapping_sub(u64::from(*to)) << 54 >> 2);
			}
			self.zero = zero;
			self.sum = sum;
		}
	}

	/// Clears the `HyperLogLog` data structure, as if it was new.
	pub fn clear(&mut self) {
		self.zero = self.m.len();
		self.sum = usize_to_f64(self.m.len());
		self.m.iter_mut().for_each(|x| {
			*x = 0;
		});
	}

	/// Returns a serialized representation of this HLL directly by BigQuery.
	/// See [Zetasketch from Google](https://github.com/google/zetasketch) for more details on the serialization format.
	/// See [HLL functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions) in BigQuery
	/// to understand how to store and use the sketches generated by this function.
	/// Note: this serialization  function doesn't support the sparse format as defined in Zetasketch mostly because
	/// this HyperLogLogPlusPlus implementation doesn't support it.
	#[cfg(feature = "protobuf")]
	pub fn serialize_as_proto(&self) -> Vec<u8> {
		let mut buf = Vec::new();
		let mut stream = CodedOutputStream::new(&mut buf);

		// We use the NoTag write methods for consistency with the parsing functions and for
		// consistency with the variable-length writes where we can't use any convenience function.
		stream.write_uint32_no_tag(proto_util::TYPE_TAG).unwrap();
		stream
			.write_enum_no_tag(proto_util::AGGREGATION_TYPE)
			.unwrap();

		stream
			.write_uint32_no_tag(proto_util::NUM_VALUES_TAG)
			.unwrap();
		// Should be the number of values inserted into this HLL, but BigQuery doesn't really care and this
		// implementation doesn't maintain this value.
		stream.write_int64_no_tag(0).unwrap();

		stream
			.write_uint32_no_tag(proto_util::ENCODING_VERSION_TAG)
			.unwrap();
		stream
			.write_int32_no_tag(proto_util::HYPERLOGLOG_PLUS_PLUS_ENCODING_VERSION)
			.unwrap();

		stream
			.write_uint32_no_tag(proto_util::VALUE_TYPE_TAG)
			.unwrap();
		stream
			.write_enum_no_tag(proto_util::BYTES_OR_UTF8_STRING_TYPE)
			.unwrap();

		let hll_size = self.serialized_hll_size();
		stream
			.write_uint32_no_tag(proto_util::HYPERLOGLOGPLUS_UNIQUE_STATE_TAG)
			.unwrap();
		stream.write_uint32_no_tag(hll_size as u32).unwrap();

		stream
			.write_uint32_no_tag(proto_util::PRECISION_OR_NUM_BUCKETS_TAG)
			.unwrap();
		stream.write_int32_no_tag(i32::from(self.p)).unwrap();

		stream.write_uint32_no_tag(proto_util::DATA_TAG).unwrap();
		stream.write_uint32_no_tag(self.m.len() as u32).unwrap();
		stream.write_raw_bytes(&self.m).unwrap();

		stream.flush().unwrap();

		buf
	}

	#[cfg(feature = "protobuf")]
	fn serialized_hll_size(&self) -> usize {
		let mut size = 0_usize;

		size += proto_util::compute_uint32_size_no_tag(proto_util::PRECISION_OR_NUM_BUCKETS_TAG);
		size += proto_util::compute_int32_size_no_tag(i32::from(self.p));

		let data_length = self.m.len();
		size += proto_util::compute_uint32_size_no_tag(proto_util::DATA_TAG);
		size += proto_util::compute_uint32_size_no_tag(data_length as u32);
		size += data_length;

		size
	}

	fn get_threshold(p: u8) -> f64 {
		TRESHOLD_DATA[p as usize]
	}

	fn get_alpha(p: u8) -> f64 {
		assert!((4..=16).contains(&p));
		match p {
			4 => 0.673,
			5 => 0.697,
			6 => 0.709,
			_ => 0.7213 / (1.0 + 1.079 / u64_to_f64(1_u64 << p)),
		}
	}

	fn get_rho(w: u64, max_width: u8) -> u8 {
		let rho = max_width - (64 - u8::try_from(w.leading_zeros()).unwrap()) + 1;
		assert!(0 < rho && rho < 65);
		rho
	}

	fn estimate_bias(e: f64, p: u8) -> f64 {
		let bias_vector = BIAS_DATA[(p - 4) as usize];
		let neighbors = Self::get_nearest_neighbors(e, RAW_ESTIMATE_DATA[(p - 4) as usize]);
		assert_eq!(neighbors.len(), 6);
		bias_vector[neighbors].iter().sum::<f64>() / 6.0_f64
	}

	fn get_nearest_neighbors(e: f64, estimate_vector: &[f64]) -> Range<usize> {
		let index = estimate_vector
			.binary_search_by(|a| a.partial_cmp(&e).unwrap_or(Ordering::Equal))
			.unwrap_or_else(identity);

		let mut min = if index > 6 { index - 6 } else { 0 };
		let mut max = cmp::min(index + 6, estimate_vector.len());

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

		min..max
	}

	fn ep(&self) -> f64 {
		let e = self.alpha * usize_to_f64(self.m.len() * self.m.len()) / self.sum;
		if e <= usize_to_f64(5 * self.m.len()) {
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
impl<'a, V: ?Sized> ops::AddAssign<&'a Self> for HyperLogLog<V>
where
	V: Hash,
{
	fn add_assign(&mut self, rhs: &'a Self) {
		self.union(rhs)
	}
}
impl<V: ?Sized> IntersectPlusUnionIsPlus for HyperLogLog<V> {
	const VAL: bool = true;
}

#[cfg(all(
	feature = "packed_simd",
	any(target_arch = "x86", target_arch = "x86_64")
))]
mod simd {
	pub use packed_simd::{self, Cast, FromBits, IntoBits};
	use std::marker::PhantomData;

	#[cfg(target_feature = "avx512bw")] // TODO
	mod simd_types {
		use super::packed_simd;
		pub type u8s = packed_simd::u8x64;
		pub type u8s_sad_out = packed_simd::u64x8;
		pub type f32s = packed_simd::f32x16;
		pub type u32s = packed_simd::u32x16;
		pub type u8sq = packed_simd::u8x16;
	}
	#[cfg(target_feature = "avx2")]
	mod simd_types {
		#![allow(non_camel_case_types)]
		use super::packed_simd;
		pub type u8s = packed_simd::u8x32;
		pub type u8s_sad_out = packed_simd::u64x4;
		pub type f32s = packed_simd::f32x8;
		pub type u32s = packed_simd::u32x8;
		pub type u8sq = packed_simd::u8x8;
	}
	#[cfg(all(not(target_feature = "avx2"), target_feature = "sse2"))]
	mod simd_types {
		#![allow(non_camel_case_types)]
		use super::packed_simd;
		pub type u8s = packed_simd::u8x16;
		pub type u8s_sad_out = packed_simd::u64x2;
		pub type f32s = packed_simd::f32x4;
		pub type u32s = packed_simd::u32x4;
		pub type u8sq = packed_simd::u8x4;
	}
	#[cfg(all(not(target_feature = "avx2"), not(target_feature = "sse2")))]
	mod simd_types {
		#![allow(non_camel_case_types)]
		use super::packed_simd;
		pub type u8s = packed_simd::u8x8;
		pub type u8s_sad_out = u64;
		pub type f32s = packed_simd::f32x2;
		pub type u32s = packed_simd::u32x2;
		pub type u8sq = packed_simd::u8x2;
	}
	pub use self::simd_types::{f32s, u32s, u8s, u8s_sad_out, u8sq};

	pub struct Sad<X>(PhantomData<fn(X)>);
	#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
	mod x86 {
		#[cfg(target_arch = "x86")]
		pub use std::arch::x86::*;
		#[cfg(target_arch = "x86_64")]
		pub use std::arch::x86_64::*;
	}
	// TODO
	// #[cfg(target_feature = "avx512bw")]
	// impl Sad<packed_simd::u8x64> {
	// 	#[inline]
	// 	#[target_feature(enable = "avx512bw")]
	// 	pub unsafe fn sad(a: packed_simd::u8x64, b: packed_simd::u8x64) -> packed_simd::u64x8 {
	// 		use std::mem::transmute;
	// 		packed_simd::Simd(transmute(x86::_mm512_sad_epu8(transmute(a.0), transmute(b.0))))
	// 	}
	// }
	#[cfg(target_feature = "avx2")]
	impl Sad<packed_simd::u8x32> {
		#[inline]
		#[target_feature(enable = "avx2")]
		pub unsafe fn sad(a: packed_simd::u8x32, b: packed_simd::u8x32) -> packed_simd::u64x4 {
			use std::mem::transmute;
			packed_simd::Simd(transmute(x86::_mm256_sad_epu8(
				transmute(a.0),
				transmute(b.0),
			)))
		}
	}
	#[cfg(target_feature = "sse2")]
	impl Sad<packed_simd::u8x16> {
		#[inline]
		#[target_feature(enable = "sse2")]
		pub unsafe fn sad(a: packed_simd::u8x16, b: packed_simd::u8x16) -> packed_simd::u64x2 {
			use std::mem::transmute;
			packed_simd::Simd(transmute(x86::_mm_sad_epu8(transmute(a.0), transmute(b.0))))
		}
	}
	#[cfg(target_feature = "sse,mmx")]
	impl Sad<packed_simd::u8x8> {
		#[inline]
		#[target_feature(enable = "sse,mmx")]
		pub unsafe fn sad(a: packed_simd::u8x8, b: packed_simd::u8x8) -> u64 {
			use std::mem::transmute;
			transmute(x86::_mm_sad_pu8(transmute(a.0), transmute(b.0)))
		}
	}
	#[cfg(not(target_feature = "sse,mmx"))]
	impl Sad<packed_simd::u8x8> {
		#[inline(always)]
		pub unsafe fn sad(a: packed_simd::u8x8, b: packed_simd::u8x8) -> u64 {
			assert_eq!(b, packed_simd::u8x8::splat(0));
			(0..8).map(|i| u64::from(a.extract(i))).sum()
		}
	}
}
#[cfg(all(
	feature = "packed_simd",
	any(target_arch = "x86", target_arch = "x86_64")
))]
use simd::{f32s, u32s, u8s, u8s_sad_out, u8sq, Cast, FromBits, IntoBits, Sad};

#[cfg(test)]
mod test {
	use super::{super::f64_to_usize, HyperLogLog};
	use std::f64;

	#[test]
	fn pow_bithack() {
		// build the float from x, manipulating it to be the mantissa we want.
		// no portability issues in theory https://doc.rust-lang.org/stable/std/primitive.f64.html#method.from_bits
		for x in 0_u8..65 {
			let a = 2.0_f64.powi(-(i32::from(x)));
			let b = f64::from_bits(u64::max_value().wrapping_sub(u64::from(x)) << 54 >> 2);
			let c = f32::from_bits(u32::max_value().wrapping_sub(u32::from(x)) << 25 >> 2);
			assert_eq!(a, b);
			assert_eq!(a, f64::from(c));
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
	#[cfg_attr(miri, ignore)]
	fn push() {
		let actual = 100_000.0;
		let p = 0.05;
		let mut hll = HyperLogLog::new(p);
		for i in 0..f64_to_usize(actual) {
			hll.push(&i);
		}

		// assert_eq!(111013.12482663046, hll.len());

		assert!(hll.len() > (actual - (actual * p * 3.0)));
		assert!(hll.len() < (actual + (actual * p * 3.0)));
	}

	#[test]
	#[cfg(feature = "protobuf")]
	fn hyperloglog_test_proto() {
		let p = 0.05;
		let mut hll = HyperLogLog::new(p);
		let actual = 10_000;

		for i in 0..actual {
			hll.push(&format!("test-{}", i * 10 + i));
		}

		assert!(hll.len() > (actual as f64 - (actual as f64 * p * 3.0)));
		assert!(hll.len() < (actual as f64 + (actual as f64 * p * 3.0)));

		// Check on bigQuery with the following query: SELECT HLL_COUNT.EXTRACT(FROM_HEX("<hex_value>"))
		let expected_ser = "087010001802200b8207850418092a800405080409040407050a0407060507040303080304070504090f08060604060503050505030904070506060904040608060505040706040905040706070b090304040609060505070406060508060505070604050404040507050409040502090605050a0505060203050605080a0605060505040404050705050304060506070608070304050306060304080604060a0704060904070804050706080704080409040604060607040d040604060304040503060406070606050308050606050409040707050605070305040707050305070a06070905040506060905050504060804030605050505080703060c03040405040a03070405090303030204030703060407040404050608050504060604050505090404060504050603080705040606060405050405040506060404050308040604080a06040607040606040707080405040505040603050607060508050307060706070305050403040304050504030407080506040407040704050605060a06050b0607030603050406050506030504050705040504090606030504040704030704030805030504050706090407070604070405060307060a08050507070605090407030604040404030706040405040804050306040504040506020505040607060604060808060508030605030604050b04080a0904050506060405060806070606080605040606040606060607";
		assert_eq!(hex::encode(&hll.serialize_as_proto()), expected_ser);
	}
}
