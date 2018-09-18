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

use bytecount;
use std::{
	cmp::{self, Ordering}, fmt, hash::{Hash, Hasher}, iter::repeat, marker::PhantomData, ops
};
use traits::{Intersect, New, UnionAssign};
use twox_hash::XxHash;

mod consts;
use self::consts::*;

/// An implementation of the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) data structure with *bias correction*.
///
/// See [*HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm*](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) and [HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm](https://ai.google/research/pubs/pub40671) for background on HyperLogLog with bias correction.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct HyperLogLog<V: ?Sized> {
	alpha: f64,
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
		assert!(p <= 64);
		let alpha = Self::get_alpha(p);
		Self {
			alpha,
			p,
			m: vec![0; 1 << p].into_boxed_slice(),
			marker: PhantomData,
		}
	}

	/// Create an empty `HyperLogLog` data structure, copying the error tolerance from `hll`.
	pub fn new_from(hll: &Self) -> Self {
		Self {
			alpha: hll.alpha,
			p: hll.p,
			m: vec![0; hll.m.len()].into_boxed_slice(),
			marker: PhantomData,
		}
	}

	/// "Visit" an element.
	pub fn push(&mut self, value: &V) {
		let mut hasher = XxHash::default();
		value.hash(&mut hasher);
		let x = hasher.finish();
		let j = x as usize & (self.m.len() - 1);
		let w = x >> self.p;
		let rho = Self::get_rho(w, 64 - self.p);
		let mjr = &mut self.m[j];
		*mjr = cmp::max(*mjr, rho);
	}

	/// Retrieve an estimate of the carginality of the stream.
	pub fn len(&self) -> f64 {
		let v = Self::vec_count_zero(&self.m);
		if v > 0 {
			let h = self.m.len() as f64 * (self.m.len() as f64 / v as f64).ln();
			if h <= Self::get_treshold(self.p - 4) {
				h
			} else {
				self.ep()
			}
		} else {
			self.ep()
		}
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
		src.m
			.iter()
			.zip(self.m.iter_mut())
			.for_each(|(src_mir, mir)| {
				*mir = cmp::max(*mir, *src_mir);
			});
	}

	/// Intersect another HyperLogLog data structure into `self`.
	///
	/// Note: This is different to an HLL approximating cardinality of the intersection of two multisets.
	pub fn intersect(&mut self, src: &Self) {
		assert_eq!(src.alpha, self.alpha);
		assert_eq!(src.p, self.p);
		assert_eq!(src.m.len(), self.m.len());
		src.m
			.iter()
			.zip(self.m.iter_mut())
			.for_each(|(src_mir, mir)| {
				*mir = cmp::min(*mir, *src_mir);
			});
	}

	/// Clears the `HyperLogLog` data structure, as if it was new.
	pub fn clear(&mut self) {
		self.m.iter_mut().for_each(|x| {
			*x = 0;
		});
	}

	fn get_treshold(p: u8) -> f64 {
		TRESHOLD_DATA[p as usize]
	}

	fn get_alpha(p: u8) -> f64 {
		assert!(4 <= p && p <= 16);
		match p {
			4 => 0.673,
			5 => 0.697,
			6 => 0.709,
			_ => 0.7213 / (1.0 + 1.079 / (1_usize << (p as usize)) as f64),
		}
	}

	fn get_rho(w: u64, max_width: u8) -> u8 {
		let rho = max_width - (64 - w.leading_zeros() as u8) + 1;
		assert!(rho > 0);
		rho
	}

	fn vec_count_zero(v: &[u8]) -> usize {
		// v.iter().filter(|&x| *x == 0).count()
		bytecount::count(v, 0)
	}

	fn estimate_bias(e: f64, p: u8) -> f64 {
		let bias_vector = BIAS_DATA[(p - 4) as usize];
		let nearest_neighbors = Self::get_nearest_neighbors(e, RAW_ESTIMATE_DATA[(p - 4) as usize]);
		let sum = nearest_neighbors
			.iter()
			.map(|&neighbor| bias_vector[neighbor])
			.sum::<f64>();
		sum / nearest_neighbors.len() as f64
	}

	fn get_nearest_neighbors(e: f64, estimate_vector: &[f64]) -> Vec<usize> {
		let ev_len = estimate_vector.len();
		let mut r: Vec<(f64, usize)> = repeat((0.0, 0)).take(ev_len).collect();
		for i in 0..ev_len {
			let dr = e - estimate_vector[i];
			r[i] = (dr * dr, i);
		}
		r.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
		r.truncate(6);
		r.iter()
			.map(|&ez| match ez {
				(_, b) => b,
			}).collect()
	}

	fn ep(&self) -> f64 {
		let sum = self
			.m
			.iter()
			.fold(0.0, |acc, &x| acc + 2.0_f64.powi(-(x as i32)));
		let e = self.alpha * (self.m.len() * self.m.len()) as f64 / sum;
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
		for x in iter {
			ret.intersect(x);
		}
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

#[cfg(test)]
mod test {
	use super::HyperLogLog;
	use std::f64;
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
