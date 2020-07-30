#![cfg(nightly)]
#![feature(test)]
#![allow(clippy::suspicious_map)]

extern crate test;

use once_cell::sync::Lazy;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::{future::Future, mem};
use test::Bencher;
use tokio::runtime::Runtime;

use amadeus::prelude::*;

static RT: Lazy<Runtime> = Lazy::new(|| {
	tokio::runtime::Builder::new()
		.threaded_scheduler()
		.enable_all()
		.build()
		.unwrap()
});
static POOL: Lazy<ThreadPool> = Lazy::new(|| ThreadPool::new(None).unwrap());

#[bench]
fn vec(b: &mut Bencher) {
	let rows: Vec<u32> = (0..1u32 << 28).collect();
	let len = rows.len() as u64;
	let sum = len * (len - 1) / 2;
	let bytes = len * mem::size_of::<u32>() as u64;
	run(b, bytes, || async {
		assert_eq!(
			rows.par_stream()
				.map(|x| x as u64)
				.sum::<_, u64>(&*POOL)
				.await,
			sum
		);
	})
}

#[bench]
fn iter(b: &mut Bencher) {
	let rows: Vec<u32> = (0..1u32 << 28).collect();
	let len = rows.len() as u64;
	let sum = len * (len - 1) / 2;
	let bytes = len * mem::size_of::<u32>() as u64;
	run(b, bytes, || async {
		assert_eq!(rows.iter().map(|&x| x as u64).sum::<u64>(), sum);
	});
}

#[bench]
fn rayon(b: &mut Bencher) {
	let rows: Vec<u32> = (0..1u32 << 28).collect();
	let len = rows.len() as u64;
	let sum = len * (len - 1) / 2;
	let bytes = len * mem::size_of::<u32>() as u64;
	run(b, bytes, || async {
		assert_eq!(rows.par_iter().map(|&x| x as u64).sum::<u64>(), sum);
	});
}

fn run<F>(b: &mut Bencher, bytes: u64, mut task: impl FnMut() -> F)
where
	F: Future<Output = ()>,
{
	RT.enter(|| {
		let _ = Lazy::force(&POOL);
		b.bytes = bytes;
		b.iter(|| RT.handle().block_on(task()))
	})
}
