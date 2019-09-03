#![allow(where_clauses_object_safety)]

#[macro_use]
extern crate serde_closure;

use amadeus::prelude::*;
use constellation::*;
use either::Either;

fn main() {
	init(Resources::default());

	let pool = ProcessPool::new(3, 1, Resources::default()).unwrap();

	<&[usize] as IntoDistributedIterator>::into_dist_iter(&[1, 2, 3])
		.map(FnMut!(|a: usize| a))
		.for_each(&pool, FnMut!(|a: usize| println!("{:?}", a)));

	// let res = [1, 2, 3].into_dist_iter().sum::<usize>(&pool);
	// assert_eq!(res, 6);

	let slice = [
		0usize, 1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73,
		79, 83, 89, 97,
	];
	for i in 0..slice.len() {
		let res = slice[..i].dist_iter().into_dist_iter().fold(
			&pool,
			FnMut!(|| 0usize),
			FnMut!(|a: usize, b: Either<usize, usize>| a + b.into_inner()),
		);
		assert_eq!(res, slice[..i].iter().sum::<usize>());
	}
	// assert_eq!(
	// 	slice.iter().cloned().dist().sum::<usize>(&pool),
	// 	slice.iter().sum::<usize>()
	// );
}
