use either::Either;

use amadeus::dist::prelude::*;

#[tokio::main]
async fn main() {
	let pool = ThreadPool::new(None).unwrap();

	<&[usize] as IntoDistributedStream>::into_dist_stream(&[1, 2, 3])
		.map(FnMut!(|a: usize| a))
		.for_each(&pool, FnMut!(|a: usize| println!("{:?}", a)))
		.await;

	let res: usize = [1, 2, 3].into_dist_stream().sum(&pool).await;
	assert_eq!(res, 6);

	let slice = [
		0_usize, 1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73,
		79, 83, 89, 97,
	];
	for i in 0..slice.len() {
		let res = slice[..i]
			.dist_stream()
			.into_dist_stream()
			.fold(
				&pool,
				FnMut!(|| 0_usize),
				FnMut!(|a: usize, b: Either<usize, usize>| a + b.into_inner()),
			)
			.await;
		assert_eq!(res, slice[..i].iter().sum::<usize>());
	}
	let sum: usize = slice.iter().cloned().dist().sum(&pool).await;
	assert_eq!(sum, slice.iter().sum::<usize>());
}
