#![warn(
	missing_copy_implementations,
	missing_debug_implementations,
	// missing_docs,
	trivial_numeric_casts,
	unused_extern_crates,
	unused_import_braces,
	unused_qualifications,
	unused_results,
	// clippy::pedantic
)] // from https://github.com/rust-unofficial/patterns/blob/master/anti_patterns/deny-warnings.md
#![allow(unreachable_code, unused_braces, clippy::type_complexity)]

#[cfg(feature = "constellation")]
use constellation::*;
use std::time::{Duration, SystemTime};

use amadeus::prelude::*;
use data::Webpage;

fn main() {
	#[cfg(feature = "constellation")]
	init(Resources::default());

	tokio::runtime::Builder::new()
		.threaded_scheduler()
		.enable_all()
		.build()
		.unwrap()
		.block_on(async {
			return; // TODO: runs for a long time

			let thread_pool_time = {
				let thread_pool = ThreadPool::new(None);
				run(&thread_pool).await
			};
			#[cfg(feature = "constellation")]
			let process_pool_time = {
				let process_pool = ProcessPool::new(None, None, Resources::default()).unwrap();
				run(&process_pool).await
			};
			#[cfg(not(feature = "constellation"))]
			let process_pool_time = "-";

			println!("in {:?} {:?}", thread_pool_time, process_pool_time);
		})
}

async fn run<P: amadeus_core::pool::ProcessPool>(pool: &P) -> Duration {
	let start = SystemTime::now();

	let webpages = CommonCrawl::new("CC-MAIN-2020-24").await.unwrap();
	let _ = webpages
		.dist_stream()
		.all(
			pool,
			FnMut!(move |x: Result<Webpage<'static>, _>| -> bool {
				let _x = x.unwrap();
				// println!("{}", x.url);
				start.elapsed().unwrap() < Duration::new(10, 0)
			}),
		)
		.await;

	let webpages = CommonCrawl::new("CC-MAIN-2020-24").await.unwrap();

	let top: (
		((
			// Vec<u32>,
			),),
		(
			u32,
			u32,
			std::collections::HashSet<u32>,
			streaming_algorithms::Top<u32,usize>,
			streaming_algorithms::Top<usize,streaming_algorithms::HyperLogLogMagnitude<Vec<u8>>>,
			streaming_algorithms::SampleUnstable<u32>,
		),
	) = webpages.dist_stream().map(FnMut!(|webpage:Result<_,_>|webpage.unwrap()))
		.fork(
			&pool,
			((
				// Identity
				// 	.map(FnMut!(|x: Webpage<'static>| -> usize { x.contents.len() }))
				// 	.map(FnMut!(|x: usize| -> u32 { x as u32 }))
				// 	.collect(),
				// (),
				// Identity
				// 	.map(FnMut!(|x: Webpage<'static>| -> usize {
				// 		x.contents.len()
				// 	}))
				// 	.map(FnMut!(|x: usize| -> u32 { x as u32 }))
				// 	.collect(),
			),),
			(
				Identity
					.map(FnMut!(|x: &Webpage<'static>| -> usize { x.contents.len() }))
					.map(FnMut!(|x: usize| -> u32 { x as u32 }))
					.fold(
						FnMut!(|| 0_u32),
						FnMut!(|a: u32, b: either::Either<u32, u32>| a + b.into_inner()),
					),
				Identity
					.map(FnMut!(|x: &Webpage<'static>| -> usize { x.contents.len() }))
					.map(FnMut!(|x: usize| -> u32 { x as u32 }))
					.sum(),
				Identity
					.map(FnMut!(|x: &Webpage<'static>| -> usize { x.contents.len() }))
					.map(FnMut!(|x: usize| -> u32 { x as u32 }))
					.collect(),
				Identity
					.map(FnMut!(|x: &Webpage<'static>| -> usize { x.contents.len() }))
					.map(FnMut!(|x: usize| -> u32 { x as u32 }))
					.most_frequent(100, 0.99, 2.0/1000.0),
				Identity
					.map(FnMut!(|x: &Webpage<'static>| { (x.contents.len(),x.contents[..5].to_owned()) }))
					.most_distinct(100, 0.99, 2.0/1000.0, 0.0808),
				Identity
					.cloned()
					.map(FnMut!(|x: Webpage<'static>| -> usize { x.contents.len() }))
					.map(FnMut!(|x: usize| -> u32 { x as u32 }))
					.sample_unstable(100),
			),
		).await;
	println!("{:?}", top);

	start.elapsed().unwrap()
}
