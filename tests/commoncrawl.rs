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

use std::time::{Duration, SystemTime};

use amadeus::{data::Webpage, prelude::*};

#[tokio::test]
async fn commoncrawl() {
	let start = SystemTime::now();

	let pool = &ThreadPool::new(None).unwrap();

	let webpages = CommonCrawl::new("CC-MAIN-2020-24").await.unwrap();
	let _ = webpages
		.par_stream()
		.all(pool, move |x: Result<Webpage<'static>, _>| -> bool {
			let _x = x.unwrap();
			// println!("{}", x.url);
			start.elapsed().unwrap() < Duration::new(10, 0)
		})
		.await;

	return; // TODO: runs for a long time

	let webpages = CommonCrawl::new("CC-MAIN-2020-24").await.unwrap();

	let top: (
		((
			// Vec<u32>,
			),),
		(
			u32,
			u32,
			std::collections::HashSet<u32>,
			amadeus_streaming::Top<u32,usize>,
			amadeus_streaming::Top<usize,amadeus_streaming::HyperLogLogMagnitude<Vec<u8>>>,
			amadeus_streaming::SampleUnstable<u32>,
		),
	) = webpages.par_stream().map(|webpage:Result<_,_>|webpage.unwrap())
		.fork(
			&pool,
			((
				// Identity
				// 	.map(|x: Webpage<'static>| -> usize { x.contents.len() })
				// 	.map(|x: usize| -> u32 { x as u32 })
				// 	.collect(),
				// (),
				// Identity
				// 	.map(|x: Webpage<'static>| -> usize {
				// 		x.contents.len()
				// 	})
				// 	.map(|x: usize| -> u32 { x as u32 })
				// 	.collect(),
			),),
			(
				Identity
					.map(|x: &Webpage<'static>| -> usize { x.contents.len() })
					.map(|x: usize| -> u32 { x as u32 })
					.fold(
						|| 0_u32,
						|a: u32, b: either::Either<u32, u32>| a + b.into_inner(),
					),
				Identity
					.map(|x: &Webpage<'static>| -> usize { x.contents.len() })
					.map(|x: usize| -> u32 { x as u32 })
					.sum(),
				Identity
					.map(|x: &Webpage<'static>| -> usize { x.contents.len() })
					.map(|x: usize| -> u32 { x as u32 })
					.collect(),
				Identity
					.map(|x: &Webpage<'static>| -> usize { x.contents.len() })
					.map(|x: usize| -> u32 { x as u32 })
					.most_frequent(100, 0.99, 2.0/1000.0),
				Identity
					.map(|x: &Webpage<'static>| { (x.contents.len(),x.contents[..5].to_owned()) })
					.most_distinct(100, 0.99, 2.0/1000.0, 0.0808),
				Identity
					.cloned()
					.map(|x: Webpage<'static>| -> usize { x.contents.len() })
					.map(|x: usize| -> u32 { x as u32 })
					.sample_unstable(100),
			),
		).await;
	println!("{:?}", top);

	println!("in {:?}", start.elapsed().unwrap());
}
