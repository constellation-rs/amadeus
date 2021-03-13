use amadeus::prelude::*;

#[tokio::main]
async fn main() {
	let pool = &ThreadPool::new(None, None).unwrap();

	let rows = Cloudfront::new_with(
		AwsRegion::UsEast1,
		"us-east-1.data-analytics",
		"cflogworkshop/raw/cf-accesslogs/",
		AwsCredentials::Anonymous,
	)
	.await
	.unwrap();

	let columnar_list: List<CloudfrontRow> =
		rows.par_stream().map(Result::unwrap).collect(pool).await;

	assert_eq!(columnar_list.len(), 207_928);

	// columnar_list is stored as a Struct of Arrays. This enables very fast
	// traversing, though this isn't exposed yet. Tracking at
	// https://github.com/constellation-rs/amadeus/issues/73
	for _el in columnar_list {}
}
