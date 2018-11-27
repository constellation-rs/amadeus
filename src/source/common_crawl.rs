use crate::{dist_iter::Consumer, DistributedIterator, IteratorExt};
use flate2::read::MultiGzDecoder;
use reqwest_resume::ClientExt;
use std::{
	io::{self, BufRead, BufReader}, iter, time
};
use warc_parser::{WarcParser, WebpageOwned};

type Closure<Env, Args, Output> =
	serde_closure::FnMut<Env, for<'r> fn(&'r mut Env, Args) -> Output>;

type CommonCrawlInner = crate::dist_iter::FlatMap<
	crate::into_dist_iter::IterIter<
		iter::Map<
			io::Lines<BufReader<MultiGzDecoder<reqwest_resume::Response>>>,
			Closure<(), (Result<String, io::Error>,), String>,
		>,
	>,
	Closure<(), (String,), WarcParser<MultiGzDecoder<reqwest_resume::Response>>>,
>;

pub struct CommonCrawl {
	i: CommonCrawlInner,
}
impl CommonCrawl {
	/// See https://commoncrawl.s3.amazonaws.com/crawl-data/index.html
	/// CC-MAIN-2018-43
	pub fn new(id: &str) -> Result<Self, reqwest::Error> {
		let url = format!(
			"https://commoncrawl.s3.amazonaws.com/crawl-data/{}/warc.paths.gz",
			id
		);
		let body = reqwest::ClientBuilder::new()
			.timeout(time::Duration::new(120, 0))
			.build()
			.unwrap()
			.resumable()
			.get(url.parse().unwrap())
			.send()?;
		let body = MultiGzDecoder::new(body); // Content-Encoding isn't set, so decode manually

		let i = BufReader::new(body)
			.lines()
			.map(FnMut!(|url: Result<String, io::Error>| -> String {
				format!("http://commoncrawl.s3.amazonaws.com/{}", url.unwrap())
			}))
			.dist()
			.flat_map(FnMut!(|url: String| {
				let body = reqwest::ClientBuilder::new()
					.timeout(time::Duration::new(120, 0))
					.build()
					.unwrap()
					.resumable()
					.get(url.parse().unwrap())
					.send()
					.unwrap();
				let body = MultiGzDecoder::new(body);
				WarcParser::new(body)
			}));
		Ok(Self { i })
	}
}

impl DistributedIterator for CommonCrawl {
	type Item = Result<WebpageOwned, io::Error>;
	type Task = CommonCrawlConsumer;

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.i.size_hint()
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.i.next_task().map(|task| CommonCrawlConsumer { task })
	}
}

#[derive(Serialize, Deserialize)]
pub struct CommonCrawlConsumer {
	task: <CommonCrawlInner as DistributedIterator>::Task,
}

impl Consumer for CommonCrawlConsumer {
	type Item = Result<WebpageOwned, io::Error>;

	fn run(self, i: &mut impl FnMut(Self::Item)) {
		self.task.run(i)
	}
}
