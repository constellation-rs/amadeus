use std::{future::Future, pin::Pin};

pub trait File {
	type Partition: Partition;

	fn partitions(&self) -> Vec<Self::Partition>;
}
pub trait Partition {
	type Page: Page;

	fn files(&self) -> Vec<Self::Page>;
}
pub trait Page {
	fn len(&self) -> u64;
	fn read<'a>(
		&'a mut self, range: std::ops::Range<u64>,
	) -> Pin<Box<dyn Future<Output = Vec<u8>> + 'a>>;
	fn write<'a>(
		&'a mut self, offset: u64, data: Vec<u8>,
	) -> Pin<Box<dyn Future<Output = ()> + 'a>>;
}
