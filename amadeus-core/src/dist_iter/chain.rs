use super::{Consumer, DistributedIterator};
use serde::{Deserialize, Serialize};

#[must_use]
pub struct Chain<A, B> {
	a: A,
	b: B,
}
impl<A, B> Chain<A, B> {
	pub(super) fn new(a: A, b: B) -> Self {
		Self { a, b }
	}
}

impl<A: DistributedIterator, B: DistributedIterator<Item = A::Item>> DistributedIterator
	for Chain<A, B>
{
	type Item = A::Item;
	type Task = ChainConsumer<A::Task, B::Task>;

	fn size_hint(&self) -> (usize, Option<usize>) {
		let (a_lower, a_upper) = self.a.size_hint();
		let (b_lower, b_upper) = self.b.size_hint();
		(
			a_lower + b_lower,
			if let (Some(a), Some(b)) = (a_upper, b_upper) {
				Some(a + b)
			} else {
				None
			},
		)
	}
	fn next_task(&mut self) -> Option<Self::Task> {
		self.a
			.next_task()
			.map(ChainConsumer::A)
			.or_else(|| self.b.next_task().map(ChainConsumer::B))
	}
}

#[derive(Serialize, Deserialize)]
pub enum ChainConsumer<A, B> {
	A(A),
	B(B),
}
impl<A: Consumer, B: Consumer<Item = A::Item>> Consumer for ChainConsumer<A, B> {
	type Item = A::Item;

	fn run(self, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		match self {
			Self::A(a) => a.run(i),
			Self::B(b) => b.run(i),
		}
	}
}
