use serde::{Deserialize, Serialize};

use super::{ConsumerMulti, DistributedIteratorMulti};

pub struct Identity;
impl<Item> DistributedIteratorMulti<Item> for Identity {
	type Item = Item;
	type Task = IdentityMultiTask;

	fn task(&self) -> Self::Task {
		IdentityMultiTask
	}
}

#[derive(Serialize, Deserialize)]
pub struct IdentityMultiTask;
impl<Item> ConsumerMulti<Item> for IdentityMultiTask {
	type Item = Item;

	fn run(&self, source: Item, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
		i(source)
	}
}
