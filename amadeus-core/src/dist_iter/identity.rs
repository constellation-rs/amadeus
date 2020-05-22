use futures::{pin_mut, stream};
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{ConsumerMulti, ConsumerMultiAsync, DistributedIteratorMulti};
use crate::sink::Sink;

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
impl<Source> ConsumerMulti<Source> for IdentityMultiTask {
	type Item = Source;
	type Async = IdentityMultiTask;
	fn into_async(self) -> Self::Async {
		IdentityMultiTask
	}
}
impl<Item> ConsumerMultiAsync<Item> for IdentityMultiTask {
	type Item = Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, source: Option<Item>,
		sink: &mut impl Sink<Self::Item>,
	) -> Poll<bool> {
		let stream = stream::iter(source);
		pin_mut!(stream);
		sink.poll_sink(cx, stream)
	}
}
