use futures::Stream;
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{ConsumerMulti, ConsumerMultiAsync, DistributedStreamMulti};
use crate::sink::Sink;

pub struct Identity;
impl<Item> DistributedStreamMulti<Item> for Identity {
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
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
		sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		sink.poll_forward(cx, stream)
	}
}
