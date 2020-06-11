use futures::Stream;
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};

use super::{DistributedPipe, PipeTask, PipeTaskAsync};
use crate::sink::Sink;

pub struct Identity;
impl<Item> DistributedPipe<Item> for Identity {
	type Item = Item;
	type Task = IdentityTask;

	fn task(&self) -> Self::Task {
		IdentityTask
	}
}

#[derive(Serialize, Deserialize)]
pub struct IdentityTask;
impl<Source> PipeTask<Source> for IdentityTask {
	type Item = Source;
	type Async = IdentityTask;
	fn into_async(self) -> Self::Async {
		IdentityTask
	}
}
impl<Item> PipeTaskAsync<Item> for IdentityTask {
	type Item = Item;

	fn poll_run(
		self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Item>>,
		sink: Pin<&mut impl Sink<Self::Item>>,
	) -> Poll<()> {
		sink.poll_forward(cx, stream)
	}
}
