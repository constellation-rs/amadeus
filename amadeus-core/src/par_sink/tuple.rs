#![allow(non_snake_case, clippy::type_complexity, irrefutable_let_patterns, clippy::new_without_default, unused_mut, unreachable_code, clippy::too_many_arguments)]

use derive_new::new;
use futures::{pin_mut, ready, stream, Stream, StreamExt};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};
use sum::{Sum0, Sum1, Sum2, Sum3, Sum4, Sum5, Sum6, Sum7, Sum8};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, PipeTask, Reducer, ReducerProcessSend, ReducerSend
};
use crate::{
	pipe::{Pipe, Sink}, pool::ProcessSend
};

fn substream<'a, 'b, 'c, 'd, 'e, S, F1, F2, O>(
	cx: &'d Context<'c>, mut stream: Pin<&'a mut Peekable<'b, S>>, mut is: F1, mut unwrap: F2,
) -> impl Stream<Item = F2::Output> + 'a
where
	S: Stream,
	F1: FnMut(&S::Item) -> bool + 'a,
	F2: FnMut(S::Item) -> O + 'a,
	'a: 'b,
{
	let waker = cx.waker().clone();
	stream::poll_fn(move |cx| match ready!(stream.as_mut().poll_peek(cx)) {
		Some(enum_) if is(enum_) => Poll::Ready(Some(
			if let Poll::Ready(Some(enum_)) = stream.as_mut().poll_next(cx) {
				unwrap(enum_)
			} else {
				unreachable!()
			},
		)),
		Some(_) => {
			let waker_ = cx.waker();
			if !waker.will_wake(waker_) {
				waker_.wake_by_ref();
			}
			Poll::Pending
		}
		None => Poll::Ready(None),
	})
	.fuse()
}

macro_rules! impl_tuple {
	($reducea:ident $reduceaasync:ident $reduceb:ident $reducebasync:ident $async:ident $enum:ident $join:ident $($copy:ident)? : $($num:tt $t:ident $s:ident $i:ident $r:ident $o:ident $c:ident $iterator:ident $reducera:ident $reducerb:ident $($copyb:ident)? , $comma:tt)*) => (
		impl<
				Item,
				$($r: ParallelSink<Item, Done = $o>,)*
				$($o,)*
			> ParallelSink<Item> for ($($r,)*)
				where Item: $($copy)*,
		{
			type Done = ($($o,)*);
			type Pipe = ($($r::Pipe,)*);
			type ReduceA = $reducea<$($r::ReduceA,)*>;
			type ReduceC = $reduceb<$($r::ReduceC,)*>;

			fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceC) {
				$(let ($iterator, $reducera, $t) = self.$num.reducers();)*
				(
					($($iterator,)*),
					$reducea{$($t: $reducera,)*},
					$reduceb{$($t,)*},
				)
			}
		}
		impl<
				Item,
				$($r: DistributedSink<Item, Done = $o>,)*
				$($o,)*
			> DistributedSink<Item> for ($($r,)*)
				where Item: $($copy)*,
		{
			type Done = ($($o,)*);
			type Pipe = ($($r::Pipe,)*);
			type ReduceA = $reducea<$($r::ReduceA,)*>;
			type ReduceB = $reduceb<$($r::ReduceB,)*>;
			type ReduceC = $reduceb<$($r::ReduceC,)*>;

			fn reducers(self) -> (Self::Pipe, Self::ReduceA, Self::ReduceB, Self::ReduceC) {
				$(let ($iterator, $reducera, $reducerb, $t) = self.$num.reducers();)*
				(
					($($iterator,)*),
					$reducea{$($t: $reducera,)*},
					$reduceb{$($t: $reducerb,)*},
					$reduceb{$($t,)*},
				)
			}
		}

		impl<Input, $($i: ParallelPipe<Input>,)*>
			ParallelPipe<Input> for ($($i,)*)
				where Input: $($copy)*,
		{
			type Output = $enum<$($i::Output,)*>;
			type Task = ($($i::Task,)*);

			fn task(&self) -> Self::Task {
				($(self.$num.task(),)*)
			}
		}
		impl<Input, $($i: DistributedPipe<Input>,)*>
			DistributedPipe<Input> for ($($i,)*)
				where Input: $($copy)*,
		{
			type Output = $enum<$($i::Output,)*>;
			type Task = ($($i::Task,)*);

			fn task(&self) -> Self::Task {
				($(self.$num.task(),)*)
			}
		}

		impl<Input, $($c: PipeTask<Input>,)*> PipeTask<Input> for ($($c,)*)
		where
			Input: $($copy)*,
		{
			type Output = $enum<$($c::Output,)*>;
			type Async = $async<Input, $($c::Async,)*>;

			fn into_async(self) -> Self::Async {
				$async{
					$($t: Some(self.$num.into_async()),)*
					pending: None,
					given: ($(false $comma)*),
				}
			}
		}

		#[pin_project]
		pub struct $async<Input, $($c,)*> {
			$(#[pin] $t: Option<$c>,)*
			pending: Option<Option<Input>>,
			given: ($(bool $comma)*),
		}

		#[allow(unused_variables)]
		impl<Input, $($c: Pipe<Input>,)*> Pipe<Input> for $async<Input, $($c,)*>
		where
			Input: $($copy)*,
		{
			type Output = $enum<$($c::Output,)*>;

			#[allow(non_snake_case)]
			fn poll_next(
				self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Input>>,
			) -> Poll<Option<Self::Output>> {
				let mut self_ = self.project();
				// buffer, copy to each
				loop {
					if self_.pending.is_none() {
						*self_.pending = Some(ready!(stream.as_mut().poll_next(cx)));
					}
					$({
						let pending: &mut Option<Input> = self_.pending.as_mut().unwrap();
						let given = &mut self_.given.$num;
						let waker = cx.waker();
						let stream_ = stream::poll_fn(|cx| {
							if !*given {
								*given = true;
								$(
									return Poll::Ready(*pending);
									let $copyb = ();
								)?
								Poll::Ready(pending.take())
							} else {
								let waker_ = cx.waker();
								if !waker.will_wake(waker_) {
									waker_.wake_by_ref();
								}
								Poll::Pending
							}
						}).fuse();
						pin_mut!(stream_);
						match self_.$t.as_mut().as_pin_mut().map(|pipe|pipe.poll_next(cx, stream_)) {
							Some(Poll::Ready(Some(item))) => break Poll::Ready(Some($enum::$t(item))),
							Some(Poll::Ready(None)) | None => { self_.$t.set(None); *given = true },
							Some(Poll::Pending) => (),
						}
					})*
					if $(self_.$t.is_none() &&)* true {
						break Poll::Ready(None);
					}
					if $(self_.given.$num &&)* true {
						$(self_.given.$num = false;)*
						*self_.pending = None;
					} else {
						assert!(self_.pending.as_ref().unwrap().is_some());
						break Poll::Pending;
					}
				}
			}
		}

		#[derive(Clone, Serialize, Deserialize, new)]
		pub struct $reducea<$($t,)*> {
			$($t: $t,)*
		}
		impl<$($t: Reducer<$s>,)* $($s,)*> Reducer<$enum<$($s,)*>> for $reducea<$($t,)*> {
			type Done = ($($t::Done,)*);
			type Async = $reduceaasync<$($t::Async,)* $($s,)*>;

			fn into_async(self) -> Self::Async {
				$reduceaasync{
					$($t: self.$t.into_async(),)*
					peeked: None,
					ready: ($(None::<$t::Done>,)*),
				}
			}
		}
		impl<$($t: Reducer<$s>,)* $($s,)*> ReducerProcessSend<$enum<$($s,)*>> for $reducea<$($t,)*> where $($t::Done: ProcessSend,)* {
			type Done = ($($t::Done,)*);
		}
		impl<$($t: Reducer<$s>,)* $($s,)*> ReducerSend<$enum<$($s,)*>> for $reducea<$($t,)*> where $($t::Done: Send,)* {
			type Done = ($($t::Done,)*);
		}
		#[pin_project]
		pub struct $reduceaasync<$($t,)* $($s,)*> where $($t: Sink<$s>,)* {
			$(#[pin] $t: $t,)*
			peeked: Option<$enum<$($s,)*>>,
			ready: ($(Option<$t::Done>,)*),
		}
		#[allow(unused_variables)]
		impl<$($t: Sink<$s>,)* $($s,)*> Sink<$enum<$($s,)*>> for $reduceaasync<$($t,)* $($s,)*> {
			type Done = ($($t::Done,)*);

			fn poll_forward(self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = $enum<$($s,)*>>>) -> Poll<Self::Done> {
				let mut self_ = self.project();
				loop {
					let mut progress = false;
					$({
						let stream = Peekable{stream:stream.as_mut(),peeked:&mut *self_.peeked};
						pin_mut!(stream);
						let stream_ = substream(cx, stream, |item| matches!(item, $enum::$t(_)), |item| { progress = true; if let $enum::$t(item) = item { item } else { unreachable!() } });
						pin_mut!(stream_);
						if self_.ready.$num.is_none() {
							if let Poll::Ready(done) = self_.$t.as_mut().poll_forward(cx, stream_) {
								self_.ready.$num = Some(done);
							}
						}
					})*
					if $(self_.ready.$num.is_some() &&)* true {
						break Poll::Ready(($(self_.ready.$num.take().unwrap(),)*));
					}
					if !progress {
						break Poll::Pending;
					}
				}
			}
		}

		#[derive(Clone, Serialize, Deserialize, new)]
		pub struct $reduceb<$($t,)*> {
			$($t: $t,)*
		}
		impl<$($t: Reducer<$s>,)* $($s,)*> Reducer<($($s,)*)> for $reduceb<$($t,)*> {
			type Done = ($($t::Done,)*);
			type Async = $reducebasync<$($t::Async,)* $($s,)*>;

			fn into_async(self) -> Self::Async {
				$reducebasync{
					$($t: self.$t.into_async(),)*
					peeked: None,
					ready: ($(None::<$t::Done>,)*),
				}
			}
		}
		impl<$($t: ReducerProcessSend<$s>,)* $($s,)*> ReducerProcessSend<($($s,)*)> for $reduceb<$($t,)*> {
			type Done = ($(<$t as ReducerProcessSend<$s>>::Done,)*);
		}
		impl<$($t: ReducerSend<$s>,)* $($s,)*> ReducerSend<($($s,)*)> for $reduceb<$($t,)*> {
			type Done = ($(<$t as ReducerSend<$s>>::Done,)*);
		}
		#[pin_project]
		pub struct $reducebasync<$($t,)* $($s,)*> where $($t: Sink<$s>,)* {
			$(#[pin] $t: $t,)*
			peeked: Option<($(Option<$s>,)*)>,
			ready: ($(Option<$t::Done>,)*),
		}
		#[allow(unused_variables)]
		impl<$($t: Sink<$s>,)* $($s,)*> Sink<($($s,)*)> for $reducebasync<$($t,)* $($s,)*> {
			type Done = ($($t::Done,)*);

			fn poll_forward(self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = ($($s,)*)>>) -> Poll<Self::Done> {
				let mut self_ = self.project();
				let stream = stream.map(|item| ($(Some(item.$num),)*));
				pin_mut!(stream);
				loop {
					let mut progress = false;
					$({
						let stream = Peekable{stream:stream.as_mut(),peeked:&mut *self_.peeked};
						pin_mut!(stream);
						let waker = cx.waker();
						let stream = stream::poll_fn(|cx| match ready!(stream.as_mut().poll_peek(cx)) {
							Some(enum_) if enum_.$num.is_some() => {
								let ret = enum_.$num.take().unwrap();
								progress = true;
								Poll::Ready(Some(ret))
							}
							Some(_) => {
								let waker_ = cx.waker();
								if !waker.will_wake(waker_) {
									waker_.wake_by_ref();
								}
								Poll::Pending
							},
							None => Poll::Ready(None),
						}).fuse();
						pin_mut!(stream);
						if self_.ready.$num.is_none() {
							if let Poll::Ready(done) = self_.$t.as_mut().poll_forward(cx, stream) {
								self_.ready.$num = Some(done);
							}
						}
					})*
					if $(self_.ready.$num.is_some() &&)* true {
						break Poll::Ready(($(self_.ready.$num.take().unwrap(),)*));
					}
					if let Some(peeked) = self_.peeked {
						if $(peeked.$num.is_none() &&)* true {
							*self_.peeked = None;
							progress = true;
						}
					}
					if !progress {
						break Poll::Pending;
					}
				}
			}
		}
	);
}
impl_tuple!(ReduceA0 ReduceA0Async ReduceC0 ReduceC0Async AsyncTuple0 Sum0 Join0:);
impl_tuple!(ReduceA1 ReduceA1Async ReduceC1 ReduceC1Async AsyncTuple1 Sum1 Join1: 0 A S0 I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0,,);
impl_tuple!(ReduceA2 ReduceA2Async ReduceC2 ReduceC2Async AsyncTuple2 Sum2 Join2 Copy: 0 A S0 I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 Copy,, 1 B S1 I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 Copy,,);
impl_tuple!(ReduceA3 ReduceA3Async ReduceC3 ReduceC3Async AsyncTuple3 Sum3 Join3 Copy: 0 A S0 I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 Copy,, 1 B S1 I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 Copy,, 2 C S2 I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 Copy,,);
impl_tuple!(ReduceA4 ReduceA4Async ReduceC4 ReduceC4Async AsyncTuple4 Sum4 Join4 Copy: 0 A S0 I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 Copy,, 1 B S1 I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 Copy,, 2 C S2 I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 Copy,, 3 D S3 I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 Copy,,);
impl_tuple!(ReduceA5 ReduceA5Async ReduceC5 ReduceC5Async AsyncTuple5 Sum5 Join5 Copy: 0 A S0 I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 Copy,, 1 B S1 I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 Copy,, 2 C S2 I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 Copy,, 3 D S3 I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 Copy,, 4 E S4 I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 Copy,,);
impl_tuple!(ReduceA6 ReduceA6Async ReduceC6 ReduceC6Async AsyncTuple6 Sum6 Join6 Copy: 0 A S0 I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 Copy,, 1 B S1 I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 Copy,, 2 C S2 I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 Copy,, 3 D S3 I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 Copy,, 4 E S4 I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 Copy,, 5 F S5 I5 R5 O5 C5 iterator_5 reducer_a_5 reducer_b_5 Copy,,);
impl_tuple!(ReduceA7 ReduceA7Async ReduceC7 ReduceC7Async AsyncTuple7 Sum7 Join7 Copy: 0 A S0 I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 Copy,, 1 B S1 I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 Copy,, 2 C S2 I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 Copy,, 3 D S3 I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 Copy,, 4 E S4 I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 Copy,, 5 F S5 I5 R5 O5 C5 iterator_5 reducer_a_5 reducer_b_5 Copy,, 6 G S6 I6 R6 O6 C6 iterator_6 reducer_a_6 reducer_b_6 Copy,,);
impl_tuple!(ReduceA8 ReduceA8Async ReduceC8 ReduceC8Async AsyncTuple8 Sum8 Join8 Copy: 0 A S0 I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 Copy,, 1 B S1 I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 Copy,, 2 C S2 I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 Copy,, 3 D S3 I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 Copy,, 4 E S4 I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 Copy,, 5 F S5 I5 R5 O5 C5 iterator_5 reducer_a_5 reducer_b_5 Copy,, 6 G S6 I6 R6 O6 C6 iterator_6 reducer_a_6 reducer_b_6 Copy,, 7 H S7 I7 R7 O7 C7 iterator_7 reducer_a_7 reducer_b_7 Copy,,);

#[pin_project(project = PeekableProj)]
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Peekable<'a, St: Stream> {
	#[pin]
	stream: St,
	peeked: &'a mut Option<St::Item>,
}

impl<'a, St: Stream> Peekable<'a, St> {
	pub fn poll_peek(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<&mut St::Item>> {
		let PeekableProj { mut stream, peeked } = self.project();

		Poll::Ready(loop {
			if peeked.is_some() {
				break peeked.as_mut();
			} else if let Some(item) = ready!(stream.as_mut().poll_next(cx)) {
				**peeked = Some(item);
			} else {
				break None;
			}
		})
	}
}

impl<'a, S: Stream> Stream for Peekable<'a, S> {
	type Item = S::Item;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let PeekableProj { stream, peeked } = self.project();
		if let Some(item) = peeked.take() {
			return Poll::Ready(Some(item));
		}
		stream.poll_next(cx)
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let peek_len = if self.peeked.is_some() { 1 } else { 0 };
		let (lower, upper) = self.stream.size_hint();
		let lower = lower.saturating_add(peek_len);
		let upper = match upper {
			Some(x) => x.checked_add(peek_len),
			None => None,
		};
		(lower, upper)
	}
}
