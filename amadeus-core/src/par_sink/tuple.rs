#![allow(non_snake_case, clippy::type_complexity, irrefutable_let_patterns, clippy::new_without_default, unused_mut, unreachable_code, clippy::too_many_arguments)]

use derive_new::new;
use futures::{
	future::{
		join as Join2, join3 as Join3, join4 as Join4, join5 as Join5, maybe_done, FusedFuture, MaybeDone
	}, pin_mut, ready, stream, Stream, StreamExt
};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{
	future::Future, pin::Pin, task::{Context, Poll}
};
use sum::{Sum0, Sum1, Sum2, Sum3, Sum4, Sum5, Sum6, Sum7, Sum8};

use super::{
	DistributedPipe, DistributedSink, ParallelPipe, ParallelSink, PipeTask, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::{
	pipe::{Pipe, Sink}, pool::ProcessSend
};

fn substream<'a, 'b, S, F1, F2, O>(
	mut stream: Pin<&'a mut Peekable<'b, S>>, mut is: F1, mut unwrap: F2,
) -> impl Stream<Item = F2::Output> + 'a
where
	S: Stream,
	F1: FnMut(&S::Item) -> bool + 'a,
	F2: FnMut(S::Item) -> O + 'a,
	'a: 'b,
{
	stream::poll_fn(move |cx| match ready!(stream.as_mut().poll_peek(cx)) {
		Some(enum_) if is(enum_) => Poll::Ready(Some(
			if let Poll::Ready(Some(enum_)) = stream.as_mut().poll_next(cx) {
				unwrap(enum_)
			} else {
				unreachable!()
			},
		)),
		Some(_) => Poll::Pending,
		None => Poll::Ready(None),
	})
	.fuse()
}

macro_rules! impl_tuple {
	($reducea:ident $reduceaasync:ident $reduceb:ident $reducebasync:ident $async:ident $enum:ident $join:ident $($copy:ident)? : $($num:tt $t:ident $s:ident $i:ident $r:ident $o:ident $c:ident $iterator:ident $reducera:ident $reducerb:ident $($copyb:ident)? , $comma:tt)*) => (
		impl<
				Source,
				$($r: ParallelSink<Source, Output = $o>,)*
				$($o,)*
			> ParallelSink<Source> for ($($r,)*)
				where Source: $($copy)*,
		{
			type Output = ($($o,)*);
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
				Source,
				$($r: DistributedSink<Source, Output = $o>,)*
				$($o,)*
			> DistributedSink<Source> for ($($r,)*)
				where Source: $($copy)*,
		{
			type Output = ($($o,)*);
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

		impl<Source, $($i: ParallelPipe<Source>,)*>
			ParallelPipe<Source> for ($($i,)*)
				where Source: $($copy)*,
		{
			type Item = $enum<$($i::Item,)*>;
			type Task = ($($i::Task,)*);

			fn task(&self) -> Self::Task {
				($(self.$num.task(),)*)
			}
		}
		impl<Source, $($i: DistributedPipe<Source>,)*>
			DistributedPipe<Source> for ($($i,)*)
				where Source: $($copy)*,
		{
			type Item = $enum<$($i::Item,)*>;
			type Task = ($($i::Task,)*);

			fn task(&self) -> Self::Task {
				($(self.$num.task(),)*)
			}
		}

		impl<Source, $($c: PipeTask<Source>,)*> PipeTask<Source> for ($($c,)*)
		where
			Source: $($copy)*,
		{
			type Item = $enum<$($c::Item,)*>;
			type Async = $async<Source, $($c::Async,)*>;
			fn into_async(self) -> Self::Async {
				$async{
					$($t: self.$num.into_async(),)*
					pending: None,
					given: ($(false $comma)*),
				}
			}
		}

		#[pin_project]
		pub struct $async<Source, $($c,)*> {
			$(#[pin] $t: $c,)*
			pending: Option<Option<Source>>,
			given: ($(bool $comma)*),
		}

		#[allow(unused_variables)]
		impl<Source, $($c: Pipe<Source>,)*> Pipe<Source> for $async<Source, $($c,)*>
		where
			Source: $($copy)*,
		{
			type Item = $enum<$($c::Item,)*>;

			#[allow(non_snake_case)]
			fn poll_next(
				self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Source>>,
			) -> Poll<Option<Self::Item>> {
				let mut self_ = self.project();
				// buffer, copy to each
				loop {
					if self_.pending.is_none() {
						*self_.pending = match stream.as_mut().poll_next(cx) { Poll::Ready(x) => Some(x), Poll::Pending => None};
					}
					$({
						let pending = &mut *self_.pending;
						let given = &mut self_.given.$num;
						let stream_ = stream::poll_fn(|cx| {
							if !*given && pending.is_some() {
								*given = true;
								$(
									return Poll::Ready(*pending.as_ref().unwrap());
									let $copyb = ();
								)?
								Poll::Ready(pending.take().unwrap().take())
							} else {
								Poll::Pending
							}
						}).fuse();
						pin_mut!(stream_);
						if let Poll::Ready(item) = self_.$t.as_mut().poll_next(cx, stream_) {
							return Poll::Ready(item.map($enum::$t));
						}
					})*
					if $(self_.given.$num &&)* true {
						$(self_.given.$num = false;)*
						*self_.pending = None;
					} else {
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
			type Output = ($($t::Output,)*);
			type Async = $reduceaasync<$($t::Async,)* $($s,)*>;

			fn into_async(self) -> Self::Async {
				$reduceaasync{
					$($t: self.$t.into_async(),)*
					peeked: None,
					output: ($(None::<$t::Output>,)*),
				}
			}
		}
		impl<$($t: Reducer<$s>,)* $($s,)*> ReducerProcessSend<$enum<$($s,)*>> for $reducea<$($t,)*> where $($t::Output: ProcessSend + 'static,)* {
			type Output = ($($t::Output,)*);
		}
		impl<$($t: Reducer<$s>,)* $($s,)*> ReducerSend<$enum<$($s,)*>> for $reducea<$($t,)*> where $($t::Output: Send + 'static,)* {
			type Output = ($($t::Output,)*);
		}
		#[pin_project]
		pub struct $reduceaasync<$($t,)* $($s,)*> where $($t: ReducerAsync<$s>,)* {
			$(#[pin] $t: $t,)*
			peeked: Option<$enum<$($s,)*>>,
			output: ($(Option<$t::Output>,)*),
		}
		#[allow(unused_variables)]
		impl<$($t: ReducerAsync<$s>,)* $($s,)*> Sink<$enum<$($s,)*>> for $reduceaasync<$($t,)* $($s,)*> {
			fn poll_pipe(self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = $enum<$($s,)*>>>) -> Poll<()> {
				let mut self_ = self.project();
				let mut ready = ($(false $comma)*);
				loop {
					let mut progress = false;
					$(if !ready.$num {
						let stream = Peekable{stream:stream.as_mut(),peeked:&mut *self_.peeked};
						pin_mut!(stream);
						let stream_ = substream(stream, |item| if let $enum::$t(_) = item { true } else { false }, |item| { progress = true; if let $enum::$t(item) = item { item } else { unreachable!() } });
						pin_mut!(stream_);
						ready.$num = self_.$t.as_mut().poll_pipe(cx, stream_).is_ready();
					})*
					if $(ready.$num &&)* true {
						break Poll::Ready(())
					}
					if !progress {
						break Poll::Pending;
					}
				}
			}
		}
		#[allow(unused_variables)]
		impl<$($t: ReducerAsync<$s>,)* $($s,)*> ReducerAsync<$enum<$($s,)*>> for $reduceaasync<$($t,)* $($s,)*> {
			type Output = ($($t::Output,)*);

			fn output<'a>(self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
				Box::pin(async move {
					let self_ = self.project();
					$join($(self_.$t.output(),)*).await
				})
			}
		}

		#[derive(Clone, Serialize, Deserialize, new)]
		pub struct $reduceb<$($t,)*> {
			$($t: $t,)*
		}
		impl<$($t: Reducer<$s>,)* $($s,)*> Reducer<($($s,)*)> for $reduceb<$($t,)*> {
			type Output = ($($t::Output,)*);
			type Async = $reducebasync<$($t::Async,)* $($s,)*>;

			fn into_async(self) -> Self::Async {
				$reducebasync{
					$($t: self.$t.into_async(),)*
					peeked: None,
					output: ($(None::<$t::Output>,)*),
				}
			}
		}
		impl<$($t: ReducerProcessSend<$s>,)* $($s,)*> ReducerProcessSend<($($s,)*)> for $reduceb<$($t,)*> {
			type Output = ($(<$t as ReducerProcessSend<$s>>::Output,)*);
		}
		impl<$($t: ReducerSend<$s>,)* $($s,)*> ReducerSend<($($s,)*)> for $reduceb<$($t,)*> {
			type Output = ($(<$t as ReducerSend<$s>>::Output,)*);
		}
		#[pin_project]
		pub struct $reducebasync<$($t,)* $($s,)*> where $($t: ReducerAsync<$s>,)* {
			$(#[pin] $t: $t,)*
			peeked: Option<($(Option<$s>,)*)>,
			output: ($(Option<$t::Output>,)*),
		}
		#[allow(unused_variables)]
		impl<$($t: ReducerAsync<$s>,)* $($s,)*> Sink<($($s,)*)> for $reducebasync<$($t,)* $($s,)*> {
			fn poll_pipe(self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = ($($s,)*)>>) -> Poll<()> {
				let mut self_ = self.project();
				let mut ready = ($(false $comma)*);
				let stream = stream.map(|item| ($(Some(item.$num),)*));
				pin_mut!(stream);
				loop {
					let mut progress = false;
					$(if !ready.$num {
						let stream = Peekable{stream:stream.as_mut(),peeked:&mut *self_.peeked};
						pin_mut!(stream);
						let stream = stream::poll_fn(|cx| match ready!(stream.as_mut().poll_peek(cx)) {
							Some(enum_) if enum_.$num.is_some() => {
								let ret = enum_.$num.take().unwrap();
								progress = true;
								Poll::Ready(Some(ret))
							}
							Some(_) => {
								Poll::Pending
							},
							None => Poll::Ready(None),
						}).fuse();
						pin_mut!(stream);
						ready.$num = self_.$t.as_mut().poll_pipe(cx, stream).is_ready();
					})*
					if let Some(peeked) = self_.peeked {
						if $(peeked.$num.is_none() &&)* true {
							*self_.peeked = None;
							progress = true;
						}
					}
					if $(ready.$num &&)* true {
						break Poll::Ready(());
					}
					if !progress {
						break Poll::Pending;
					}
				}
			}
		}
		#[allow(unused_variables)]
		impl<$($t: ReducerAsync<$s>,)* $($s,)*> ReducerAsync<($($s,)*)> for $reducebasync<$($t,)* $($s,)*> {
			type Output = ($($t::Output,)*);

			fn output<'a>(self: Pin<&'a mut Self>) -> Pin<Box<dyn Future<Output = Self::Output> + 'a>> {
				Box::pin(async move {
					let self_ = self.project();
					$join($(self_.$t.output(),)*).await
				})
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

// from https://github.com/rust-lang/futures-rs/blob/19831dbecd13961fbd4243c1114663e24299c33d/futures-util/src/future/join.rs
macro_rules! generate {
	($(
		($Join:ident, <$($Fut:ident),*>),
	)*) => ($(
		#[pin_project]
		struct $Join<$($Fut: Future),*> {
			$(#[pin] $Fut: MaybeDone<$Fut>,)*
			dummy: (),
		}

		fn $Join<$($Fut: Future),*>($($Fut: $Fut),*) -> $Join<$($Fut),*> {
			$Join {
				$($Fut: maybe_done($Fut),)*
				dummy: (),
			}
		}

		#[allow(unused_variables)]
		impl<$($Fut: Future),*> Future for $Join<$($Fut),*> {
			type Output = ($($Fut::Output,)*);

			fn poll(
				self: Pin<&mut Self>, cx: &mut Context<'_>
			) -> Poll<Self::Output> {
				let mut all_done = true;
				let mut futures = self.project();
				$(
					all_done &= futures.$Fut.as_mut().poll(cx).is_ready();
				)*

				if all_done {
					Poll::Ready(($(futures.$Fut.take_output().unwrap(),)*))
				} else {
					Poll::Pending
				}
			}
		}

		impl<$($Fut: FusedFuture),*> FusedFuture for $Join<$($Fut),*> {
			fn is_terminated(&self) -> bool {
				$(
					self.$Fut.is_terminated() &&
				)* true
			}
		}
	)*)
}

generate! {
	(Join0, <>),
	(Join1, <Fut1>),
	(Join6, <Fut1, Fut2, Fut3, Fut4, Fut5, Fut6>),
	(Join7, <Fut1, Fut2, Fut3, Fut4, Fut5, Fut6, Fut7>),
	(Join8, <Fut1, Fut2, Fut3, Fut4, Fut5, Fut6, Fut7, Fut8>),
}
