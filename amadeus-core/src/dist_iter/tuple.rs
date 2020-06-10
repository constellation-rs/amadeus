#![allow(non_snake_case, clippy::type_complexity, irrefutable_let_patterns, clippy::new_without_default, unused_mut, unreachable_code)]

use futures::{pin_mut, ready, stream, Stream, StreamExt};
use pin_project::{pin_project, project};
use serde::{Deserialize, Serialize};
use std::{
	pin::Pin, task::{Context, Poll}
};
use sum::*;

use super::{
	ConsumerMulti, ConsumerMultiAsync, DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, ReducerAsync, ReducerProcessSend, ReducerSend
};
use crate::{
	pool::ProcessSend, sink::{Sink, SinkMap}
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
				unreachable!("a")
			},
		)),
		Some(_) => Poll::Pending,
		None => Poll::Ready(None),
	})
	.fuse()
}

macro_rules! impl_iterator_multi_tuple {
	($reduceafactory:ident $reducea:ident $reduceaasync:ident $reducebfactory:ident $reduceb:ident $reducebasync:ident $async:ident $enum:ident $($copy:ident)? : $($i:ident $r:ident $o:ident $c:ident $iterator:ident $reducera:ident $reducerb:ident $num:tt $t:ident $($copyb:ident)? , $comma:tt)*) => (
		impl<
				$($i: DistributedIteratorMulti<Source>,)*
				Source,
				$($r: DistributedReducer<$i, Source, $o>,)*
				$($o,)*
			> DistributedReducer<($($i,)*), Source, ($($o,)*)> for ($($r,)*)
				where Source: $($copy)*,
		{
			type ReduceAFactory = $reduceafactory<$($r::ReduceAFactory,)*>;
			type ReduceBFactory = $reducebfactory<$($r::ReduceBFactory,)*>;
			type ReduceA = $reducea<$($r::ReduceA,)*>;
			type ReduceB = $reduceb<$($r::ReduceB,)*>;
			type ReduceC = $reduceb<$($r::ReduceC,)*>;

			fn reducers(self) -> (($($i,)*), Self::ReduceAFactory, Self::ReduceBFactory, Self::ReduceC) {
				$(let ($iterator, $reducera, $reducerb, $t) = self.$num.reducers();)*
				(
					($($iterator,)*),
					$reduceafactory($($reducera,)*),
					$reducebfactory($($reducerb,)*),
					$reduceb{$($t,)*},
				)
			}
		}

		impl<Source, $($i: DistributedIteratorMulti<Source>,)*>
			DistributedIteratorMulti<Source> for ($($i,)*)
				where Source: $($copy)*,
		{
			type Item = $enum<$($i::Item,)*>;
			type Task = ($($i::Task,)*);

			fn task(&self) -> Self::Task {
				($(self.$num.task(),)*)
			}
		}

		impl<Source, $($c: ConsumerMulti<Source>,)*> ConsumerMulti<Source> for ($($c,)*)
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
		impl<Source, $($c: ConsumerMultiAsync<Source>,)*> ConsumerMultiAsync<Source> for $async<Source, $($c,)*>
		where
			Source: $($copy)*,
		{
			type Item = $enum<$($c::Item,)*>;

			#[allow(non_snake_case)]
			fn poll_run(
				self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Source>>,
				mut sink: Pin<&mut impl Sink<Self::Item>>,
			) -> Poll<()> {
				let mut self_ = self.project();
				// buffer, copy to each
				let mut ready = ($(false $comma)*);
				loop {
					let mut progress = false;
					if self_.pending.is_none() {
						*self_.pending = match stream.as_mut().poll_next(cx) { Poll::Ready(x) => Some(x), Poll::Pending => None};
					}
					$(
						if !ready.$num {
							let sink_ = SinkMap::new(sink.as_mut(), $enum::$t);
							pin_mut!(sink_);
							let pending = &mut self_.pending;
							let given = &mut self_.given.$num;
							let stream_ = stream::poll_fn(|cx| {
								if !*given && pending.is_some() {
									*given = true;
									progress = true;
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
							ready.$num = self_.$t.as_mut().poll_run(cx, stream_, sink_).is_ready();
							if ready.$num {
								self_.given.$num = true;
								progress = true; // TODO remove
							} else {
							}
						}
					)*
					if $(ready.$num &&)* true {
						break Poll::Ready(());
					}
					if !progress {
						break Poll::Pending;
					}
					if $(self_.given.$num &&)* true {
						$(self_.given.$num = false;)*
						*self_.pending = None;
					}
				}
			}
		}

		#[derive(Clone, Serialize, Deserialize)]
		pub struct $reduceafactory<$($r,)*>($(pub(super) $r,)*);
		impl<$($r:ReduceFactory,)*> ReduceFactory for $reduceafactory<$($r,)*> {
			type Reducer = $reducea<$($r::Reducer,)*>;
			fn make(&self) -> Self::Reducer {
				$reducea{
					$($t: self.$num.make(),)*
				}
			}
		}

		#[derive(Serialize, Deserialize)]
		pub struct $reducea<$($t,)*> {
			$($t: $t,)*
		}
		impl<$($t: Reducer,)*> Reducer for $reducea<$($t,)*> {
			type Item = $enum<$($t::Item,)*>;
			type Output = ($($t::Output,)*);
			type Async = $reduceaasync<$($t::Async,)*>;

			fn into_async(self) -> Self::Async {
				$reduceaasync{
					$($t: self.$t.into_async(),)*
					peeked: None,
					output: ($(None::<$t::Output>,)*),
				}
			}
		}
		#[pin_project]
		pub struct $reduceaasync<$($t,)*> where $($t: ReducerAsync,)* {
			$(#[pin] $t: $t,)*
			peeked: Option<$enum<$($t::Item,)*>>,
			output: ($(Option<$t::Output>,)*),
		}
		#[allow(unused_variables)]
		impl<$($t: ReducerAsync,)*> ReducerAsync for $reduceaasync<$($t,)*> {
			type Item = $enum<$($t::Item,)*>;
			type Output = ($($t::Output,)*);

			fn poll_forward(self: Pin<&mut Self>, cx: &mut Context, mut stream: Pin<&mut impl Stream<Item = Self::Item>>) -> Poll<()> {
				let mut self_ = self.project();
				let mut ready = ($(false $comma)*);
				loop {
					let mut progress = false;
					$(if !ready.$num {
						let stream = Peekable{stream:stream.as_mut(),peeked:&mut *self_.peeked};
						pin_mut!(stream);
						ready.$num = {
							let stream_ = substream(stream, |item| if let $enum::$t(_) = item { true } else { false }, |item| { progress = true; if let $enum::$t(item) = item { item } else { unreachable!("b") } });
							pin_mut!(stream_);
							self_.$t.as_mut().poll_forward(cx, stream_).is_ready()
						};
						if ready.$num {
							progress = true; // TODO remove
						}
					})*
					if $(ready.$num &&)* true {
						break Poll::Ready(())
					}
					if !progress {
						// if let Poll::Ready(None) = stream.poll_next(cx) {
						// 	panic!("xdfwrg");
						// }
						let ret = stream.as_mut().poll_next(cx);
						break Poll::Pending;
					}
				}
			}
			fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
				let mut self_ = self.project();
				let mut pending = false;
				$(
					if self_.output.$num.is_none() {
						if let Poll::Ready(output) = self_.$t.poll_output(cx) {
							self_.output.$num = Some(output);
						} else {
							pending = true;
						}
					}
				)*
				if !pending {
					Poll::Ready(($(self_.output.$num.take().unwrap(),)*))
				} else {
					Poll::Pending
				}
			}
		}
		impl<$($t: Reducer,)*> ReducerProcessSend for $reducea<$($t,)*> where $($t: ProcessSend,)* $($t::Output: ProcessSend,)* {
			type Output = ($($t::Output,)*);
		}
		impl<$($t: Reducer,)*> ReducerSend for $reducea<$($t,)*> where $($t: Send + 'static,)* $($t::Output: Send + 'static,)* {
			type Output = ($($t::Output,)*);
		}

		pub struct $reducebfactory<$($r,)*>($(pub(super) $r,)*);
		impl<$($r:ReduceFactory,)*> ReduceFactory for $reducebfactory<$($r,)*> {
			type Reducer = $reduceb<$($r::Reducer,)*>;
			fn make(&self) -> Self::Reducer {
				$reduceb{
					$($t: self.$num.make(),)*
				}
			}
		}

		#[derive(Serialize, Deserialize)]
		pub struct $reduceb<$($t,)*> {
			$($t: $t,)*
		}
		impl<$($t,)*> $reduceb<$($t,)*> {
			#[allow(clippy::too_many_arguments)]
			pub fn new($($t: $t,)*) -> Self {
				Self {
					$($t,)*
				}
			}
		}
		impl<$($t: Reducer,)*> Reducer for $reduceb<$($t,)*> {
			type Item = ($($t::Item,)*);
			type Output = ($($t::Output,)*);
			type Async = $reducebasync<$($t::Async,)*>;

			fn into_async(self) -> Self::Async {
				$reducebasync{
					$($t: self.$t.into_async(),)*
					peeked: None,
					output: ($(None::<$t::Output>,)*),
				}
			}
		}
		impl<$($t: ReducerProcessSend,)*> ReducerProcessSend for $reduceb<$($t,)*> {
			type Output = ($(<$t as ReducerProcessSend>::Output,)*);
		}
		impl<$($t: ReducerSend,)*> ReducerSend for $reduceb<$($t,)*> {
			type Output = ($(<$t as ReducerSend>::Output,)*);
		}
		#[pin_project]
		pub struct $reducebasync<$($t,)*> where $($t: ReducerAsync,)* {
			$(#[pin] $t: $t,)*
			peeked: Option<($(Option<$t::Item>,)*)>,
			output: ($(Option<$t::Output>,)*),
		}
		#[allow(unused_variables)]
		impl<$($t: ReducerAsync,)*> ReducerAsync for $reducebasync<$($t,)*> {
			type Item = ($($t::Item,)*);
			type Output = ($($t::Output,)*);

			fn poll_forward(self: Pin<&mut Self>, cx: &mut Context, stream: Pin<&mut impl Stream<Item = Self::Item>>) -> Poll<()> {
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
						ready.$num = self_.$t.as_mut().poll_forward(cx, stream).is_ready();
						if ready.$num {
							progress = true; // TODO remove
						}
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
			fn poll_output(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
				let mut self_ = self.project();
				let mut pending = false;
				$(
					if self_.output.$num.is_none() {
						if let Poll::Ready(output) = self_.$t.poll_output(cx) {
							self_.output.$num = Some(output);
						} else {
							pending = true;
						}
					}
				)*
				if !pending {
					Poll::Ready(($(self_.output.$num.take().unwrap(),)*))
				} else {
					Poll::Pending
				}
			}
		}
	);
}
impl_iterator_multi_tuple!(ReduceA0Factory ReduceA0 ReduceA0Async ReduceC0Factory ReduceC0 ReduceC0Async AsyncTuple0 Sum0:);
impl_iterator_multi_tuple!(ReduceA1Factory ReduceA1 ReduceA1Async ReduceC1Factory ReduceC1 ReduceC1Async AsyncTuple1 Sum1: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A,,);
impl_iterator_multi_tuple!(ReduceA2Factory ReduceA2 ReduceA2Async ReduceC2Factory ReduceC2 ReduceC2Async AsyncTuple2 Sum2 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A Copy,, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B Copy,,);
impl_iterator_multi_tuple!(ReduceA3Factory ReduceA3 ReduceA3Async ReduceC3Factory ReduceC3 ReduceC3Async AsyncTuple3 Sum3 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A Copy,, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B Copy,, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C Copy,,);
impl_iterator_multi_tuple!(ReduceA4Factory ReduceA4 ReduceA4Async ReduceC4Factory ReduceC4 ReduceC4Async AsyncTuple4 Sum4 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A Copy,, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B Copy,, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C Copy,, I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 3 D Copy,,);
impl_iterator_multi_tuple!(ReduceA5Factory ReduceA5 ReduceA5Async ReduceC5Factory ReduceC5 ReduceC5Async AsyncTuple5 Sum5 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A Copy,, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B Copy,, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C Copy,, I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 3 D Copy,, I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 4 E Copy,,);
impl_iterator_multi_tuple!(ReduceA6Factory ReduceA6 ReduceA6Async ReduceC6Factory ReduceC6 ReduceC6Async AsyncTuple6 Sum6 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A Copy,, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B Copy,, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C Copy,, I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 3 D Copy,, I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 4 E Copy,, I5 R5 O5 C5 iterator_5 reducer_a_5 reducer_b_5 5 F Copy,,);
impl_iterator_multi_tuple!(ReduceA7Factory ReduceA7 ReduceA7Async ReduceC7Factory ReduceC7 ReduceC7Async AsyncTuple7 Sum7 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A Copy,, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B Copy,, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C Copy,, I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 3 D Copy,, I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 4 E Copy,, I5 R5 O5 C5 iterator_5 reducer_a_5 reducer_b_5 5 F Copy,, I6 R6 O6 C6 iterator_6 reducer_a_6 reducer_b_6 6 G Copy,,);
impl_iterator_multi_tuple!(ReduceA8Factory ReduceA8 ReduceA8Async ReduceC8Factory ReduceC8 ReduceC8Async AsyncTuple8 Sum8 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A Copy,, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B Copy,, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C Copy,, I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 3 D Copy,, I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 4 E Copy,, I5 R5 O5 C5 iterator_5 reducer_a_5 reducer_b_5 5 F Copy,, I6 R6 O6 C6 iterator_6 reducer_a_6 reducer_b_6 6 G Copy,, I7 R7 O7 C7 iterator_7 reducer_a_7 reducer_b_7 7 H Copy,,);

#[pin_project]
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Peekable<'a, St: Stream> {
	#[pin]
	stream: St,
	peeked: &'a mut Option<St::Item>,
}

impl<'a, St: Stream> Peekable<'a, St> {
	#[project]
	pub fn poll_peek(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<&mut St::Item>> {
		#[project]
		let Peekable { mut stream, peeked } = self.project();

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

	#[project]
	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		#[project]
		let Peekable { stream, peeked } = self.project();
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
