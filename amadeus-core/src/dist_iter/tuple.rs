use serde::{Deserialize, Serialize};
use sum::*;

use super::{
	ConsumerMulti, DistributedIteratorMulti, DistributedReducer, ReduceFactory, Reducer, ReducerA
};

macro_rules! impl_iterator_multi_tuple {
	($reduceafactory:ident $reducea:ident $reduceb:ident $enum:ident $($copy:ident)* : $($i:ident $r:ident $o:ident $c:ident $iterator:ident $reducera:ident $reducerb:ident $num:tt $t:ident),*) => (
		impl<
				$($i: DistributedIteratorMulti<Source>,)*
				Source,
				$($r: DistributedReducer<$i, Source, $o>,)*
				$($o,)*
			> DistributedReducer<($($i,)*), Source, ($($o,)*)> for ($($r,)*)
				where Source: $($copy)*,
		{
			type ReduceAFactory = $reduceafactory<$($r::ReduceAFactory,)*>;
			type ReduceA = $reducea<$($r::ReduceA,)*>;
			type ReduceB = $reduceb<$($r::ReduceB,)*>;

			fn reducers(self) -> (($($i,)*), Self::ReduceAFactory, Self::ReduceB) {
				$(let ($iterator, $reducera, $reducerb) = self.$num.reducers();)*
				(
					($($iterator,)*),
					$reduceafactory($($reducera,)*),
					$reduceb($($reducerb,)*),
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

		#[allow(unused_variables)]
		impl<Source, $($c: ConsumerMulti<Source>,)*> ConsumerMulti<Source>
			for ($($c,)*)
				where Source: $($copy)*,
		{
			type Item = $enum<$($c::Item,)*>;

			fn run(&self, source: Source, i: &mut impl FnMut(Self::Item) -> bool) -> bool {
				$(self.$num.run(source, &mut |x| i($enum::$t(x))) | )* false
			}
		}

		pub struct $reduceafactory<$($r,)*>($(pub(super) $r,)*);
		impl<$($r:ReduceFactory,)*> ReduceFactory for $reduceafactory<$($r,)*> {
			type Reducer = $reducea<$($r::Reducer,)*>;
			fn make(&self) -> Self::Reducer {
				$reducea($((self.$num.make(),true),)*)
			}
		}

		#[derive(Serialize, Deserialize)]
		pub struct $reducea<$($t,)*>($(($t,bool),)*);
		#[allow(unused_variables)]
		impl<$($t: Reducer,)*> Reducer for $reducea<$($t,)*> {
			type Item = $enum<$($t::Item,)*>;
			type Output = ($($t::Output,)*);

			fn push(&mut self, item: Self::Item) -> bool {
				match item {
					$($enum::$t(item) => self.$num.1 = self.$num.1 && self.$num.0.push(item),)*
				}
				#[allow(unreachable_code)] {
					$(self.$num.1 |)* false
				}
			}
			fn ret(self) -> Self::Output {
				($(self.$num.0.ret(),)*)
			}
		}
		impl<$($t: Reducer,)*> ReducerA for $reducea<$($t,)*> where $($t: Serialize + for<'de> Deserialize<'de> + 'static,)* $($t::Output: Serialize + for<'de> Deserialize<'de> + Send + 'static,)* {
			type Output = ($($t::Output,)*);
		}

		pub struct $reduceb<$($t,)*>($(pub(super) $t,)*);
		#[allow(unused_variables)]
		impl<$($t: Reducer,)*> Reducer for $reduceb<$($t,)*> {
			type Item = ($($t::Item,)*);
			type Output = ($($t::Output,)*);

			fn push(&mut self, item: Self::Item) -> bool {
				$(self.$num.push(item.$num) |)* false
			}
			fn ret(self) -> Self::Output {
				($(self.$num.ret(),)*)
			}
		}
	);
}
impl_iterator_multi_tuple!(ReduceA0Factory ReduceA0 ReduceB0 Sum0:);
impl_iterator_multi_tuple!(ReduceA1Factory ReduceA1 ReduceB1 Sum1: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A);
impl_iterator_multi_tuple!(ReduceA2Factory ReduceA2 ReduceB2 Sum2 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B);
impl_iterator_multi_tuple!(ReduceA3Factory ReduceA3 ReduceB3 Sum3 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C);
impl_iterator_multi_tuple!(ReduceA4Factory ReduceA4 ReduceB4 Sum4 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C, I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 3 D);
impl_iterator_multi_tuple!(ReduceA5Factory ReduceA5 ReduceB5 Sum5 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C, I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 3 D, I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 4 E);
impl_iterator_multi_tuple!(ReduceA6Factory ReduceA6 ReduceB6 Sum6 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C, I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 3 D, I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 4 E, I5 R5 O5 C5 iterator_5 reducer_a_5 reducer_b_5 5 F);
impl_iterator_multi_tuple!(ReduceA7Factory ReduceA7 ReduceB7 Sum7 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C, I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 3 D, I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 4 E, I5 R5 O5 C5 iterator_5 reducer_a_5 reducer_b_5 5 F, I6 R6 O6 C6 iterator_6 reducer_a_6 reducer_b_6 6 G);
impl_iterator_multi_tuple!(ReduceA8Factory ReduceA8 ReduceB8 Sum8 Copy: I0 R0 O0 C0 iterator_0 reducer_a_0 reducer_b_0 0 A, I1 R1 O1 C1 iterator_1 reducer_a_1 reducer_b_1 1 B, I2 R2 O2 C2 iterator_2 reducer_a_2 reducer_b_2 2 C, I3 R3 O3 C3 iterator_3 reducer_a_3 reducer_b_3 3 D, I4 R4 O4 C4 iterator_4 reducer_a_4 reducer_b_4 4 E, I5 R5 O5 C5 iterator_5 reducer_a_5 reducer_b_5 5 F, I6 R6 O6 C6 iterator_6 reducer_a_6 reducer_b_6 6 G, I7 R7 O7 C7 iterator_7 reducer_a_7 reducer_b_7 7 H);
