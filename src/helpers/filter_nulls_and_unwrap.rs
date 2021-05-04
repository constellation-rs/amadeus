#![allow(clippy::type_complexity, clippy::option_option)]

use crate::par_stream::{Filter, Map};
use amadeus_core::par_stream::ParallelStream;

pub trait FilterNullsAndUnwrap<O, T>
where
	O: Send + 'static,
{
	fn filter_nulls_and_unwrap(self)
		-> Map<Filter<T, OptionFilterNullHandler>, fn(Option<O>) -> O>;
}

impl<T, O> FilterNullsAndUnwrap<O, T> for T
where
	T: ParallelStream<Item = Option<O>>,
	O: Send + 'static,
{
	fn filter_nulls_and_unwrap(
		self,
	) -> Map<Filter<T, OptionFilterNullHandler>, fn(Option<O>) -> O> {
		self.filter(OptionFilterNullHandler {}).map(Option::unwrap)
	}
}

pub trait FilterNullsAndDoubleUnwrap<O, T>
where
	O: Send + 'static,
{
	fn filter_nulls_and_double_unwrap(
		self,
	) -> Map<Filter<T, DoubleOptionFilterNullHandler>, fn(Option<Option<O>>) -> O>;
}

impl<T, O> FilterNullsAndDoubleUnwrap<O, T> for T
where
	T: ParallelStream<Item = Option<Option<O>>>,
	O: Send + 'static,
{
	fn filter_nulls_and_double_unwrap(
		self,
	) -> Map<Filter<T, DoubleOptionFilterNullHandler>, fn(Option<Option<O>>) -> O> {
		self.filter(DoubleOptionFilterNullHandler {})
			.map(|unwrapped_value: Option<Option<O>>| unwrapped_value.unwrap().unwrap())
	}
}

#[derive(Clone)]
pub struct DoubleOptionFilterNullHandler {}

impl<T> FnOnce<(&Option<Option<T>>,)> for DoubleOptionFilterNullHandler
where
	T: Send + 'static,
{
	type Output = bool;

	extern "rust-call" fn call_once(mut self, args: (&Option<Option<T>>,)) -> Self::Output {
		self.call_mut(args)
	}
}

impl<T> FnMut<(&Option<Option<T>>,)> for DoubleOptionFilterNullHandler
where
	T: Send + 'static,
{
	extern "rust-call" fn call_mut(&mut self, args: (&Option<Option<T>>,)) -> Self::Output {
		args.0.is_some() && args.0.as_ref().unwrap().is_some()
	}
}

#[derive(Clone)]
pub struct OptionFilterNullHandler {}

impl<T> FnOnce<(&Option<T>,)> for OptionFilterNullHandler
where
	T: Send + 'static,
{
	type Output = bool;

	extern "rust-call" fn call_once(mut self, args: (&Option<T>,)) -> Self::Output {
		self.call_mut(args)
	}
}

impl<T> FnMut<(&Option<T>,)> for OptionFilterNullHandler
where
	T: Send + 'static,
{
	extern "rust-call" fn call_mut(&mut self, args: (&Option<T>,)) -> Self::Output {
		args.0.is_some()
	}
}
