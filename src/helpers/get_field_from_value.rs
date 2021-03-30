use crate::par_stream::{Map, ParallelStream};
use amadeus_types::{DowncastFrom, Value};
use std::marker::PhantomData;

pub trait GetFieldFromValue<T, E> {
	fn get_field_from_value<O>(self, field_name: String) -> Map<T, UnwrapFieldHandler<O>>
	where
		O: DowncastFrom<Value> + Clone + Send + 'static;
}

impl<T, E> GetFieldFromValue<T, E> for T
where
	T: ParallelStream<Item = Result<Value, E>>,
	E: Send + 'static,
{
	fn get_field_from_value<O>(self, field_name: String) -> Map<T, UnwrapFieldHandler<O>>
	where
		O: DowncastFrom<Value> + Clone + Send + 'static,
	{
		self.map(UnwrapFieldHandler::<O> {
			field_name,
			marker: PhantomData,
		})
	}
}

#[derive(Clone)]
pub struct UnwrapFieldHandler<T>
where
	T: DowncastFrom<Value> + Clone + Send + 'static,
{
	field_name: String,
	marker: PhantomData<T>,
}

impl<E, T> FnOnce<(Result<Value, E>,)> for UnwrapFieldHandler<T>
where
	T: DowncastFrom<Value> + Clone + Send + 'static,
{
	type Output = Option<T>;

	extern "rust-call" fn call_once(mut self, args: (Result<Value, E>,)) -> Self::Output {
		self.call_mut(args)
	}
}

impl<E, T> FnMut<(Result<Value, E>,)> for UnwrapFieldHandler<T>
where
	T: DowncastFrom<Value> + Clone + Send + 'static,
{
	extern "rust-call" fn call_mut(&mut self, args: (Result<Value, E>,)) -> Self::Output {
		let field = args
			.0
			.ok()
			.unwrap()
			.into_group()
			.ok()
			.unwrap()
			.get(self.field_name.as_ref())
			.unwrap()
			.clone();
		T::downcast_from(field).ok()
	}
}
