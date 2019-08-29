pub struct ResultExpand<T, E>(pub Result<T, E>); // TODO: unpub
impl<T, E> IntoIterator for ResultExpand<T, E>
where
	T: IntoIterator,
{
	type Item = Result<T::Item, E>;
	type IntoIter = ResultExpandIter<T::IntoIter, E>;
	fn into_iter(self) -> Self::IntoIter {
		ResultExpandIter(self.0.map(IntoIterator::into_iter).map_err(Some))
	}
}
pub struct ResultExpandIter<T, E>(Result<T, Option<E>>);
impl<T, E> Iterator for ResultExpandIter<T, E>
where
	T: Iterator,
{
	type Item = Result<T::Item, E>;
	fn next(&mut self) -> Option<Self::Item> {
		transpose(self.0.as_mut().map(Iterator::next).map_err(Option::take))
	}
}
fn transpose<T, E>(result: Result<Option<T>, Option<E>>) -> Option<Result<T, E>> {
	match result {
		Ok(Some(x)) => Some(Ok(x)),
		Err(Some(e)) => Some(Err(e)),
		Ok(None) | Err(None) => None,
	}
}
