/// Union `Self` with `Rhs` in place.
pub trait UnionAssign<Rhs = Self> {
	/// Union.
	fn union_assign(&mut self, rhs: Rhs);
}
impl<'a> UnionAssign<&'a u8> for u8 {
	fn union_assign(&mut self, rhs: &'a Self) {
		*self = (*self).max(*rhs);
	}
}
impl<'a> UnionAssign<&'a u16> for u16 {
	fn union_assign(&mut self, rhs: &'a Self) {
		*self = (*self).max(*rhs);
	}
}
impl<'a> UnionAssign<&'a u32> for u32 {
	fn union_assign(&mut self, rhs: &'a Self) {
		*self = (*self).max(*rhs);
	}
}
impl<'a> UnionAssign<&'a u64> for u64 {
	fn union_assign(&mut self, rhs: &'a Self) {
		*self = (*self).max(*rhs);
	}
}
impl UnionAssign for usize {
	fn union_assign(&mut self, rhs: Self) {
		*self = (*self).max(rhs);
	}
}
impl<'a> UnionAssign<&'a usize> for usize {
	fn union_assign(&mut self, rhs: &'a Self) {
		*self = (*self).max(*rhs);
	}
}

/// Intersect zero or more `&Self` to create `Option<Self>`.
pub trait Intersect {
	/// Intersect.
	fn intersect<'a>(iter: impl Iterator<Item = &'a Self>) -> Option<Self>
	where
		Self: Sized + 'a;
}
impl Intersect for u8 {
	fn intersect<'a>(iter: impl Iterator<Item = &'a Self>) -> Option<Self>
	where
		Self: Sized + 'a,
	{
		iter.cloned().min()
	}
}
impl Intersect for u16 {
	fn intersect<'a>(iter: impl Iterator<Item = &'a Self>) -> Option<Self>
	where
		Self: Sized + 'a,
	{
		iter.cloned().min()
	}
}
impl Intersect for u32 {
	fn intersect<'a>(iter: impl Iterator<Item = &'a Self>) -> Option<Self>
	where
		Self: Sized + 'a,
	{
		iter.cloned().min()
	}
}
impl Intersect for u64 {
	fn intersect<'a>(iter: impl Iterator<Item = &'a Self>) -> Option<Self>
	where
		Self: Sized + 'a,
	{
		iter.cloned().min()
	}
}
impl Intersect for usize {
	fn intersect<'a>(iter: impl Iterator<Item = &'a Self>) -> Option<Self>
	where
		Self: Sized + 'a,
	{
		iter.cloned().min()
	}
}

/// New instances are instantiable given a specified input of `<Self as New>::Config`.
pub trait New {
	/// The type of data required to instantiate a new `Self`.
	type Config: Clone;
	/// Instantiate a new `Self` with the given `<Self as New>::Config`.
	fn new(config: &Self::Config) -> Self;
}
impl New for u8 {
	type Config = ();
	fn new(_config: &Self::Config) -> Self {
		0
	}
}
impl New for u16 {
	type Config = ();
	fn new(_config: &Self::Config) -> Self {
		0
	}
}
impl New for u32 {
	type Config = ();
	fn new(_config: &Self::Config) -> Self {
		0
	}
}
impl New for u64 {
	type Config = ();
	fn new(_config: &Self::Config) -> Self {
		0
	}
}
impl New for usize {
	type Config = ();
	fn new(_config: &Self::Config) -> Self {
		0
	}
}
