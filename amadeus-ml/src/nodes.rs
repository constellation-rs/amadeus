use hibitset::BitSet;
use itertools::izip;
use ndarray::Axis;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{
	cell::{Cell, Ref, RefCell}, convert::TryInto, fmt, ops::{AddAssign, Deref}, rc::Rc, sync::Arc
};

use super::{clamp, Arr, Variable};
use crate::{
	numerics, numerics::{ArraySlice, ArraySliceMut, ArraySliceOps}
};

#[derive(Debug, PartialEq)]
pub enum ForwardAction {
	Evaluate,
	Cached,
}

#[derive(Debug, PartialEq)]
pub enum BackwardAction {
	Set,
	Increment,
}

#[derive(Debug, Default)]
pub struct PassCounter {
	forward_count: Cell<usize>,
	backward_count: Cell<usize>,
}

impl PassCounter {
	pub fn clear(&self) {
		self.forward_count.set(0);
		self.backward_count.set(0);
	}
	#[inline(always)]
	pub fn is_zero(&self) -> bool {
		debug_assert!(self.recurse_backward(), "Not fully backpropagated.");

		self.forward_count.get() == 0
	}
	pub fn recurse_backward(&self) -> bool {
		let backward_count = self.backward_count.get();
		let forward_count = self.forward_count.get();

		assert!(backward_count <= forward_count);

		if backward_count == forward_count {
			self.clear();

			true
		} else {
			false
		}
	}
	#[inline(always)]
	pub fn forward(&self) -> ForwardAction {
		let count = self.forward_count.get();
		self.forward_count.set(count + 1);

		match count {
			0 => ForwardAction::Evaluate,
			_ => ForwardAction::Cached,
		}
	}
	#[inline(always)]
	pub fn backward(&self) -> BackwardAction {
		let backward_count = self.backward_count.get();

		let action = match backward_count {
			0 => BackwardAction::Set,
			_ => BackwardAction::Increment,
		};

		self.backward_count.set(backward_count + 1);

		action
	}
}

/// Generalisation over borrowed `RefCell` values
/// and simple references.
#[derive(Debug)]
pub enum Bor<'value, T: 'value> {
	/// Ref from a `RefCell`.
	RefGuard(Ref<'value, T>),
	/// Plain reference.
	Reference(&'value T),
}

impl<'value, T: 'value> Deref for Bor<'value, T> {
	type Target = T;
	fn deref(&self) -> &T {
		match *self {
			Bor::RefGuard(ref val) => &*val,
			Bor::Reference(ref val) => &*val,
		}
	}
}

impl<'value, T: 'value + fmt::Display> fmt::Display for Bor<'value, T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", &**self)
	}
}

/// Trait representing a computation node. Structs implementing
/// this trait can be used as elements of the computation graph.
pub trait Node: fmt::Debug + 'static {
	/// Type of the node's value.
	type Value;
	/// Type of the input gradient the node receives
	/// during backpropagation.
	type InputGradient;
	/// Perform the forward step. Should recursively call
	/// the forward methods of its ancestors.
	fn forward(&self);
	/// Perform the backward step. Should recursively call
	/// the backward methods of its ancestors.
	fn backward(&self, gradient: &Ref<Self::InputGradient>);
	/// Return the value of the node.
	fn value(&self) -> Bor<Self::Value>;
	/// If the node needs to be used in the backward step.
	fn needs_gradient(&self) -> bool;
	/// Reset the caches of this node and its parents.
	fn clear(&self);
}

impl Node for Rc<dyn Node<Value = Arr, InputGradient = Arr>> {
	type Value = Arr;
	type InputGradient = Arr;
	#[inline(always)]
	fn forward(&self) {
		(**self).forward()
	}
	#[inline(always)]
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		(**self).backward(gradient)
	}
	#[inline(always)]
	fn value(&self) -> Bor<Self::Value> {
		(**self).value()
	}
	#[inline(always)]
	fn needs_gradient(&self) -> bool {
		(**self).needs_gradient()
	}
	#[inline(always)]
	fn clear(&self) {
		(**self).clear()
	}
}

#[derive(Debug)]
pub struct AddNode<LHS, RHS> {
	value: RefCell<Arr>,
	gradient: RefCell<Arr>,
	lhs: Rc<LHS>,
	rhs: Rc<RHS>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<LHS, RHS> AddNode<LHS, RHS>
where
	LHS: Node<Value = Arr>,
	RHS: Node<Value = Arr>,
{
	pub fn new(lhs: Rc<LHS>, rhs: Rc<RHS>) -> Self {
		let needs_gradient = lhs.needs_gradient() || rhs.needs_gradient();
		let value = &*lhs.value() + &*rhs.value();
		let gradient = &*rhs.value() * 0.0;

		AddNode {
			value: RefCell::new(value),
			gradient: RefCell::new(gradient),
			lhs,
			rhs,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<LHS, RHS> Node for AddNode<LHS, RHS>
where
	LHS: Node<Value = Arr, InputGradient = Arr>,
	RHS: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.lhs.forward();
		self.rhs.forward();

		let lhs_value = self.lhs.value();
		let rhs_value = self.rhs.value();

		debug_assert_eq!(
			lhs_value.shape(),
			self.value().shape(),
			"LHS operand changed shape."
		);
		debug_assert_eq!(
			rhs_value.shape(),
			self.value().shape(),
			"RHS operand changed shape."
		);

		let mut self_value = self.value.borrow_mut();

		for (v, &lhs, &rhs) in izip!(
			self_value.fast_slice_mut(),
			lhs_value.fast_slice(),
			rhs_value.fast_slice()
		) {
			*v = lhs + rhs;
		}
	}
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				let mut operand_gradient = self.gradient.borrow_mut();
				operand_gradient.slice_assign(&**gradient);
			}
			BackwardAction::Increment => {
				let mut operand_gradient = self.gradient.borrow_mut();
				operand_gradient.slice_add_assign(&**gradient);
			}
		}

		if self.counter.recurse_backward() {
			let gradient = self.gradient.borrow();
			self.lhs.backward(&gradient);
			self.rhs.backward(&gradient);
		}
	}
	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}
	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}
	fn clear(&self) {
		if !self.counter.is_zero() {
			self.lhs.clear();
			self.rhs.clear();
			self.counter.clear();
		}
	}
}

fn row_wise_stack(dest: &mut Arr, lhs: &Arr, rhs: &Arr) {
	for (mut dest_row, source_row) in dest
		.genrows_mut()
		.into_iter()
		.zip(lhs.genrows().into_iter().chain(rhs.genrows()))
	{
		numerics::slice_assign(
			dest_row.as_slice_mut().unwrap(),
			source_row.as_slice().unwrap(),
		);
	}
}

fn column_wise_stack(dest: &mut Arr, lhs: &Arr, rhs: &Arr) {
	for (mut dest_row, lhs_row, rhs_row) in izip!(
		dest.genrows_mut().into_iter(),
		lhs.genrows().into_iter(),
		rhs.genrows().into_iter()
	) {
		let dest_row = dest_row.as_slice_mut().unwrap();
		let lhs_row = lhs_row.as_slice().unwrap();
		let rhs_row = rhs_row.as_slice().unwrap();

		let (left, right) = dest_row.split_at_mut(lhs_row.len());
		numerics::slice_assign(left, lhs_row);
		numerics::slice_assign(right, rhs_row);
	}
}

fn column_wise_stack_gradient(gradient: &Arr, lhs: &mut Arr, rhs: &mut Arr, op: &BackwardAction) {
	for (grad_row, mut lhs_row, mut rhs_row) in izip!(
		gradient.genrows().into_iter(),
		lhs.genrows_mut().into_iter(),
		rhs.genrows_mut().into_iter()
	) {
		let grad_row = grad_row.fast_slice();
		let lhs_row = lhs_row.fast_slice_mut();
		let rhs_row = rhs_row.fast_slice_mut();

		let (left, right) = grad_row.split_at(lhs_row.len());

		match *op {
			BackwardAction::Increment => {
				for (x, y) in lhs_row.iter_mut().zip(left.iter()) {
					*x += y;
				}
				for (x, y) in rhs_row.iter_mut().zip(right.iter()) {
					*x += y;
				}
			}
			BackwardAction::Set => {
				lhs_row.copy_from_slice(left);
				rhs_row.copy_from_slice(right);
			}
		}
	}
}

fn row_wise_stack_gradient(gradient: &Arr, lhs: &mut Arr, rhs: &mut Arr, op: &BackwardAction) {
	for (grad_row, mut dest_row) in gradient
		.genrows()
		.into_iter()
		.zip(lhs.genrows_mut().into_iter().chain(rhs.genrows_mut()))
	{
		let grad_row = grad_row.as_slice().unwrap();
		let dest_row = dest_row.as_slice_mut().unwrap();

		match *op {
			BackwardAction::Increment => {
				for (x, y) in dest_row.iter_mut().zip(grad_row.iter()) {
					*x += y;
				}
			}
			BackwardAction::Set => {
				for (x, &y) in dest_row.iter_mut().zip(grad_row.iter()) {
					*x = y;
				}
			}
		}
	}
}

#[derive(Debug)]
pub struct ConcatenateNode<LHS, RHS> {
	axis: Axis,
	value: RefCell<Arr>,
	lhs_gradient: RefCell<Arr>,
	rhs_gradient: RefCell<Arr>,
	lhs: Rc<LHS>,
	rhs: Rc<RHS>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<LHS, RHS> ConcatenateNode<LHS, RHS>
where
	LHS: Node<Value = Arr>,
	RHS: Node<Value = Arr>,
{
	pub fn new(lhs: Rc<LHS>, rhs: Rc<RHS>, axis: Axis) -> Self {
		let needs_gradient = lhs.needs_gradient() || rhs.needs_gradient();

		let value = ndarray::stack(axis, &[lhs.value().view(), rhs.value().view()])
			.expect("Unable to concatenate arrays.");

		let lhs_gradient = &*lhs.value() * 0.0;
		let rhs_gradient = &*rhs.value() * 0.0;

		ConcatenateNode {
			axis,
			value: RefCell::new(value),
			lhs_gradient: RefCell::new(lhs_gradient),
			rhs_gradient: RefCell::new(rhs_gradient),
			lhs,
			rhs,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<LHS, RHS> Node for ConcatenateNode<LHS, RHS>
where
	LHS: Node<Value = Arr, InputGradient = Arr>,
	RHS: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.lhs.forward();
		self.rhs.forward();

		let lhs_value = self.lhs.value();
		let rhs_value = self.rhs.value();

		let mut self_value = self.value.borrow_mut();

		match self.axis {
			// Vertically
			Axis(0) => row_wise_stack(&mut *self_value, &*lhs_value, &*rhs_value),
			// Horizontally
			Axis(1) => column_wise_stack(&mut *self_value, &*lhs_value, &*rhs_value),
			// Not allowed
			_ => panic!("Stacking tensors not allowed."),
		}
	}
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		{
			let mut lhs_grad = self.lhs_gradient.borrow_mut();
			let mut rhs_grad = self.rhs_gradient.borrow_mut();

			match self.axis {
				Axis(0) => row_wise_stack_gradient(
					gradient,
					&mut *lhs_grad,
					&mut *rhs_grad,
					&self.counter.backward(),
				),
				Axis(1) => column_wise_stack_gradient(
					gradient,
					&mut *lhs_grad,
					&mut *rhs_grad,
					&self.counter.backward(),
				),
				_ => panic!("Stacking tensors not allowed."),
			}
		}

		if self.counter.recurse_backward() {
			self.lhs.backward(&self.lhs_gradient.borrow());
			self.rhs.backward(&self.rhs_gradient.borrow());
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}
	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}
	fn clear(&self) {
		if !self.counter.is_zero() {
			self.lhs.clear();
			self.rhs.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct SliceNode<LHS> {
	slice: ndarray::SliceInfo<[ndarray::SliceOrIndex; 2], ndarray::Ix2>,
	value: RefCell<Arr>,
	lhs_gradient: RefCell<Arr>,
	lhs: Rc<LHS>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<LHS> SliceNode<LHS>
where
	LHS: Node<Value = Arr>,
{
	pub fn new(
		lhs: Rc<LHS>, slice: &ndarray::SliceInfo<[ndarray::SliceOrIndex; 2], ndarray::Ix2>,
	) -> Self {
		let needs_gradient = lhs.needs_gradient();

		let value = {
			let val = lhs.value();
			let sliced = val.slice(slice);
			let mut value = Arr::zeros((sliced.nrows(), sliced.ncols()));
			value.assign(&sliced);

			value
		};

		let lhs_gradient = &*lhs.value() * 0.0;

		SliceNode {
			slice: *slice,
			value: RefCell::new(value),
			lhs_gradient: RefCell::new(lhs_gradient),
			lhs,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<LHS> Node for SliceNode<LHS>
where
	LHS: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.lhs.forward();

		let lhs_value = self.lhs.value();
		let mut self_value = self.value.borrow_mut();
		self_value.assign(&lhs_value.slice(&self.slice));
	}
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				self.lhs_gradient
					.borrow_mut()
					.slice_mut(&self.slice)
					.assign(&*gradient);
			}
			BackwardAction::Increment => {
				self.lhs_gradient
					.borrow_mut()
					.slice_mut(&self.slice)
					.add_assign(&**gradient);
			}
		}

		if self.counter.recurse_backward() {
			self.lhs.backward(&self.lhs_gradient.borrow());
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}
	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}
	fn clear(&self) {
		if !self.counter.is_zero() {
			self.lhs.clear();
			self.counter.clear();
		}
	}
}

/// Input node for the graph.
#[derive(Debug)]
pub struct InputNode {
	pub(crate) value: RefCell<Arr>,
}

impl InputNode {
	/// Create a new input node with a given value. This fixes the shape
	/// of the node in the graph.
	pub fn new(value: Arr) -> Variable<Self> {
		Variable::new(
			Rc::new(InputNode {
				value: RefCell::new(value),
			}),
			Vec::new(),
		)
	}
}

impl Node for InputNode {
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {}
	fn backward(&self, _: &Ref<Self::InputGradient>) {}
	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}
	fn needs_gradient(&self) -> bool {
		false
	}
	fn clear(&self) {}
}

#[derive(Debug)]
pub(crate) struct GradientAccumulator {
	gradient: Arr,
	sparse_index: BitSet,
	dense: bool,
	sparse: bool,
}

impl GradientAccumulator {
	pub fn new(shape: (usize, usize)) -> Self {
		Self {
			gradient: Arr::zeros(shape),
			sparse_index: BitSet::with_capacity(100),
			dense: false,
			sparse: false,
		}
	}

	pub fn add_dense(&mut self, grad: &Arr) {
		if !self.dense {
			self.gradient.slice_assign(grad);
		} else {
			self.gradient.slice_add_assign(grad);
		}

		self.dense = true;
	}

	pub fn add_sparse(&mut self, indices: &[usize], grad: &Arr) {
		for (&idx, row) in izip!(indices.iter(), grad.genrows().into_iter()) {
			self.add_sparse_row(idx, &row);
		}
	}

	pub fn add_sparse_row(&mut self, idx: usize, grad: &ndarray::ArrayView<f32, ndarray::Ix1>) {
		if self.sparse_index.add(idx.try_into().unwrap()) {
			self.gradient
				.index_axis_mut(Axis(0), idx)
				.slice_add_assign(grad);
		} else {
			self.gradient
				.index_axis_mut(Axis(0), idx)
				.slice_assign(grad);
		}

		self.sparse = true;
	}

	pub fn sparse_iter(
		&self,
	) -> impl Iterator<Item = (usize, ndarray::ArrayView<f32, ndarray::Ix1>)> {
		let idx = &self.sparse_index;
		let grad = &self.gradient;

		idx.into_iter().map(move |idx| {
			let idx = idx as usize;
			(idx, grad.index_axis(Axis(0), idx))
		})
	}

	pub fn zero_gradient(&mut self) {
		if self.sparse {
			self.sparse_index.clear()
		}

		self.dense = false;
		self.sparse = false;
	}

	pub fn gradient(&self) -> &Arr {
		&self.gradient
	}

	/// With sparse gradients we don't reset to zero, so we
	/// need this to provide correct dense gradients to
	/// finite difference methods.
	pub fn materialized_gradient(&self) -> Arr {
		if self.has_dense() {
			self.gradient.clone()
		} else {
			let mut grad = &self.gradient * 0.0;
			for (idx, row) in self.sparse_iter() {
				grad.index_axis_mut(Axis(0), idx).slice_assign(&row);
			}
			grad
		}
	}

	pub fn has_dense(&self) -> bool {
		self.dense
	}

	pub fn clamp(&mut self, min: f32, max: f32) {
		if self.has_dense() {
			self.gradient
				.fast_slice_mut()
				.iter_mut()
				.for_each(|x| *x = clamp(*x, min, max));
		} else {
			unimplemented!();
			// for (idx, row) in self.sparse_iter() {
			// 	self.gradient
			// 		.index_axis_mut(Axis(0), idx)
			// 		.fast_slice_mut()
			// 		.iter_mut()
			// 		.for_each(|x| *x = clamp(*x, min, max));
			// }
		}
	}
}

pub trait GradientSink<T> {
	fn accumulate_gradient(&mut self, gradient: T);
}

impl<'a, 'b> GradientSink<&'a Ref<'b, Arr>> for GradientAccumulator {
	fn accumulate_gradient(&mut self, gradient: &Ref<Arr>) {
		self.add_dense(gradient);
	}
}

impl<'a> GradientSink<&'a Arr> for GradientAccumulator {
	fn accumulate_gradient(&mut self, gradient: &'a Arr) {
		self.add_dense(gradient);
	}
}

impl<'a> GradientSink<&'a mut Arr> for GradientAccumulator {
	fn accumulate_gradient(&mut self, gradient: &'a mut Arr) {
		self.add_dense(gradient);
	}
}

impl<'a> GradientSink<(&'a [usize], &'a Arr)> for GradientAccumulator {
	fn accumulate_gradient(&mut self, gradient: (&'a [usize], &'a Arr)) {
		let (idx, grad) = gradient;
		self.add_sparse(idx, grad);
	}
}

impl<'a, 'b: 'a> GradientSink<(usize, &'a ndarray::ArrayView<'b, f32, ndarray::Ix1>)>
	for GradientAccumulator
{
	fn accumulate_gradient(
		&mut self, gradient: (usize, &'a ndarray::ArrayView<'b, f32, ndarray::Ix1>),
	) {
		let (idx, grad) = gradient;
		self.add_sparse_row(idx, grad);
	}
}

unsafe impl Sync for HogwildParameter {}

/// Struct used to hold parameters that need to be shared among
/// multiple `ParameterNode`s for asynchronous, parallel optimization.
#[derive(Debug, Serialize, Deserialize)]
pub struct HogwildParameter {
	shape: (usize, usize),
	pub(crate) value: RefCell<Arr>,
	pub(crate) squared_gradients: RefCell<Arr>,
	pub(crate) moments: RefCell<Arr>,
	num_updates: Cell<i32>,
}

impl Clone for HogwildParameter {
	fn clone(&self) -> HogwildParameter {
		HogwildParameter {
			shape: self.shape,
			value: self.value.clone(),
			squared_gradients: self.squared_gradients.clone(),
			moments: self.moments.clone(),
			num_updates: self.num_updates.clone(),
		}
	}
}

#[allow(clippy::mut_from_ref)]
impl HogwildParameter {
	/// Create a new parameter object.
	pub fn new(value: Arr) -> Self {
		let squared_gradients = &value * 0.0;
		let moments = &value * 0.0;
		let shape = (value.nrows(), value.ncols());

		HogwildParameter {
			shape,
			value: RefCell::new(value),
			squared_gradients: RefCell::new(squared_gradients),
			moments: RefCell::new(moments),
			num_updates: Cell::new(0),
		}
	}

	/// Get the parameter value.
	pub fn value(&self) -> &Arr {
		unsafe { &*(self.value.as_ptr()) }
	}

	pub(crate) unsafe fn value_mut(&self) -> &mut Arr {
		&mut *(self.value.as_ptr())
	}

	pub(crate) unsafe fn squared_gradient_mut(&self) -> &mut Arr {
		&mut *(self.squared_gradients.as_ptr())
	}

	pub(crate) unsafe fn moments_mut(&self) -> &mut Arr {
		&mut *(self.moments.as_ptr())
	}

	pub(crate) unsafe fn num_updates_mut(&self) -> &mut i32 {
		&mut *(self.num_updates.as_ptr())
	}
}

/// Parameter node, holds the optimizable parameters of the model.
#[derive(Debug)]
pub struct ParameterNode {
	pub(crate) value: Arc<HogwildParameter>,
	pub(crate) gradient: RefCell<GradientAccumulator>,
}

impl ParameterNode {
	/// Create a parameter node that shares its parameter values
	/// with other parameter nodes via the `HogwildParameter` object.
	pub fn shared(value: Arc<HogwildParameter>) -> Variable<Self> {
		let shape = unsafe {
			// This method can be called in multiple threads, so borrowing
			// (even immutably) will read to borrow failures.
			(
				(*value.value.as_ptr()).nrows(),
				(*value.value.as_ptr()).ncols(),
			)
		};

		let node = Rc::new(ParameterNode {
			value,
			gradient: RefCell::new(GradientAccumulator::new(shape)),
		});
		let params = vec![Variable::new(Rc::clone(&node), Vec::new())];

		Variable::new(node, params)
	}
	/// Create a new parameter node. The parameters held by this node
	/// cannot be shared and optimized in parallel.
	pub fn new(value: Arr) -> Variable<Self> {
		let shape = (value.nrows(), value.ncols());

		let node = Rc::new(ParameterNode {
			value: Arc::new(HogwildParameter::new(value)),
			gradient: RefCell::new(GradientAccumulator::new(shape)),
		});
		let params = vec![Variable::new(Rc::clone(&node), Vec::new())];

		Variable::new(node, params)
	}

	pub(crate) fn zero_gradient(&self) {
		self.gradient.borrow_mut().zero_gradient();
	}
}

impl Node for ParameterNode {
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {}
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		self.gradient.borrow_mut().accumulate_gradient(gradient);
	}
	fn value(&self) -> Bor<Self::Value> {
		Bor::Reference(unsafe { &*self.value.value.as_ptr() })
	}
	fn needs_gradient(&self) -> bool {
		true
	}
	fn clear(&self) {}
}

#[derive(Debug)]
pub struct SubNode<LHS, RHS>
where
	LHS: Node<Value = Arr, InputGradient = Arr>,
	RHS: Node<Value = Arr, InputGradient = Arr>,
{
	value: RefCell<Arr>,
	lhs_gradient: RefCell<Arr>,
	rhs_gradient: RefCell<Arr>,
	lhs: Rc<LHS>,
	rhs: Rc<RHS>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<LHS, RHS> SubNode<LHS, RHS>
where
	LHS: Node<Value = Arr, InputGradient = Arr>,
	RHS: Node<Value = Arr, InputGradient = Arr>,
{
	pub fn new(lhs: Rc<LHS>, rhs: Rc<RHS>) -> Self {
		let needs_gradient = lhs.needs_gradient() || rhs.needs_gradient();
		let value = &*lhs.value() - &*rhs.value();

		let rhs_gradient = &*rhs.value() * 0.0;
		let lhs_gradient = &*lhs.value() * 0.0;

		SubNode {
			value: RefCell::new(value),
			rhs_gradient: RefCell::new(rhs_gradient),
			lhs_gradient: RefCell::new(lhs_gradient),
			lhs,
			rhs,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<LHS, RHS> Node for SubNode<LHS, RHS>
where
	LHS: Node<Value = Arr, InputGradient = Arr>,
	RHS: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.lhs.forward();
		self.rhs.forward();

		let mut dest = self.value.borrow_mut();

		numerics::sub(&*self.lhs.value(), &*self.rhs.value(), &mut *dest);
	}

	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				let mut rhs_gradient = self.rhs_gradient.borrow_mut();

				numerics::simd_scaled_assign(
					rhs_gradient.as_slice_mut().unwrap(),
					gradient.as_slice().unwrap(),
					-1.0,
				);

				let mut lhs_gradient = self.lhs_gradient.borrow_mut();

				numerics::simd_scaled_assign(
					lhs_gradient.as_slice_mut().unwrap(),
					gradient.as_slice().unwrap(),
					1.0,
				);
			}
			BackwardAction::Increment => {
				let mut rhs_gradient = self.rhs_gradient.borrow_mut();
				rhs_gradient.slice_sub_assign(&**gradient);

				let mut lhs_gradient = self.lhs_gradient.borrow_mut();
				lhs_gradient.slice_add_assign(&**gradient);
			}
		}

		if self.counter.recurse_backward() {
			self.lhs.backward(&self.lhs_gradient.borrow());
			self.rhs.backward(&self.rhs_gradient.borrow());
		}
	}
	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}
	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}
	fn clear(&self) {
		if !self.counter.is_zero() {
			self.lhs.clear();
			self.rhs.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct MulNode<LHS, RHS> {
	value: RefCell<Arr>,
	lhs_gradient: RefCell<Arr>,
	rhs_gradient: RefCell<Arr>,
	lhs: Rc<LHS>,
	rhs: Rc<RHS>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<LHS, RHS> MulNode<LHS, RHS>
where
	LHS: Node<Value = Arr>,
	RHS: Node<Value = Arr>,
{
	pub fn new(lhs: Rc<LHS>, rhs: Rc<RHS>) -> Self {
		let needs_gradient = lhs.needs_gradient() || rhs.needs_gradient();
		let value = &*lhs.value() * &*rhs.value();

		let lhs_gradient = &value * 0.0;
		let rhs_gradient = &value * 0.0;

		MulNode {
			value: RefCell::new(value),
			lhs_gradient: RefCell::new(lhs_gradient),
			rhs_gradient: RefCell::new(rhs_gradient),
			lhs,
			rhs,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<LHS, RHS> Node for MulNode<LHS, RHS>
where
	LHS: Node<Value = Arr, InputGradient = Arr>,
	RHS: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.lhs.forward();
		self.rhs.forward();

		let mut dest = self.value.borrow_mut();

		numerics::mul(&*self.lhs.value(), &*self.rhs.value(), &mut *dest);
	}
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				let mut lhs_gradient = self.lhs_gradient.borrow_mut();

				numerics::mul(&*self.rhs.value(), &*gradient, &mut *lhs_gradient);

				let mut rhs_gradient = self.rhs_gradient.borrow_mut();

				numerics::mul(&*self.lhs.value(), &*gradient, &mut *rhs_gradient);
			}
			BackwardAction::Increment => {
				let mut lhs_gradient = self.lhs_gradient.borrow_mut();
				let mut rhs_gradient = self.rhs_gradient.borrow_mut();

				numerics::increment_mul(&*self.rhs.value(), &*gradient, &mut *lhs_gradient);
				numerics::increment_mul(&*self.lhs.value(), &*gradient, &mut *rhs_gradient);
			}
		}

		if self.counter.recurse_backward() {
			self.lhs.backward(&self.lhs_gradient.borrow());
			self.rhs.backward(&self.rhs_gradient.borrow());
		}
	}
	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}
	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}
	fn clear(&self) {
		if !self.counter.is_zero() {
			self.lhs.clear();
			self.rhs.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct DivNode<LHS, RHS> {
	value: RefCell<Arr>,
	lhs_gradient: RefCell<Arr>,
	rhs_gradient: RefCell<Arr>,
	lhs: Rc<LHS>,
	rhs: Rc<RHS>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<LHS, RHS> DivNode<LHS, RHS>
where
	LHS: Node<Value = Arr>,
	RHS: Node<Value = Arr>,
{
	pub fn new(lhs: Rc<LHS>, rhs: Rc<RHS>) -> Self {
		let needs_gradient = lhs.needs_gradient() || rhs.needs_gradient();
		let value = &*lhs.value() / &*rhs.value();

		let lhs_gradient = &value * 0.0;
		let rhs_gradient = &value * 0.0;

		DivNode {
			value: RefCell::new(value),
			lhs_gradient: RefCell::new(lhs_gradient),
			rhs_gradient: RefCell::new(rhs_gradient),
			lhs,
			rhs,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<LHS, RHS> Node for DivNode<LHS, RHS>
where
	LHS: Node<Value = Arr, InputGradient = Arr>,
	RHS: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.lhs.forward();
		self.rhs.forward();

		let mut dest = self.value.borrow_mut();

		numerics::div(&*self.lhs.value(), &*self.rhs.value(), &mut *dest);
	}
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				let mut lhs_gradient = self.lhs_gradient.borrow_mut();
				let rhs_value = self.rhs.value();

				numerics::div(&*gradient, &*rhs_value, &mut *lhs_gradient);

				let mut rhs_gradient = self.rhs_gradient.borrow_mut();

				izip!(
					rhs_gradient.iter_mut(),
					self.lhs.value().iter(),
					rhs_value.iter(),
					gradient.iter()
				)
				.for_each(|(dest, lhs_val, rhs_val, grad_val)| {
					*dest = -lhs_val / rhs_val.powi(2) * grad_val
				});
			}
			BackwardAction::Increment => {
				let mut lhs_gradient = self.lhs_gradient.borrow_mut();
				let rhs_value = self.rhs.value();

				numerics::increment_div(&*gradient, &*rhs_value, &mut *lhs_gradient);

				let mut rhs_gradient = self.rhs_gradient.borrow_mut();

				izip!(
					rhs_gradient.iter_mut(),
					self.lhs.value().iter(),
					rhs_value.iter(),
					gradient.iter()
				)
				.for_each(|(dest, lhs_val, rhs_val, grad_val)| {
					*dest += -lhs_val / rhs_val.powi(2) * grad_val
				});
			}
		}

		if self.counter.recurse_backward() {
			self.lhs.backward(&self.lhs_gradient.borrow());
			self.rhs.backward(&self.rhs_gradient.borrow());
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}

	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}

	fn clear(&self) {
		if !self.counter.is_zero() {
			self.lhs.clear();
			self.rhs.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct DotNode<LHS, RHS> {
	value: RefCell<Arr>,
	gradient: RefCell<Arr>,
	lhs_gradient: RefCell<Arr>,
	rhs_gradient: RefCell<Arr>,
	lhs: Rc<LHS>,
	rhs: Rc<RHS>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<LHS, RHS> DotNode<LHS, RHS>
where
	LHS: Node<Value = Arr>,
	RHS: Node<Value = Arr>,
{
	pub fn new(lhs: Rc<LHS>, rhs: Rc<RHS>) -> Self {
		let needs_gradient = lhs.needs_gradient() || rhs.needs_gradient();
		let value = lhs.value().dot(&*rhs.value());
		let gradient = &value * 0.0;

		let lhs_gradient = &*lhs.value() * 0.0;
		let rhs_gradient = &*rhs.value() * 0.0;

		DotNode {
			value: RefCell::new(value),
			gradient: RefCell::new(gradient),
			lhs_gradient: RefCell::new(lhs_gradient),
			rhs_gradient: RefCell::new(rhs_gradient),
			lhs,
			rhs,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<LHS, RHS> Node for DotNode<LHS, RHS>
where
	LHS: Node<Value = Arr, InputGradient = Arr>,
	RHS: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;

	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.lhs.forward();
		self.rhs.forward();

		numerics::mat_mul(
			1.0,
			&*self.lhs.value(),
			&*self.rhs.value(),
			0.0,
			&mut *self.value.borrow_mut(),
		);
	}

	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				self.gradient.borrow_mut().slice_assign(&**gradient);
			}
			BackwardAction::Increment => {
				self.gradient.borrow_mut().slice_add_assign(&**gradient);
			}
		}

		if self.counter.recurse_backward() {
			{
				let rhs_value = self.rhs.value();
				let lhs_value = self.lhs.value();

				let gradient = self.gradient.borrow();

				let mut lhs_gradient = self.lhs_gradient.borrow_mut();
				let mut rhs_gradient = self.rhs_gradient.borrow_mut();

				numerics::mat_mul(1.0, &*gradient, &rhs_value.t(), 0.0, &mut lhs_gradient);
				numerics::mat_mul(1.0, &lhs_value.t(), &*gradient, 0.0, &mut rhs_gradient);
			}

			self.lhs.backward(&self.lhs_gradient.borrow());
			self.rhs.backward(&self.rhs_gradient.borrow());
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}

	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}
	fn clear(&self) {
		if !self.counter.is_zero() {
			self.lhs.clear();
			self.rhs.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct VectorDotNode<LHS, RHS> {
	value: RefCell<Arr>,
	lhs_gradient: RefCell<Arr>,
	rhs_gradient: RefCell<Arr>,
	lhs: Rc<LHS>,
	rhs: Rc<RHS>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<LHS, RHS> VectorDotNode<LHS, RHS>
where
	LHS: Node<Value = Arr, InputGradient = Arr>,
	RHS: Node<Value = Arr, InputGradient = Arr>,
{
	pub fn new(lhs: Rc<LHS>, rhs: Rc<RHS>) -> Self {
		let (value, lhs_gradient, rhs_gradient, needs_gradient) = {
			let lhs_value = lhs.value();
			let rhs_value = rhs.value();

			let needs_gradient = lhs.needs_gradient() || rhs.needs_gradient();

			assert_eq!(
				lhs_value.shape(),
				rhs_value.shape(),
				"LHS and RHS must be the same shape for vector dot product."
			);

			let mut value = Arr::zeros((lhs_value.shape()[0], 1));

			for (result, lhs, rhs) in izip!(
				value.as_slice_mut().unwrap(),
				lhs_value
					.genrows()
					.into_iter()
					.map(|x| x.to_slice().unwrap()),
				rhs_value
					.genrows()
					.into_iter()
					.map(|x| x.to_slice().unwrap())
			) {
				*result = numerics::simd_dot(lhs, rhs);
			}

			let lhs_gradient = &*lhs_value * 0.0;
			let rhs_gradient = &*rhs_value * 0.0;

			(value, lhs_gradient, rhs_gradient, needs_gradient)
		};

		VectorDotNode {
			value: RefCell::new(value),
			lhs_gradient: RefCell::new(lhs_gradient),
			rhs_gradient: RefCell::new(rhs_gradient),
			lhs,
			rhs,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<LHS, RHS> Node for VectorDotNode<LHS, RHS>
where
	LHS: Node<Value = Arr, InputGradient = Arr>,
	RHS: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;

	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.lhs.forward();
		self.rhs.forward();

		let lhs_value = self.lhs.value();
		let rhs_value = self.rhs.value();

		for (result, lhs, rhs) in izip!(
			self.value.borrow_mut().as_slice_mut().unwrap(),
			lhs_value
				.genrows()
				.into_iter()
				.map(|x| x.to_slice().unwrap()),
			rhs_value
				.genrows()
				.into_iter()
				.map(|x| x.to_slice().unwrap())
		) {
			*result = numerics::simd_dot(lhs, rhs);
		}
	}

	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		let lhs_value = self.lhs.value();
		let rhs_value = self.rhs.value();

		match self.counter.backward() {
			BackwardAction::Set => {
				let mut lhs_grad = self.lhs_gradient.borrow_mut();
				let mut rhs_grad = self.rhs_gradient.borrow_mut();

				for (backward_row, rhs_row, &gradient) in izip!(
					lhs_grad
						.genrows_mut()
						.into_iter()
						.map(|x| x.into_slice().unwrap()),
					rhs_value
						.genrows()
						.into_iter()
						.map(|x| x.to_slice().unwrap()),
					gradient.as_slice().unwrap()
				) {
					numerics::simd_scaled_assign(backward_row, rhs_row, gradient)
				}
				for (backward_row, lhs_row, &gradient) in izip!(
					rhs_grad
						.genrows_mut()
						.into_iter()
						.map(|x| x.into_slice().unwrap()),
					lhs_value
						.genrows()
						.into_iter()
						.map(|x| x.to_slice().unwrap()),
					gradient.as_slice().unwrap()
				) {
					numerics::simd_scaled_assign(backward_row, lhs_row, gradient)
				}
			}
			BackwardAction::Increment => {
				let mut lhs_grad = self.lhs_gradient.borrow_mut();
				let mut rhs_grad = self.rhs_gradient.borrow_mut();

				for (backward_row, rhs_row, &gradient) in izip!(
					lhs_grad
						.genrows_mut()
						.into_iter()
						.map(|x| x.into_slice().unwrap()),
					rhs_value
						.genrows()
						.into_iter()
						.map(|x| x.to_slice().unwrap()),
					gradient.as_slice().unwrap()
				) {
					numerics::simd_scaled_add(backward_row, rhs_row, gradient)
				}
				for (backward_row, lhs_row, &gradient) in izip!(
					rhs_grad
						.genrows_mut()
						.into_iter()
						.map(|x| x.into_slice().unwrap()),
					lhs_value
						.genrows()
						.into_iter()
						.map(|x| x.to_slice().unwrap()),
					gradient.as_slice().unwrap()
				) {
					numerics::simd_scaled_add(backward_row, lhs_row, gradient)
				}
			}
		}

		if self.counter.recurse_backward() {
			self.lhs.backward(&self.lhs_gradient.borrow());
			self.rhs.backward(&self.rhs_gradient.borrow());
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}

	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}

	fn clear(&self) {
		if !self.counter.is_zero() {
			self.lhs.clear();
			self.rhs.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct SquareNode<OP> {
	value: RefCell<Arr>,
	operand_gradient: RefCell<Arr>,
	operand: Rc<OP>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<OP> SquareNode<OP>
where
	OP: Node<Value = Arr>,
{
	pub fn new(operand: Rc<OP>) -> Self {
		let value = operand.value().map(|x| x.powi(2));
		let gradient = &value * 0.0;
		let needs_gradient = operand.needs_gradient();

		SquareNode {
			value: RefCell::new(value),
			operand_gradient: RefCell::new(gradient),
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<OP> Node for SquareNode<OP>
where
	OP: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}
		self.operand.forward();

		let mut dest = self.value.borrow_mut();

		dest.assign(&*self.operand.value());
		dest.map_inplace(|x| *x = x.powi(2));
	}

	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				for (dest, operand_val, grad_val) in izip!(
					self.operand_gradient.borrow_mut().iter_mut(),
					self.operand.value().iter(),
					gradient.iter()
				) {
					*dest = operand_val * 2.0 * grad_val;
				}
			}
			BackwardAction::Increment => {
				for (dest, operand_val, grad_val) in izip!(
					self.operand_gradient.borrow_mut().iter_mut(),
					self.operand.value().iter(),
					gradient.iter()
				) {
					*dest += operand_val * 2.0 * grad_val;
				}
			}
		}

		if self.counter.recurse_backward() {
			self.operand.backward(&self.operand_gradient.borrow());
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}

	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}

	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct LogNode<OP> {
	value: RefCell<Arr>,
	operand_gradient: RefCell<Arr>,
	operand: Rc<OP>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<OP> LogNode<OP>
where
	OP: Node<Value = Arr>,
{
	pub fn new(operand: Rc<OP>) -> Self {
		let value = operand.value().map(|&x| numerics::ln(x));
		let gradient = &value * 0.0;
		let needs_gradient = operand.needs_gradient();

		LogNode {
			value: RefCell::new(value),
			operand_gradient: RefCell::new(gradient),
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<OP> Node for LogNode<OP>
where
	OP: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.operand.forward();

		let mut dest = self.value.borrow_mut();

		dest.assign(&*self.operand.value());
		dest.map_inplace(|x| *x = numerics::ln(*x));
	}

	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				for (dest, operand_val, grad_val) in izip!(
					self.operand_gradient.borrow_mut().iter_mut(),
					self.operand.value().iter(),
					gradient.iter()
				) {
					*dest = grad_val / operand_val;
				}
			}
			BackwardAction::Increment => {
				for (dest, operand_val, grad_val) in izip!(
					self.operand_gradient.borrow_mut().iter_mut(),
					self.operand.value().iter(),
					gradient.iter()
				) {
					*dest += grad_val / operand_val;
				}
			}
		}

		if self.counter.recurse_backward() {
			self.operand.backward(&self.operand_gradient.borrow());
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}

	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}

	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct TanhNode<OP> {
	value: RefCell<Arr>,
	operand_gradient: RefCell<Arr>,
	operand: Rc<OP>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<OP> TanhNode<OP>
where
	OP: Node<Value = Arr>,
{
	pub fn new(operand: Rc<OP>) -> Self {
		let value = operand.value().map(|&x| numerics::tanh(x));
		let gradient = &value * 0.0;
		let needs_gradient = operand.needs_gradient();

		TanhNode {
			value: RefCell::new(value),
			operand_gradient: RefCell::new(gradient),
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<OP> Node for TanhNode<OP>
where
	OP: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.operand.forward();

		let mut dest = self.value.borrow_mut();
		numerics::map_assign(&mut *dest, &*self.operand.value(), numerics::tanh);
	}

	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				for (dest, value, grad_val) in izip!(
					self.operand_gradient.borrow_mut().as_slice_mut().unwrap(),
					self.value().as_slice().unwrap(),
					gradient.as_slice().unwrap()
				) {
					*dest = grad_val * (1.0 - value.powi(2));
				}
			}
			BackwardAction::Increment => {
				for (dest, value, grad_val) in izip!(
					self.operand_gradient.borrow_mut().as_slice_mut().unwrap(),
					self.value().as_slice().unwrap(),
					gradient.as_slice().unwrap()
				) {
					*dest += grad_val * (1.0 - value.powi(2));
				}
			}
		}

		if self.counter.recurse_backward() {
			self.operand.backward(&self.operand_gradient.borrow());
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}

	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}

	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct SigmoidNode<T> {
	value: RefCell<Arr>,
	operand_gradient: RefCell<Arr>,
	operand: Rc<T>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<T> SigmoidNode<T>
where
	T: Node<Value = Arr>,
{
	pub fn new(operand: Rc<T>) -> Self {
		let value = operand.value().map(|&x| numerics::sigmoid(x));
		let gradient = &value * 0.0;
		let needs_gradient = operand.needs_gradient();

		SigmoidNode {
			value: RefCell::new(value),
			operand_gradient: RefCell::new(gradient),
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<T> Node for SigmoidNode<T>
where
	T: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.operand.forward();

		{
			let mut dest = self.value.borrow_mut();

			numerics::map_assign(&mut *dest, &*self.operand.value(), numerics::sigmoid);
		}
	}

	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				let mut operand_gradient = self.operand_gradient.borrow_mut();

				numerics::map_assign_binary(
					&mut operand_gradient,
					&*self.value.borrow(),
					gradient,
					|sigmoid, grad| grad * sigmoid * (1.0 - sigmoid),
				);
			}
			BackwardAction::Increment => {
				let mut operand_gradient = self.operand_gradient.borrow_mut();

				numerics::map_inplace_assign_binary(
					&mut operand_gradient,
					&*self.value.borrow(),
					gradient,
					|dest, sigmoid, grad| *dest += grad * sigmoid * (1.0 - sigmoid),
				);
			}
		}

		if self.counter.recurse_backward() {
			self.operand.backward(&self.operand_gradient.borrow())
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}

	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}

	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct ReluNode<T> {
	value: RefCell<Arr>,
	operand_gradient: RefCell<Arr>,
	operand: Rc<T>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<T> ReluNode<T>
where
	T: Node<Value = Arr>,
{
	pub fn new(operand: Rc<T>) -> Self {
		let value = operand.value().map(|&x| if x < 0.0 { 0.0 } else { x });
		let gradient = &value * 0.0;
		let needs_gradient = operand.needs_gradient();

		ReluNode {
			value: RefCell::new(value),
			operand_gradient: RefCell::new(gradient),
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<T> Node for ReluNode<T>
where
	T: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.operand.forward();

		let mut dest = self.value.borrow_mut();

		numerics::map_assign(&mut *dest, &*self.operand.value(), |x| {
			if x < 0.0 {
				0.0
			} else {
				x
			}
		});
	}

	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				let mut operand_gradient = self.operand_gradient.borrow_mut();

				numerics::map_assign_binary(
					&mut operand_gradient,
					&*self.value.borrow(),
					gradient,
					|x, grad| if x <= 0.0 { 0.0 } else { grad },
				);
			}
			BackwardAction::Increment => {
				let mut operand_gradient = self.operand_gradient.borrow_mut();

				numerics::map_inplace_assign_binary(
					&mut operand_gradient,
					&*self.value.borrow(),
					gradient,
					|dest, x, grad| *dest += if x <= 0.0 { 0.0 } else { grad },
				);
			}
		}

		if self.counter.recurse_backward() {
			self.operand.backward(&self.operand_gradient.borrow())
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}

	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}

	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct NegNode<T> {
	value: RefCell<Arr>,
	operand_gradient: RefCell<Arr>,
	operand: Rc<T>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<T> NegNode<T>
where
	T: Node<Value = Arr>,
{
	pub fn new(operand: Rc<T>) -> Self {
		let value = -&*operand.value();
		let gradient = &value * 0.0;
		let needs_gradient = operand.needs_gradient();

		NegNode {
			value: RefCell::new(value),
			operand_gradient: RefCell::new(gradient),
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<T> Node for NegNode<T>
where
	T: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;

	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.operand.forward();

		let mut dest = self.value.borrow_mut();

		dest.assign(&*self.operand.value());
		dest.map_inplace(|x| *x = -*x);
	}

	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				for (dest, grad_val) in izip!(
					self.operand_gradient.borrow_mut().iter_mut(),
					gradient.iter()
				) {
					*dest = -grad_val;
				}
			}
			BackwardAction::Increment => {
				for (dest, grad_val) in izip!(
					self.operand_gradient.borrow_mut().iter_mut(),
					gradient.iter()
				) {
					*dest += -grad_val;
				}
			}
		}

		if self.counter.recurse_backward() {
			self.operand.backward(&self.operand_gradient.borrow());
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}

	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}
	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct ExpNode<OP> {
	value: RefCell<Arr>,
	operand_gradient: RefCell<Arr>,
	operand: Rc<OP>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<OP> ExpNode<OP>
where
	OP: Node<Value = Arr>,
{
	pub fn new(operand: Rc<OP>) -> Self {
		let value = operand.value().map(|&x| numerics::exp(x));
		let gradient = &value * 0.0;
		let needs_gradient = operand.needs_gradient();

		ExpNode {
			value: RefCell::new(value),
			operand_gradient: RefCell::new(gradient),
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<OP> Node for ExpNode<OP>
where
	OP: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.operand.forward();
		let mut dest = self.value.borrow_mut();

		dest.assign(&*self.operand.value());
		dest.map_inplace(|x| *x = numerics::exp(*x));
	}
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				for (dest, self_val, grad_val) in izip!(
					self.operand_gradient.borrow_mut().iter_mut(),
					self.value.borrow().iter(),
					gradient.iter()
				) {
					*dest = self_val * grad_val;
				}
			}
			BackwardAction::Increment => {
				for (dest, self_val, grad_val) in izip!(
					self.operand_gradient.borrow_mut().iter_mut(),
					self.value.borrow().iter(),
					gradient.iter()
				) {
					*dest += self_val * grad_val;
				}
			}
		}
		if self.counter.recurse_backward() {
			self.operand.backward(&self.operand_gradient.borrow());
		}
	}
	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}
	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}

	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct TransposeNode<OP> {
	value: RefCell<Arr>,
	gradient: RefCell<Arr>,
	operand: Rc<OP>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<OP> TransposeNode<OP>
where
	OP: Node<Value = Arr>,
{
	pub fn new(operand: Rc<OP>) -> Self {
		let needs_gradient = operand.needs_gradient();
		let mut value = Arr::zeros((operand.value().ncols(), operand.value().nrows()));
		value.assign(&operand.value().t());
		let value = RefCell::new(value);
		let gradient = RefCell::new(&*operand.value() * 0.0);

		TransposeNode {
			value,
			gradient,
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<OP> Node for TransposeNode<OP>
where
	OP: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.operand.forward();
		self.value.borrow_mut().assign(&self.operand.value().t());
	}
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		match self.counter.backward() {
			BackwardAction::Set => {
				self.gradient.borrow_mut().assign(&gradient.t());
			}
			BackwardAction::Increment => {
				self.gradient.borrow_mut().slice_add_assign(&gradient.t());
			}
		}

		if self.counter.recurse_backward() {
			self.operand.backward(&self.gradient.borrow());
		}
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}

	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}

	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct SoftmaxNode<OP> {
	value: RefCell<Arr>,
	jacobian: RefCell<Arr>,
	operand_gradient: RefCell<Arr>,
	operand: Rc<OP>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<OP> SoftmaxNode<OP>
where
	OP: Node<Value = Arr>,
{
	pub fn new(operand: Rc<OP>) -> Self {
		let value = {
			let max = operand
				.value()
				.as_slice()
				.unwrap()
				.iter()
				.fold(std::f32::MIN, |x, y| x.max(*y));
			let numerator = operand.value().map(|x| numerics::exp(x - max));
			let denominator = numerator.scalar_sum();

			numerator / denominator
		};

		let gradient = &value * 0.0;
		let needs_gradient = operand.needs_gradient();
		let dim = value.shape()[1];

		SoftmaxNode {
			value: RefCell::new(value),
			jacobian: RefCell::new(ndarray::Array2::zeros((dim, dim))),
			operand_gradient: RefCell::new(gradient),
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<OP> Node for SoftmaxNode<OP>
where
	OP: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.operand.forward();
		let mut dest = self.value.borrow_mut();
		dest.slice_assign(&*self.operand.value());

		let max = self
			.operand
			.value()
			.fast_slice()
			.iter()
			.fold(std::f32::MIN, |x, y| x.max(*y));
		dest.map_inplace(|x| *x = numerics::exp(*x - max));
		let denominator = dest.scalar_sum();
		dest.map_inplace(|x| *x /= denominator);
	}
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		// TODO: accumulate gradients
		let value = self.value.borrow();
		let mut jacobian = self.jacobian.borrow_mut();

		let beta = match self.counter.backward() {
			BackwardAction::Set => 0.0,
			BackwardAction::Increment => 1.0,
		};

		for (row_idx, (mut row, row_val)) in jacobian
			.genrows_mut()
			.into_iter()
			.zip(value.iter())
			.enumerate()
		{
			for (col_idx, (grad, col_val)) in row
				.as_slice_mut()
				.unwrap()
				.iter_mut()
				.zip(value.as_slice().unwrap())
				.enumerate()
			{
				if row_idx == col_idx {
					*grad = row_val * (1.0 - col_val);
				} else {
					*grad = -row_val * col_val;
				}
			}
		}

		{
			numerics::mat_mul(
				1.0,
				gradient,
				&*jacobian,
				beta,
				&mut *self.operand_gradient.borrow_mut(),
			);
		}

		if self.counter.recurse_backward() {
			self.operand.backward(&self.operand_gradient.borrow());
		}
	}
	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}
	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}
	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct LogSoftmaxNode<OP> {
	value: RefCell<Arr>,
	operand_gradient: RefCell<Arr>,
	operand: Rc<OP>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<OP> LogSoftmaxNode<OP>
where
	OP: Node<Value = Arr>,
{
	pub fn new(operand: Rc<OP>) -> Self {
		let value = {
			let operand_value = operand.value();
			let operand_slice = operand_value.as_slice().unwrap();
			let max = operand_slice.iter().fold(std::f32::MIN, |x, y| x.max(*y));

			let denominator = max
				+ operand_slice
					.iter()
					.map(|&x| numerics::exp(x - max))
					.sum::<f32>()
					.ln();

			&*operand_value - denominator
		};

		let gradient = &value * 0.0;
		let needs_gradient = operand.needs_gradient();

		LogSoftmaxNode {
			value: RefCell::new(value),
			operand_gradient: RefCell::new(gradient),
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}

	/// An additional method for zeroing the counter for use in the
	/// log-softmax loss, where the actuall log-softmax layer is skipped
	/// when backpropagating.
	pub fn zero_counter(&self) {
		self.counter.clear();
	}
}

impl<OP> Node for LogSoftmaxNode<OP>
where
	OP: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.operand.forward();
		let mut dest = self.value.borrow_mut();
		dest.assign(&*self.operand.value());

		let operand_value = self.operand.value();
		let operand_slice = operand_value.as_slice().unwrap();
		let max = operand_slice.iter().fold(std::f32::MIN, |x, y| x.max(*y));

		let denominator = max + numerics::softmax_exp_sum(operand_slice, max).ln();

		dest.as_slice_mut()
			.unwrap()
			.iter_mut()
			.for_each(|x| *x -= denominator);
	}
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		let beta = match self.counter.backward() {
			BackwardAction::Set => 0.0,
			BackwardAction::Increment => 1.0,
		};

		{
			let value = self.value.borrow();
			let value_slice = value.as_slice().expect("Can't get value slice.");

			let gradient_slice = gradient
				.as_slice()
				.expect("Can't get input gradient slice.");
			let mut downstream_gradient = self.operand_gradient.borrow_mut();
			let downstream_gradient_slice = downstream_gradient
				.as_slice_mut()
				.expect("Can't get output gradient slice");

			let gradient_sum = numerics::simd_sum(gradient_slice);

			for (out_grad, in_grad, &val) in
				izip!(downstream_gradient_slice, gradient_slice, value_slice)
			{
				*out_grad = beta * *out_grad + in_grad - numerics::exp(val) * gradient_sum;
			}
		}

		if self.counter.recurse_backward() {
			self.operand.backward(&self.operand_gradient.borrow());
		}
	}
	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}
	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}
	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

#[derive(Debug)]
pub struct SumNode<OP> {
	value: RefCell<Arr>,
	operand_gradient: RefCell<Arr>,
	operand: Rc<OP>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<OP> SumNode<OP>
where
	OP: Node<Value = Arr>,
{
	pub fn new(operand: Rc<OP>) -> Self {
		let value = {
			let mut value = Arr::zeros((1, 1));
			value.fill(operand.value().scalar_sum());
			value
		};

		let gradient = &*operand.value() * 0.0;
		let needs_gradient = operand.needs_gradient();

		SumNode {
			value: RefCell::new(value),
			operand_gradient: RefCell::new(gradient),
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl<OP> Node for SumNode<OP>
where
	OP: Node<Value = Arr, InputGradient = Arr>,
{
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		self.operand.forward();

		let mut dest = self.value.borrow_mut();
		dest[(0, 0)] = self.operand.value().scalar_sum();
	}
	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		debug_assert!(gradient.len() == 1, "Input gradient must be a scalar.");

		match self.counter.backward() {
			BackwardAction::Set => {
				self.operand_gradient.borrow_mut().fill(gradient[(0, 0)]);
			}
			BackwardAction::Increment => {
				self.operand_gradient
					.borrow_mut()
					.slice_add_assign(gradient[(0, 0)]);
			}
		}

		if self.counter.recurse_backward() {
			self.operand.backward(&self.operand_gradient.borrow());
		}
	}
	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}
	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}

	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

/// An input node for integer indices into `ParameterNode`s, used
/// for implementing indexable embedding layers.
#[derive(Debug)]
pub struct IndexInputNode {
	pub(crate) value: RefCell<SmallVec<[usize; 4]>>,
}

impl IndexInputNode {
	/// Create a new index input node.
	pub fn new(value: &[usize]) -> Variable<Self> {
		Variable::new(
			Rc::new(IndexInputNode {
				value: RefCell::new(SmallVec::from(value)),
			}),
			Vec::new(),
		)
	}
}

impl Node for IndexInputNode {
	type Value = SmallVec<[usize; 4]>;
	type InputGradient = Arr;
	fn forward(&self) {}
	fn backward(&self, _: &Ref<Self::InputGradient>) {}
	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}
	fn needs_gradient(&self) -> bool {
		false
	}
	fn clear(&self) {}
}

#[derive(Debug)]
pub struct IndexNode<OP> {
	value: RefCell<Arr>,
	index_value: RefCell<SmallVec<[usize; 4]>>,
	operand_gradient: RefCell<Arr>,
	index: Rc<IndexInputNode>,
	operand: Rc<OP>,
	needs_gradient: bool,
	counter: PassCounter,
}

impl<OP> IndexNode<OP>
where
	OP: Node<Value = Arr>,
{
	pub fn new(operand: Rc<OP>, index: Rc<IndexInputNode>) -> Self {
		let value = operand.value().select(Axis(0), &index.value()[..]);
		let grad = &value * 0.0;
		let idx_value = index.value().clone();
		let needs_gradient = operand.needs_gradient();

		IndexNode {
			value: RefCell::new(value),
			index_value: RefCell::new(idx_value),
			operand_gradient: RefCell::new(grad),
			index,
			operand,
			needs_gradient,
			counter: PassCounter::default(),
		}
	}
}

impl Node for IndexNode<ParameterNode> {
	type Value = Arr;
	type InputGradient = Arr;
	fn forward(&self) {
		if self.counter.forward() == ForwardAction::Cached {
			return;
		}

		let operand_value = self.operand.value();

		let mut idx_value = self.index_value.borrow_mut();
		idx_value.clear();
		idx_value.extend_from_slice(&self.index.value()[..]);

		let mut arr_value = self.value.borrow_mut();

		debug_assert_eq!(
			arr_value.shape()[0],
			idx_value.len(),
			"Result of indexing operation must maintain consistent shape between iterations."
		);

		for (&idx, mut row) in idx_value.iter().zip(arr_value.genrows_mut()) {
			let new_val = operand_value.index_axis(Axis(0), idx);

			row.slice_assign(&new_val);
		}
	}

	fn backward(&self, gradient: &Ref<Self::InputGradient>) {
		let _ = self.counter.backward();
		self.operand
			.gradient
			.borrow_mut()
			.accumulate_gradient((&self.index_value.borrow()[..], &**gradient));
		let _ = self.counter.recurse_backward();
	}

	fn value(&self) -> Bor<Self::Value> {
		Bor::RefGuard(self.value.borrow())
	}

	fn needs_gradient(&self) -> bool {
		self.needs_gradient
	}
	fn clear(&self) {
		if !self.counter.is_zero() {
			self.operand.clear();
			self.counter.clear();
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::nn;

	#[test]
	fn test_sub_counter() {
		let x = ParameterNode::new(nn::xavier_normal(1, 1));
		let y = x.clone() - x;

		let mut z = y.clone() + y.clone() + y.clone();

		z.forward();
		assert_eq!(y.node.counter.forward_count.get(), 3);
		z.backward(1.0);
		assert_eq!(y.node.counter.backward_count.get(), 0);
		assert_eq!(y.node.counter.forward_count.get(), 0);
	}
}
