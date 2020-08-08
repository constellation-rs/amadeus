//! Harmonious distributed data processing & analysis in Rust.
//!
//! <p style="font-family: 'Fira Sans',sans-serif;padding:0.3em 0"><strong>
//! <a href="https://crates.io/crates/amadeus">📦&nbsp;&nbsp;Crates.io</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://github.com/constellation-rs/amadeus">📑&nbsp;&nbsp;GitHub</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://constellation.zulipchat.com/#narrow/stream/213231-amadeus">💬&nbsp;&nbsp;Chat</a>
//! </strong></p>
//!
//! This is a support crate of [Amadeus](https://github.com/constellation-rs/amadeus) and is not intended to be used directly. These types are re-exposed in [`amadeus::source`](https://docs.rs/amadeus/0.3/amadeus/source/index.html).
//!
//! ---
//!
//! A reverse mode, define-by-run, low-overhead autodifferentiation library.
//!
//! # Features
//!
//! Performs backpropagation through arbitrary, define-by-run computation graphs,
//! emphasizing low overhead estimation of sparse, small models on the CPU.
//!
//! Highlights:
//!
//! 1. Low overhead.
//! 2. Built-in support for sparse gradients.
//! 3. Define-by-run.
//! 4. Trivial Hogwild-style parallelisation, scaling linearly with the number of CPU cores available.
//!
//! # Quickstart
//!
//! The following defines a univariate linear regression model, then
//! backpropagates through it.
//!
//! ```
//! # use amadeus_ml::*;
//! #
//! # fn random_matrix(rows: usize, cols: usize) -> Arr {
//! #      Arr::zeros((rows, cols)).map(|_| rand::random::<f32>())
//! # }
//! #
//! let slope = ParameterNode::new(random_matrix(1, 1));
//! let intercept = ParameterNode::new(random_matrix(1, 1));
//!
//! let x = InputNode::new(random_matrix(1, 1));
//! let y = InputNode::new(random_matrix(1, 1));
//!
//! let y_hat = slope.clone() * x.clone() + intercept.clone();
//! let mut loss = (y.clone() - y_hat).square();
//! ```
//!
//! To optimize the parameters, create an optimizer object and
//! go through several epochs of learning:
//!
//! ```
//! # use amadeus_ml::{*, optim::*};
//! #
//! # fn random_matrix(rows: usize, cols: usize) -> Arr {
//! #      Arr::zeros((rows, cols)).map(|_| rand::random::<f32>())
//! # }
//! #
//! # let slope = ParameterNode::new(random_matrix(1, 1));
//! # let intercept = ParameterNode::new(random_matrix(1, 1));
//! # let x = InputNode::new(random_matrix(1, 1));
//! # let y = InputNode::new(random_matrix(1, 1));
//! # let y_hat = slope.clone() * x.clone() + intercept.clone();
//! # let mut loss = (y.clone() - y_hat).square();
//! # let num_epochs = 10;
//! let mut optimizer = SGD::new().learning_rate(0.1);
//!
//! for _ in 0..num_epochs {
//!     let x_value: f32 = rand::random();
//!     let y_value = 3.0 * x_value + 5.0;
//!
//!     // You can re-use the computation graph
//!     // by giving the input nodes new values.
//!     x.set_value(x_value);
//!     y.set_value(y_value);
//!
//!     loss.forward();
//!     loss.backward(1.0);
//!
//!     optimizer.step(loss.parameters());
//! }
//! ```
//!
//! You can use `rayon` to fit your model in parallel, by first creating a set of shared
//! parameters, then building a per-thread copy of the model:
//!
//! ```
//! # use rayon::prelude::*;
//! # use std::sync::Arc;
//! #
//! # use amadeus_ml::{*, optim::*};
//! #
//! # fn random_matrix(rows: usize, cols: usize) -> Arr {
//! #      Arr::zeros((rows, cols)).map(|_| rand::random::<f32>())
//! # }
//! #
//! let slope_param = Arc::new(HogwildParameter::new(random_matrix(1, 1)));
//! let intercept_param = Arc::new(HogwildParameter::new(random_matrix(1, 1)));
//! let num_epochs = 10;
//!
//! (0..rayon::current_num_threads())
//!     .into_par_iter()
//!        .for_each(|_| {
//!            let slope = ParameterNode::shared(slope_param.clone());
//!            let intercept = ParameterNode::shared(intercept_param.clone());
//!            let x = InputNode::new(random_matrix(1, 1));
//!            let y = InputNode::new(random_matrix(1, 1));
//!            let y_hat = slope.clone() * x.clone() + intercept.clone();
//!            let mut loss = (y.clone() - y_hat).square();
//!
//!            let optimizer = SGD::new().learning_rate(0.1);
//!
//!            for _ in 0..num_epochs {
//!                let x_value: f32 = rand::random();
//!                let y_value = 3.0 * x_value + 5.0;
//!
//!                x.set_value(x_value);
//!                y.set_value(y_value);
//!
//!                loss.forward();
//!                loss.backward(1.0);
//!
//!                optimizer.step(loss.parameters());
//!            }
//!        });
//! ```
//!
//! ## BLAS support
//!
//! You should enable BLAS support to get (much) better performance out of matrix-multiplication-heavy
//! workloads. To do so, enable the `openblas` feature.
//!
//! ## Fast numerics
//!
//! Enable the `fast-math` option to use fast approximations to transcendental functions.
//! This should give substantial speed gains in networks that are `exp`, `ln`, or `tanh`-heavy.

#![doc(html_root_url = "https://docs.rs/amadeus-ml/0.4.0")]
#![warn(
	missing_debug_implementations,
	missing_docs,
	trivial_casts,
	trivial_numeric_casts,
	unused_import_braces,
	unused_qualifications,
	unused_results,
	clippy::pedantic
)]
// from https://github.com/rust-unofficial/patterns/blob/master/anti_patterns/deny-warnings.md
#![allow(
	clippy::cast_precision_loss,
	clippy::doc_markdown,
	clippy::float_cmp,
	clippy::if_not_else,
	clippy::inline_always,
	clippy::module_name_repetitions,
	clippy::must_use_candidate,
	clippy::unsafe_derive_deserialize
)]

mod fast_approx;
pub mod nn;
mod nodes;
mod numerics;
pub mod optim;

use approx::AbsDiffEq;
use itertools::Itertools;
use std::{
	cell::RefCell, clone::Clone, ops::{Add, Div, Mul, Neg, Sub}, rc::Rc
};

use nodes::{
	AddNode, ConcatenateNode, DivNode, DotNode, ExpNode, IndexNode, LogNode, LogSoftmaxNode, MulNode, NegNode, ReluNode, SigmoidNode, SliceNode, SoftmaxNode, SquareNode, SubNode, SumNode, TanhNode, TransposeNode, VectorDotNode
};

pub use nodes::{Bor, HogwildParameter, IndexInputNode, InputNode, Node, ParameterNode};
pub use numerics::simd_dot;

/// Alias for a `f32` `ndarray` matrix.
pub type Arr = ndarray::Array2<f32>;

fn clamp(x: f32, min: f32, max: f32) -> f32 {
	if x > max {
		max
	} else if x < min {
		min
	} else {
		x
	}
}

/// Trait describing nodes that can accept new values once
/// the graph has been defined.
pub trait DataInput<T> {
	/// Set the value of this node.
	fn set_value(&self, value: T);
}

fn merge_parameters(
	xs: &[Variable<ParameterNode>], ys: &[Variable<ParameterNode>],
) -> Vec<Variable<ParameterNode>> {
	xs.iter()
		.merge_join_by(ys.iter(), |x, y| x.as_ptr().cmp(&y.as_ptr()))
		.map(|either| match either {
			itertools::EitherOrBoth::Left(x)
			| itertools::EitherOrBoth::Right(x)
			| itertools::EitherOrBoth::Both(x, _) => x,
		})
		.cloned()
		.collect()
}

/// Handle to a node in the computation graph. The underlying nodes
/// are reference counted, so the handles can be freely cloned to
/// use the nodes multiple times in the same graph.
#[derive(Debug)]
pub struct Variable<T>
where
	T: Node,
{
	node: Rc<T>,
	grad: Option<RefCell<Arr>>,
	parameters: Vec<Variable<ParameterNode>>,
}

impl<T: Node> Clone for Variable<T> {
	fn clone(&self) -> Self {
		Variable {
			node: Rc::clone(&self.node),
			grad: None,
			parameters: self.parameters.clone(),
		}
	}
}

impl<T> Variable<T>
where
	T: Node,
{
	fn new(node: Rc<T>, parameters: Vec<Variable<ParameterNode>>) -> Self {
		Variable {
			node,
			grad: None,
			parameters,
		}
	}
	/// Get the value of the node.
	pub fn value(&self) -> Bor<T::Value> {
		self.node.value()
	}
	/// Run the forward pass through the subgraph terminating at this node,
	/// recursing through the ancestor nodes.
	pub fn forward(&self) {
		self.node.forward()
	}
	/// Clear the graph caches. Must be called whenever inputs change and [backward] is not
	/// called.
	pub fn clear(&self) {
		self.node.clear();
	}

	/// Zero the accumulated gradients for the parameter nodes in this graph.
	pub fn zero_gradient(&self) {
		for param in self.parameters() {
			param.node.zero_gradient();
		}
	}

	/// Return the parameters of the graph.
	pub fn parameters(&self) -> &[Variable<ParameterNode>] {
		&self.parameters[..]
	}

	/// Mutably return the parameters of the graph.
	pub fn parameters_mut(&mut self) -> &mut [Variable<ParameterNode>] {
		&mut self.parameters[..]
	}
}

/// An alias for a node whose concrete type has been erased.
pub type BoxedNode = Rc<dyn Node<Value = Arr, InputGradient = Arr>>;

impl<T> Variable<T>
where
	T: Node<Value = Arr, InputGradient = Arr>,
{
	/// Box the variable, erasing its specific type. Use to manage the complexity
	/// of variable types in deep computation graphs.
	pub fn boxed(&self) -> Variable<Rc<dyn Node<Value = Arr, InputGradient = Arr>>> {
		Variable::new(Rc::new(self.node.clone()), self.parameters.clone())
	}

	/// Run the backward pass through the subgraph terminating at this node.
	/// The weight parameter scales the gradients.
	pub fn backward(&mut self, weight: f32) {
		let val = self.node.value();

		self.grad
			.get_or_insert_with(|| RefCell::new(val.map(|_| weight)))
			.borrow_mut()
			.as_slice_mut()
			.unwrap()
			.iter_mut()
			.for_each(|x| *x = weight);

		if let Some(ref grad) = self.grad {
			self.node.backward(&grad.borrow());
		}
	}

	/// Clip the value. Useful for clipping losses.
	pub fn clip(&mut self, min: f32, max: f32) {
		let bor_value = self.node.value();
		let value: *const Arr = &*bor_value;
		#[allow(clippy::cast_ref_to_mut)]
		let value = unsafe { &mut *(value as *mut Arr) };

		value
			.as_slice_mut()
			.unwrap()
			.iter_mut()
			.for_each(|x| *x = 100.0 * clamp(*x, min, max));
	}

	/// Square this variable.
	pub fn square(&self) -> Variable<SquareNode<T>> {
		Variable::new(
			Rc::new(SquareNode::new(Rc::clone(&self.node))),
			self.parameters.clone(),
		)
	}

	/// Sum this variable.
	pub fn scalar_sum(&self) -> Variable<SumNode<T>> {
		Variable::new(
			Rc::new(SumNode::new(Rc::clone(&self.node))),
			self.parameters.clone(),
		)
	}

	/// Take the natural logarithm of this variable.
	pub fn ln(&self) -> Variable<LogNode<T>> {
		Variable::new(
			Rc::new(LogNode::new(Rc::clone(&self.node))),
			self.parameters.clone(),
		)
	}

	/// Take the tanh of this variable.
	pub fn tanh(&self) -> Variable<TanhNode<T>> {
		Variable::new(
			Rc::new(TanhNode::new(Rc::clone(&self.node))),
			self.parameters.clone(),
		)
	}

	/// Transpose this variable.
	pub fn t(&self) -> Variable<TransposeNode<T>> {
		Variable::new(
			Rc::new(TransposeNode::new(Rc::clone(&self.node))),
			self.parameters.clone(),
		)
	}

	/// Exponentiate this variable.
	pub fn exp(&self) -> Variable<ExpNode<T>> {
		Variable::new(
			Rc::new(ExpNode::new(Rc::clone(&self.node))),
			self.parameters.clone(),
		)
	}

	/// Compute the softmax of this variable.
	pub fn softmax(&self) -> Variable<SoftmaxNode<T>> {
		Variable::new(
			Rc::new(SoftmaxNode::new(Rc::clone(&self.node))),
			self.parameters.clone(),
		)
	}

	/// Compute the log-softmax of this variable.
	pub fn log_softmax(&self) -> Variable<LogSoftmaxNode<T>> {
		Variable::new(
			Rc::new(LogSoftmaxNode::new(Rc::clone(&self.node))),
			self.parameters.clone(),
		)
	}

	/// Compute the sigmoid of this variable.
	pub fn sigmoid(&self) -> Variable<SigmoidNode<T>> {
		Variable::new(
			Rc::new(SigmoidNode::new(Rc::clone(&self.node))),
			self.parameters.clone(),
		)
	}

	/// Compute the ReLU of this variable.
	pub fn relu(&self) -> Variable<ReluNode<T>> {
		Variable::new(
			Rc::new(ReluNode::new(Rc::clone(&self.node))),
			self.parameters.clone(),
		)
	}

	/// Compute the row-wise vector dot product of LHS and RHS.
	pub fn vector_dot<S>(&self, other: &Variable<S>) -> Variable<VectorDotNode<T, S>>
	where
		S: Node<Value = Arr, InputGradient = Arr>,
	{
		Variable::new(
			Rc::new(VectorDotNode::new(
				Rc::clone(&self.node),
				Rc::clone(&other.node),
			)),
			merge_parameters(&self.parameters, &other.parameters),
		)
	}

	/// Compute the matrix multiplication of LHS and RHS.
	pub fn dot<S>(&self, other: &Variable<S>) -> Variable<DotNode<T, S>>
	where
		S: Node<Value = Arr, InputGradient = Arr>,
	{
		Variable::new(
			Rc::new(DotNode::new(Rc::clone(&self.node), Rc::clone(&other.node))),
			merge_parameters(&self.parameters, &other.parameters),
		)
	}

	/// Stack/concatenate LHS and RHS, either row-wise (`ndarray::Axis(0)`) or
	/// column-wise (`ndarray::Axis(1)`).
	pub fn stack<S>(
		&self, other: &Variable<S>, axis: ndarray::Axis,
	) -> Variable<ConcatenateNode<T, S>>
	where
		S: Node<Value = Arr, InputGradient = Arr>,
	{
		Variable::new(
			Rc::new(ConcatenateNode::new(
				Rc::clone(&self.node),
				Rc::clone(&other.node),
				axis,
			)),
			merge_parameters(&self.parameters, &other.parameters),
		)
	}

	/// Slice the node according to the `ndarray` slice syntax.
	pub fn slice(
		&self, slice: &ndarray::SliceInfo<[ndarray::SliceOrIndex; 2], ndarray::Ix2>,
	) -> Variable<SliceNode<T>> {
		Variable::new(
			Rc::new(SliceNode::new(Rc::clone(&self.node), slice)),
			self.parameters.clone(),
		)
	}
}

impl Variable<ParameterNode> {
	/// Return the (dense) gradient value of this node.
	pub fn gradient(&self) -> Arr {
		self.node.gradient.borrow().materialized_gradient()
	}

	fn as_ptr(&self) -> *const ParameterNode {
		&*self.node
	}

	/// Row-wise indexing of this parameter node. Primiarily used
	/// to implement embedding layers.
	pub fn index(&self, index: &Variable<IndexInputNode>) -> Variable<IndexNode<ParameterNode>> {
		Variable::new(
			Rc::new(IndexNode::new(
				Rc::clone(&self.node),
				Rc::clone(&index.node),
			)),
			merge_parameters(&self.parameters, &index.parameters),
		)
	}
}

impl<T> Variable<nn::losses::SparseCategoricalCrossentropyNode<T>>
where
	T: Node<Value = Arr, InputGradient = Arr>,
{
	/// Return the log-softmax predictions from a sparse categorical
	/// cross-entropy node.
	///
	/// Calling `.value()` on the node returns the value of the loss;
	/// this function allows getting the predictins with low overhead.
	pub fn predictions(&self) -> Bor<Arr> {
		self.node.predictions()
	}
}

impl<'value> DataInput<&'value Arr> for Variable<ParameterNode> {
	fn set_value(&self, value: &Arr) {
		let param_value = unsafe { &mut *(self.node.value.value.as_ptr()) };
		param_value.assign(value)
	}
}

impl<'value> DataInput<&'value Arr> for Variable<InputNode> {
	fn set_value(&self, value: &Arr) {
		self.node.value.borrow_mut().assign(value);
	}
}

impl DataInput<f32> for Variable<InputNode> {
	fn set_value(&self, value: f32) {
		self.node.value.borrow_mut()[(0, 0)] = value;
	}
}

impl<'value> DataInput<&'value [usize]> for Variable<IndexInputNode> {
	fn set_value(&self, value: &[usize]) {
		let mut node_value = self.node.value.borrow_mut();
		node_value.clear();
		node_value.extend_from_slice(value);
	}
}

impl DataInput<usize> for Variable<IndexInputNode> {
	fn set_value(&self, value: usize) {
		let mut node_value = self.node.value.borrow_mut();
		node_value.clear();
		node_value.push(value);
	}
}

macro_rules! impl_arithmetic_op {
	($trait:ident, $fn:ident, $node:ident) => {
		impl<LHS, RHS> $trait<Variable<RHS>> for Variable<LHS>
		where
			RHS: Node<Value = Arr, InputGradient = Arr>,
			LHS: Node<Value = Arr, InputGradient = Arr>,
		{
			type Output = Variable<$node<LHS, RHS>>;
			fn $fn(self, other: Variable<RHS>) -> Self::Output {
				Variable::new(
					Rc::new($node::new(self.node, other.node)),
					merge_parameters(&self.parameters, &other.parameters),
				)
			}
		}

		/// The constant will be broadcast to have the same shape
		/// as the LHS.
		impl<LHS> $trait<f32> for Variable<LHS>
		where
			LHS: Node<Value = Arr, InputGradient = Arr>,
		{
			type Output = Variable<$node<LHS, InputNode>>;
			fn $fn(self, other: f32) -> Self::Output {
				let constant = InputNode::new(&*self.value() * 0.0 + other);

				Variable::new(
					Rc::new($node::new(self.node, constant.node)),
					merge_parameters(&self.parameters, &constant.parameters),
				)
			}
		}

		/// The constant will be broadcast to have the same shape
		/// as the RHS.
		impl<RHS> $trait<Variable<RHS>> for f32
		where
			RHS: Node<Value = Arr, InputGradient = Arr>,
		{
			type Output = Variable<$node<InputNode, RHS>>;
			fn $fn(self, other: Variable<RHS>) -> Self::Output {
				let constant = InputNode::new(&*other.value() * 0.0 + self);

				Variable::new(
					Rc::new($node::new(constant.node, other.node)),
					merge_parameters(&constant.parameters, &other.parameters),
				)
			}
		}
	};
}

impl_arithmetic_op!(Add, add, AddNode);
impl_arithmetic_op!(Sub, sub, SubNode);
impl_arithmetic_op!(Mul, mul, MulNode);
impl_arithmetic_op!(Div, div, DivNode);

impl<T> Neg for Variable<T>
where
	T: Node<Value = Arr, InputGradient = Arr>,
{
	type Output = Variable<NegNode<T>>;
	fn neg(self) -> Self::Output {
		Variable::new(Rc::new(NegNode::new(self.node)), self.parameters)
	}
}

/// Compute finite difference gradient estimates of the output variable
/// with respect to the input. Use to verify correctness of gradient
/// computations.
pub fn finite_difference<T>(
	input: &mut Variable<ParameterNode>, output: &mut Variable<T>,
) -> (Arr, Arr)
where
	T: Node<Value = Arr, InputGradient = Arr>,
{
	let delta_x = 1e-4;

	let initial_input = { input.value().clone() };
	let mut central_difference = &initial_input * 0.0;

	for (idx, diff) in central_difference.indexed_iter_mut() {
		let positive_difference = {
			output.zero_gradient();
			let mut changed_input = initial_input.clone();
			changed_input[idx] += 0.5 * delta_x;
			input.set_value(&changed_input);
			output.forward();
			output.backward(1.0);
			output.value().clone()
		};

		let negative_difference = {
			output.zero_gradient();
			let mut changed_input = initial_input.clone();
			changed_input[idx] -= 0.5 * delta_x;
			input.set_value(&changed_input);
			output.forward();
			output.backward(1.0);
			output.value().clone()
		};

		let central_difference = positive_difference - negative_difference;

		*diff = central_difference.scalar_sum() / delta_x;
	}

	let gradient = {
		output.zero_gradient();
		input.set_value(&initial_input);
		output.forward();
		output.backward(1.0);

		input.gradient()
	};

	output.zero_gradient();

	(central_difference, gradient)
}

/// Assert two arrays are within `tol` of each other.
pub fn assert_close(x: &Arr, y: &Arr, tol: f32) {
	assert!(
		x.abs_diff_eq(y, tol),
		"{:#?} not within {} of {:#?}",
		x,
		tol,
		y
	);
}

#[cfg(test)]
mod tests {
	use ndarray::{arr2, s};
	use rand::{
		distributions::{Distribution, Uniform}, Rng
	};
	use rayon::prelude::*;
	use std::sync::Arc;

	use super::{optim::Synchronizable, *};
	use optim::{Adagrad, Optimizer, SGD};

	const TOLERANCE: f32 = 0.05;

	fn random_matrix(rows: usize, cols: usize) -> Arr {
		nn::xavier_normal(rows, cols)
	}

	fn random_index(rows: usize) -> usize {
		Uniform::new(0, rows).sample(&mut rand::thread_rng())
	}

	#[test]
	fn test_constant_sub() {
		let mut x = ParameterNode::new(Arr::zeros((10, 10)) + 1.0);
		let mut y = (1.0 - x.clone()) * 2.0;

		assert_eq!(y.value().scalar_sum(), 0.0);
		y.zero_gradient();
		y.forward();
		y.backward(1.0);
		assert_eq!(y.value().scalar_sum(), 0.0);

		let (difference, gradient) = finite_difference(&mut x, &mut y);
		assert_close(&difference, &gradient, TOLERANCE);
	}

	#[test]
	fn parameter_deduplication() {
		let x = ParameterNode::new(random_matrix(1, 1));
		let y = ParameterNode::new(random_matrix(1, 1));

		let z = x + y;
		let z = z.clone() + z;

		assert_eq!(z.parameters().len(), 2);
	}

	#[test]
	fn add_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(1, 1));
		let mut y = ParameterNode::new(random_matrix(1, 1));
		let mut z = x.clone() + y.clone() + x.clone() + x.clone();

		let (difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
		let (difference, gradient) = finite_difference(&mut y, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
	}
	#[test]
	fn sub_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(1, 1));
		let mut y = ParameterNode::new(random_matrix(1, 1));
		let z = x.clone() - (y.clone() - x.clone());
		let mut z = z.clone() * 2.0 + z.sigmoid();

		let (difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
		let (difference, gradient) = finite_difference(&mut y, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
	}
	#[test]
	fn mul_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 10));
		let mut y = ParameterNode::new(random_matrix(10, 10));
		let z = x.clone() * y.clone();
		let mut z = z.clone() + z;

		let (difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
		let (difference, gradient) = finite_difference(&mut y, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
	}
	#[test]
	fn div_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(1, 1));
		let y = ParameterNode::new(random_matrix(1, 1));
		let mut z = (x.clone() + x.clone()) / y;

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE);
	}
	#[test]
	fn vector_dot_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let mut y = ParameterNode::new(random_matrix(10, 5));
		let z = x.vector_dot(&y);
		let mut z = z.clone() + z;

		let (difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);

		let (difference, gradient) = finite_difference(&mut y, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
	}
	#[test]
	fn dot_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let mut y = ParameterNode::new(random_matrix(5, 10));
		let mut z = (x.clone() + x.clone()).dot(&y);

		let (difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);

		let (difference, gradient) = finite_difference(&mut y, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
	}
	#[test]
	fn dot_accumulation_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let mut y = ParameterNode::new(random_matrix(5, 10));
		let z = x.dot(&y);
		let mut v = z.clone() * z;

		let (difference, gradient) = finite_difference(&mut x, &mut v);
		assert_close(&difference, &gradient, TOLERANCE);

		let (difference, gradient) = finite_difference(&mut y, &mut v);
		assert_close(&difference, &gradient, TOLERANCE);
	}
	#[test]
	fn square_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let mut z = x.square();

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE);
	}
	#[test]
	fn ln_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(2, 2));
		let mut z = (x.clone() + x.clone()).exp().ln();

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE);
	}
	#[test]
	fn tanh_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(2, 2));
		let mut z = (x.clone() + x.clone()).tanh();

		let (difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
	}
	#[test]
	fn sum_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let mut z = (x.clone() + x.clone()).scalar_sum();

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE * 2.0);
	}
	#[test]
	fn squared_sum_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let mut z = x.square().scalar_sum();

		let (difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
	}
	#[test]
	fn transpose_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let mut z = (x.clone() + x.clone()).t();

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE);
	}
	#[test]
	fn exp_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let mut z = (x.clone() + x.clone()).exp();

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE);
	}
	#[test]
	fn dot_square_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let y = ParameterNode::new(random_matrix(10, 5));
		let mut z = x.vector_dot(&y).square();

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE);
	}
	#[test]
	fn sigmoid_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let z = (x.clone() + x.clone()).sigmoid();
		let mut z = z.clone() + z;

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE);
	}
	#[test]
	fn relu_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let z = (x.clone() + x.clone()).relu();
		let mut z = z * 3.0;

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE);
	}
	#[test]
	fn neg_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let mut z = -(x.clone() + x.clone());

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE);
	}
	#[test]
	fn softmax_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(1, 10));
		let mut z = (x.clone() + x.clone()).softmax();

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE);
	}
	#[test]
	fn log_softmax_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(1, 10));
		let mut z = (x.clone() + x.clone()).log_softmax();
		let v = (x.clone() + x.clone()).softmax().ln();

		assert_close(&*v.value(), &*z.value(), TOLERANCE * 4.0);

		let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&finite_difference, &gradient, TOLERANCE * 4.0);
	}
	#[test]
	fn sparse_categorical_cross_entropy_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(1, 10));
		let z = x.clone() + x.clone();
		let idx = IndexInputNode::new(&[0][..]);
		let mut loss = nn::losses::sparse_categorical_crossentropy(&z, &idx);

		let (finite_difference, gradient) = finite_difference(&mut x, &mut loss);
		assert_close(&finite_difference, &gradient, TOLERANCE);
	}
	#[test]
	fn rowwise_stack_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let mut y = ParameterNode::new(random_matrix(10, 5));
		//let v = x.clone() + y.clone();

		let z = x.stack(&y, ndarray::Axis(0));
		let mut z = z.sigmoid() * z.relu();

		assert_eq!(z.value().nrows(), 20);
		assert_eq!(z.value().ncols(), 5);

		let (difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);

		let (difference, gradient) = finite_difference(&mut y, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
	}
	#[test]
	fn columnwise_stack_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 5));
		let mut y = ParameterNode::new(random_matrix(10, 5));
		//let v = x.clone() + y.clone();

		let mut z = x.stack(&y, ndarray::Axis(1)).sigmoid();

		assert_eq!(z.value().nrows(), 10);
		assert_eq!(z.value().ncols(), 10);

		let (difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);

		let (difference, gradient) = finite_difference(&mut y, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
	}
	#[test]
	fn columnwise_view_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(10, 30));

		let x_0 = x.slice(s![.., 0..10]);
		let x_1 = x.slice(s![.., 10..20]);
		let x_2 = x.slice(s![.., 20..30]);

		assert_eq!(x_0.value().nrows(), 10);
		assert_eq!(x_0.value().ncols(), 10);
		assert_eq!(x_1.value().nrows(), 10);
		assert_eq!(x_1.value().ncols(), 10);
		assert_eq!(x_2.value().nrows(), 10);
		assert_eq!(x_2.value().ncols(), 10);

		let mut z = (x_0 + x_1 + x_2).sigmoid();

		let (difference, gradient) = finite_difference(&mut x, &mut z);
		assert_close(&difference, &gradient, TOLERANCE);
	}
	#[test]
	fn sparse_index_finite_difference() {
		let mut x = ParameterNode::new(random_matrix(100, 5));

		for _ in 0..10 {
			let idx_0 = IndexInputNode::new(&[random_index(10)]);
			let idx_1 = IndexInputNode::new(&[random_index(10)]);

			let mut z = (x.index(&idx_0).tanh() * x.index(&idx_1)).square();

			let (difference, gradient) = finite_difference(&mut x, &mut z);
			assert_close(&difference, &gradient, TOLERANCE);
		}
	}
	#[test]
	fn univariate_regression() {
		let slope = ParameterNode::new(random_matrix(1, 1));
		let intercept = ParameterNode::new(random_matrix(1, 1));

		let num_epochs = 200;

		let x = InputNode::new(random_matrix(1, 1));
		let y = InputNode::new(random_matrix(1, 1));

		let y_hat = slope.clone() * x.clone() + intercept.clone();
		let diff = y.clone() - y_hat.clone();
		let mut loss = diff.square();

		let optimizer = Adagrad::new().learning_rate(0.5);

		for _ in 0..num_epochs {
			let x_ = arr2(&[[rand::thread_rng().gen()]]);
			let y_ = 0.5 * &x_ + 0.2;

			x.set_value(&x_);
			y.set_value(&y_);

			loss.forward();
			loss.backward(1.0);

			optimizer.step(loss.parameters());
		}

		println!(
			"Predicted: {} Loss: {} Slope {} Intercept {}",
			y_hat.value(),
			loss.value(),
			slope.value(),
			intercept.value()
		);

		assert!(loss.value().scalar_sum() < 1.0e-2);
	}

	#[test]
	fn multivariate_regression() {
		let slope = ParameterNode::new(random_matrix(1, 3));
		let intercept = ParameterNode::new(random_matrix(1, 1));

		let num_epochs = 200;

		let coefficients = arr2(&[[1.0], [2.0], [3.0]]);

		let x = InputNode::new(random_matrix(1, 3));
		let y = InputNode::new(random_matrix(1, 1));

		let y_hat = x.vector_dot(&slope) + intercept.clone();
		let diff = y.clone() - y_hat.clone();
		let mut loss = diff.square();

		let optimizer = SGD::new().learning_rate(0.1);

		for _ in 0..num_epochs {
			let x_ = arr2(&[[
				rand::thread_rng().gen(),
				rand::thread_rng().gen(),
				rand::thread_rng().gen(),
			]]);
			let y_ = &x_.dot(&coefficients) + 5.0;

			x.set_value(&x_);
			y.set_value(&y_);

			loss.forward();
			loss.backward(1.0);

			optimizer.step(loss.parameters());
		}

		println!(
			"Predicted: {} Loss: {} Slope {} Intercept {}",
			y_hat.value(),
			loss.value(),
			slope.value(),
			intercept.value()
		);

		assert!(loss.value().scalar_sum() < 1.0e-1);
	}

	#[test]
	fn embedding_factorization() {
		let (rows, cols) = (10, 4);

		let true_u = random_matrix(rows, 10);
		let true_v = random_matrix(cols, 10);
		let x = true_u.dot(&true_v.t());

		let y = random_matrix(1, 1);
		let u_input = vec![0];
		let v_input = vec![0];

		let output = InputNode::new(y);

		let u_embedding = ParameterNode::new(random_matrix(rows, 10));
		let v_embedding = ParameterNode::new(random_matrix(cols, 10));

		let u_index = IndexInputNode::new(&u_input);
		let v_index = IndexInputNode::new(&v_input);

		let u_vec = u_embedding.index(&u_index);
		let v_vec = v_embedding.index(&v_index);

		let y_hat = u_vec.vector_dot(&v_vec);
		let mut loss = (output.clone() - y_hat).square();

		let num_epochs = 200;
		let optimizer = Adagrad::new().learning_rate(0.1);

		let mut loss_val = 0.0;

		for _ in 0..num_epochs {
			loss_val = 0.0;

			for row_idx in 0..rows {
				for col_idx in 0..cols {
					u_index.set_value(row_idx);
					v_index.set_value(col_idx);

					output.set_value(x[(row_idx, col_idx)]);

					loss.forward();
					loss.backward(1.0);

					loss_val += loss.value().scalar_sum();

					optimizer.step(loss.parameters());
				}
			}

			println!("Loss {}", loss_val)
		}

		assert!(loss_val < 1e-2);
	}

	#[test]
	fn hogwild_embedding_factorization() {
		let (rows, cols) = (10, 4);

		let true_u = random_matrix(rows, 10);
		let true_v = random_matrix(cols, 10);
		let x = true_u.dot(&true_v.t());

		let u_input = vec![0];
		let v_input = vec![0];

		let u_parameters = Arc::new(HogwildParameter::new(random_matrix(rows, 10)));
		let v_parameters = Arc::new(HogwildParameter::new(random_matrix(cols, 10)));

		let losses: Vec<f32> = (0..rayon::current_num_threads())
			.into_par_iter()
			.map(|_| {
				let u_embedding = ParameterNode::shared(u_parameters.clone());
				let v_embedding = ParameterNode::shared(v_parameters.clone());

				let u_index = IndexInputNode::new(&u_input);
				let v_index = IndexInputNode::new(&v_input);
				let output = InputNode::new(random_matrix(1, 1));

				let u_vec = u_embedding.index(&u_index);
				let v_vec = v_embedding.index(&v_index);

				let y_hat = u_vec.vector_dot(&v_vec);
				let mut loss = (output.clone() - y_hat).square();

				let num_epochs = 100;

				let optimizer = SGD::new();

				let mut loss_val = 0.0;

				for _ in 0..num_epochs {
					loss_val = 0.0;

					for row_idx in 0..rows {
						for col_idx in 0..cols {
							u_index.set_value(row_idx);
							v_index.set_value(col_idx);

							output.set_value(x[(row_idx, col_idx)]);

							loss.forward();
							loss.backward(1.0);

							loss_val += loss.value().scalar_sum();

							optimizer.step(loss.parameters());
						}
					}
				}

				println!("Loss val {}", loss_val);

				loss_val
			})
			.collect();

		let sum_loss: f32 = losses.iter().sum();

		assert!(sum_loss / (losses.len() as f32) < 1e-3);
	}

	#[test]
	fn synchronized_embedding_factorization() {
		let (rows, cols) = (10, 4);

		let true_u = random_matrix(rows, 10);
		let true_v = random_matrix(cols, 10);
		let x = true_u.dot(&true_v.t());

		let u_input = vec![0];
		let v_input = vec![0];

		let u_parameters = Arc::new(HogwildParameter::new(random_matrix(rows, 10)));
		let v_parameters = Arc::new(HogwildParameter::new(random_matrix(cols, 10)));

		let optimizer = SGD::new();

		let losses: Vec<f32> = optimizer
			.synchronized(2.min(rayon::current_num_threads()))
			.into_par_iter()
			.map(|optimizer| {
				let u_embedding = ParameterNode::shared(u_parameters.clone());
				let v_embedding = ParameterNode::shared(v_parameters.clone());

				let u_index = IndexInputNode::new(&u_input);
				let v_index = IndexInputNode::new(&v_input);
				let output = InputNode::new(random_matrix(1, 1));

				let u_vec = u_embedding.index(&u_index);
				let v_vec = v_embedding.index(&v_index);

				let y_hat = u_vec.vector_dot(&v_vec);
				let mut loss = (output.clone() - y_hat).square();

				let num_epochs = 100;

				let mut loss_val = 0.0;

				for _ in 0..num_epochs {
					loss_val = 0.0;

					for row_idx in 0..rows {
						for col_idx in 0..cols {
							u_index.set_value(row_idx);
							v_index.set_value(col_idx);

							output.set_value(x[(row_idx, col_idx)]);

							loss.forward();
							loss.backward(1.0);

							loss_val += loss.value().scalar_sum();

							optimizer.step(loss.parameters());
						}
					}
				}

				println!("Loss val {}", loss_val);

				loss_val
			})
			.collect();

		let sum_loss: f32 = losses.iter().sum();

		assert!(sum_loss / (losses.len() as f32) < 1e-2);
	}
}
