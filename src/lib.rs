#![feature(test)]
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
//! Requires the nightly compiler due to use of SIMD intrinsics.
//!
//! # Quickstart
//!
//! The following defines a univariate linear regression model, then
//! backpropagates through it.
//!
//! ```rust
//! # extern crate rand;
//! # extern crate wyrm;
//! # use wyrm::*;
//! # fn random_matrix(rows: usize, cols: usize) -> Arr {
//! #      Arr::zeros((rows, cols)).map(|_| rand::random::<f32>())
//! # }
//! # fn main() {
//! let slope = ParameterNode::new(random_matrix(1, 1));
//! let intercept = ParameterNode::new(random_matrix(1, 1));
//!
//! let x = InputNode::new(random_matrix(1, 1));
//! let y = InputNode::new(random_matrix(1, 1));
//!
//! let y_hat = slope.clone() * x.clone() + intercept.clone();
//! let mut loss = (y.clone() - y_hat).square();
//! # }
//! ```
//!
//! To optimize the parameters, create an optimizer object and
//! go through several epochs of learning:
//!
//! ```rust
//! # extern crate rand;
//! # extern crate wyrm;
//! # use wyrm::*;
//! # fn random_matrix(rows: usize, cols: usize) -> Arr {
//! #      Arr::zeros((rows, cols)).map(|_| rand::random::<f32>())
//! # }
//! # fn main() {
//! # let slope = ParameterNode::new(random_matrix(1, 1));
//! # let intercept = ParameterNode::new(random_matrix(1, 1));
//! # let x = InputNode::new(random_matrix(1, 1));
//! # let y = InputNode::new(random_matrix(1, 1));
//! # let y_hat = slope.clone() * x.clone() + intercept.clone();
//! # let mut loss = (y.clone() - y_hat).square();
//! # let num_epochs = 10;
//! let mut optimizer = SGD::new(0.1, loss.parameters());
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
//!     optimizer.step();
//!     loss.zero_gradient();
//! }
//! # }
//! ```
//!
//! You can use `rayon` to fit your model in parallel, by first creating a set of shared
//! parameters, then building a per-thread copy of the model:
//!
//! ```rust
//! # extern crate rand;
//! # extern crate wyrm;
//! # extern crate rayon;
//! # use std::sync::Arc;
//! # use rayon::prelude::*;
//! # use wyrm::*;
//! # fn random_matrix(rows: usize, cols: usize) -> Arr {
//! #      Arr::zeros((rows, cols)).map(|_| rand::random::<f32>())
//! # }
//! # fn main() {
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
//!            let mut optimizer = SGD::new(0.1, loss.parameters());
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
//!                optimizer.step();
//!                loss.zero_gradient();
//!            }
//!        });
//! # }
//! ```
//!
//! ## BLAS support
//! You should enable BLAS support to get (much) better performance out of matrix-multiplication-heavy
//! workloads. To do so, add the following to your `Cargo.toml`:
//!
//! ```text
//! ndarray = { version = "0.11.0", features = ["blas", "serde-1"] }
//! blas-src = { version = "0.1.2", default-features = false, features = ["openblas"] }
//! openblas-src = { version = "0.5.6", default-features = false, features = ["cblas"] }
//! ```
//!
//! ## Fast numerics
//!
//! Enable the `fast-math` option to use fast approximations to transcendental functions.
//! This should give substantial speed gains in networks that are `exp`, `ln`, or `tanh`-heavy.

// TODO: pass through of parent values in .value(),
// optimizations in forward
// check for needs gradient
#[macro_use]
extern crate serde_derive;

extern crate serde;

extern crate ndarray;
extern crate rand;
extern crate rayon;
extern crate smallvec;
extern crate stdsimd;
extern crate test;

#[macro_use]
extern crate itertools;

use ndarray::Axis;

/// Alias for a `f32` `ndarray` matrix.
pub type Arr = ndarray::Array2<f32>;

use std::cell::RefCell;
use std::ops::{Add, Deref, Div, Mul, Neg, Sub};
use std::rc::Rc;
use std::clone::Clone;

mod nodes;
mod numerics;
mod fast_approx;
pub mod nn;

use nodes::*;

pub use numerics::simd_dot;
pub use nodes::{Bor, HogwildParameter, IndexInputNode, InputNode, Node, ParameterNode};

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
    fn set_value(&self, T);
}

fn merge_parameters(xs: &[Rc<ParameterNode>], ys: &[Rc<ParameterNode>]) -> Vec<Rc<ParameterNode>> {
    let mut unique_params: Vec<_> = xs.iter().chain(ys.iter()).cloned().collect();

    unique_params.sort_unstable_by_key(|x| x.deref() as *const ParameterNode);
    unique_params.dedup_by_key(|x| (*x).deref() as *const ParameterNode);

    unique_params
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
    parameters: Vec<Rc<ParameterNode>>,
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
    fn new(node: Rc<T>, parameters: Vec<Rc<ParameterNode>>) -> Self {
        Variable {
            node: node,
            grad: None,
            parameters: parameters,
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
    /// Zero the gradients. Must be called after a backward step or whenever inputs change.
    pub fn zero_gradient(&self) {
        self.node.zero_gradient();
    }

    pub fn needs_gradient(&self) -> bool {
        self.node.needs_gradient()
    }

    /// Return the parameters of the graph.
    pub fn parameters(&self) -> Vec<Variable<ParameterNode>> {
        let mut unique_params = self.parameters.clone();
        unique_params.sort_unstable_by_key(|x| x.deref() as *const ParameterNode);
        unique_params.dedup_by_key(|x| (*x).deref() as *const ParameterNode);

        unique_params
            .iter()
            .map(|x| Variable::new(Rc::clone(x), Vec::new()))
            .collect()
    }
}

impl<T> Variable<T>
where
    T: Node<Value = Arr, InputGradient = Arr>,
{
    /// Box the variable, erasing its specific type. Use to manage the complexity
    /// of variable types in deep computation graphs.
    pub fn boxed(&self) -> Variable<Rc<Node<Value = Arr, InputGradient = Arr>>> {
        Variable::new(
            Rc::new(self.node.clone() as Rc<Node<Value = Arr, InputGradient = Arr>>),
            self.parameters.clone(),
        )
    }

    /// Run the backward pass through the subgraph terminating at this node.
    /// The weight parameter scales the gradients.
    pub fn backward(&mut self, weight: f32) {
        self.grad
            .get_or_insert(RefCell::new(self.node.value().map(|_| weight)));
        if let Some(ref grad) = self.grad {
            {
                grad.borrow_mut().map_inplace(|x| *x = weight);
            }
            self.node.backward(&grad.borrow());
        }
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
        &self,
        other: &Variable<S>,
        axis: ndarray::Axis,
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
}

impl Variable<ParameterNode> {
    /// Return the (dense) gradient value of this node.
    pub fn dense_gradient(&self) -> Option<Arr> {
        match self.node.gradient.borrow().dense_gradient {
            Some(ref arr) => Some(arr.clone()),
            None => None,
        }
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
        let param_value = unsafe { &mut *(self.node.value.deref().value.as_ptr()) };
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

impl<LHS, RHS> Add<Variable<RHS>> for Variable<LHS>
where
    RHS: Node<Value = Arr, InputGradient = Arr>,
    LHS: Node<Value = Arr, InputGradient = Arr>,
{
    type Output = Variable<AddNode<LHS, RHS>>;
    fn add(self, other: Variable<RHS>) -> Self::Output {
        Variable::new(
            Rc::new(AddNode::new(self.node, other.node)),
            merge_parameters(&self.parameters, &other.parameters),
        )
    }
}

impl<LHS, RHS> Sub<Variable<RHS>> for Variable<LHS>
where
    RHS: Node<Value = Arr, InputGradient = Arr>,
    LHS: Node<Value = Arr, InputGradient = Arr>,
{
    type Output = Variable<SubNode<LHS, RHS>>;
    fn sub(self, other: Variable<RHS>) -> Self::Output {
        Variable::new(
            Rc::new(SubNode::new(self.node, other.node)),
            merge_parameters(&self.parameters, &other.parameters),
        )
    }
}

impl<LHS, RHS> Mul<Variable<RHS>> for Variable<LHS>
where
    RHS: Node<Value = Arr, InputGradient = Arr>,
    LHS: Node<Value = Arr, InputGradient = Arr>,
{
    type Output = Variable<MulNode<LHS, RHS>>;
    fn mul(self, other: Variable<RHS>) -> Self::Output {
        Variable::new(
            Rc::new(MulNode::new(self.node, other.node)),
            merge_parameters(&self.parameters, &other.parameters),
        )
    }
}

impl<LHS, RHS> Div<Variable<RHS>> for Variable<LHS>
where
    RHS: Node<Value = Arr, InputGradient = Arr>,
    LHS: Node<Value = Arr, InputGradient = Arr>,
{
    type Output = Variable<DivNode<LHS, RHS>>;
    fn div(self, other: Variable<RHS>) -> Self::Output {
        Variable::new(
            Rc::new(DivNode::new(self.node, other.node)),
            merge_parameters(&self.parameters, &other.parameters),
        )
    }
}

impl<T> Neg for Variable<T>
where
    T: Node<Value = Arr, InputGradient = Arr>,
{
    type Output = Variable<NegNode<T>>;
    fn neg(self) -> Self::Output {
        Variable::new(Rc::new(NegNode::new(self.node)), self.parameters.clone())
    }
}

/// Standard stochastic gradient descent optimizer with a fixed learning rate.
pub struct SGD {
    learning_rate: f32,
    parameters: Vec<Variable<ParameterNode>>,
}

impl SGD {
    /// Create a new optimizer instance with a given set of parameters.
    pub fn new(learning_rate: f32, parameters: Vec<Variable<ParameterNode>>) -> Self {
        SGD {
            learning_rate: learning_rate,
            parameters: parameters,
        }
    }

    /// Perform a single SGD step.
    pub fn step(&mut self) {
        let learning_rate = self.learning_rate;
        for parameter in &self.parameters {
            let mut sink = parameter.node.gradient.borrow_mut();
            let mut param_value = unsafe { parameter.node.value.value_mut() };

            if sink.has_dense {
                param_value.scaled_add(-self.learning_rate, sink.dense_gradient());
            }

            if sink.has_sparse {
                for &(ref index_vec, ref grad) in sink.sparse_gradient.iter() {
                    for (grad_idx, &param_idx) in index_vec.iter().enumerate() {
                        let grad_row = grad.subview(Axis(0), grad_idx);
                        let mut param_row = param_value.subview_mut(Axis(0), param_idx);

                        numerics::map_add_assign_slice(
                            param_row.into_slice().unwrap(),
                            grad_row.into_slice().unwrap(),
                            |x| -learning_rate * clamp(x, -10.0, 10.0),
                        );
                    }
                }
            }
        }
    }
}

/// Adagrad optimizer, scaled the learning rate by the inverse of previously
/// accumulated gradients.
pub struct Adagrad {
    learning_rate: f32,
    parameters: Vec<Variable<ParameterNode>>,
}

impl Adagrad {
    /// Create a new optimizer instance with a given set of parameters.
    pub fn new(learning_rate: f32, parameters: Vec<Variable<ParameterNode>>) -> Self {
        Adagrad {
            learning_rate: learning_rate,
            parameters: parameters,
        }
    }

    /// Perform a single SGD step.
    pub fn step(&mut self) {
        let learning_rate = self.learning_rate;

        for parameter in &self.parameters {
            let mut sink = parameter.node.gradient.borrow_mut();
            let mut param_value = unsafe { parameter.node.value.value_mut() };
            let mut squared_gradient = unsafe { parameter.node.value.squared_gradient_mut() };

            if sink.has_dense {
                for (value, &gradient, squared_gradient) in izip!(
                    param_value.as_slice_mut().unwrap(),
                    sink.dense_gradient().as_slice().unwrap(),
                    squared_gradient.as_slice_mut().unwrap()
                ) {
                    *value -= learning_rate * gradient / squared_gradient.sqrt();
                    *squared_gradient += numerics::pow2(gradient);
                }
            }

            if sink.has_sparse {
                for &(ref index_vec, ref grad) in sink.sparse_gradient.iter() {
                    for (grad_idx, &param_idx) in index_vec.iter().enumerate() {
                        let grad_row = grad.subview(Axis(0), grad_idx);
                        let mut param_row = param_value.subview_mut(Axis(0), param_idx);
                        let mut squared_row = squared_gradient.subview_mut(Axis(0), param_idx);

                        for (value, &gradient, squared_gradient) in izip!(
                            param_row.as_slice_mut().unwrap(),
                            grad_row.into_slice().unwrap(),
                            squared_row.as_slice_mut().unwrap()
                        ) {
                            *value -= learning_rate * gradient / squared_gradient.sqrt();
                            *squared_gradient += numerics::pow2(gradient);
                        }
                    }
                }
            }
        }
    }
}

/// Compute finite difference gradient estimates of the output variable
/// with respect to the input. Use to verify correctness of gradient
/// computations.
pub fn finite_difference<T>(
    input: &mut Variable<ParameterNode>,
    output: &mut Variable<T>,
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
            output.value().clone()
        };

        let negative_difference = {
            output.zero_gradient();
            let mut changed_input = initial_input.clone();
            changed_input[idx] -= 0.5 * delta_x;
            input.set_value(&changed_input);
            output.forward();
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

        input
            .dense_gradient()
            .expect("Expecting a gradient but gradient not present.")
    };

    output.zero_gradient();

    (central_difference, gradient)
}

#[allow(dead_code)]
fn assert_close(x: &Arr, y: &Arr, tol: f32) {
    assert!(
        x.all_close(y, tol),
        "{:#?} not within {} of {:#?}",
        x,
        tol,
        y
    );
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;
    use ndarray::arr2;
    use rayon::prelude::*;

    use super::*;

    const TOLERANCE: f32 = 0.2;

    fn random_matrix(rows: usize, cols: usize) -> Arr {
        Arr::zeros((rows, cols)).map(|_| rand::random::<f32>())
    }

    #[test]
    fn parameter_deduplication() {
        let x = ParameterNode::new(random_matrix(1, 1));
        let y = ParameterNode::new(random_matrix(1, 1));

        let z = x + y;
        let z = z.clone() + z.clone();

        assert_eq!(z.parameters().len(), 2);
    }

    #[test]
    fn add_finite_difference() {
        let mut x = ParameterNode::new(random_matrix(1, 1));
        let y = ParameterNode::new(random_matrix(1, 1));
        let mut z = x.clone() + y.clone();

        let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
        assert_close(&finite_difference, &gradient, TOLERANCE);
    }
    #[test]
    fn mul_finite_difference() {
        let mut x = ParameterNode::new(random_matrix(1, 1));
        let y = ParameterNode::new(random_matrix(1, 1));
        let mut z = x.clone() * y.clone();

        let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
        assert_close(&finite_difference, &gradient, TOLERANCE);
    }
    #[test]
    fn div_finite_difference() {
        let mut x = ParameterNode::new(random_matrix(1, 1));
        let y = ParameterNode::new(random_matrix(1, 1));
        let mut z = (x.clone() + x.clone()) / y.clone();

        let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
        assert_close(&finite_difference, &gradient, TOLERANCE);
    }
    #[test]
    fn vector_dot_finite_difference() {
        let mut x = ParameterNode::new(random_matrix(10, 5));
        let y = ParameterNode::new(random_matrix(10, 5));
        let mut z = x.vector_dot(&y);

        let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
        assert_close(&finite_difference, &gradient, TOLERANCE);
    }
    #[test]
    fn dot_finite_difference() {
        let mut x = ParameterNode::new(random_matrix(10, 5));
        let y = ParameterNode::new(random_matrix(5, 10));
        let mut z = (x.clone() + x.clone()).dot(&y);

        let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
        assert_close(&finite_difference, &gradient, TOLERANCE);
    }
    #[test]
    fn dot_accumulation_finite_difference() {
        let mut x = ParameterNode::new(random_matrix(10, 5));
        let mut y = ParameterNode::new(random_matrix(5, 10));
        let z = x.clone().dot(&y);
        let mut v = z.clone() * z.clone();

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
        let mut z = (x.clone() + x.clone()).ln();

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
        assert_close(&finite_difference, &gradient, TOLERANCE);
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
        let mut z = (x.clone() + x.clone()).sigmoid();

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

        assert_close(v.value().deref(), z.value().deref(), TOLERANCE);

        let (finite_difference, gradient) = finite_difference(&mut x, &mut z);
        assert_close(&finite_difference, &gradient, TOLERANCE);
    }
    #[test]
    fn sparse_categorical_cross_entropy_finite_difference() {
        let mut x = ParameterNode::new(random_matrix(1, 10));
        let z = x.clone() + x.clone();
        let idx = IndexInputNode::new(&vec![0][..]);
        let mut loss = nn::losses::sparse_categorical_crossentropy(&z, &idx);

        let (finite_difference, gradient) = finite_difference(&mut x, &mut loss);
        assert_close(&finite_difference, &gradient, TOLERANCE);
    }
    #[test]
    fn rowwise_stack_finite_difference() {
        let mut x = ParameterNode::new(random_matrix(10, 5));
        let mut y = ParameterNode::new(random_matrix(10, 5));
        let v = x.clone() + y.clone();

        let mut z = v.stack(&v, ndarray::Axis(0)).sigmoid();

        assert_eq!(z.value().rows(), 20);
        assert_eq!(z.value().cols(), 5);

        let (difference, gradient) = finite_difference(&mut x, &mut z);
        assert_close(&difference, &gradient, TOLERANCE);

        let (difference, gradient) = finite_difference(&mut y, &mut z);
        assert_close(&difference, &gradient, TOLERANCE);
    }
    #[test]
    fn columnwise_stack_finite_difference() {
        let mut x = ParameterNode::new(random_matrix(10, 5));
        let mut y = ParameterNode::new(random_matrix(10, 5));
        let v = x.clone() + y.clone();

        let mut z = v.stack(&v, ndarray::Axis(1)).sigmoid();

        assert_eq!(z.value().rows(), 10);
        assert_eq!(z.value().cols(), 10);

        let (difference, gradient) = finite_difference(&mut x, &mut z);
        assert_close(&difference, &gradient, TOLERANCE);

        let (difference, gradient) = finite_difference(&mut y, &mut z);
        assert_close(&difference, &gradient, TOLERANCE);
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

        let mut optimizer = Adagrad::new(0.5, loss.parameters());

        for _ in 0..num_epochs {
            let _x = arr2(&[[rand::random::<f32>()]]);
            let _y = 0.5 * &_x + 0.2;

            x.set_value(&_x);
            y.set_value(&_y);

            loss.forward();
            loss.backward(1.0);

            optimizer.step();
            loss.zero_gradient();
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

        let mut optimizer = SGD::new(0.1, loss.parameters());

        for _ in 0..num_epochs {
            let _x = arr2(&[
                [
                    rand::random::<f32>(),
                    rand::random::<f32>(),
                    rand::random::<f32>(),
                ],
            ]);
            let _y = &_x.dot(&coefficients) + 5.0;

            x.set_value(&_x);
            y.set_value(&_y);

            loss.forward();
            loss.backward(1.0);

            optimizer.step();
            loss.zero_gradient();
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
        let mut loss = (output.clone() - y_hat.clone()).square();

        let num_epochs = 200;
        let mut optimizer = Adagrad::new(0.1, loss.parameters());

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

                    optimizer.step();
                    loss.zero_gradient();
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
                let mut loss = (output.clone() - y_hat.clone()).square();

                let num_epochs = 100;

                let mut optimizer = SGD::new(0.1, loss.parameters());

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

                            optimizer.step();
                            loss.zero_gradient();
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

    use test::Bencher;

    #[bench]
    fn bench_node_reuse(b: &mut Bencher) {
        let dim = 128;

        let x = ParameterNode::new(random_matrix(1, dim));
        let y = ParameterNode::new(random_matrix(dim, 10));
        let v = x.dot(&y);
        let z = v.clone() + v.clone() + v.clone() + v.clone();

        b.iter(|| {
            z.forward();
            z.zero_gradient();
        });
    }

    #[bench]
    fn bench_matrix_multiply(b: &mut Bencher) {
        let dim = 64;
        let num_epochs = 20;

        let x_data = Arc::new(HogwildParameter::new(random_matrix(1, dim)));
        let y_data = Arc::new(HogwildParameter::new(random_matrix(dim, 10)));

        b.iter(|| {
            (0..rayon::current_num_threads())
                .into_par_iter()
                .for_each(|_| {
                    let x = ParameterNode::shared(x_data.clone());
                    let y = ParameterNode::shared(y_data.clone());

                    let v = x.dot(&y);

                    for _ in 0..num_epochs {
                        v.forward();
                        v.zero_gradient();
                    }
                });
        });
    }
}
