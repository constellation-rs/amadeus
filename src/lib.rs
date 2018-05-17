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

#[macro_use]
extern crate itertools;

use ndarray::Axis;

/// Alias for a `f32` `ndarray` matrix.
pub type Arr = ndarray::Array2<f32>;

use std::cell::RefCell;
use std::clone::Clone;
use std::ops::{Add, Deref, Div, Mul, Neg, Sub};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};

mod barrier;
mod fast_approx;
pub mod nn;
mod nodes;
mod numerics;
pub mod optim;

use nodes::*;
use optim::Optimizer;

pub use nodes::{Bor, HogwildParameter, IndexInputNode, InputNode, Node, ParameterNode};
pub use numerics::simd_dot;
use numerics::{ArraySlice, ArraySliceMut};

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

pub type BoxedNode = Rc<Node<Value = Arr, InputGradient = Arr>>;

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
    pub fn clip(&self, min: f32, max: f32) {
        let bor_value = self.node.value();
        let value: &Arr = bor_value.deref();
        let value = unsafe { &mut *(value as *const Arr as *mut Arr) };

        value
            .as_slice_mut()
            .unwrap()
            .iter_mut()
            .for_each(|x| *x = 100.0 * clamp(*x, min, max));

        println!("value {:#?}", value);
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
            Some(ref gradients) => Some(gradients.clone()),
            None => None,
        }
    }

    /// Return the (dense) gradient value of this node.
    fn sparse_gradient(&self) -> SparseGradientStore {
        self.node.gradient.borrow().sparse_gradient.clone()
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
                let constant = InputNode::new(self.value().deref() * 0.0 + other);

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
                let constant = InputNode::new(other.value().deref() * 0.0 + self);

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
        Variable::new(Rc::new(NegNode::new(self.node)), self.parameters.clone())
    }
}

/// Standard stochastic gradient descent optimizer with a fixed learning rate.
pub struct SGD {
    learning_rate: f32,
    parameters: Vec<Variable<ParameterNode>>,
    clamp: Option<(f32, f32)>,
}

impl SGD {
    /// Create a new optimizer instance with a given set of parameters.
    pub fn new(learning_rate: f32, parameters: Vec<Variable<ParameterNode>>) -> Self {
        SGD {
            learning_rate: learning_rate,
            parameters: parameters,
            clamp: None,
        }
    }

    /// Set the clamp bounds.
    pub fn clamp(mut self, min: f32, max: f32) -> Self {
        self.clamp = Some((min, max));
        self
    }

    /// Perform a single SGD step.
    pub fn step(&mut self) {
        let learning_rate = self.learning_rate;
        for parameter in &self.parameters {
            let mut sink = parameter.node.gradient.borrow_mut();
            let mut param_value = unsafe { parameter.node.value.value_mut() };

            if let Some((min, max)) = self.clamp {
                sink.clamp(min, max);
            }

            if sink.has_dense {
                param_value.scaled_add(-self.learning_rate, sink.dense_gradient());
            }

            for (ref index_vec, ref grad) in sink.sparse_gradient.as_slice() {
                for (grad_idx, &param_idx) in index_vec.iter().enumerate() {
                    let grad_row = grad.subview(Axis(0), grad_idx);
                    let mut param_row = param_value.subview_mut(Axis(0), param_idx);

                    numerics::map_add_assign_slice(
                        param_row.into_slice().unwrap(),
                        grad_row.into_slice().unwrap(),
                        |x| -learning_rate * x,
                    );
                }
            }
        }
    }
}

pub struct SynchronizationBarrier {
    core: Arc<SynchronizationBarrierCore>,
}

impl SynchronizationBarrier {
    pub fn new() -> Self {
        SynchronizationBarrier {
            core: Arc::new(SynchronizationBarrierCore::new()),
        }
    }

    fn register_thread(&self) -> SynchronizationBarrierGuard {
        self.core.register_thread();
        SynchronizationBarrierGuard {
            barrier: Arc::clone(&self.core),
        }
    }
}

struct SynchronizationBarrierCore {
    start_barrier: barrier::Barrier,
    end_barrier: barrier::Barrier,
    parameter_lock: Mutex<()>,
}

impl SynchronizationBarrierCore {
    fn new() -> Self {
        Self {
            start_barrier: barrier::Barrier::new(0),
            end_barrier: barrier::Barrier::new(0),
            parameter_lock: Mutex::default(),
        }
    }

    fn register_thread(&self) {
        self.start_barrier.increment_num_threads();
        self.end_barrier.increment_num_threads();
    }

    fn deregister_thread(&self) {
        self.start_barrier.decrement_num_threads();
        self.end_barrier.decrement_num_threads();
    }

    fn start_wait(&self) {
        self.start_barrier.wait();
    }

    fn end_wait(&self) {
        self.end_barrier.wait();
    }
}

pub struct SynchronizationBarrierGuard {
    barrier: Arc<SynchronizationBarrierCore>,
}

impl SynchronizationBarrierGuard {
    fn start_wait(&self) {
        self.barrier.start_wait();
    }

    fn end_wait(&self) {
        self.barrier.end_wait();
    }

    fn lock(&self) -> MutexGuard<()> {
        self.barrier.parameter_lock.lock().unwrap()
    }
}

impl Drop for SynchronizationBarrierGuard {
    fn drop(&mut self) {
        self.barrier.deregister_thread();
    }
}

/// Adagrad optimizer, scaled the learning rate by the inverse of previously
/// accumulated gradients.
pub struct Adagrad {
    learning_rate: f32,
    l2: f32,
    parameters: Vec<Variable<ParameterNode>>,
    clamp: Option<(f32, f32)>,
    eps: f32,
    sync_barrier: Option<SynchronizationBarrierGuard>,
}

impl Adagrad {
    /// Create a new optimizer instance with a given set of parameters.
    pub fn new(learning_rate: f32, parameters: Vec<Variable<ParameterNode>>) -> Self {
        Adagrad {
            learning_rate: learning_rate,
            l2: 0.0,
            parameters: parameters,
            clamp: None,
            eps: 1e-10,
            sync_barrier: None,
        }
    }

    pub fn synchronized(mut self, barrier: &SynchronizationBarrier) -> Self {
        self.sync_barrier = Some(barrier.register_thread());
        self
    }

    /// Set the clamp bounds.
    pub fn clamp(mut self, min: f32, max: f32) -> Self {
        self.clamp = Some((min, max));
        self
    }

    /// Set the L2 penalty.
    pub fn l2_penalty(mut self, l2_penalty: f32) -> Self {
        self.l2 = l2_penalty;
        self
    }

    /// Decay weights.
    pub fn decay_weights(&mut self, penalty: f32) {
        for parameter in &self.parameters {
            let mut param_value = unsafe { parameter.node.value.value_mut() };

            param_value
                .as_slice_mut()
                .unwrap()
                .iter_mut()
                .for_each(|x| *x -= x.signum() * penalty * numerics::pow2(*x));
        }
    }

    fn do_step(&self, parameter: &Variable<ParameterNode>) {
        let learning_rate = self.learning_rate;

        let mut sink = parameter.node.gradient.borrow_mut();

        if let Some((min, max)) = self.clamp {
            sink.clamp(min, max);
        }

        let param_value = unsafe { parameter.node.value.value_mut() };
        let squared_gradient = unsafe { parameter.node.value.squared_gradient_mut() };

        if sink.has_dense {
            for (value, &gradient, squared_gradient) in izip!(
                param_value.fast_slice_mut(),
                sink.dense_gradient().fast_slice(),
                squared_gradient.fast_slice_mut()
            ) {
                let gradient = gradient + *value * self.l2;
                *squared_gradient += numerics::pow2(gradient);
                *value -= learning_rate / (self.eps + squared_gradient.sqrt()) * gradient;
            }
        }

        sink.sparse_gradient
            .as_slice()
            .iter()
            .for_each(|(ref index_vec, ref grad)| {
                for (grad_idx, &param_idx) in index_vec.iter().enumerate() {
                    let grad_row = grad.subview(Axis(0), grad_idx);
                    let mut param_row = param_value.subview_mut(Axis(0), param_idx);
                    let mut squared_row = squared_gradient.subview_mut(Axis(0), param_idx);

                    for (value, &gradient, squared_gradient) in izip!(
                        param_row.fast_slice_mut(),
                        grad_row.into_slice().unwrap(),
                        squared_row.fast_slice_mut()
                    ) {
                        let gradient = gradient + *value * self.l2;
                        *squared_gradient += numerics::pow2(gradient);
                        *value -= learning_rate / (self.eps + squared_gradient.sqrt()) * gradient;
                    }
                }
            });
    }
}

impl optim::Optimizer for Adagrad {
    /// Perform a single SGD step.
    fn step(&self) {
        if let Some(ref barrier) = self.sync_barrier {
            barrier.start_wait();
            {
                let _ = barrier.lock();

                for parameter in &self.parameters {
                    self.do_step(parameter);
                }
            }

            barrier.end_wait();
        } else {
            for parameter in &self.parameters {
                self.do_step(parameter);
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

        let mut gradient = input.dense_gradient().unwrap_or(initial_input * 0.0);

        let sparse_gradient = input.sparse_gradient();

        for (indices, grad) in sparse_gradient.as_slice() {
            for &row_idx in indices.iter() {
                for (dest, orig) in gradient.row_mut(row_idx).iter_mut().zip(grad.iter()) {
                    *dest += orig;
                }
            }
        }

        gradient
    };

    output.zero_gradient();

    (central_difference, gradient)
}

/// Assert two arrays are within `tol` of each other.
pub fn assert_close(x: &Arr, y: &Arr, tol: f32) {
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

    use ndarray::arr2;

    use rand::distributions::{Distribution, Uniform};
    use rand::Rng;
    use rayon::prelude::*;
    use std::sync::Arc;

    use super::*;

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
        let z = z.clone() + z.clone();

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
        let mut z = z.clone() * 2.0 + z.clone().sigmoid();

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
        let mut z = z.clone() + z.clone();

        let (difference, gradient) = finite_difference(&mut x, &mut z);
        assert_close(&difference, &gradient, TOLERANCE);
        let (difference, gradient) = finite_difference(&mut y, &mut z);
        assert_close(&difference, &gradient, TOLERANCE);
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
        let mut y = ParameterNode::new(random_matrix(10, 5));
        let z = x.vector_dot(&y);
        let mut z = z.clone() + z.clone();

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
        let mut z = z.clone() + z.clone();

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
        //let v = x.clone() + y.clone();

        let z = x.stack(&y, ndarray::Axis(0));
        let mut z = z.clone().sigmoid() * z.clone().relu();

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
        //let v = x.clone() + y.clone();

        let mut z = x.stack(&y, ndarray::Axis(1)).sigmoid();

        assert_eq!(z.value().rows(), 10);
        assert_eq!(z.value().cols(), 10);

        let (difference, gradient) = finite_difference(&mut x, &mut z);
        assert_close(&difference, &gradient, TOLERANCE);

        let (difference, gradient) = finite_difference(&mut y, &mut z);
        assert_close(&difference, &gradient, TOLERANCE);
    }
    #[test]
    fn sparse_index_finite_difference() {
        let mut x = ParameterNode::new(random_matrix(10, 5));
        let idx_0 = IndexInputNode::new(&[random_index(10)]);
        let idx_1 = IndexInputNode::new(&[random_index(10)]);

        let mut z = (x.index(&idx_0).tanh() * x.index(&idx_1)).square();

        let (difference, gradient) = finite_difference(&mut x, &mut z);
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

        let optimizer = Adagrad::new(0.5, loss.parameters());

        for _ in 0..num_epochs {
            let _x = arr2(&[[rand::thread_rng().gen()]]);
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
            let _x = arr2(&[[
                rand::thread_rng().gen(),
                rand::thread_rng().gen(),
                rand::thread_rng().gen(),
            ]]);
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
        let optimizer = Adagrad::new(0.1, loss.parameters());

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
}
