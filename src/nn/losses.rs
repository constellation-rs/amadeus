//! Loss functions.
use std::cell::{Ref, RefCell};
use std::ops::Deref;
use std::rc::Rc;

use nodes::{BackwardAction, Bor, ForwardAction, IndexInputNode, LogSoftmaxNode, PassCounter};
use numerics;
use {Arr, Node, Variable};

/// Sparse categorical cross entropy loss.
///
/// Note that this performs a log-softmax operation
/// internally, so there is no need to perform a softmax
/// manually.
pub fn sparse_categorical_crossentropy<T>(
    x: &Variable<T>,
    y: &Variable<IndexInputNode>,
) -> Variable<SparseCategoricalCrossentropyNode<T>>
where
    T: Node<Value = Arr, InputGradient = Arr>,
{
    let node = SparseCategoricalCrossentropyNode::new(Rc::clone(&x.node), Rc::clone(&y.node));

    Variable::new(Rc::new(node), x.parameters.clone())
}

#[derive(Debug)]
pub struct SparseCategoricalCrossentropyNode<LHS> {
    operand: Rc<LHS>,
    log_softmax: LogSoftmaxNode<LHS>,
    y: Rc<IndexInputNode>,
    loss_value: RefCell<Arr>,
    gradient: RefCell<Arr>,
    needs_gradient: bool,
    counter: PassCounter,
}

impl<LHS> SparseCategoricalCrossentropyNode<LHS>
where
    LHS: Node<Value = Arr, InputGradient = Arr>,
{
    pub fn new(operand: Rc<LHS>, y: Rc<IndexInputNode>) -> Self {
        assert!(
            operand.value().rows() == 1,
            "Minibatches not supported: rows must be 1."
        );

        let log_softmax = LogSoftmaxNode::new(Rc::clone(&operand));
        let scalar_loss = {
            let log_softmax_value = log_softmax.value();

            let mut scalar_loss = 0.0;

            for &idx in y.value().iter() {
                scalar_loss += -log_softmax_value[(0, idx)];
            }

            scalar_loss
        };

        let mut loss_value = Arr::zeros((1, 1));
        loss_value.fill(scalar_loss);

        let gradient = operand.value().deref() * 0.0;
        let needs_gradient = operand.needs_gradient();

        SparseCategoricalCrossentropyNode {
            operand: operand,
            log_softmax: log_softmax,
            y: y,
            loss_value: RefCell::new(loss_value),
            gradient: RefCell::new(gradient),
            needs_gradient: needs_gradient,
            counter: PassCounter::default(),
        }
    }

    pub fn predictions(&self) -> Bor<Arr> {
        self.log_softmax.value()
    }
}

impl<LHS> Node for SparseCategoricalCrossentropyNode<LHS>
where
    LHS: Node<Value = Arr, InputGradient = Arr>,
{
    type Value = Arr;
    type InputGradient = Arr;

    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }

        self.log_softmax.forward();
        self.y.forward();

        let softmax_value = self.log_softmax.value();
        debug_assert!(
            softmax_value.rows() == 1,
            "Minibatches not supported: rows must be 1."
        );
        let softmax_slice = softmax_value.deref().as_slice().unwrap();

        let mut loss_value = 0.0;

        for &idx in self.y.value().iter() {
            loss_value += -softmax_slice[idx];
        }

        self.loss_value.borrow_mut().fill(loss_value);
    }
    /// The backpropagation mechanics for this node are a little strange,
    /// because it uses the log-softmax node for the forward pass but not
    /// for the backward pass.
    fn backward(&self, _: &Ref<Self::InputGradient>) {
        // TODO: actually use the input gradient
        let beta = match self.counter.backward() {
            BackwardAction::Set => 0.0,
            BackwardAction::Increment => 1.0,
        };

        {
            let mut gradient = self.gradient.borrow_mut();
            let gradient_slice = gradient.as_slice_mut().unwrap();

            let value = self.log_softmax.value();
            let value_slice = value.as_slice().unwrap();

            for (grad, &val) in izip!(gradient_slice.iter_mut(), value_slice.iter()) {
                *grad = beta * *grad + numerics::exp(val);
            }

            for &idx in self.y.value().iter() {
                gradient_slice[idx] -= 1.0;
            }
        }

        if self.counter.recurse_backward() {
            self.operand.backward(&self.gradient.borrow());
        }
    }
    fn value(&self) -> Bor<Self::Value> {
        Bor::RefGuard(self.loss_value.borrow())
    }
    fn needs_gradient(&self) -> bool {
        self.needs_gradient
    }
    fn zero_gradient(&self) {
        if !self.counter.is_zero() {
            self.operand.zero_gradient();
            self.log_softmax.zero_counter();
            self.y.zero_gradient();
            self.counter.clear();
        }
    }
}
