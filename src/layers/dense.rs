use std::cell::{Cell, Ref, RefCell};
use std::ops::{AddAssign, Deref, DerefMut, DivAssign, MulAssign, SubAssign};

use std::sync::Arc;
use std::rc::Rc;

use rand;
use rand::distributions::{IndependentSample, Normal};
use ndarray;

use nodes;
use nodes::{AddNode, BackwardAction, Bor, DotNode, ForwardAction, HogwildParameter, Node,
            ParameterNode, PassCounter};

use {Arr, Variable};

#[derive(Debug)]
pub struct DenseNode<OP>
where
    OP: Node<Value = Arr, InputGradient = Arr>,
{
    weights: Variable<ParameterNode>,
    biases: Variable<ParameterNode>,
    // activation: Rc<
    output: Variable<AddNode<DotNode<OP, ParameterNode>, ParameterNode>>,
    needs_gradient: bool,
    counter: PassCounter,
}

impl<OP> DenseNode<OP>
where
    OP: Node<Value = Arr, InputGradient = Arr>,
{
    pub fn new(
        weights: Variable<ParameterNode>,
        biases: Variable<ParameterNode>,
        operand: Variable<OP>,
    ) -> Self {
        let result = operand.dot(&weights) + biases.clone();

        let needs_gradient =
            operand.needs_gradient() || weights.needs_gradient() || biases.needs_gradient();

        Self {
            weights: weights,
            biases: biases,
            output: result,
            needs_gradient: needs_gradient,
            counter: PassCounter::default(),
        }
    }
}

impl<OP> Node for DenseNode<OP>
where
    OP: Node<Value = Arr, InputGradient = Arr>,
{
    type Value = Arr;
    type InputGradient = Arr;
    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }
        self.output.forward();
    }

    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        self.output.node.backward(gradient)
    }

    fn value(&self) -> Bor<Self::Value> {
        self.output.value()
    }

    fn needs_gradient(&self) -> bool {
        self.needs_gradient
    }

    fn zero_gradient(&self) {
        self.counter.clear();
        self.output.zero_gradient();
    }
}
