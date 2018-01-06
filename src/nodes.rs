use std;
use std::fmt;
use std::ops::{AddAssign, Deref, DerefMut, DivAssign, MulAssign, SubAssign};
use std::cell::{Cell, Ref, RefCell};
use std::sync::Arc;
use std::rc::Rc;

use ndarray;
use ndarray::Axis;
use ndarray::linalg::general_mat_mul;

use smallvec::SmallVec;

use numerics;

use super::{Arr, Variable};

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
struct PassCounter {
    forward_count: Cell<usize>,
    backward_count: Cell<usize>,
}

impl PassCounter {
    fn clear(&self) {
        self.forward_count.set(0);
        self.backward_count.set(0);
    }
    fn recurse_backward(&self) -> bool {
        let backward_count = self.backward_count.get();
        let forward_count = self.forward_count.get();

        backward_count == forward_count
    }
    fn forward(&self) -> ForwardAction {
        let count = self.forward_count.get();
        self.forward_count.set(count + 1);

        match count {
            0 => ForwardAction::Evaluate,
            _ => ForwardAction::Cached,
        }
    }
    fn backward(&self) -> BackwardAction {
        let backward_count = self.backward_count.get();

        let action = match backward_count {
            0 => BackwardAction::Set,
            _ => BackwardAction::Increment,
        };

        self.backward_count.set(backward_count + 1);

        action
    }
}

/// Generalisation over borrowed RefCell values
/// and simple references.
#[derive(Debug)]
pub enum Bor<'value, T: 'value> {
    RefGuard(Ref<'value, T>),
    Reference(&'value T),
}

impl<'value, T: 'value> Deref for Bor<'value, T> {
    type Target = T;
    fn deref(&self) -> &T {
        match *self {
            Bor::RefGuard(ref val) => val.deref(),
            Bor::Reference(ref val) => val.deref(),
        }
    }
}

impl<'value, T: 'value + fmt::Display> fmt::Display for Bor<'value, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.deref())
    }
}

/// Trait representing a computation node. Structs implementing
/// this trait can be used as elements of the computation graph.
pub trait Node: fmt::Debug + Sized {
    /// Type of the node's value.
    type Value;
    /// Type of the input gradient the node receives
    /// during backpropagation.
    type InputGradient;
    /// Type of the gradient the node passes down
    /// to its ancestors during backpropagation.
    type OutputGradient;
    /// Perform the forward step. Should recursively call
    /// the forward methods of its ancestors.
    fn forward(&self);
    /// Perform the backward step. Should recursively call
    /// the backward methods of its ancestors.
    fn backward(&self, &Ref<Self::InputGradient>);
    /// Return the value of the node.
    fn value(&self) -> Bor<Self::Value>;
    /// If the node needs to be used in the backward step.
    fn needs_gradient(&self) -> bool;
    fn zero_gradient(&self);
}

#[derive(Debug)]
pub struct AddNode<LHS, RHS> {
    value: RefCell<Arr>,
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
        let value = lhs.value().deref() + rhs.value().deref();

        AddNode {
            value: RefCell::new(value),
            lhs: lhs,
            rhs: rhs,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
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

        numerics::map_assign_binary(
            self_value.deref_mut(),
            lhs_value.deref(),
            rhs_value.deref(),
            |x, y| x + y,
        );
    }
    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        self.lhs.backward(gradient);
        self.rhs.backward(gradient);
    }
    fn value(&self) -> Bor<Self::Value> {
        Bor::RefGuard(self.value.borrow())
    }
    fn needs_gradient(&self) -> bool {
        self.needs_gradient
    }
    fn zero_gradient(&self) {
        self.counter.clear();
        self.lhs.zero_gradient();
        self.rhs.zero_gradient();
    }
}

fn row_wise_stack(dest: &mut Arr, lhs: &Arr, rhs: &Arr) {
    for (mut dest_row, source_row) in dest.genrows_mut()
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

fn column_wise_stack_gradient(gradient: &Arr, lhs: &mut Arr, rhs: &mut Arr, op: BackwardAction) {
    for (grad_row, mut lhs_row, mut rhs_row) in izip!(
        gradient.genrows().into_iter(),
        lhs.genrows_mut().into_iter(),
        rhs.genrows_mut().into_iter()
    ) {
        let grad_row = grad_row.as_slice().unwrap();
        let lhs_row = lhs_row.as_slice_mut().unwrap();
        let rhs_row = rhs_row.as_slice_mut().unwrap();

        let (left, right) = grad_row.split_at(lhs_row.len());

        match op {
            BackwardAction::Increment => {
                for (x, y) in lhs_row.iter_mut().zip(left.iter()) {
                    *x += y;
                }
                for (x, y) in rhs_row.iter_mut().zip(right.iter()) {
                    *x += y;
                }
            }
            BackwardAction::Set => {
                for (x, &y) in lhs_row.iter_mut().zip(left.iter()) {
                    *x = y;
                }
                for (x, &y) in rhs_row.iter_mut().zip(right.iter()) {
                    *x = y;
                }
            }
        }
    }
}

fn row_wise_stack_gradient(gradient: &Arr, lhs: &mut Arr, rhs: &mut Arr, op: BackwardAction) {
    for (grad_row, mut dest_row) in gradient
        .genrows()
        .into_iter()
        .zip(lhs.genrows_mut().into_iter().chain(rhs.genrows_mut()))
    {
        let grad_row = grad_row.as_slice().unwrap();
        let dest_row = dest_row.as_slice_mut().unwrap();

        match op {
            BackwardAction::Increment => for (x, y) in dest_row.iter_mut().zip(grad_row.iter()) {
                *x += y;
            },
            BackwardAction::Set => for (x, &y) in dest_row.iter_mut().zip(grad_row.iter()) {
                *x = y;
            },
        }
    }
}

#[derive(Debug)]
pub struct ConcatenateNode<LHS, RHS> {
    axis: ndarray::Axis,
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
    pub fn new(lhs: Rc<LHS>, rhs: Rc<RHS>, axis: ndarray::Axis) -> Self {
        let needs_gradient = lhs.needs_gradient() || rhs.needs_gradient();

        let value = ndarray::stack(
            axis,
            &[lhs.value().deref().view(), rhs.value().deref().view()],
        ).expect("Unable to concatenate arrays.");

        let lhs_gradient = lhs.value().deref() * 0.0;
        let rhs_gradient = rhs.value().deref() * 0.0;

        ConcatenateNode {
            axis: axis,
            value: RefCell::new(value),
            lhs_gradient: RefCell::new(lhs_gradient),
            rhs_gradient: RefCell::new(rhs_gradient),
            lhs: lhs,
            rhs: rhs,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
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
            ndarray::Axis(0) => {
                row_wise_stack(self_value.deref_mut(), lhs_value.deref(), rhs_value.deref())
            }
            // Horizontally
            ndarray::Axis(1) => {
                column_wise_stack(self_value.deref_mut(), lhs_value.deref(), rhs_value.deref())
            }
            // Not allowed
            _ => panic!("Stacking tensors not allowed."),
        }
    }
    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        {
            let mut lhs_grad = self.lhs_gradient.borrow_mut();
            let mut rhs_grad = self.rhs_gradient.borrow_mut();

            match self.axis {
                ndarray::Axis(0) => row_wise_stack_gradient(
                    gradient,
                    lhs_grad.deref_mut(),
                    rhs_grad.deref_mut(),
                    self.counter.backward(),
                ),
                ndarray::Axis(1) => column_wise_stack_gradient(
                    gradient,
                    lhs_grad.deref_mut(),
                    rhs_grad.deref_mut(),
                    self.counter.backward(),
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
    fn zero_gradient(&self) {
        self.counter.clear();
        self.lhs.zero_gradient();
        self.rhs.zero_gradient();
    }
}

/// Input node for the graph.
#[derive(Debug)]
pub struct InputNode {
    pub value: RefCell<Arr>,
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
    type OutputGradient = ();
    fn forward(&self) {}
    fn backward(&self, _: &Ref<Self::InputGradient>) {}
    fn value(&self) -> Bor<Self::Value> {
        Bor::RefGuard(self.value.borrow())
    }
    fn needs_gradient(&self) -> bool {
        false
    }
    fn zero_gradient(&self) {}
}

#[derive(Debug)]
pub struct GradientAccumulator {
    pub dense_shape: (usize, usize),
    pub dense_gradient: Option<Arr>,
    pub sparse_gradient: SmallVec<[(SmallVec<[usize; 4]>, Arr); 4]>,
    pub has_dense: bool,
    pub has_sparse: bool,
}

impl GradientAccumulator {
    fn new(dense_shape: (usize, usize)) -> Self {
        GradientAccumulator {
            dense_shape: dense_shape,
            dense_gradient: None,
            sparse_gradient: SmallVec::new(),
            has_dense: false,
            has_sparse: false,
        }
    }
    pub fn dense_gradient(&mut self) -> &mut Arr {
        self.dense_gradient
            .get_or_insert(Arr::zeros(self.dense_shape))
    }
    fn zero_gradient(&mut self) {
        if self.has_dense {
            self.dense_gradient().fill(0.0);
        }

        if self.has_sparse {
            for &mut (ref mut index_vec, ref mut grad) in self.sparse_gradient.iter_mut() {
                index_vec.clear();
                grad.fill(0.0)
            }
        }

        self.has_dense = false;
        self.has_sparse = false;
    }
}

pub trait GradientSink<T> {
    fn accumulate_gradient(&mut self, gradient: T);
}

impl<'a, 'b> GradientSink<&'a Ref<'b, Arr>> for GradientAccumulator {
    fn accumulate_gradient(&mut self, gradient: &Ref<Arr>) {
        self.dense_gradient().add_assign(gradient.deref());
        self.has_dense = true;
    }
}

impl<'a> GradientSink<(&'a [usize], &'a Arr)> for GradientAccumulator {
    fn accumulate_gradient(&mut self, gradient: (&'a [usize], &'a Arr)) {
        let (index, value) = gradient;
        self.has_sparse = true;
        let gradients = &mut self.sparse_gradient;

        // Check if we can reuse one of the gradient accumulators
        for &mut (ref mut index_vec, ref mut grad) in gradients.iter_mut() {
            if index_vec.is_empty() {
                index_vec.extend_from_slice(&index[..]);
                grad.assign(value);
                return;
            }
        }

        // Otherwise create one
        gradients.push((SmallVec::from(&index[..]), value.clone()));
    }
}

unsafe impl Sync for HogwildParameter {}

/// Struct used to hold parameters that need to be shared among
/// multiple `ParameterNode`s for asynchronous, parallel optimization.
#[derive(Debug, Serialize, Deserialize)]
pub struct HogwildParameter {
    pub value: Arr,
}

impl HogwildParameter {
    /// Create a new parameter object.
    pub fn new(value: Arr) -> Self {
        HogwildParameter { value: value }
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
        let shape = (value.value.rows(), value.value.cols());

        let node = Rc::new(ParameterNode {
            value: value,
            gradient: RefCell::new(GradientAccumulator::new(shape)),
        });
        let params = vec![node.clone()];

        Variable::new(node, params)
    }
    /// Create a new parameter node. The parameters held by this node
    /// cannot be shared and optimized in parallel.
    pub fn new(value: Arr) -> Variable<Self> {
        let shape = (value.rows(), value.cols());

        let node = Rc::new(ParameterNode {
            value: Arc::new(HogwildParameter::new(value)),
            gradient: RefCell::new(GradientAccumulator::new(shape)),
        });
        let params = vec![node.clone()];

        Variable::new(node, params)
    }
    /// Zero the accumulated gradients of this node.
    pub fn zero_gradient(&self) {
        self.gradient.borrow_mut().zero_gradient();
    }
}

impl Node for ParameterNode {
    type Value = Arr;
    type InputGradient = Arr;
    type OutputGradient = ();
    fn forward(&self) {}
    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        self.gradient.borrow_mut().accumulate_gradient(gradient);
    }
    fn value(&self) -> Bor<Self::Value> {
        Bor::Reference(&self.value.value)
    }
    fn needs_gradient(&self) -> bool {
        true
    }
    fn zero_gradient(&self) {
        self.gradient.borrow_mut().zero_gradient();
    }
}

#[derive(Debug)]
pub struct SubNode<LHS, RHS>
where
    LHS: Node<Value = Arr, InputGradient = Arr>,
    RHS: Node<Value = Arr, InputGradient = Arr>,
{
    value: RefCell<Arr>,
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
        let value = lhs.value().deref() + rhs.value().deref();

        let rhs_gradient = rhs.value().deref() * 0.0;

        SubNode {
            value: RefCell::new(value),
            rhs_gradient: RefCell::new(rhs_gradient),
            lhs: lhs,
            rhs: rhs,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }

        self.lhs.forward();
        self.rhs.forward();

        let mut dest = self.value.borrow_mut();

        dest.assign(self.lhs.value().deref());
        dest.sub_assign(self.rhs.value().deref());
    }

    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        match self.counter.backward() {
            BackwardAction::Set => {
                let mut rhs_gradient = self.rhs_gradient.borrow_mut();

                rhs_gradient.assign(gradient);
                rhs_gradient.mul_assign(-1.0);
            }
            BackwardAction::Increment => {
                let mut rhs_gradient = self.rhs_gradient.borrow_mut();

                rhs_gradient.scaled_add(-1.0, gradient);
            }
        }

        if self.counter.recurse_backward() {
            self.lhs.backward(gradient);
            self.rhs.backward(&self.rhs_gradient.borrow());
        }
    }
    fn value(&self) -> Bor<Self::Value> {
        Bor::RefGuard(self.value.borrow())
    }
    fn needs_gradient(&self) -> bool {
        self.needs_gradient
    }
    fn zero_gradient(&self) {
        self.counter.clear();
        self.lhs.zero_gradient();
        self.rhs.zero_gradient();
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
        let value = lhs.value().deref() * rhs.value().deref();

        let lhs_gradient = &value * 0.0;
        let rhs_gradient = &value * 0.0;

        MulNode {
            value: RefCell::new(value),
            lhs_gradient: RefCell::new(lhs_gradient),
            rhs_gradient: RefCell::new(rhs_gradient),
            lhs: lhs,
            rhs: rhs,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }

        self.lhs.forward();
        self.rhs.forward();

        let mut dest = self.value.borrow_mut();

        dest.assign(self.lhs.value().deref());
        dest.mul_assign(self.rhs.value().deref());
    }
    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        match self.counter.backward() {
            BackwardAction::Set => {
                let mut lhs_gradient = self.lhs_gradient.borrow_mut();
                lhs_gradient.assign(self.rhs.value().deref());
                lhs_gradient.mul_assign(gradient.deref());

                let mut rhs_gradient = self.rhs_gradient.borrow_mut();

                rhs_gradient.assign(self.lhs.value().deref());
                rhs_gradient.mul_assign(gradient.deref());
            }
            BackwardAction::Increment => {
                let mut lhs_gradient = self.lhs_gradient.borrow_mut();
                let mut rhs_gradient = self.rhs_gradient.borrow_mut();

                numerics::map_inplace_assign_binary(
                    lhs_gradient.deref_mut(),
                    self.rhs.value().deref(),
                    gradient.deref(),
                    |out, rhs, grad| *out += rhs * grad,
                );
                numerics::map_inplace_assign_binary(
                    rhs_gradient.deref_mut(),
                    self.lhs.value().deref(),
                    gradient.deref(),
                    |out, rhs, grad| *out += rhs * grad,
                );
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
    fn zero_gradient(&self) {
        self.counter.clear();
        self.lhs.zero_gradient();
        self.rhs.zero_gradient();
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
        let value = lhs.value().deref() / rhs.value().deref();

        let lhs_gradient = &value * 0.0;
        let rhs_gradient = &value * 0.0;

        DivNode {
            value: RefCell::new(value),
            lhs_gradient: RefCell::new(lhs_gradient),
            rhs_gradient: RefCell::new(rhs_gradient),
            lhs: lhs,
            rhs: rhs,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }

        self.lhs.forward();
        self.rhs.forward();

        let mut dest = self.value.borrow_mut();

        dest.assign(self.lhs.value().deref());
        dest.div_assign(self.rhs.value().deref());
    }
    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        match self.counter.backward() {
            BackwardAction::Set => {
                let mut lhs_gradient = self.lhs_gradient.borrow_mut();
                let rhs_value = self.rhs.value();

                izip!(lhs_gradient.iter_mut(), rhs_value.iter(), gradient.iter())
                    .for_each(|(dest, rhs_val, grad_val)| *dest = grad_val / rhs_val);

                let mut rhs_gradient = self.rhs_gradient.borrow_mut();

                izip!(
                    rhs_gradient.iter_mut(),
                    self.lhs.value().iter(),
                    rhs_value.iter(),
                    gradient.iter()
                ).for_each(|(dest, lhs_val, rhs_val, grad_val)| {
                    *dest = -lhs_val / rhs_val.powi(2) * grad_val
                });
            }
            BackwardAction::Increment => {
                let mut lhs_gradient = self.lhs_gradient.borrow_mut();
                let rhs_value = self.rhs.value();

                izip!(lhs_gradient.iter_mut(), rhs_value.iter(), gradient.iter())
                    .for_each(|(dest, rhs_val, grad_val)| *dest += grad_val / rhs_val);

                let mut rhs_gradient = self.rhs_gradient.borrow_mut();

                izip!(
                    rhs_gradient.iter_mut(),
                    self.lhs.value().iter(),
                    rhs_value.iter(),
                    gradient.iter()
                ).for_each(|(dest, lhs_val, rhs_val, grad_val)| {
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

    fn zero_gradient(&self) {
        self.counter.clear();
        self.lhs.zero_gradient();
        self.rhs.zero_gradient();
    }
}

#[derive(Debug)]
pub struct DotNode<LHS, RHS> {
    value: RefCell<Arr>,
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
        let value = lhs.value().dot(rhs.value().deref());

        let lhs_gradient = lhs.value().deref() * 0.0;
        let rhs_gradient = rhs.value().deref() * 0.0;

        DotNode {
            value: RefCell::new(value),
            lhs_gradient: RefCell::new(lhs_gradient),
            rhs_gradient: RefCell::new(rhs_gradient),
            lhs: lhs,
            rhs: rhs,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;

    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }

        self.lhs.forward();
        self.rhs.forward();

        general_mat_mul(
            1.0,
            self.lhs.value().deref(),
            self.rhs.value().deref(),
            0.0,
            self.value.borrow_mut().deref_mut(),
        );
    }

    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        // TODO: handle the non-overwrite case
        match self.counter.backward() {
            _ => {
                let rhs_value = self.rhs.value();
                let lhs_value = self.lhs.value();

                let mut lhs_gradient = self.lhs_gradient.borrow_mut();
                let mut rhs_gradient = self.rhs_gradient.borrow_mut();

                general_mat_mul(1.0, gradient, &rhs_value.t(), 0.0, &mut lhs_gradient);
                general_mat_mul(1.0, &lhs_value.t(), gradient, 0.0, &mut rhs_gradient);
            }
        }

        // Always recurse because we haven't actually handled accumulation yet
        self.lhs.backward(&self.lhs_gradient.borrow());
        self.rhs.backward(&self.rhs_gradient.borrow());
    }

    fn value(&self) -> Bor<Self::Value> {
        Bor::RefGuard(self.value.borrow())
    }

    fn needs_gradient(&self) -> bool {
        self.needs_gradient
    }
    fn zero_gradient(&self) {
        self.counter.clear();
        self.lhs.zero_gradient();
        self.rhs.zero_gradient();
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
                    .map(|x| x.into_slice().unwrap()),
                rhs_value
                    .genrows()
                    .into_iter()
                    .map(|x| x.into_slice().unwrap())
            ) {
                *result = numerics::simd_dot(lhs, rhs);
            }

            let lhs_gradient = lhs_value.deref() * 0.0;
            let rhs_gradient = rhs_value.deref() * 0.0;

            (value, lhs_gradient, rhs_gradient, needs_gradient)
        };

        VectorDotNode {
            value: RefCell::new(value),
            lhs_gradient: RefCell::new(lhs_gradient),
            rhs_gradient: RefCell::new(rhs_gradient),
            lhs: lhs,
            rhs: rhs,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;

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
                .map(|x| x.into_slice().unwrap()),
            rhs_value
                .genrows()
                .into_iter()
                .map(|x| x.into_slice().unwrap())
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
                        .map(|x| x.into_slice().unwrap()),
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
                        .map(|x| x.into_slice().unwrap()),
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
                        .map(|x| x.into_slice().unwrap()),
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
                        .map(|x| x.into_slice().unwrap()),
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

    fn zero_gradient(&self) {
        self.counter.clear();
        self.lhs.zero_gradient();
        self.rhs.zero_gradient();
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
            operand: operand,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }
        self.operand.forward();

        let mut dest = self.value.borrow_mut();

        dest.assign(self.operand.value().deref());
        dest.map_inplace(|x| *x = x.powi(2));
    }

    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        match self.counter.backward() {
            BackwardAction::Set => for (dest, operand_val, grad_val) in izip!(
                self.operand_gradient.borrow_mut().iter_mut(),
                self.operand.value().iter(),
                gradient.iter()
            ) {
                *dest = operand_val * 2.0 * grad_val;
            },
            BackwardAction::Increment => for (dest, operand_val, grad_val) in izip!(
                self.operand_gradient.borrow_mut().iter_mut(),
                self.operand.value().iter(),
                gradient.iter()
            ) {
                *dest += operand_val * 2.0 * grad_val;
            },
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

    fn zero_gradient(&self) {
        self.counter.clear();
        self.operand.zero_gradient();
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
        let value = operand.value().map(|x| x.ln());
        let gradient = &value * 0.0;
        let needs_gradient = operand.needs_gradient();

        LogNode {
            value: RefCell::new(value),
            operand_gradient: RefCell::new(gradient),
            operand: operand,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }

        self.operand.forward();

        let mut dest = self.value.borrow_mut();

        dest.assign(self.operand.value().deref());
        dest.map_inplace(|x| *x = x.ln());
    }

    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        match self.counter.backward() {
            BackwardAction::Set => for (dest, operand_val, grad_val) in izip!(
                self.operand_gradient.borrow_mut().iter_mut(),
                self.operand.value().iter(),
                gradient.iter()
            ) {
                *dest = grad_val / operand_val;
            },
            BackwardAction::Increment => for (dest, operand_val, grad_val) in izip!(
                self.operand_gradient.borrow_mut().iter_mut(),
                self.operand.value().iter(),
                gradient.iter()
            ) {
                *dest += grad_val / operand_val;
            },
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

    fn zero_gradient(&self) {
        self.counter.clear();
        self.operand.zero_gradient();
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
        let value = operand.value().deref().map(|x| 1.0 / (1.0 + (-x).exp()));
        let gradient = &value * 0.0;
        let needs_gradient = operand.needs_gradient();

        SigmoidNode {
            value: RefCell::new(value),
            operand_gradient: RefCell::new(gradient),
            operand: operand,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }

        self.operand.forward();

        let mut dest = self.value.borrow_mut();

        numerics::map_assign(dest.deref_mut(), self.operand.value().deref(), |x| {
            1.0 / (1.0 + (-x).exp())
        });
    }

    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        match self.counter.backward() {
            BackwardAction::Set => {
                let mut operand_gradient = self.operand_gradient.borrow_mut();

                numerics::map_assign_binary(
                    &mut operand_gradient,
                    self.value.borrow().deref(),
                    gradient,
                    |sigmoid, grad| grad * sigmoid * (1.0 - sigmoid),
                );
            }
            BackwardAction::Increment => {
                let mut operand_gradient = self.operand_gradient.borrow_mut();

                numerics::map_inplace_assign_binary(
                    &mut operand_gradient,
                    self.value.borrow().deref(),
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

    fn zero_gradient(&self) {
        self.counter.clear();
        self.operand.zero_gradient();
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
        let value = -operand.value().deref();
        let gradient = &value * 0.0;
        let needs_gradient = operand.needs_gradient();

        NegNode {
            value: RefCell::new(value),
            operand_gradient: RefCell::new(gradient),
            operand: operand,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;

    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }

        self.operand.forward();

        let mut dest = self.value.borrow_mut();

        dest.assign(self.operand.value().deref());
        dest.map_inplace(|x| *x = -*x);
    }

    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        match self.counter.backward() {
            BackwardAction::Set => for (dest, grad_val) in izip!(
                self.operand_gradient.borrow_mut().iter_mut(),
                gradient.iter()
            ) {
                *dest = -grad_val;
            },
            BackwardAction::Increment => for (dest, grad_val) in izip!(
                self.operand_gradient.borrow_mut().iter_mut(),
                gradient.iter()
            ) {
                *dest += -grad_val;
            },
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
    fn zero_gradient(&self) {
        self.counter.clear();
        self.operand.zero_gradient();
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
        let value = operand.value().deref().map(|x| x.exp());
        let gradient = &value * 0.0;
        let needs_gradient = operand.needs_gradient();

        ExpNode {
            value: RefCell::new(value),
            operand_gradient: RefCell::new(gradient),
            operand: operand,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }

        self.operand.forward();
        let mut dest = self.value.borrow_mut();

        dest.assign(self.operand.value().deref());
        dest.map_inplace(|x| *x = x.exp());
    }
    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        match self.counter.backward() {
            BackwardAction::Set => for (dest, self_val, grad_val) in izip!(
                self.operand_gradient.borrow_mut().iter_mut(),
                self.value.borrow().iter(),
                gradient.iter()
            ) {
                *dest = self_val * grad_val;
            },
            BackwardAction::Increment => for (dest, self_val, grad_val) in izip!(
                self.operand_gradient.borrow_mut().iter_mut(),
                self.value.borrow().iter(),
                gradient.iter()
            ) {
                *dest += self_val * grad_val;
            },
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

    fn zero_gradient(&self) {
        self.counter.clear();
        self.operand.zero_gradient();
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
        let value = RefCell::new(&operand.value().t() * 1.0);
        let gradient = RefCell::new(operand.value().deref() * 0.0);

        TransposeNode {
            value: value,
            gradient: gradient,
            operand: operand,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
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
                self.gradient.borrow_mut().add_assign(&gradient.t());
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

    fn zero_gradient(&self) {
        self.counter.clear();
        self.operand.zero_gradient();
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
                .deref()
                .as_slice()
                .unwrap()
                .iter()
                .fold(std::f32::MIN, |x, y| x.max(*y));
            let numerator = operand.value().map(|x| (x - max).exp());
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
            operand: operand,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
    fn forward(&self) {
        if self.counter.forward() == ForwardAction::Cached {
            return;
        }

        self.operand.forward();
        let mut dest = self.value.borrow_mut();
        dest.assign(self.operand.value().deref());

        let max = self.operand
            .value()
            .deref()
            .iter()
            .fold(std::f32::MIN, |x, y| x.max(*y));
        dest.map_inplace(|x| *x = (*x - max).exp());
        let denominator = dest.scalar_sum();
        dest.map_inplace(|x| *x /= denominator);
    }
    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        // TODO: accumulate gradients
        let value = self.value.borrow();
        let mut jacobian = self.jacobian.borrow_mut();

        for (row_idx, (mut row, row_val)) in jacobian
            .genrows_mut()
            .into_iter()
            .zip(value.iter())
            .enumerate()
        {
            for (col_idx, (grad, col_val)) in row.iter_mut().zip(value.iter()).enumerate() {
                if row_idx == col_idx {
                    *grad = row_val * (1.0 - col_val);
                } else {
                    *grad = -row_val * col_val;
                }
            }
        }

        {
            general_mat_mul(
                1.0,
                gradient,
                jacobian.deref_mut(),
                0.0,
                self.operand_gradient.borrow_mut().deref_mut(),
            );
        }

        self.operand.backward(&self.operand_gradient.borrow());
    }
    fn value(&self) -> Bor<Self::Value> {
        Bor::RefGuard(self.value.borrow())
    }
    fn needs_gradient(&self) -> bool {
        self.needs_gradient
    }
    fn zero_gradient(&self) {
        self.counter.clear();
        self.operand.zero_gradient();
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

        let gradient = operand.value().deref() * 0.0;
        let needs_gradient = operand.needs_gradient();

        SumNode {
            value: RefCell::new(value),
            operand_gradient: RefCell::new(gradient),
            operand: operand,
            needs_gradient: needs_gradient,
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
    type OutputGradient = Arr;
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
                    .add_assign(gradient[(0, 0)]);
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

    fn zero_gradient(&self) {
        self.counter.clear();
        self.operand.zero_gradient();
    }
}

/// An input node for integer indices into `ParameterNode`s, used
/// for implementing indexable embedding layers.
#[derive(Debug)]
pub struct IndexInputNode {
    pub value: RefCell<SmallVec<[usize; 4]>>,
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
    type OutputGradient = ();
    fn forward(&self) {}
    fn backward(&self, _: &Ref<Self::InputGradient>) {}
    fn value(&self) -> Bor<Self::Value> {
        Bor::RefGuard(self.value.borrow())
    }
    fn needs_gradient(&self) -> bool {
        false
    }
    fn zero_gradient(&self) {}
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
            index: index,
            operand: operand,
            needs_gradient: needs_gradient,
            counter: PassCounter::default(),
        }
    }
}

impl Node for IndexNode<ParameterNode> {
    type Value = Arr;
    type InputGradient = Arr;
    type OutputGradient = Arr;
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
            let new_val = operand_value.subview(Axis(0), idx);

            numerics::slice_assign(row.into_slice().unwrap(), new_val.as_slice().unwrap())
        }
    }

    fn backward(&self, gradient: &Ref<Self::InputGradient>) {
        self.operand
            .gradient
            .borrow_mut()
            .accumulate_gradient((&self.index_value.borrow()[..], gradient.deref()));
    }

    fn value(&self) -> Bor<Self::Value> {
        Bor::RefGuard(self.value.borrow())
    }

    fn needs_gradient(&self) -> bool {
        self.needs_gradient
    }
    fn zero_gradient(&self) {
        self.counter.clear();
        self.operand.zero_gradient();
    }
}

#[cfg(test)]
mod tests {
    use rand;

    use super::*;

    fn random_matrix(rows: usize, cols: usize) -> Arr {
        Arr::zeros((rows, cols)).map(|_| rand::random::<f32>())
    }

    #[test]
    fn test_sub_counter() {
        let x = ParameterNode::new(random_matrix(1, 1));
        let y = x.clone() - x.clone();

        let mut z = y.clone() + y.clone() + y.clone();

        z.forward();
        assert_eq!(y.node.counter.forward_count.get(), 3);
        z.backward(1.0);
        assert_eq!(y.node.counter.backward_count.get(), 3);
    }
}
