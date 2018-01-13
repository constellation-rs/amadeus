//! Module holding building blocks for recurrent neural networks.
//!
//! You can create an LSTM layer by repeatedly applying an LSTM cell
//! to your inputs:
//!
//! ```rust
//! # extern crate rand;
//! # extern crate wyrm;
//! # extern crate ndarray;
//! # use std::sync::Arc;
//! # use std::rc::Rc;
//! #
//! # use wyrm::{HogwildParameter, InputNode, Node, ParameterNode};
//! #
//! # use wyrm::nn::xavier_normal;
//! # use wyrm::nn::recurrent::*;
//! #
//! # use wyrm::{Arr, Variable};
//! # fn main() {
//! let input_dim = 10;
//! let hidden_dim = 5;
//!
//! // Initialize the parameters.
//! let lstm_params = LSTMParameters::new(input_dim, hidden_dim);
//! let lstm = lstm_params.build();
//!
//! // Initialize the cell state and hidden state.
//! let state = InputNode::new(Arr::zeros((1, hidden_dim)));
//! let hidden = InputNode::new(Arr::zeros((1, hidden_dim)));
//!
//! // Construct the input node.
//! let input = InputNode::new(xavier_normal(1, input_dim));
//!
//! // The forward method outputs a tuple of (cell_state, hidden_state).
//! let mut state = lstm.forward((state, hidden), input.clone());
//!
//! // Construct an LSTM with 200 steps of recursion. Usually
//! // we'd use different inputs for every step, but here we re-use
//! // the same input for simplicity.
//! for _ in 0..200 {
//!     state = lstm.forward(state.clone(), input.clone());
//! }
//!
//! // Unpack the cell and hidden state.
//! let (_, mut hidden) = state;
//!
//! // Run as usual.
//! hidden.forward();
//! hidden.backward(1.0);
//! hidden.zero_gradient();
//! # }
//! ```
use std::sync::Arc;
use std::rc::Rc;

use ndarray;

use nodes;
use nodes::{HogwildParameter, Node, ParameterNode};

use nn::xavier_normal;

use {Arr, Variable};

/// Holds shared parameters for an LSTM cell.
///
/// Construct this first, then use the `build` method to instantiate
/// LSTM cell nodes.
pub struct LSTMParameters {
    input_dim: usize,
    hidden_dim: usize,

    forget_weights: Arc<nodes::HogwildParameter>,
    forget_biases: Arc<nodes::HogwildParameter>,

    update_gate_weights: Arc<nodes::HogwildParameter>,
    update_gate_biases: Arc<nodes::HogwildParameter>,

    update_value_weights: Arc<nodes::HogwildParameter>,
    update_value_biases: Arc<nodes::HogwildParameter>,

    output_gate_weights: Arc<nodes::HogwildParameter>,
    output_gate_biases: Arc<nodes::HogwildParameter>,
}

impl LSTMParameters {
    /// Create a new LSTM parameters object.
    pub fn new(input_dim: usize, hidden_dim: usize) -> Self {
        Self {
            input_dim: input_dim,
            hidden_dim: hidden_dim,

            forget_weights: Arc::new(HogwildParameter::new(xavier_normal(
                input_dim + hidden_dim,
                hidden_dim,
            ))),
            forget_biases: Arc::new(HogwildParameter::new(Arr::zeros((1, hidden_dim)))),

            update_gate_weights: Arc::new(HogwildParameter::new(xavier_normal(
                input_dim + hidden_dim,
                hidden_dim,
            ))),
            update_gate_biases: Arc::new(HogwildParameter::new(Arr::zeros((1, hidden_dim)))),

            update_value_weights: Arc::new(HogwildParameter::new(xavier_normal(
                input_dim + hidden_dim,
                hidden_dim,
            ))),
            update_value_biases: Arc::new(HogwildParameter::new(Arr::zeros((1, hidden_dim)))),

            output_gate_weights: Arc::new(HogwildParameter::new(xavier_normal(
                input_dim + hidden_dim,
                hidden_dim,
            ))),
            output_gate_biases: Arc::new(HogwildParameter::new(Arr::zeros((1, hidden_dim)))),
        }
    }

    /// Build an LSTM cell.
    pub fn build(&self) -> LSTMCell {
        LSTMCell {
            _input_dim: self.input_dim,
            _hidden_dim: self.hidden_dim,

            forget_weights: ParameterNode::shared(self.forget_weights.clone()),
            forget_biases: ParameterNode::shared(self.forget_biases.clone()),

            update_gate_weights: ParameterNode::shared(self.update_gate_weights.clone()),
            update_gate_biases: ParameterNode::shared(self.update_gate_biases.clone()),

            update_value_weights: ParameterNode::shared(self.update_value_weights.clone()),
            update_value_biases: ParameterNode::shared(self.update_value_biases.clone()),

            output_gate_weights: ParameterNode::shared(self.output_gate_weights.clone()),
            output_gate_biases: ParameterNode::shared(self.output_gate_biases.clone()),
        }
    }
}

/// An LSTM cell.
pub struct LSTMCell {
    _input_dim: usize,
    _hidden_dim: usize,

    forget_weights: Variable<ParameterNode>,
    forget_biases: Variable<ParameterNode>,

    update_gate_weights: Variable<ParameterNode>,
    update_gate_biases: Variable<ParameterNode>,

    update_value_weights: Variable<ParameterNode>,
    update_value_biases: Variable<ParameterNode>,

    output_gate_weights: Variable<ParameterNode>,
    output_gate_biases: Variable<ParameterNode>,
}

impl LSTMCell {
    /// Run a single LSTM iteration over inputs.
    ///
    /// If this is the first cell, initialize the cell state and the hidden state;
    /// otherwise pass the cell and hidden states from previous iterations.
    pub fn forward<C, H, I>(
        &self,
        state: (Variable<C>, Variable<H>),
        input: Variable<I>,
    ) -> (
        Variable<Rc<Node<Value = Arr, InputGradient = Arr>>>,
        Variable<Rc<Node<Value = Arr, InputGradient = Arr>>>,
    )
    where
        C: Node<Value = Arr, InputGradient = Arr>,
        H: Node<Value = Arr, InputGradient = Arr>,
        I: Node<Value = Arr, InputGradient = Arr>,
    {
        let (cell, hidden) = state;

        let stacked_input = hidden.stack(&input, ndarray::Axis(1));

        // Forget part of the cell state
        let forget_gate =
            (stacked_input.dot(&self.forget_weights) + self.forget_biases.clone()).sigmoid();
        let cell = forget_gate * cell;

        // Update the cell state with new input
        let update_gate = (stacked_input.dot(&self.update_gate_weights)
            + self.update_gate_biases.clone())
            .sigmoid();
        let update_value = (stacked_input.dot(&self.update_value_weights)
            + self.update_value_biases.clone())
            .tanh();
        let update = update_gate * update_value;
        let cell = cell + update;

        // Emit a hidden state
        let output_value = cell.tanh();
        let output_gate = (stacked_input.dot(&self.output_gate_weights)
            + self.output_gate_biases.clone())
            .sigmoid();
        let hidden = output_gate * output_value;

        (cell.boxed(), hidden.boxed())
    }
}

#[cfg(test)]
mod tests {

    use std::ops::Deref;

    use super::*;
    use DataInput;
    use SGD;
    use nodes::InputNode;

    fn pi_digits(num: usize) -> Vec<usize> {
        let pi_str = include_str!("pi.txt");
        pi_str
            .chars()
            .filter_map(|x| x.to_digit(10))
            .map(|x| x as usize)
            .take(num)
            .collect()
    }

    #[test]
    fn test_basic_lstm() {
        let input_dim = 10;
        let hidden_dim = 5;

        // Initialize the parameters.
        let lstm_params = LSTMParameters::new(input_dim, hidden_dim);
        let lstm = lstm_params.build();

        // Initialize the cell state and hidden state.
        let state = InputNode::new(Arr::zeros((1, hidden_dim)));
        let hidden = InputNode::new(Arr::zeros((1, hidden_dim)));

        // Construct the input node.
        let input = InputNode::new(xavier_normal(1, input_dim));

        // The forward method outputs a tuple of (cell_state, hidden_state).
        let mut state = lstm.forward((state, hidden), input.clone());

        // Construct a deep RNN.
        for _ in 0..200 {
            state = lstm.forward(state.clone(), input.clone());
        }

        // Unpack the cell and hidden state.
        let (_, mut hidden) = state;

        // Run as usual.
        hidden.forward();
        hidden.backward(1.0);
        hidden.zero_gradient();
    }

    fn predicted_label(softmax_output: &Arr) -> usize {
        softmax_output
            .iter()
            .enumerate()
            .max_by(|&(_, x), &(_, y)| x.partial_cmp(y).unwrap())
            .unwrap()
            .0
    }

    #[test]
    fn test_pi_digits() {
        let num_epochs = 50;

        let sequence_length = 4;
        let num_digits = 10;
        let input_dim = 16;
        let hidden_dim = 32;

        let lstm_params = LSTMParameters::new(input_dim, hidden_dim);
        let lstm = lstm_params.build();

        let final_layer = ParameterNode::new(xavier_normal(hidden_dim, num_digits));
        let embeddings = ParameterNode::new(xavier_normal(num_digits, input_dim));

        let mut labels = Arr::zeros((1, num_digits));
        let y = nodes::InputNode::new(labels.clone());

        let state = InputNode::new(Arr::zeros((1, hidden_dim)));
        let hidden = InputNode::new(Arr::zeros((1, hidden_dim)));

        let inputs: Vec<_> = (0..sequence_length)
            .map(|_| nodes::IndexInputNode::new(&vec![0]))
            .collect();
        let embeddings: Vec<_> = inputs
            .iter()
            .map(|input| embeddings.index(&input))
            .collect();

        let mut state = lstm.forward((state, hidden), embeddings[0].clone());

        for i in 1..sequence_length {
            state = lstm.forward(state.clone(), embeddings[i].clone());
        }

        let (_, hidden) = state;

        let prediction = hidden.dot(&final_layer).softmax();
        let mut loss = (-(y.clone() * prediction.ln())).scalar_sum();

        let mut optimizer = SGD::new(0.05, loss.parameters());

        let digits = pi_digits(100);

        let mut correct = 0;
        let mut total = 0;

        for _ in 0..num_epochs {
            let mut loss_val = 0.0;

            correct = 0;
            total = 0;

            for i in 0..(digits.len() - sequence_length - 1) {
                let digit_chunk = &digits[i..(i + sequence_length + 1)];
                if digit_chunk.len() < sequence_length + 1 {
                    break;
                }

                for (&digit, input) in digit_chunk[..digit_chunk.len() - 1].iter().zip(&inputs) {
                    input.set_value(digit);
                }

                let target_digit = *digit_chunk.last().unwrap();
                labels *= 0.0;
                labels[(0, target_digit)] = 1.0;

                y.set_value(&labels);

                loss.forward();
                loss.backward(1.0);

                loss_val += loss.value().scalar_sum();

                optimizer.step();
                loss.zero_gradient();

                if target_digit == predicted_label(prediction.value().deref()) {
                    correct += 1;
                }

                total += 1;
            }

            println!(
                "Loss {}, accuracy {}",
                loss_val,
                correct as f32 / total as f32
            );
        }

        assert!((correct as f32 / total as f32) > 0.75);
    }
}
