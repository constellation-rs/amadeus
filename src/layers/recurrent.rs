use std::sync::Arc;

use rand;
use ndarray;

use nodes;
use nodes::{HogwildParameter, InputNode, Node, ParameterNode};

use {Arr, Variable};

fn random_matrix(rows: usize, cols: usize) -> Arr {
    Arr::zeros((rows, cols)).map(|_| rand::random::<f32>())
}

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
    pub fn new(input_dim: usize, hidden_dim: usize) -> Self {
        Self {
            input_dim: input_dim,
            hidden_dim: hidden_dim,

            forget_weights: Arc::new(HogwildParameter::new(random_matrix(
                input_dim + hidden_dim,
                hidden_dim,
            ))),
            forget_biases: Arc::new(HogwildParameter::new(random_matrix(1, hidden_dim))),

            update_gate_weights: Arc::new(HogwildParameter::new(random_matrix(
                input_dim + hidden_dim,
                hidden_dim,
            ))),
            update_gate_biases: Arc::new(HogwildParameter::new(random_matrix(1, hidden_dim))),

            update_value_weights: Arc::new(HogwildParameter::new(random_matrix(
                input_dim + hidden_dim,
                hidden_dim,
            ))),
            update_value_biases: Arc::new(HogwildParameter::new(random_matrix(1, hidden_dim))),

            output_gate_weights: Arc::new(HogwildParameter::new(random_matrix(
                input_dim + hidden_dim,
                hidden_dim,
            ))),
            output_gate_biases: Arc::new(HogwildParameter::new(random_matrix(1, hidden_dim))),
        }
    }

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
    pub fn forward<C, H, I>(
        &self,
        cell: Variable<C>,
        hidden: Variable<H>,
        input: Variable<I>,
    ) -> (
        Variable<impl Node<Value = Arr, InputGradient = Arr>>,
        Variable<impl Node<Value = Arr, InputGradient = Arr>>,
    )
    where
        C: Node<Value = Arr, InputGradient = Arr>,
        H: Node<Value = Arr, InputGradient = Arr>,
        I: Node<Value = Arr, InputGradient = Arr>,
    {
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

        (cell, hidden)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_basic_lstm() {
        let input_dim = 10;
        let hidden_dim = 5;

        let lstm_params = LSTMParameters::new(input_dim, hidden_dim);
        let lstm = lstm_params.build();

        let state = InputNode::new(Arr::zeros((1, hidden_dim)));
        let hidden = InputNode::new(Arr::zeros((1, hidden_dim)));
        let input = InputNode::new(random_matrix(1, input_dim));

        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), input.clone());
        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), input.clone());
        let (_, hidden) = lstm.forward(state.clone(), hidden.clone(), input.clone());

        hidden.zero_gradient();
        hidden.forward();
    }

}
