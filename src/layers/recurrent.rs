use std::sync::Arc;
use std::ops::Deref;

use rand;
use rand::distributions::{IndependentSample, Normal};
use ndarray;

use nodes;
use nodes::{HogwildParameter, InputNode, Node, ParameterNode};

use {Arr, Variable};

fn random_matrix(rows: usize, cols: usize) -> Arr {
    Arr::zeros((rows, cols)).map(|_| rand::random::<f32>())
}

fn xavier_normal(rows: usize, cols: usize) -> Arr {
    let normal = Normal::new(0.0, 1.0 / (rows as f64).sqrt());
    Arr::zeros((rows, cols)).map(|_| normal.ind_sample(&mut rand::thread_rng()) as f32)
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
    use DataInput;
    use SGD;

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

    fn predicted_label(softmax_output: &Arr) -> usize {
        softmax_output
            .iter()
            .enumerate()
            .max_by(|&(_, x), &(_, y)| x.partial_cmp(y).unwrap())
            .unwrap()
            .0
    }

    #[test]
    fn test_sequential_numbers() {
        let max_number = 100;
        let num_epochs = 1000;

        let input_dim = 16;
        let hidden_dim = 32;

        let lstm_params = LSTMParameters::new(input_dim, hidden_dim);
        let lstm = lstm_params.build();

        let final_layer = ParameterNode::new(random_matrix(hidden_dim, max_number));
        let embeddings = ParameterNode::new(random_matrix(max_number, input_dim));
        let index = nodes::IndexInputNode::new(&vec![0]);
        let mut labels = Arr::zeros((1, max_number));
        let y = nodes::InputNode::new(labels.clone());

        let state = InputNode::new(Arr::zeros((1, hidden_dim)));
        let hidden = InputNode::new(Arr::zeros((1, hidden_dim)));
        let input = embeddings.index(&index);

        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), input.clone());
        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), input.clone());
        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), input.clone());
        //let (_, hidden) = lstm.forward(state.clone(), hidden.clone(), input.clone());

        let prediction = hidden.dot(&final_layer).softmax();
        let mut loss = (-(y.clone() * prediction.ln())).scalar_sum();

        let mut optimizer = SGD::new(0.5, loss.parameters());

        for _ in 0..num_epochs {
            let mut loss_val = 0.0;
            let mut correct = 0;
            let mut total = 0;

            for number in 0..max_number {
                index.set_value(number);

                labels *= 0.0;
                labels[(0, number)] = 1.0;

                y.set_value(&labels);

                loss.forward();
                loss.backward(1.0);

                loss_val += loss.value().scalar_sum();

                optimizer.step();
                loss.zero_gradient();

                if number == predicted_label(prediction.value().deref()) {
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
    }

}
