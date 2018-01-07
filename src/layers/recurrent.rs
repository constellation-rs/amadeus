use std::sync::Arc;
use std::rc::Rc;
use std::cell::Ref;
use std::ops::Deref;

use rand;
use ndarray;

use nodes;
use nodes::{HogwildParameter, Node, ParameterNode};

use layers::xavier_normal;

use {Arr, Bor, Variable};

pub fn random_matrix(rows: usize, cols: usize) -> Arr {
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
        Variable<Rc<Node<Value = Arr, InputGradient = Arr>>>,
        Variable<Rc<Node<Value = Arr, InputGradient = Arr>>>,
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

        (cell.boxed(), hidden.boxed())
    }
}

// fn lstm_cell_accumulate<C, H, I>(lstm: &LSTMCell, cell: Variable<C>, hidden: Variable<H>, inputs: &[Variable<I>]) ->
//     Variable<impl Node<Value = Arr, InputGradient = Arr>>
//     where
//         C: Node<Value = Arr, InputGradient = Arr>,
//         H: Node<Value = Arr, InputGradient = Arr>,
//         I: Node<Value = Arr, InputGradient = Arr>,
// {
//     let (head, tail) = inputs.split_at(1);
//     let (cell, hidden) = lstm.forward(cell.clone(), hidden.clone(), head.first().unwrap().clone());

//     if tail.len() == 0 {
//         hidden
//     } else {
//         lstm_cell_accumulate(lstm, cell, hidden, tail)
//     }
// }

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
    fn test_pi_digits() {
        let num_epochs = 50;

        let sequence_length = 8;
        let num_digits = 10;
        let input_dim = 16;
        let hidden_dim = 32;

        let lstm_params = LSTMParameters::new(input_dim, hidden_dim);
        let lstm = lstm_params.build();

        let final_layer = ParameterNode::new(random_matrix(hidden_dim, num_digits));
        let embeddings = ParameterNode::new(random_matrix(num_digits, input_dim));

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

        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), embeddings[0].clone());
        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), embeddings[1].clone());
        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), embeddings[2].clone());
        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), embeddings[3].clone());
        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), embeddings[4].clone());
        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), embeddings[5].clone());
        let (state, hidden) = lstm.forward(state.clone(), hidden.clone(), embeddings[6].clone());
        let (_, hidden) = lstm.forward(state.clone(), hidden.clone(), embeddings[7].clone());

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
