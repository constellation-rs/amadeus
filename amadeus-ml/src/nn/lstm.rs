//! Module for LSTM layers.
//!
//! You can create an LSTM layer by first initializing its parameters,
//! then applying it to your inputs:
//!
//! ```
//! # use amadeus_ml::{nn::{lstm, xavier_normal}, InputNode};
//! #
//! # // Overflows with 1MiB stack (default on Windows), so we spawn a thread which uses Rust's
//! # // [2MiB](https://github.com/rust-lang/rust/blob/c989ac132a81503ceaaa1054da0a3a8c5c305ef0/library/std/src/sys/windows/thread.rs#L13)
//! # std::thread::spawn(|| {
//! let input_dim = 10;
//! let hidden_dim = 5;
//!
//! // Initialize the parameters.
//! let parameters = lstm::Parameters::new(input_dim, hidden_dim, &mut rand::thread_rng());
//! let lstm = parameters.build();
//!
//! // Construct the input nodes.
//! let input: Vec<_> = (0..200)
//!     .map(|_| InputNode::new(xavier_normal(1, input_dim))).collect();
//!
//! // Construct an LSTM with 200 steps of recursion.
//! let mut hidden = lstm.forward(&input);
//!
//! let last_hidden = hidden.last_mut().unwrap();
//!
//! // Run as usual.
//! last_hidden.forward();
//! last_hidden.backward(1.0);
//! last_hidden.zero_gradient();
//!
//! // Reset the hidden state between sequences
//! lstm.reset_state();
//! # }).join().unwrap();
//! ```

use serde::{Deserialize, Serialize};
use std::{rc::Rc, sync::Arc};

use super::uniform;
use crate::{
	nodes::{HogwildParameter, InputNode, Node, ParameterNode}, Arr, DataInput, Variable
};

/// Holds shared parameters for an LSTM cell.
///
/// Construct this first, then use the `build` method to instantiate
/// LSTM cell nodes.
#[derive(Debug, Serialize, Deserialize)]
pub struct Parameters {
	input_dim: usize,
	hidden_dim: usize,

	forget_weights: Arc<HogwildParameter>,
	forget_biases: Arc<HogwildParameter>,

	update_gate_weights: Arc<HogwildParameter>,
	update_gate_biases: Arc<HogwildParameter>,

	update_value_weights: Arc<HogwildParameter>,
	update_value_biases: Arc<HogwildParameter>,

	output_gate_weights: Arc<HogwildParameter>,
	output_gate_biases: Arc<HogwildParameter>,
}

impl Clone for Parameters {
	/// Clones the parameter values.
	///
	/// (This is in contrast to creating a shared reference to
	/// the same paratmer object.)
	fn clone(&self) -> Self {
		Parameters {
			input_dim: self.input_dim,
			hidden_dim: self.hidden_dim,

			forget_weights: Arc::new(self.forget_weights.as_ref().clone()),
			forget_biases: Arc::new(self.forget_biases.as_ref().clone()),

			update_gate_weights: Arc::new(self.update_gate_weights.as_ref().clone()),
			update_gate_biases: Arc::new(self.update_gate_biases.as_ref().clone()),

			update_value_weights: Arc::new(self.update_gate_weights.as_ref().clone()),
			update_value_biases: Arc::new(self.update_value_biases.as_ref().clone()),

			output_gate_weights: Arc::new(self.output_gate_weights.as_ref().clone()),
			output_gate_biases: Arc::new(self.output_gate_biases.as_ref().clone()),
		}
	}
}

impl Parameters {
	/// Create a new LSTM parameters object.
	pub fn new<R: rand::Rng>(input_dim: usize, hidden_dim: usize, rng: &mut R) -> Self {
		let max = 1.0 / (hidden_dim as f32).sqrt();
		let min = -max;

		Self {
			input_dim,
			hidden_dim,

			forget_weights: Arc::new(HogwildParameter::new(uniform(
				input_dim + hidden_dim,
				hidden_dim,
				min,
				max,
				rng,
			))),
			forget_biases: Arc::new(HogwildParameter::new(uniform(1, hidden_dim, min, max, rng))),

			update_gate_weights: Arc::new(HogwildParameter::new(uniform(
				input_dim + hidden_dim,
				hidden_dim,
				min,
				max,
				rng,
			))),
			update_gate_biases: Arc::new(HogwildParameter::new(uniform(
				1, hidden_dim, min, max, rng,
			))),

			update_value_weights: Arc::new(HogwildParameter::new(uniform(
				input_dim + hidden_dim,
				hidden_dim,
				min,
				max,
				rng,
			))),
			update_value_biases: Arc::new(HogwildParameter::new(uniform(
				1, hidden_dim, min, max, rng,
			))),

			output_gate_weights: Arc::new(HogwildParameter::new(uniform(
				input_dim + hidden_dim,
				hidden_dim,
				min,
				max,
				rng,
			))),
			output_gate_biases: Arc::new(HogwildParameter::new(uniform(
				1, hidden_dim, min, max, rng,
			))),
		}
	}

	fn inner_build_cell(&self, coupled: bool) -> Cell {
		Cell {
			input_dim: self.input_dim,
			hidden_dim: self.hidden_dim,
			coupled_input: coupled,

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

	/// Build an LSTM layer.
	pub fn build(&self) -> Layer {
		Layer::new(self.build_cell())
	}

	/// Build an LSTM layer where the input and forget gates
	/// are coupled.
	pub fn build_coupled(&self) -> Layer {
		Layer::new(self.build_coupled_cell())
	}

	/// Build an LSTM cell.
	pub fn build_cell(&self) -> Cell {
		self.inner_build_cell(false)
	}

	/// Build an LSTM cell where the input and forget gates
	/// are coupled.
	pub fn build_coupled_cell(&self) -> Cell {
		self.inner_build_cell(true)
	}
}

/// An LSTM cell.
#[derive(Debug)]
pub struct Cell {
	input_dim: usize,
	hidden_dim: usize,
	coupled_input: bool,

	forget_weights: Variable<ParameterNode>,
	forget_biases: Variable<ParameterNode>,

	update_gate_weights: Variable<ParameterNode>,
	update_gate_biases: Variable<ParameterNode>,

	update_value_weights: Variable<ParameterNode>,
	update_value_biases: Variable<ParameterNode>,

	output_gate_weights: Variable<ParameterNode>,
	output_gate_biases: Variable<ParameterNode>,
}

impl Cell {
	/// Run a single LSTM iteration over inputs.
	///
	/// If this is the first cell, initialize the cell state and the hidden state;
	/// otherwise pass the cell and hidden states from previous iterations.
	#[allow(clippy::needless_pass_by_value, clippy::type_complexity)]
	pub fn forward<C, H, I>(
		&self, state: (Variable<C>, Variable<H>), input: Variable<I>,
	) -> (
		Variable<Rc<dyn Node<Value = Arr, InputGradient = Arr>>>,
		Variable<Rc<dyn Node<Value = Arr, InputGradient = Arr>>>,
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
		let cell = forget_gate.clone() * cell;

		// Update the cell state with new input
		let update_gate = if self.coupled_input {
			(1.0 - forget_gate).boxed()
		} else {
			(stacked_input.dot(&self.update_gate_weights) + self.update_gate_biases.clone())
				.sigmoid()
				.boxed()
		};
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

/// An LSTM layer.
#[derive(Debug)]
pub struct Layer {
	cell: Cell,
	state: Variable<InputNode>,
	hidden: Variable<InputNode>,
}

impl Layer {
	fn new(cell: Cell) -> Self {
		let hidden_dim = cell.hidden_dim;

		Layer {
			cell,
			state: InputNode::new(Arr::zeros((1, hidden_dim))),
			hidden: InputNode::new(Arr::zeros((1, hidden_dim))),
		}
	}
	/// Construct an LSTM layer over given inputs, returning the emitted
	/// hidden states.
	///
	/// The state of the layer is initialized with zero vectors. Use
	/// `Cell` for custom initialization.
	pub fn forward<T>(
		&self, inputs: &[Variable<T>],
	) -> Vec<Variable<Rc<dyn Node<Value = Arr, InputGradient = Arr>>>>
	where
		T: Node<Value = Arr, InputGradient = Arr>,
	{
		let mut state = (self.state.clone().boxed(), self.hidden.clone().boxed());

		let outputs: Vec<_> = inputs
			.iter()
			.map(|input| {
				state = self.cell.forward(state.clone(), input.clone());
				state.1.clone()
			})
			.collect();

		outputs
	}
	/// Reset the internal state of the layer.
	pub fn reset_state(&self) {
		self.state.set_value(0.0);
		self.hidden.set_value(0.0);
	}
}

#[cfg(test)]
mod tests {
	use approx::AbsDiffEq;

	use super::*;
	use crate::{
		finite_difference, nn::{losses::sparse_categorical_crossentropy, xavier_normal}, nodes::{IndexInputNode, InputNode}, optim::{Adam, Optimizer}
	};

	const TOLERANCE: f32 = 0.2;

	fn assert_close(x: &Arr, y: &Arr, tol: f32) {
		assert!(
			x.abs_diff_eq(y, tol),
			"{:#?} not within {} of {:#?}",
			x,
			tol,
			y
		);
	}

	fn pi_digits(num: usize) -> Vec<usize> {
		let pi_str = include_str!("pi.txt");
		pi_str
			.chars()
			.map(|x| x.to_digit(10).unwrap() as usize)
			.take(num)
			.collect()
	}

	#[test]
	fn lstm_finite_difference() {
		let num_steps = 10;
		let dim = 10;

		let mut xs: Vec<_> = (0..num_steps)
			.map(|_| ParameterNode::new(xavier_normal(1, dim)))
			.collect();

		let lstm_params = Parameters::new(dim, dim, &mut rand::thread_rng());
		let lstm = lstm_params.build();

		let mut hidden_states = lstm.forward(&xs);
		let mut hidden = hidden_states.last_mut().unwrap();

		for x in &mut xs {
			let (difference, gradient) = finite_difference(x, &mut hidden);
			assert_close(&difference, &gradient, TOLERANCE);
		}

		let mut params = hidden.parameters().to_owned();

		for x in &mut params {
			let (difference, gradient) = finite_difference(x, hidden);
			assert_close(&difference, &gradient, TOLERANCE);
		}
	}

	#[test]
	fn test_basic_lstm() {
		let input_dim = 10;
		let hidden_dim = 5;

		// Initialize the parameters.
		let lstm_params = Parameters::new(input_dim, hidden_dim, &mut rand::thread_rng());
		let lstm = lstm_params.build_cell();

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

		let lstm_params = Parameters::new(input_dim, hidden_dim, &mut rand::thread_rng());
		let lstm = lstm_params.build();

		let final_layer = ParameterNode::new(xavier_normal(hidden_dim, num_digits));
		let embeddings = ParameterNode::new(xavier_normal(num_digits, input_dim));
		let y = IndexInputNode::new(&[0]);

		let inputs: Vec<_> = (0..sequence_length)
			.map(|_| IndexInputNode::new(&[0]))
			.collect();
		let embeddings: Vec<_> = inputs
			.iter()
			.map(|input| embeddings.index(&input))
			.collect();

		let hidden_states = lstm.forward(&embeddings);
		let hidden = hidden_states.last().unwrap();

		let prediction = hidden.dot(&final_layer);
		let mut loss = sparse_categorical_crossentropy(&prediction, &y);
		let optimizer = Adam::new().learning_rate(0.01);

		let digits = pi_digits(100);

		let mut correct = 0;
		let mut total = 0;

		for _ in 0..num_epochs {
			let mut loss_val = 0.0;

			correct = 0;
			total = 0;

			for digit_chunk in digits.windows(sequence_length + 1) {
				for (&digit, input) in digit_chunk[..digit_chunk.len() - 1].iter().zip(&inputs) {
					input.set_value(digit);
				}

				let target_digit = *digit_chunk.last().unwrap();
				y.set_value(target_digit);

				loss.forward();
				loss.backward(1.0);

				loss_val += loss.value().scalar_sum();

				optimizer.step(loss.parameters());
				loss.zero_gradient();

				if target_digit == predicted_label(&*prediction.value()) {
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
