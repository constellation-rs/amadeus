#[macro_use]
extern crate criterion;

use criterion::Criterion;

fn bench_node_reuse(c: &mut Criterion) {
    let dim = 128;

    let x = ParameterNode::new(random_matrix(1, dim));
    let y = ParameterNode::new(random_matrix(dim, 10));
    let v = x.dot(&y);
    let z = v.clone() + v.clone() + v.clone() + v.clone();

    c.bench_function(
        "node_reuse",
        b.iter(|| {
            z.forward();
            z.zero_gradient();
        }),
    );
}

fn bench_matrix_multiply(b: &mut Criterion) {
    let dim = 64;
    let num_epochs = 20;

    let x_data = Arc::new(HogwildParameter::new(random_matrix(1, dim)));
    let y_data = Arc::new(HogwildParameter::new(random_matrix(dim, 10)));

    b.bench_function(
        "bench_matrix_multiply",
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
        }),
    );
}

// fn bench_sofmax_exp_sum(b: &mut Criterion) {
//     let x = vec![1.0; 32];
//     let max = 1.0;

//     b.iter(|| x.iter().map(|&x| numerics::exp(x - max)).sum::<f32>().ln())
// }

// #[bench]
// fn bench_sofmax_exp_sum_unrolled(b: &mut Criterion) {
//     let x = vec![1.0; 32];
//     let max = 1.0;

//     b.iter(|| softmax_exp_sum(&x, max).ln())
// }

// fn bench_exp(b: &mut Criterion) {
//     let x: Vec<f32> = vec![1.0; 32];

//     let mut v = 0.0;

//     b.iter(|| x.iter().for_each(|&y| v += y.exp()));
// }

// fn bench_fastexp(b: &mut Criterion) {
//     let x: Vec<f32> = vec![1.0; 32];

//     let mut v = 0.0;

//     b.iter(|| x.iter().for_each(|&y| v += fastexp(y)));
// }

// fn bench_dot(b: &mut Criterion) {
//     let xs = vec![0.0; 256];
//     let ys = vec![0.0; 256];

//     b.iter(|| dot(&xs[..], &ys[..]));
// }

// fn bench_unrolled_dot(b: &mut Criterion) {
//     let xs = vec![0.0; 256];
//     let ys = vec![0.0; 256];

//     b.iter(|| unrolled_dot(&xs[..], &ys[..]));
// }

// fn bench_simd_dot(b: &mut Criterion) {
//     let xs = vec![0.0; 256];
//     let ys = vec![0.0; 256];

//     b.iter(|| simd_dot(&xs[..], &ys[..]));
// }

// fn bench_array_scaled_assign(b: &mut Criterion) {
//     let mut xs = random_matrix(256, 1);
//     let ys = random_matrix(256, 1);

//     b.iter(|| array_scaled_assign(&mut xs, &ys, 3.5));
// }

// fn bench_slice_scaled_assign(b: &mut Criterion) {
//     let mut xs = random_matrix(256, 1);
//     let ys = random_matrix(256, 1);

//     b.iter(|| scaled_assign(&mut xs, &ys, 3.5));
// }

// fn bench_array_assign(b: &mut Criterion) {
//     let mut xs = random_matrix(256, 1);
//     let ys = random_matrix(256, 1);

//     b.iter(|| array_assign(&mut xs, &ys));
// }

// fn bench_slice_assign(b: &mut Criterion) {
//     let mut xs = random_matrix(256, 1);
//     let ys = random_matrix(256, 1);

//     b.iter(|| assign(&mut xs, &ys));
// }

// fn dot_node_specializations_mm(b: &mut Criterion) {
//     let x = random_matrix(64, 64);
//     let y = random_matrix(64, 64);
//     let mut z = random_matrix(64, 64);

//     b.iter(|| mat_mul(1.0, &x, &y, 0.0, &mut z));
// }

// fn dot_node_general_vm(b: &mut Criterion) {
//     let x = random_matrix(1, 64);
//     let y = random_matrix(64, 64);
//     let mut z = random_matrix(1, 64);

//     b.iter(|| general_mat_mul(1.0, &x, &y, 0.0, &mut z));
// }

// fn dot_node_specializations_vm(b: &mut Criterion) {
//     let x = random_matrix(1, 64);
//     let y = random_matrix(64, 64);
//     let mut z = random_matrix(1, 64);

//     b.iter(|| mat_mul(1.0, &x, &y, 0.0, &mut z));
// }

// fn dot_node_specializations_mv(b: &mut Criterion) {
//     let x = random_matrix(64, 64);
//     let y = random_matrix(64, 1);
//     let mut z = random_matrix(64, 1);

//     b.iter(|| mat_mul(1.0, &x, &y, 0.0, &mut z));
// }

// fn dot_node_general_mv(b: &mut Criterion) {
//     let x = random_matrix(64, 64);
//     let y = random_matrix(64, 1);
//     let mut z = random_matrix(64, 1);

//     b.iter(|| general_mat_mul(1.0, &x, &y, 0.0, &mut z));
// }

// fn bench_lstm(b: &mut Criterion) {
//     let sequence_length = 4;
//     let num_digits = 10;
//     let input_dim = 16;
//     let hidden_dim = 32;

//     let lstm_params = Parameters::new(input_dim, hidden_dim);
//     let lstm = lstm_params.build();

//     let final_layer = ParameterNode::new(xavier_normal(hidden_dim, num_digits));
//     let embeddings = ParameterNode::new(xavier_normal(num_digits, input_dim));
//     let y = nodes::IndexInputNode::new(&vec![0]);

//     let inputs: Vec<_> = (0..sequence_length)
//         .map(|_| nodes::IndexInputNode::new(&vec![0]))
//         .collect();
//     let embeddings: Vec<_> = inputs
//         .iter()
//         .map(|input| embeddings.index(&input))
//         .collect();

//     let hidden_states = lstm.forward(&embeddings);
//     let hidden = hidden_states.last().unwrap();

//     let prediction = hidden.dot(&final_layer);
//     let mut loss = sparse_categorical_crossentropy(&prediction, &y);
//     let mut optimizer = SGD::new(0.05, loss.parameters());

//     let digits = pi_digits(100);

//     b.iter(|| {
//         for i in 0..(digits.len() - sequence_length - 1) {
//             let digit_chunk = &digits[i..(i + sequence_length + 1)];
//             if digit_chunk.len() < sequence_length + 1 {
//                 break;
//             }

//             for (&digit, input) in digit_chunk[..digit_chunk.len() - 1].iter().zip(&inputs) {
//                 input.set_value(digit);
//             }

//             let target_digit = *digit_chunk.last().unwrap();
//             y.set_value(target_digit);

//             loss.forward();
//             loss.backward(1.0);

//             optimizer.step();
//             loss.zero_gradient();
//         }
//     });
// }

criterion_group!(benches, bench_node_reuse, bench_matrix_multiply);
criterion_main!(benches);
