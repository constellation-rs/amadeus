use std;

use ndarray::linalg::{general_mat_mul, general_mat_vec_mul};
use ndarray::{ArrayBase, ArrayViewMut, Axis, Data, DataMut, Ix1, Ix2};

use fast_approx::{fastexp, fastlog, tanhf_fast};

use super::Arr;

pub trait ArraySlice {
    fn fast_slice(&self) -> &[f32];
}

pub trait ArraySliceMut {
    fn fast_slice_mut(&mut self) -> &mut [f32];
}

macro_rules! fast_slice {
    ($x:ty) => {
        impl<T> ArraySlice for $x
        where
            T: Data<Elem = f32>,
        {
            fn fast_slice(&self) -> &[f32] {
                if cfg!(debug_assertions) {
                    self.as_slice().unwrap()
                } else {
                    self.as_slice_memory_order().unwrap()
                }
            }
        }
        impl<T> ArraySliceMut for $x
        where
            T: DataMut<Elem = f32>,
        {
            fn fast_slice_mut(&mut self) -> &mut [f32] {
                if cfg!(debug_assertions) {
                    self.as_slice_mut().unwrap()
                } else {
                    self.as_slice_memory_order_mut().unwrap()
                }
            }
        }
    };
}

fast_slice!(ArrayBase<T, Ix1>);
fast_slice!(ArrayBase<T, Ix2>);

pub trait ArraySliceOps<RHS> {
    fn slice_assign(&mut self, RHS);
    fn slice_add_assign(&mut self, RHS);
    fn slice_sub_assign(&mut self, RHS);
}

macro_rules! slice_op {
    ($lhs:ty, $($rhs:ty),*) => {
        $(
        impl<'a, 'b, T> ArraySliceOps<&'a $rhs> for $lhs
        where
            T: Data<Elem = f32>,
        {
            fn slice_assign(&mut self, other: &$rhs) {
                let lhs_slice = self.fast_slice_mut();
                let rhs_slice = other.fast_slice();

                lhs_slice.copy_from_slice(rhs_slice);
            }
            fn slice_add_assign(&mut self, other: &$rhs) {
                let lhs_slice = self.fast_slice_mut();
                let rhs_slice = other.fast_slice();

                for (lhs, &rhs) in lhs_slice.iter_mut().zip(rhs_slice.iter()) {
                    *lhs += rhs;
                }
            }
            fn slice_sub_assign(&mut self, other: &$rhs) {
                let lhs_slice = self.fast_slice_mut();
                let rhs_slice = other.fast_slice();

                for (lhs, &rhs) in lhs_slice.iter_mut().zip(rhs_slice.iter()) {
                    *lhs -= rhs;
                }
            }
        }
        )*
    }
}

slice_op!(Arr, ArrayBase<T, Ix2>);
slice_op!(ArrayViewMut<'b, f32, Ix1>, ArrayBase<T, Ix1>);

impl ArraySliceOps<f32> for Arr {
    fn slice_assign(&mut self, rhs: f32) {
        for lhs in self.fast_slice_mut().iter_mut() {
            *lhs = rhs;
        }
    }
    fn slice_add_assign(&mut self, rhs: f32) {
        for lhs in self.fast_slice_mut().iter_mut() {
            *lhs += rhs;
        }
    }
    fn slice_sub_assign(&mut self, rhs: f32) {
        for lhs in self.fast_slice_mut().iter_mut() {
            *lhs -= rhs;
        }
    }
}

/// Uses approximate e^x when the fast-math feature is enabled.
#[inline(always)]
pub fn exp(x: f32) -> f32 {
    if cfg!(feature = "fast-math") {
        fastexp(x)
    } else {
        x.exp()
    }
}

/// Uses approximate ln(x) when the fast-math feature is enabled.
#[inline(always)]
pub fn ln(x: f32) -> f32 {
    if cfg!(feature = "fast-math") {
        fastlog(x)
    } else {
        x.ln()
    }
}

/// Uses approximate ln(x) when the fast-math feature is enabled.
#[inline(always)]
pub fn tanh(x: f32) -> f32 {
    if cfg!(feature = "fast-math") {
        tanhf_fast(x)
    } else {
        x.tanh()
    }
}

#[inline(always)]
pub fn sigmoid(x: f32) -> f32 {
    let critical_value = 10.0;

    if x > critical_value {
        1.0
    } else if x < -critical_value {
        0.0
    } else {
        1.0 / (1.0 + exp(-x))
    }
}

#[inline(always)]
pub fn pow2(x: f32) -> f32 {
    x.powi(2)
}

pub fn softmax_exp_sum(xs: &[f32], max: f32) -> f32 {
    let mut xs = xs;
    let mut s = 0.;

    let (mut p0, mut p1, mut p2, mut p3, mut p4, mut p5, mut p6, mut p7) =
        (0., 0., 0., 0., 0., 0., 0., 0.);

    while xs.len() >= 8 {
        p0 += exp(xs[0] - max);
        p1 += exp(xs[1] - max);
        p2 += exp(xs[2] - max);
        p3 += exp(xs[3] - max);
        p4 += exp(xs[4] - max);
        p5 += exp(xs[5] - max);
        p6 += exp(xs[6] - max);
        p7 += exp(xs[7] - max);

        xs = &xs[8..];
    }
    s += p0 + p4;
    s += p1 + p5;
    s += p2 + p6;
    s += p3 + p7;

    for i in 0..xs.len() {
        s += exp(xs[i] - max)
    }

    s
}

pub fn mat_mul<S1, S2, S3>(
    alpha: f32,
    lhs: &ArrayBase<S1, Ix2>,
    rhs: &ArrayBase<S2, Ix2>,
    beta: f32,
    out: &mut ArrayBase<S3, Ix2>,
) where
    S1: Data<Elem = f32>,
    S2: Data<Elem = f32>,
    S3: DataMut<Elem = f32>,
{
    match (lhs.rows(), rhs.cols()) {
        (_, 1) => {
            general_mat_vec_mul(
                alpha,
                lhs,
                &rhs.subview(Axis(1), 0),
                beta,
                &mut out.subview_mut(Axis(1), 0),
            );
        }
        (1, _) => {
            general_mat_vec_mul(
                alpha,
                &rhs.t(),
                &lhs.subview(Axis(0), 0),
                beta,
                &mut out.subview_mut(Axis(0), 0),
            );
        }
        _ => {
            general_mat_mul(alpha, lhs, rhs, beta, out);
        }
    }
}

/// SIMD-enabled vector-vector dot product.
pub fn simd_dot(xs: &[f32], ys: &[f32]) -> f32 {
    let len = std::cmp::min(xs.len(), ys.len());
    let mut xs = &xs[..len];
    let mut ys = &ys[..len];

    let mut s = 0.;
    let (mut p0, mut p1, mut p2, mut p3, mut p4, mut p5, mut p6, mut p7) =
        (0., 0., 0., 0., 0., 0., 0., 0.);

    while xs.len() >= 8 {
        p0 += xs[0] * ys[0];
        p1 += xs[1] * ys[1];
        p2 += xs[2] * ys[2];
        p3 += xs[3] * ys[3];
        p4 += xs[4] * ys[4];
        p5 += xs[5] * ys[5];
        p6 += xs[6] * ys[6];
        p7 += xs[7] * ys[7];

        xs = &xs[8..];
        ys = &ys[8..];
    }
    s += p0 + p4;
    s += p1 + p5;
    s += p2 + p6;
    s += p3 + p7;

    for i in 0..xs.len() {
        s += xs[i] * ys[i];
    }

    s
}

pub fn simd_sum(xs: &[f32]) -> f32 {
    let mut xs = xs;

    let mut s = 0.;
    let (mut p0, mut p1, mut p2, mut p3, mut p4, mut p5, mut p6, mut p7) =
        (0., 0., 0., 0., 0., 0., 0., 0.);

    while xs.len() >= 8 {
        p0 += xs[0];
        p1 += xs[1];
        p2 += xs[2];
        p3 += xs[3];
        p4 += xs[4];
        p5 += xs[5];
        p6 += xs[6];
        p7 += xs[7];

        xs = &xs[8..];
    }

    s += p0 + p4;
    s += p1 + p5;
    s += p2 + p6;
    s += p3 + p7;

    for i in 0..xs.len() {
        s += xs[i];
    }

    s
}

pub fn simd_scaled_assign(xs: &mut [f32], ys: &[f32], alpha: f32) {
    for (x, y) in xs.iter_mut().zip(ys.iter()) {
        *x = y * alpha;
    }
}

pub fn simd_scaled_add(xs: &mut [f32], ys: &[f32], alpha: f32) {
    for (x, y) in xs.iter_mut().zip(ys.iter()) {
        *x += y * alpha;
    }
}

macro_rules! slice_binary_op {
    ( $name:ident, $slice_name:ident,
      $increment_name:ident,$slice_increment_name:ident, $op:tt ) => {
        pub fn $name(xs: &Arr, ys: &Arr, out: &mut Arr) {
            $slice_name(xs.fast_slice(),
                       ys.fast_slice(),
                       out.fast_slice_mut());
        }

        fn $slice_name(xs: &[f32], ys: &[f32], outs: &mut [f32]) {
            for (&x_scalar, &y_scalar, out_scalar) in
                izip!(xs.iter(), ys.iter(), outs.iter_mut())
            {
                *out_scalar = x_scalar $op y_scalar;
            }
        }

        #[allow(dead_code)]
        pub fn $increment_name(xs: &Arr, ys: &Arr, out: &mut Arr) {
            $slice_increment_name(xs.fast_slice(),
                                 ys.fast_slice(),
                                 out.fast_slice_mut());
        }

        #[allow(dead_code)]
        fn $slice_increment_name(xs: &[f32], ys: &[f32], outs: &mut [f32]) {
            for (&x_scalar, &y_scalar, out_scalar) in
                izip!(xs.iter(), ys.iter(), outs.iter_mut())
            {
                *out_scalar += x_scalar $op y_scalar;
            }
        }
    }
}

slice_binary_op!(sub, slice_sub, increment_sub, increment_slice_sub, -);
slice_binary_op!(mul, slice_mul, increment_mul, increment_slice_mul, *);
slice_binary_op!(div, slice_div, increment_div, increment_slice_div, /);

pub fn slice_assign(xs: &mut [f32], ys: &[f32]) {
    for (x, &y) in xs.iter_mut().zip(ys.iter()) {
        *x = y;
    }
}

pub fn map_assign<F>(xs: &mut Arr, ys: &Arr, func: F)
where
    F: Fn(f32) -> f32,
{
    let xs = xs.fast_slice_mut();
    let ys = ys.fast_slice();

    for (x, &y) in xs.iter_mut().zip(ys.iter()) {
        *x = func(y);
    }
}

pub fn map_add_assign_slice<F>(xs: &mut [f32], ys: &[f32], func: F)
where
    F: Fn(f32) -> f32,
{
    for (x, &y) in xs.iter_mut().zip(ys.iter()) {
        *x += func(y);
    }
}

pub fn map_assign_binary<F>(xs: &mut Arr, ys: &Arr, zs: &Arr, func: F)
where
    F: Fn(f32, f32) -> f32,
{
    let xs = xs.fast_slice_mut();
    let ys = ys.fast_slice();
    let zs = zs.fast_slice();

    for (x, &y, &z) in izip!(xs.iter_mut(), ys.iter(), zs.iter()) {
        *x = func(y, z);
    }
}

#[allow(dead_code)]
pub fn map_inplace_assign<F>(xs: &mut Arr, ys: &Arr, func: F)
where
    F: Fn(&mut f32, f32),
{
    let xs = xs.fast_slice_mut();
    let ys = ys.fast_slice();

    for (x, &y) in izip!(xs.iter_mut(), ys.iter()) {
        func(x, y);
    }
}

#[allow(dead_code)]
pub fn map_inplace_assign_binary<F>(xs: &mut Arr, ys: &Arr, zs: &Arr, func: F)
where
    F: Fn(&mut f32, f32, f32),
{
    let xs = xs.fast_slice_mut();
    let ys = ys.fast_slice();
    let zs = zs.fast_slice();

    for (x, &y, &z) in izip!(xs.iter_mut(), ys.iter(), zs.iter()) {
        func(x, y, z);
    }
}

#[cfg(test)]
mod tests {

    use std;

    use super::*;

    use rand;
    use rand::Rng;

    use nn;

    fn random_matrix(rows: usize, cols: usize) -> Arr {
        nn::xavier_normal(rows, cols)
    }

    fn array_scaled_assign(xs: &mut Arr, ys: &Arr, alpha: f32) {
        for (x, y) in xs.iter_mut().zip(ys.iter()) {
            *x = y * alpha;
        }
    }

    fn scaled_assign(xs: &mut Arr, ys: &Arr, alpha: f32) {
        // assert_eq!(xs.shape(), ys.shape(), "Operands do not have the same shape.");

        let xs = xs.as_slice_mut().expect("Unable to convert LHS to slice.");
        let ys = ys.as_slice().expect("Unable to convert RHS to slice.");

        simd_scaled_assign(xs, ys, alpha);
    }

    fn dot(lhs: &[f32], rhs: &[f32]) -> f32 {
        lhs.iter().zip(rhs.iter()).map(|(x, y)| x * y).sum()
    }

    fn unrolled_dot(xs: &[f32], ys: &[f32]) -> f32 {
        let len = std::cmp::min(xs.len(), ys.len());
        let mut xs = &xs[..len];
        let mut ys = &ys[..len];

        let mut s = 0.;
        let (mut p0, mut p1, mut p2, mut p3, mut p4, mut p5, mut p6, mut p7) =
            (0., 0., 0., 0., 0., 0., 0., 0.);

        while xs.len() >= 8 {
            p0 += xs[0] * ys[0];
            p1 += xs[1] * ys[1];
            p2 += xs[2] * ys[2];
            p3 += xs[3] * ys[3];
            p4 += xs[4] * ys[4];
            p5 += xs[5] * ys[5];
            p6 += xs[6] * ys[6];
            p7 += xs[7] * ys[7];

            xs = &xs[8..];
            ys = &ys[8..];
        }
        s += p0 + p4;
        s += p1 + p5;
        s += p2 + p6;
        s += p3 + p7;

        for i in 0..xs.len() {
            s += xs[i] * ys[i];
        }

        s
    }

    #[test]
    fn test_fastexp() {
        let values: Vec<f32> = vec![-0.5, -0.1, 0.0, 0.1, 0.5];
        for &x in &values {
            println!("Input: {}, stdlib: {}, fast: {}", x, x.exp(), fastexp(x));
        }
    }

    #[test]
    fn test_fastlog() {
        let values: Vec<f32> = vec![0.1, 0.5, 1.0, 5.0, 10.0];
        for &x in &values {
            println!("Input: {}, stdlib: {}, fast: {}", x, x.ln(), fastlog(x));
        }
    }

    #[test]
    fn test_tanh() {
        let values: Vec<f32> = vec![-0.5, -0.1, 0.0, 0.1, 0.5];
        for &x in &values {
            println!(
                "Input: {}, stdlib: {}, fast: {}",
                x,
                x.tanh(),
                tanhf_fast(x)
            );
        }
    }

    #[test]
    fn test_dot() {
        for len in 0..32 {
            let xs = (0..len)
                .map(|_| rand::thread_rng().gen())
                .collect::<Vec<f32>>();
            let ys = (0..len)
                .map(|_| rand::thread_rng().gen())
                .collect::<Vec<f32>>();

            let _dot = dot(&xs[..], &ys[..]);
            let _unrolled_dot = unrolled_dot(&xs[..], &ys[..]);
            let _simd_dot = simd_dot(&xs[..], &ys[..]);

            let epsilon = 1e-5;

            assert!((_dot - _unrolled_dot).abs() < epsilon);
            assert!((_dot - _simd_dot).abs() < epsilon, "{} {}", _dot, _simd_dot);
        }
    }

    #[test]
    fn test_scaled_assign() {
        for len in 0..32 {
            let mut xs_1 = random_matrix(len, 1);
            let mut xs_2 = xs_1.clone();
            let ys = random_matrix(len, 1);

            let alpha = 3.5;

            array_scaled_assign(&mut xs_1, &ys, alpha);
            scaled_assign(&mut xs_2, &ys, alpha);

            assert_eq!(xs_1, xs_2);
        }
    }

    #[allow(dead_code)]
    fn assert_close(x: &Arr, y: &Arr, tol: f32) {
        assert!(
            x.all_close(y, tol),
            "{:#?} not within {} of {:#?}",
            x,
            tol,
            y
        );
    }

    #[test]
    fn test_dot_node_specializations_mm() {
        let x = random_matrix(64, 64);
        let y = random_matrix(64, 64);

        let mut result = random_matrix(64, 64);
        let mut expected = random_matrix(64, 64);

        mat_mul(1.0, &x, &y, 0.0, &mut result);
        general_mat_mul(1.0, &x, &y, 0.0, &mut expected);

        assert_close(&result, &expected, 0.001);
    }

    #[test]
    fn test_dot_node_specializations_mv() {
        let x = random_matrix(64, 64);
        let y = random_matrix(64, 1);

        let mut result = random_matrix(64, 1);
        let mut expected = random_matrix(64, 1);

        mat_mul(1.0, &x, &y, 0.0, &mut result);
        general_mat_mul(1.0, &x, &y, 0.0, &mut expected);

        assert_close(&result, &expected, 0.001);
    }

    #[test]
    fn test_dot_node_specializations_vm() {
        let x = random_matrix(1, 64);
        let y = random_matrix(64, 64);

        let mut result = random_matrix(1, 64);
        let mut expected = random_matrix(1, 64);

        mat_mul(1.0, &x, &y, 0.0, &mut result);
        general_mat_mul(1.0, &x, &y, 0.0, &mut expected);

        assert_close(&result, &expected, 0.001);
    }
}
