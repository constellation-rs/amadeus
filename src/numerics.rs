use std::cmp;
use stdsimd;

use super::Arr;

/// SIMD-enabled vector-vector dot product.
pub fn simd_dot(xs: &[f32], ys: &[f32]) -> f32 {
    let mut simd_result = stdsimd::simd::f32x8::splat(0.0);
    let mut scalar_result = 0.0;
    let stride = 8;

    let split_idx = cmp::min(xs.len(), ys.len()) / stride * stride;
    let (simd_xs, scalar_xs) = xs.split_at(split_idx);
    let (simd_ys, scalar_ys) = ys.split_at(split_idx);

    for (x, y) in simd_xs.chunks(stride).zip(simd_ys.chunks(stride)) {
        unsafe {
            simd_result = simd_result
                + stdsimd::simd::f32x8::load_unchecked(x, 0)
                    * stdsimd::simd::f32x8::load_unchecked(y, 0);
        }
    }

    for (x_scalar, y_scalar) in scalar_xs.iter().zip(scalar_ys.iter()) {
        scalar_result += x_scalar * y_scalar;
    }

    scalar_result
        + (0..stride as u32)
            .map(|idx| simd_result.extract(idx))
            .sum::<f32>()
}

pub fn simd_sum(xs: &[f32]) -> f32 {
    let mut simd_result = stdsimd::simd::f32x8::splat(0.0);
    let mut scalar_result = 0.0;
    let stride = 8;

    let split_idx = (xs.len() / stride) * stride;
    let (simd_xs, scalar_xs) = xs.split_at(split_idx);

    for x in simd_xs.chunks(stride) {
        unsafe { simd_result = simd_result + stdsimd::simd::f32x8::load_unchecked(x, 0) }
    }

    for x_scalar in scalar_xs.iter() {
        scalar_result += x_scalar;
    }

    scalar_result
        + (0..stride as u32)
            .map(|idx| simd_result.extract(idx))
            .sum::<f32>()
}

pub fn simd_scaled_assign(xs: &mut [f32], ys: &[f32], alpha: f32) {
    let stride = 8;
    let simd_alpha = stdsimd::simd::f32x8::splat(alpha);

    let split_idx = xs.len() / stride * stride;
    let (simd_xs, scalar_xs) = xs.split_at_mut(split_idx);
    let (simd_ys, scalar_ys) = ys.split_at(split_idx);

    for (x, y) in simd_xs.chunks_mut(stride).zip(simd_ys.chunks(stride)) {
        unsafe {
            let elem = stdsimd::simd::f32x8::load_unchecked(y, 0) * simd_alpha;
            elem.store_unchecked(x, 0);
        }
    }

    for (x_scalar, y_scalar) in scalar_xs.iter_mut().zip(scalar_ys.iter()) {
        *x_scalar = y_scalar * alpha;
    }
}

pub fn simd_scaled_add(xs: &mut [f32], ys: &[f32], alpha: f32) {
    let stride = 8;
    let simd_alpha = stdsimd::simd::f32x8::splat(alpha);

    let split_idx = xs.len() / stride * stride;
    let (simd_xs, scalar_xs) = xs.split_at_mut(split_idx);
    let (simd_ys, scalar_ys) = ys.split_at(split_idx);

    for (x, y) in simd_xs.chunks_mut(stride).zip(simd_ys.chunks(stride)) {
        unsafe {
            let elem = stdsimd::simd::f32x8::load_unchecked(x, 0)
                + stdsimd::simd::f32x8::load_unchecked(y, 0) * simd_alpha;
            elem.store_unchecked(x, 0);
        }
    }

    for (x_scalar, y_scalar) in scalar_xs.iter_mut().zip(scalar_ys.iter()) {
        *x_scalar += y_scalar * alpha;
    }
}

pub fn slice_assign(xs: &mut [f32], ys: &[f32]) {
    for (x, &y) in xs.iter_mut().zip(ys.iter()) {
        *x = y;
    }
}

pub fn map_assign<F>(xs: &mut Arr, ys: &Arr, func: F)
where
    F: Fn(f32) -> f32,
{
    let xs = xs.as_slice_mut().expect("Unable to convert LHS to slice.");
    let ys = ys.as_slice().expect("Unable to convert RHS to slice.");

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
    let xs = xs.as_slice_mut()
        .expect("Unable to convert operand to slice.");
    let ys = ys.as_slice().expect("Unable to convert operand to slice.");
    let zs = zs.as_slice().expect("Unable to convert operand to slice.");

    for (x, &y, &z) in izip!(xs.iter_mut(), ys.iter(), zs.iter()) {
        *x = func(y, z);
    }
}

#[allow(dead_code)]
pub fn map_inplace_assign<F>(xs: &mut Arr, ys: &Arr, func: F)
where
    F: Fn(&mut f32, f32),
{
    let xs = xs.as_slice_mut()
        .expect("Unable to convert operand to slice.");
    let ys = ys.as_slice().expect("Unable to convert operand to slice.");

    for (x, &y) in izip!(xs.iter_mut(), ys.iter()) {
        func(x, y);
    }
}

#[allow(dead_code)]
pub fn map_inplace_assign_binary<F>(xs: &mut Arr, ys: &Arr, zs: &Arr, func: F)
where
    F: Fn(&mut f32, f32, f32),
{
    let xs = xs.as_slice_mut()
        .expect("Unable to convert operand to slice.");
    let ys = ys.as_slice().expect("Unable to convert operand to slice.");
    let zs = zs.as_slice().expect("Unable to convert operand to slice.");

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
    use test::Bencher;

    fn random_matrix(rows: usize, cols: usize) -> Arr {
        Arr::zeros((rows, cols)).map(|_| rand::random::<f32>())
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

    fn array_assign(xs: &mut Arr, ys: &Arr) {
        xs.assign(ys);
    }

    fn assign(xs: &mut Arr, ys: &Arr) {
        assert_eq!(
            xs.shape(),
            ys.shape(),
            "Operands do not have the same shape."
        );

        let xs = xs.as_slice_mut().expect("Unable to convert LHS to slice.");
        let ys = ys.as_slice().expect("Unable to convert RHS to slice.");

        for (x, &y) in xs.iter_mut().zip(ys.iter()) {
            *x = y;
        }
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

    #[bench]
    fn bench_dot(b: &mut Bencher) {
        let xs = vec![0.0; 256];
        let ys = vec![0.0; 256];

        b.iter(|| dot(&xs[..], &ys[..]));
    }

    #[bench]
    fn bench_unrolled_dot(b: &mut Bencher) {
        let xs = vec![0.0; 256];
        let ys = vec![0.0; 256];

        b.iter(|| unrolled_dot(&xs[..], &ys[..]));
    }

    #[bench]
    fn bench_simd_dot(b: &mut Bencher) {
        let xs = vec![0.0; 256];
        let ys = vec![0.0; 256];

        b.iter(|| simd_dot(&xs[..], &ys[..]));
    }

    #[bench]
    fn bench_array_scaled_assign(b: &mut Bencher) {
        let mut xs = random_matrix(256, 1);
        let ys = random_matrix(256, 1);

        b.iter(|| array_scaled_assign(&mut xs, &ys, 3.5));
    }

    #[bench]
    fn bench_slice_scaled_assign(b: &mut Bencher) {
        let mut xs = random_matrix(256, 1);
        let ys = random_matrix(256, 1);

        b.iter(|| scaled_assign(&mut xs, &ys, 3.5));
    }

    #[bench]
    fn bench_array_assign(b: &mut Bencher) {
        let mut xs = random_matrix(256, 1);
        let ys = random_matrix(256, 1);

        b.iter(|| array_assign(&mut xs, &ys));
    }

    #[bench]
    fn bench_slice_assign(b: &mut Bencher) {
        let mut xs = random_matrix(256, 1);
        let ys = random_matrix(256, 1);

        b.iter(|| assign(&mut xs, &ys));
    }
}
