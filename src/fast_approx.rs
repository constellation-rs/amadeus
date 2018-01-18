// Direct translations from
// https://github.com/etheory/fastapprox/blob/master/fastapprox/src/fastlog.h

/*=====================================================================*
 *                   Copyright (C) 2011 Paul Mineiro                   *
 * All rights reserved.                                                *
 *                                                                     *
 * Redistribution and use in source and binary forms, with             *
 * or without modification, are permitted provided that the            *
 * following conditions are met:                                       *
 *                                                                     *
 *     * Redistributions of source code must retain the                *
 *     above copyright notice, this list of conditions and             *
 *     the following disclaimer.                                       *
 *                                                                     *
 *     * Redistributions in binary form must reproduce the             *
 *     above copyright notice, this list of conditions and             *
 *     the following disclaimer in the documentation and/or            *
 *     other materials provided with the distribution.                 *
 *                                                                     *
 *     * Neither the name of Paul Mineiro nor the names                *
 *     of other contributors may be used to endorse or promote         *
 *     products derived from this software without specific            *
 *     prior written permission.                                       *
 *                                                                     *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND              *
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,         *
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES               *
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE             *
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER               *
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                 *
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES            *
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE           *
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR                *
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF          *
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT           *
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY              *
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE             *
 * POSSIBILITY OF SUCH DAMAGE.                                         *
 *                                                                     *
 * Contact: Paul Mineiro <paul@mineiro.com>                            *
 *=====================================================================*/

use std::mem;

#[repr(C)]
union FloatUint {
    f: f32,
    i: u32,
}

#[repr(C)]
union UintFloat {
    i: u32,
    f: f32,
}

#[inline(always)]
pub fn fastlog2(x: f32) -> f32 {
    unsafe {
        let vx = FloatUint { f: x };
        let mx = UintFloat {
            i: (vx.i & 0x007FFFFF) | 0x3f000000,
        };
        let mut y = vx.i as f32;
        y *= 1.1920928955078125e-7;

        y - 124.22551499 - 1.498030302 * mx.f - 1.72587999 / (0.3520887068 + mx.f)
    }
}

#[inline(always)]
pub fn fastlog(x: f32) -> f32 {
    0.69314718 * fastlog2(x)
}

#[inline(always)]
pub fn expf_fast(x: f32) -> f32 {
    let u = (12102203.0 * x + 1064866805.0) as i32;

    unsafe { mem::transmute::<i32, f32>(u) }
}

#[inline(always)]
pub fn tanhf_fast(x: f32) -> f32 {
    if x < -3.0 {
        -1.0
    } else if x > 3.0 {
        1.0
    } else {
        x * (27.0 + x.powi(2)) / (27.0 + 9.0 * x.powi(2))
    }
}
