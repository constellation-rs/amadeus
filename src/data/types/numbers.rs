//! Implement [`Record`] for `bool`, `i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `i64`, `u64`,
//! `f32`, and `f64`.

use std::{collections::HashMap, marker::PhantomData};

use super::super::Data;
