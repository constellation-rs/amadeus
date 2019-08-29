// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! An Apache Parquet implementation in Rust.
//!
//! **[Crates.io](https://crates.io/crates/parchet) │ [Repo](https://github.com/alecmocatta/parchet)**

#![doc(html_root_url = "https://docs.rs/parchet/0.1.0")]
#![feature(specialization, type_alias_impl_trait)]
#![warn(
	// missing_copy_implementations,
	// missing_debug_implementations,
	// missing_docs,
	// trivial_casts,
	// trivial_numeric_casts,
	unused_import_braces,
	unused_qualifications,
	unused_results,
	// clippy::pedantic
)]
// from https://github.com/rust-unofficial/patterns/blob/master/anti_patterns/deny-warnings.md
#![allow(dead_code, clippy::all)]

#[macro_use]
pub mod errors;
pub mod basic;
pub mod data_type;

// Exported for external use, such as benchmarks
pub use self::{
	encodings::{decoding, encoding}, util::memory
};

#[macro_use]
mod util;
pub mod column;
pub mod compression;
mod encodings;
pub mod file;
pub mod record;
pub mod schema;
