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
#[allow(unused_results, renamed_and_removed_lints, clippy::too_many_arguments, clippy::type_complexity, clippy::redundant_field_names)]
#[rustfmt::skip]
mod format;
pub mod record;
pub mod schema;
