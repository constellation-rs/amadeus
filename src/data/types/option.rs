//! Implement [`Record`] for `Option<T> where T: Record`.

use std::collections::HashMap;

use super::super::Data;
use amadeus_parquet::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::{ParquetError, Result},
    record::{reader::OptionReader, schemas::OptionSchema, Record},
    schema::types::{ColumnPath, Type},
};

// `Option<T>` corresponds to Parquet fields marked as "optional".
