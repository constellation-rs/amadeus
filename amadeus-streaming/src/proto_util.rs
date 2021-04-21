const WIRE_TYPE_VAR_INT: u32 = 0;
const PRECISION_OR_NUM_BUCKETS_FIELD_NUMBER: u32 = 3;
pub const PRECISION_OR_NUM_BUCKETS_TAG: u32 =
	PRECISION_OR_NUM_BUCKETS_FIELD_NUMBER << 3 | WIRE_TYPE_VAR_INT;

const DATA_FIELD_NUMBER: u32 = 5;
const WIRE_TYPE_LENGTH_DELIMITED: u32 = 2;
pub const DATA_TAG: u32 = DATA_FIELD_NUMBER << 3 | WIRE_TYPE_LENGTH_DELIMITED;

const AGGREGATOR_STATE_PROTO_TYPE_FIELD_NUMBER: u32 = 1;
pub const TYPE_TAG: u32 = AGGREGATOR_STATE_PROTO_TYPE_FIELD_NUMBER << 3 | WIRE_TYPE_VAR_INT;

const AGGREGATOR_STATE_PROTO_NUM_VALUES_FIELD_NUMBER: u32 = 2;
pub const AGGREGATION_TYPE: i32 = 112;
pub const NUM_VALUES_TAG: u32 =
	AGGREGATOR_STATE_PROTO_NUM_VALUES_FIELD_NUMBER << 3 | WIRE_TYPE_VAR_INT;

const AGGREGATOR_STATE_PROTO_ENCODING_VERSION_FIELD_NUMBER: u32 = 3;
pub const ENCODING_VERSION_TAG: u32 =
	AGGREGATOR_STATE_PROTO_ENCODING_VERSION_FIELD_NUMBER << 3 | WIRE_TYPE_VAR_INT;

const DEFAULT_ENCODING_VERSION: u32 = 1;
pub const HYPERLOGLOG_PLUS_PLUS_ENCODING_VERSION: i32 = 2;

const AGGREGATOR_STATE_PROTO_VALUE_TYPE_FIELD_NUMBER: u32 = 4;
pub const VALUE_TYPE_TAG: u32 =
	AGGREGATOR_STATE_PROTO_VALUE_TYPE_FIELD_NUMBER << 3 | WIRE_TYPE_VAR_INT;
pub const BYTES_OR_UTF8_STRING_TYPE: i32 = 11;

const AGGREGATOR_STATE_PROTO_HYPERLOGLOG_PLUS_UNIQUE_STATE_FIELD_NUMBER: u32 = 112;
pub const HYPERLOGLOGPLUS_UNIQUE_STATE_TAG: u32 =
	AGGREGATOR_STATE_PROTO_HYPERLOGLOG_PLUS_UNIQUE_STATE_FIELD_NUMBER << 3
		| WIRE_TYPE_LENGTH_DELIMITED;

const SPARSE_PRECISION_OR_NUM_BUCKETS_FIELD_NUMBER: u32 = 4;
pub const SPARSE_PRECISION_OR_NUM_BUCKETS_TAG: u32 =
	SPARSE_PRECISION_OR_NUM_BUCKETS_FIELD_NUMBER << 3 | WIRE_TYPE_VAR_INT;

const MAX_VAR_INT_SIZE: usize = 10;

pub fn compute_uint32_size_no_tag(value: u32) -> usize {
	if value & (!0 << 7) == 0 {
		return 1;
	}
	if value & (!0 << 14) == 0 {
		return 2;
	}
	if value & (!0 << 21) == 0 {
		return 3;
	}
	if value & (!0 << 28) == 0 {
		return 4;
	}
	return 5;
}

pub fn compute_int32_size_no_tag(value: i32) -> usize {
	if value >= 0 {
		compute_uint32_size_no_tag(value as u32)
	} else {
		// Must sign-extend.
		MAX_VAR_INT_SIZE
	}
}

pub fn compute_enum_size_no_tag(value: i32) -> usize {
	compute_int32_size_no_tag(value)
}

pub fn compute_uint64_size_no_tag(value: i64) -> usize {
	// handle two popular special cases up front ...
	if value & (!0_i64 << 7) == 0_i64 {
		return 1;
	}
	if value < 0_i64 {
		return 10;
	}

	let mut value = value as u64;
	// ... leaving us with 8 remaining, which we can divide and conquer
	let mut n = 2;
	if value & (!0_u64 << 35) != 0_u64 {
		n += 4;
		value >>= 28;
	}
	if value & (!0_u64 << 21) != 0_u64 {
		n += 2;
		value >>= 14;
	}
	if value & (!0_u64 << 14) != 0_u64 {
		n += 1;
	}
	n
}

pub fn compute_int64_size_no_tag(value: i64) -> usize {
	compute_uint64_size_no_tag(value)
}

#[cfg(test)]
mod test {
	use crate::proto_util::{
		compute_enum_size_no_tag, compute_int32_size_no_tag, compute_int64_size_no_tag, compute_uint32_size_no_tag
	};

	#[test]
	fn test_compute_uint32_size_no_tag() {
		assert_eq!(1, compute_uint32_size_no_tag(0));
		assert_eq!(1, compute_uint32_size_no_tag(10));
		assert_eq!(1, compute_uint32_size_no_tag(100));
		assert_eq!(2, compute_uint32_size_no_tag(1000));
		assert_eq!(2, compute_uint32_size_no_tag(10000));
		assert_eq!(3, compute_uint32_size_no_tag(100000));
		assert_eq!(3, compute_uint32_size_no_tag(1000000));
	}

	#[test]
	fn test_compute_int32_size_no_tag() {
		assert_eq!(1, compute_int32_size_no_tag(0));
		assert_eq!(1, compute_int32_size_no_tag(10));
		assert_eq!(1, compute_int32_size_no_tag(100));
		assert_eq!(2, compute_int32_size_no_tag(1000));
		assert_eq!(2, compute_int32_size_no_tag(10000));
		assert_eq!(3, compute_int32_size_no_tag(100000));
		assert_eq!(3, compute_int32_size_no_tag(1000000));
		assert_eq!(10, compute_int32_size_no_tag(-1000000));
	}

	#[test]
	fn test_compute_enum_size_no_tag() {
		assert_eq!(1, compute_enum_size_no_tag(0));
		assert_eq!(1, compute_enum_size_no_tag(10));
		assert_eq!(1, compute_enum_size_no_tag(112));
	}

	#[test]
	fn test_compute_int64_size_no_tag() {
		assert_eq!(1, compute_int64_size_no_tag(0));
		assert_eq!(3, compute_int64_size_no_tag(100000));
		assert_eq!(4, compute_int64_size_no_tag(123456789));
		assert_eq!(10, compute_int64_size_no_tag(-100000));
		assert_eq!(10, compute_int64_size_no_tag(-1000000));
	}
}
