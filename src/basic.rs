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

//! Contains Rust mappings for Thrift definition.
//! Refer to `parquet.thrift` file to see raw definitions.

use std::{fmt, str};

use parquet_format as parquet;

use crate::errors::ParquetError;

// ----------------------------------------------------------------------
// Types from the Thrift definition

// ----------------------------------------------------------------------
// Mirrors `parquet::Type`

/// Types supported by Parquet.
/// These physical types are intended to be used in combination with the encodings to
/// control the on disk storage format.
/// For example INT16 is not included as a type since a good encoding of INT32
/// would handle this.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::ConvertedType`

/// Common types (logical types) used by frameworks when using Parquet.
/// This helps map between types in those frameworks to the base types in Parquet.
/// This is only metadata and not needed to read or write the data.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogicalType {
    None,
    /// A BYTE_ARRAY actually contains UTF8 encoded chars.
    Utf8,

    /// A map is converted as an optional field containing a repeated key/value pair.
    Map,

    /// A key/value pair is converted into a group of two fields.
    MapKeyValue,

    /// A list is converted into an optional field containing a repeated field for its
    /// values.
    List,

    /// An enum is converted into a binary field
    Enum,

    /// A decimal value.
    /// This may be used to annotate binary or fixed primitive types. The
    /// underlying byte array stores the unscaled value encoded as two's
    /// complement using big-endian byte order (the most significant byte is the
    /// zeroth element).
    ///
    /// This must be accompanied by a (maximum) precision and a scale in the
    /// SchemaElement. The precision specifies the number of digits in the decimal
    /// and the scale stores the location of the decimal point. For example 1.23
    /// would have precision 3 (3 total digits) and scale 2 (the decimal point is
    /// 2 digits over).
    Decimal,

    /// A date stored as days since Unix epoch, encoded as the INT32 physical type.
    Date,

    /// The total number of milliseconds since midnight. The value is stored as an INT32
    /// physical type.
    TimeMillis,

    /// The total number of microseconds since midnight. The value is stored as an INT64
    /// physical type.
    TimeMicros,

    /// Date and time recorded as milliseconds since the Unix epoch.
    /// Recorded as a physical type of INT64.
    TimestampMillis,

    /// Date and time recorded as microseconds since the Unix epoch.
    /// The value is stored as an INT64 physical type.
    TimestampMicros,

    /// An unsigned 8 bit integer value stored as INT32 physical type.
    Uint8,

    /// An unsigned 16 bit integer value stored as INT32 physical type.
    Uint16,

    /// An unsigned 32 bit integer value stored as INT32 physical type.
    Uint32,

    /// An unsigned 64 bit integer value stored as INT64 physical type.
    Uint64,

    /// A signed 8 bit integer value stored as INT32 physical type.
    Int8,

    /// A signed 16 bit integer value stored as INT32 physical type.
    Int16,

    /// A signed 32 bit integer value stored as INT32 physical type.
    Int32,

    /// A signed 64 bit integer value stored as INT64 physical type.
    Int64,

    /// A JSON document embedded within a single UTF8 column.
    Json,

    /// A BSON document embedded within a single BINARY column.
    Bson,

    /// An interval of time.
    ///
    /// This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12.
    /// This data is composed of three separate little endian unsigned integers.
    /// Each stores a component of a duration of time. The first integer identifies
    /// the number of months associated with the duration, the second identifies
    /// the number of days associated with the duration and the third identifies
    /// the number of milliseconds associated with the provided duration.
    /// This duration of time is independent of any particular timezone or date.
    Interval,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::FieldRepetitionType`

/// Representation of field types in schema.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Repetition {
    /// Field is required (can not be null) and each record has exactly 1 value.
    Required,
    /// Field is optional (can be null) and each record has 0 or 1 values.
    Optional,
    /// Field is repeated and can contain 0 or more values.
    Repeated,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::Encoding`

/// Encodings supported by Parquet.
/// Not all encodings are valid for all types. These enums are also used to specify the
/// encoding of definition and repetition levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// Default byte encoding.
    /// - BOOLEAN - 1 bit per value, 0 is false; 1 is true.
    /// - INT32 - 4 bytes per value, stored as little-endian.
    /// - INT64 - 8 bytes per value, stored as little-endian.
    /// - FLOAT - 4 bytes per value, stored as little-endian.
    /// - DOUBLE - 8 bytes per value, stored as little-endian.
    /// - BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
    /// - FIXED_LEN_BYTE_ARRAY - just the bytes are stored.
    Plain,

    /// **Deprecated** dictionary encoding.
    ///
    /// The values in the dictionary are encoded using PLAIN encoding.
    /// Since it is deprecated, RLE_DICTIONARY encoding is used for a data page, and
    /// PLAIN encoding is used for dictionary page.
    PlainDictionary,

    /// Group packed run length encoding.
    ///
    /// Usable for definition/repetition levels encoding and boolean values.
    Rle,

    /// Bit packed encoding.
    ///
    /// This can only be used if the data has a known max width.
    /// Usable for definition/repetition levels encoding.
    BitPacked,

    /// Delta encoding for integers, either INT32 or INT64.
    ///
    /// Works best on sorted data.
    DeltaBinaryPacked,

    /// Encoding for byte arrays to separate the length values and the data.
    ///
    /// The lengths are encoded using ::DeltaBinaryPacked encoding.
    DeltaLengthByteArray,

    /// Incremental encoding for byte arrays.
    ///
    /// Prefix lengths are encoded using ::DeltaBinaryPacked encoding.
    /// Suffixes are stored using DELTA_LENGTH_BYTE_ARRAY encoding.
    DeltaByteArray,

    /// Dictionary encoding.
    ///
    /// The ids are encoded using the RLE encoding.
    RleDictionary,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::CompressionCodec`

/// Supported compression algorithms.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::PageType`

/// Available data pages for Parquet file format.
/// Note that some of the page types may not be supported.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
    DataPage,
    IndexPage,
    DictionaryPage,
    DataPageV2,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::ColumnOrder`

/// Sort order for page and column statistics.
///
/// Types are associated with sort orders and column stats are aggregated using a sort
/// order, and a sort order should be considered when comparing values with statistics
/// min/max.
///
/// See reference in
/// https://github.com/apache/parquet-cpp/blob/master/src/parquet/types.h
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortOrder {
    /// Signed (either value or legacy byte-wise) comparison.
    Signed,
    /// Unsigned (depending on physical type either value or byte-wise) comparison.
    Unsigned,
    /// Comparison is undefined.
    Undefined,
}

/// Column order that specifies what method was used to aggregate min/max values for
/// statistics.
///
/// If column order is undefined, then it is the legacy behaviour and all values should
/// be compared as signed values/bytes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnOrder {
    /// Column uses the order defined by its logical or physical type
    /// (if there is no logical type), parquet-format 2.4.0+.
    TypeDefinedOrder(SortOrder),
    /// Undefined column order, means legacy behaviour before parquet-format 2.4.0.
    /// Sort order is always SIGNED.
    Undefined,
}

impl ColumnOrder {
    /// Returns sort order for a physical/logical type.
    pub fn get_sort_order(logical_type: LogicalType, physical_type: Type) -> SortOrder {
        match logical_type {
            // Unsigned byte-wise comparison.
            LogicalType::Utf8
            | LogicalType::Json
            | LogicalType::Bson
            | LogicalType::Enum => SortOrder::Unsigned,

            LogicalType::Int8
            | LogicalType::Int16
            | LogicalType::Int32
            | LogicalType::Int64 => SortOrder::Signed,

            LogicalType::Uint8
            | LogicalType::Uint16
            | LogicalType::Uint32
            | LogicalType::Uint64 => SortOrder::Unsigned,

            // Signed comparison of the represented value.
            LogicalType::Decimal => SortOrder::Signed,

            LogicalType::Date => SortOrder::Signed,

            LogicalType::TimeMillis
            | LogicalType::TimeMicros
            | LogicalType::TimestampMillis
            | LogicalType::TimestampMicros => SortOrder::Signed,

            LogicalType::Interval => SortOrder::Unsigned,

            LogicalType::List | LogicalType::Map | LogicalType::MapKeyValue => {
                SortOrder::Undefined
            }

            // Fall back to physical type.
            LogicalType::None => Self::get_default_sort_order(physical_type),
        }
    }

    /// Returns default sort order based on physical type.
    fn get_default_sort_order(physical_type: Type) -> SortOrder {
        match physical_type {
            // Order: false, true
            Type::Boolean => SortOrder::Unsigned,
            Type::Int32 | Type::Int64 => SortOrder::Signed,
            Type::Int96 => SortOrder::Undefined,
            // Notes to remember when comparing float/double values:
            // If the min is a NaN, it should be ignored.
            // If the max is a NaN, it should be ignored.
            // If the min is +0, the row group may contain -0 values as well.
            // If the max is -0, the row group may contain +0 values as well.
            // When looking for NaN values, min and max should be ignored.
            Type::Float | Type::Double => SortOrder::Signed,
            // unsigned byte-wise comparison
            Type::ByteArray | Type::FixedLenByteArray => SortOrder::Unsigned,
        }
    }

    /// Returns sort order associated with this column order.
    pub fn sort_order(&self) -> SortOrder {
        match *self {
            ColumnOrder::TypeDefinedOrder(order) => order,
            ColumnOrder::Undefined => SortOrder::Signed,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Type::Boolean => "BOOLEAN",
            Type::Int32 => "INT32",
            Type::Int64 => "INT64",
            Type::Int96 => "INT96",
            Type::Float => "FLOAT",
            Type::Double => "DOUBLE",
            Type::ByteArray => "BYTE_ARRAY",
            Type::FixedLenByteArray => "FIXED_LEN_BYTE_ARRAY",
        })
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            LogicalType::None => "NONE",
            LogicalType::Utf8 => "UTF8",
            LogicalType::Map => "MAP",
            LogicalType::MapKeyValue => "MAP_KEY_VALUE",
            LogicalType::List => "LIST",
            LogicalType::Enum => "ENUM",
            LogicalType::Decimal => "DECIMAL",
            LogicalType::Date => "DATE",
            LogicalType::TimeMillis => "TIME_MILLIS",
            LogicalType::TimeMicros => "TIME_MICROS",
            LogicalType::TimestampMillis => "TIMESTAMP_MILLIS",
            LogicalType::TimestampMicros => "TIMESTAMP_MICROS",
            LogicalType::Uint8 => "UINT_8",
            LogicalType::Uint16 => "UINT_16",
            LogicalType::Uint32 => "UINT_32",
            LogicalType::Uint64 => "UINT_64",
            LogicalType::Int8 => "INT_8",
            LogicalType::Int16 => "INT_16",
            LogicalType::Int32 => "INT_32",
            LogicalType::Int64 => "INT_64",
            LogicalType::Json => "JSON",
            LogicalType::Bson => "BSON",
            LogicalType::Interval => "INTERVAL",
        })
    }
}

impl fmt::Display for Repetition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Repetition::Required => "REQUIRED",
            Repetition::Optional => "OPTIONAL",
            Repetition::Repeated => "REPEATED",
        })
    }
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Encoding::Plain => "PLAIN",
            Encoding::PlainDictionary => "PLAIN_DICTIONARY",
            Encoding::Rle => "RLE",
            Encoding::BitPacked => "BIT_PACKED",
            Encoding::DeltaBinaryPacked => "DELTA_BINARY_PACKED",
            Encoding::DeltaLengthByteArray => "DELTA_LENGTH_BYTE_ARRAY",
            Encoding::DeltaByteArray => "DELTA_BYTE_ARRAY",
            Encoding::RleDictionary => "RLE_DICTIONARY",
        })
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Compression::Uncompressed => "UNCOMPRESSED",
            Compression::Snappy => "SNAPPY",
            Compression::Gzip => "GZIP",
            Compression::Lzo => "LZO",
            Compression::Brotli => "BROTLI",
            Compression::Lz4 => "LZ4",
            Compression::Zstd => "ZSTD",
        })
    }
}

impl fmt::Display for PageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            PageType::DataPage => "DATA_PAGE",
            PageType::IndexPage => "INDEX_PAGE",
            PageType::DictionaryPage => "DICTIONARY_PAGE",
            PageType::DataPageV2 => "DATA_PAGE_V2",
        })
    }
}

impl fmt::Display for SortOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            SortOrder::Signed => "SIGNED",
            SortOrder::Unsigned => "UNSIGNED",
            SortOrder::Undefined => "UNDEFINED",
        })
    }
}

impl fmt::Display for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ColumnOrder::TypeDefinedOrder(sort_order) => {
                write!(f, "TYPE_DEFINED_ORDER({})", sort_order)
            }
            ColumnOrder::Undefined => f.write_str("UNDEFINED"),
        }
    }
}

// ----------------------------------------------------------------------
// parquet::Type <=> Type conversion

impl From<parquet::Type> for Type {
    fn from(value: parquet::Type) -> Self {
        match value {
            parquet::Type::Boolean => Type::Boolean,
            parquet::Type::Int32 => Type::Int32,
            parquet::Type::Int64 => Type::Int64,
            parquet::Type::Int96 => Type::Int96,
            parquet::Type::Float => Type::Float,
            parquet::Type::Double => Type::Double,
            parquet::Type::ByteArray => Type::ByteArray,
            parquet::Type::FixedLenByteArray => Type::FixedLenByteArray,
        }
    }
}

impl From<Type> for parquet::Type {
    fn from(value: Type) -> Self {
        match value {
            Type::Boolean => parquet::Type::Boolean,
            Type::Int32 => parquet::Type::Int32,
            Type::Int64 => parquet::Type::Int64,
            Type::Int96 => parquet::Type::Int96,
            Type::Float => parquet::Type::Float,
            Type::Double => parquet::Type::Double,
            Type::ByteArray => parquet::Type::ByteArray,
            Type::FixedLenByteArray => parquet::Type::FixedLenByteArray,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::ConvertedType <=> LogicalType conversion

impl From<Option<parquet::ConvertedType>> for LogicalType {
    fn from(option: Option<parquet::ConvertedType>) -> Self {
        match option {
            None => LogicalType::None,
            Some(value) => match value {
                parquet::ConvertedType::Utf8 => LogicalType::Utf8,
                parquet::ConvertedType::Map => LogicalType::Map,
                parquet::ConvertedType::MapKeyValue => LogicalType::MapKeyValue,
                parquet::ConvertedType::List => LogicalType::List,
                parquet::ConvertedType::Enum => LogicalType::Enum,
                parquet::ConvertedType::Decimal => LogicalType::Decimal,
                parquet::ConvertedType::Date => LogicalType::Date,
                parquet::ConvertedType::TimeMillis => LogicalType::TimeMillis,
                parquet::ConvertedType::TimeMicros => LogicalType::TimeMicros,
                parquet::ConvertedType::TimestampMillis => LogicalType::TimestampMillis,
                parquet::ConvertedType::TimestampMicros => LogicalType::TimestampMicros,
                parquet::ConvertedType::Uint8 => LogicalType::Uint8,
                parquet::ConvertedType::Uint16 => LogicalType::Uint16,
                parquet::ConvertedType::Uint32 => LogicalType::Uint32,
                parquet::ConvertedType::Uint64 => LogicalType::Uint64,
                parquet::ConvertedType::Int8 => LogicalType::Int8,
                parquet::ConvertedType::Int16 => LogicalType::Int16,
                parquet::ConvertedType::Int32 => LogicalType::Int32,
                parquet::ConvertedType::Int64 => LogicalType::Int64,
                parquet::ConvertedType::Json => LogicalType::Json,
                parquet::ConvertedType::Bson => LogicalType::Bson,
                parquet::ConvertedType::Interval => LogicalType::Interval,
            },
        }
    }
}

impl From<LogicalType> for Option<parquet::ConvertedType> {
    fn from(value: LogicalType) -> Self {
        match value {
            LogicalType::None => None,
            LogicalType::Utf8 => Some(parquet::ConvertedType::Utf8),
            LogicalType::Map => Some(parquet::ConvertedType::Map),
            LogicalType::MapKeyValue => Some(parquet::ConvertedType::MapKeyValue),
            LogicalType::List => Some(parquet::ConvertedType::List),
            LogicalType::Enum => Some(parquet::ConvertedType::Enum),
            LogicalType::Decimal => Some(parquet::ConvertedType::Decimal),
            LogicalType::Date => Some(parquet::ConvertedType::Date),
            LogicalType::TimeMillis => Some(parquet::ConvertedType::TimeMillis),
            LogicalType::TimeMicros => Some(parquet::ConvertedType::TimeMicros),
            LogicalType::TimestampMillis => Some(parquet::ConvertedType::TimestampMillis),
            LogicalType::TimestampMicros => Some(parquet::ConvertedType::TimestampMicros),
            LogicalType::Uint8 => Some(parquet::ConvertedType::Uint8),
            LogicalType::Uint16 => Some(parquet::ConvertedType::Uint16),
            LogicalType::Uint32 => Some(parquet::ConvertedType::Uint32),
            LogicalType::Uint64 => Some(parquet::ConvertedType::Uint64),
            LogicalType::Int8 => Some(parquet::ConvertedType::Int8),
            LogicalType::Int16 => Some(parquet::ConvertedType::Int16),
            LogicalType::Int32 => Some(parquet::ConvertedType::Int32),
            LogicalType::Int64 => Some(parquet::ConvertedType::Int64),
            LogicalType::Json => Some(parquet::ConvertedType::Json),
            LogicalType::Bson => Some(parquet::ConvertedType::Bson),
            LogicalType::Interval => Some(parquet::ConvertedType::Interval),
        }
    }
}

// ----------------------------------------------------------------------
// parquet::FieldRepetitionType <=> Repetition conversion

impl From<parquet::FieldRepetitionType> for Repetition {
    fn from(value: parquet::FieldRepetitionType) -> Self {
        match value {
            parquet::FieldRepetitionType::Required => Repetition::Required,
            parquet::FieldRepetitionType::Optional => Repetition::Optional,
            parquet::FieldRepetitionType::Repeated => Repetition::Repeated,
        }
    }
}

impl From<Repetition> for parquet::FieldRepetitionType {
    fn from(value: Repetition) -> Self {
        match value {
            Repetition::Required => parquet::FieldRepetitionType::Required,
            Repetition::Optional => parquet::FieldRepetitionType::Optional,
            Repetition::Repeated => parquet::FieldRepetitionType::Repeated,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::Encoding <=> Encoding conversion

impl From<parquet::Encoding> for Encoding {
    fn from(value: parquet::Encoding) -> Self {
        match value {
            parquet::Encoding::Plain => Encoding::Plain,
            parquet::Encoding::PlainDictionary => Encoding::PlainDictionary,
            parquet::Encoding::Rle => Encoding::Rle,
            parquet::Encoding::BitPacked => Encoding::BitPacked,
            parquet::Encoding::DeltaBinaryPacked => Encoding::DeltaBinaryPacked,
            parquet::Encoding::DeltaLengthByteArray => Encoding::DeltaLengthByteArray,
            parquet::Encoding::DeltaByteArray => Encoding::DeltaByteArray,
            parquet::Encoding::RleDictionary => Encoding::RleDictionary,
        }
    }
}

impl From<Encoding> for parquet::Encoding {
    fn from(value: Encoding) -> Self {
        match value {
            Encoding::Plain => parquet::Encoding::Plain,
            Encoding::PlainDictionary => parquet::Encoding::PlainDictionary,
            Encoding::Rle => parquet::Encoding::Rle,
            Encoding::BitPacked => parquet::Encoding::BitPacked,
            Encoding::DeltaBinaryPacked => parquet::Encoding::DeltaBinaryPacked,
            Encoding::DeltaLengthByteArray => parquet::Encoding::DeltaLengthByteArray,
            Encoding::DeltaByteArray => parquet::Encoding::DeltaByteArray,
            Encoding::RleDictionary => parquet::Encoding::RleDictionary,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::CompressionCodec <=> Compression conversion

impl From<parquet::CompressionCodec> for Compression {
    fn from(value: parquet::CompressionCodec) -> Self {
        match value {
            parquet::CompressionCodec::Uncompressed => Compression::Uncompressed,
            parquet::CompressionCodec::Snappy => Compression::Snappy,
            parquet::CompressionCodec::Gzip => Compression::Gzip,
            parquet::CompressionCodec::Lzo => Compression::Lzo,
            parquet::CompressionCodec::Brotli => Compression::Brotli,
            parquet::CompressionCodec::Lz4 => Compression::Lz4,
            parquet::CompressionCodec::Zstd => Compression::Zstd,
        }
    }
}

impl From<Compression> for parquet::CompressionCodec {
    fn from(value: Compression) -> Self {
        match value {
            Compression::Uncompressed => parquet::CompressionCodec::Uncompressed,
            Compression::Snappy => parquet::CompressionCodec::Snappy,
            Compression::Gzip => parquet::CompressionCodec::Gzip,
            Compression::Lzo => parquet::CompressionCodec::Lzo,
            Compression::Brotli => parquet::CompressionCodec::Brotli,
            Compression::Lz4 => parquet::CompressionCodec::Lz4,
            Compression::Zstd => parquet::CompressionCodec::Zstd,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::PageType <=> PageType conversion

impl From<parquet::PageType> for PageType {
    fn from(value: parquet::PageType) -> Self {
        match value {
            parquet::PageType::DataPage => PageType::DataPage,
            parquet::PageType::IndexPage => PageType::IndexPage,
            parquet::PageType::DictionaryPage => PageType::DictionaryPage,
            parquet::PageType::DataPageV2 => PageType::DataPageV2,
        }
    }
}

impl From<PageType> for parquet::PageType {
    fn from(value: PageType) -> Self {
        match value {
            PageType::DataPage => parquet::PageType::DataPage,
            PageType::IndexPage => parquet::PageType::IndexPage,
            PageType::DictionaryPage => parquet::PageType::DictionaryPage,
            PageType::DataPageV2 => parquet::PageType::DataPageV2,
        }
    }
}

// ----------------------------------------------------------------------
// String conversions for schema parsing.

impl str::FromStr for Repetition {
    type Err = ParquetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "REQUIRED" => Ok(Repetition::Required),
            "OPTIONAL" => Ok(Repetition::Optional),
            "REPEATED" => Ok(Repetition::Repeated),
            other => Err(general_err!("Invalid repetition {}", other)),
        }
    }
}

impl str::FromStr for Type {
    type Err = ParquetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "BOOLEAN" => Ok(Type::Boolean),
            "INT32" => Ok(Type::Int32),
            "INT64" => Ok(Type::Int64),
            "INT96" => Ok(Type::Int96),
            "FLOAT" => Ok(Type::Float),
            "DOUBLE" => Ok(Type::Double),
            "BYTE_ARRAY" | "BINARY" => Ok(Type::ByteArray),
            "FIXED_LEN_BYTE_ARRAY" => Ok(Type::FixedLenByteArray),
            other => Err(general_err!("Invalid type {}", other)),
        }
    }
}

impl str::FromStr for LogicalType {
    type Err = ParquetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NONE" => Ok(LogicalType::None),
            "UTF8" => Ok(LogicalType::Utf8),
            "MAP" => Ok(LogicalType::Map),
            "MAP_KEY_VALUE" => Ok(LogicalType::MapKeyValue),
            "LIST" => Ok(LogicalType::List),
            "ENUM" => Ok(LogicalType::Enum),
            "DECIMAL" => Ok(LogicalType::Decimal),
            "DATE" => Ok(LogicalType::Date),
            "TIME_MILLIS" => Ok(LogicalType::TimeMillis),
            "TIME_MICROS" => Ok(LogicalType::TimeMicros),
            "TIMESTAMP_MILLIS" => Ok(LogicalType::TimestampMillis),
            "TIMESTAMP_MICROS" => Ok(LogicalType::TimestampMicros),
            "UINT_8" => Ok(LogicalType::Uint8),
            "UINT_16" => Ok(LogicalType::Uint16),
            "UINT_32" => Ok(LogicalType::Uint32),
            "UINT_64" => Ok(LogicalType::Uint64),
            "INT_8" => Ok(LogicalType::Int8),
            "INT_16" => Ok(LogicalType::Int16),
            "INT_32" => Ok(LogicalType::Int32),
            "INT_64" => Ok(LogicalType::Int64),
            "JSON" => Ok(LogicalType::Json),
            "BSON" => Ok(LogicalType::Bson),
            "INTERVAL" => Ok(LogicalType::Interval),
            other => Err(general_err!("Invalid logical type {}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_type() {
        assert_eq!(Type::Boolean.to_string(), "BOOLEAN");
        assert_eq!(Type::Int32.to_string(), "INT32");
        assert_eq!(Type::Int64.to_string(), "INT64");
        assert_eq!(Type::Int96.to_string(), "INT96");
        assert_eq!(Type::Float.to_string(), "FLOAT");
        assert_eq!(Type::Double.to_string(), "DOUBLE");
        assert_eq!(Type::ByteArray.to_string(), "BYTE_ARRAY");
        assert_eq!(Type::FixedLenByteArray.to_string(), "FIXED_LEN_BYTE_ARRAY");
    }

    #[test]
    fn test_from_type() {
        assert_eq!(Type::from(parquet::Type::Boolean), Type::Boolean);
        assert_eq!(Type::from(parquet::Type::Int32), Type::Int32);
        assert_eq!(Type::from(parquet::Type::Int64), Type::Int64);
        assert_eq!(Type::from(parquet::Type::Int96), Type::Int96);
        assert_eq!(Type::from(parquet::Type::Float), Type::Float);
        assert_eq!(Type::from(parquet::Type::Double), Type::Double);
        assert_eq!(Type::from(parquet::Type::ByteArray), Type::ByteArray);
        assert_eq!(
            Type::from(parquet::Type::FixedLenByteArray),
            Type::FixedLenByteArray
        );
    }

    #[test]
    fn test_into_type() {
        assert_eq!(parquet::Type::Boolean, Type::Boolean.into());
        assert_eq!(parquet::Type::Int32, Type::Int32.into());
        assert_eq!(parquet::Type::Int64, Type::Int64.into());
        assert_eq!(parquet::Type::Int96, Type::Int96.into());
        assert_eq!(parquet::Type::Float, Type::Float.into());
        assert_eq!(parquet::Type::Double, Type::Double.into());
        assert_eq!(parquet::Type::ByteArray, Type::ByteArray.into());
        assert_eq!(
            parquet::Type::FixedLenByteArray,
            Type::FixedLenByteArray.into()
        );
    }

    #[test]
    fn test_from_string_into_type() {
        assert_eq!(
            Type::Boolean.to_string().parse::<Type>().unwrap(),
            Type::Boolean
        );
        assert_eq!(
            Type::Int32.to_string().parse::<Type>().unwrap(),
            Type::Int32
        );
        assert_eq!(
            Type::Int64.to_string().parse::<Type>().unwrap(),
            Type::Int64
        );
        assert_eq!(
            Type::Int96.to_string().parse::<Type>().unwrap(),
            Type::Int96
        );
        assert_eq!(
            Type::Float.to_string().parse::<Type>().unwrap(),
            Type::Float
        );
        assert_eq!(
            Type::Double.to_string().parse::<Type>().unwrap(),
            Type::Double
        );
        assert_eq!(
            Type::ByteArray.to_string().parse::<Type>().unwrap(),
            Type::ByteArray
        );
        assert_eq!("BINARY".parse::<Type>().unwrap(), Type::ByteArray);
        assert_eq!(
            Type::FixedLenByteArray.to_string().parse::<Type>().unwrap(),
            Type::FixedLenByteArray
        );
    }

    #[test]
    fn test_display_logical_type() {
        assert_eq!(LogicalType::None.to_string(), "NONE");
        assert_eq!(LogicalType::Utf8.to_string(), "UTF8");
        assert_eq!(LogicalType::Map.to_string(), "MAP");
        assert_eq!(LogicalType::MapKeyValue.to_string(), "MAP_KEY_VALUE");
        assert_eq!(LogicalType::List.to_string(), "LIST");
        assert_eq!(LogicalType::Enum.to_string(), "ENUM");
        assert_eq!(LogicalType::Decimal.to_string(), "DECIMAL");
        assert_eq!(LogicalType::Date.to_string(), "DATE");
        assert_eq!(LogicalType::TimeMillis.to_string(), "TIME_MILLIS");
        assert_eq!(LogicalType::Date.to_string(), "DATE");
        assert_eq!(LogicalType::TimeMicros.to_string(), "TIME_MICROS");
        assert_eq!(LogicalType::TimestampMillis.to_string(), "TIMESTAMP_MILLIS");
        assert_eq!(LogicalType::TimestampMicros.to_string(), "TIMESTAMP_MICROS");
        assert_eq!(LogicalType::Uint8.to_string(), "UINT_8");
        assert_eq!(LogicalType::Uint16.to_string(), "UINT_16");
        assert_eq!(LogicalType::Uint32.to_string(), "UINT_32");
        assert_eq!(LogicalType::Uint64.to_string(), "UINT_64");
        assert_eq!(LogicalType::Int8.to_string(), "INT_8");
        assert_eq!(LogicalType::Int16.to_string(), "INT_16");
        assert_eq!(LogicalType::Int32.to_string(), "INT_32");
        assert_eq!(LogicalType::Int64.to_string(), "INT_64");
        assert_eq!(LogicalType::Json.to_string(), "JSON");
        assert_eq!(LogicalType::Bson.to_string(), "BSON");
        assert_eq!(LogicalType::Interval.to_string(), "INTERVAL");
    }

    #[test]
    fn test_from_logical_type() {
        assert_eq!(LogicalType::from(None), LogicalType::None);
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Utf8)),
            LogicalType::Utf8
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Map)),
            LogicalType::Map
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::MapKeyValue)),
            LogicalType::MapKeyValue
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::List)),
            LogicalType::List
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Enum)),
            LogicalType::Enum
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Decimal)),
            LogicalType::Decimal
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Date)),
            LogicalType::Date
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::TimeMillis)),
            LogicalType::TimeMillis
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::TimeMicros)),
            LogicalType::TimeMicros
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::TimestampMillis)),
            LogicalType::TimestampMillis
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::TimestampMicros)),
            LogicalType::TimestampMicros
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Uint8)),
            LogicalType::Uint8
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Uint16)),
            LogicalType::Uint16
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Uint32)),
            LogicalType::Uint32
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Uint64)),
            LogicalType::Uint64
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Int8)),
            LogicalType::Int8
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Int16)),
            LogicalType::Int16
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Int32)),
            LogicalType::Int32
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Int64)),
            LogicalType::Int64
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Json)),
            LogicalType::Json
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Bson)),
            LogicalType::Bson
        );
        assert_eq!(
            LogicalType::from(Some(parquet::ConvertedType::Interval)),
            LogicalType::Interval
        );
    }

    #[test]
    fn test_into_logical_type() {
        let converted_type: Option<parquet::ConvertedType> = None;
        assert_eq!(converted_type, LogicalType::None.into());
        assert_eq!(Some(parquet::ConvertedType::Utf8), LogicalType::Utf8.into());
        assert_eq!(Some(parquet::ConvertedType::Map), LogicalType::Map.into());
        assert_eq!(
            Some(parquet::ConvertedType::MapKeyValue),
            LogicalType::MapKeyValue.into()
        );
        assert_eq!(Some(parquet::ConvertedType::List), LogicalType::List.into());
        assert_eq!(Some(parquet::ConvertedType::Enum), LogicalType::Enum.into());
        assert_eq!(
            Some(parquet::ConvertedType::Decimal),
            LogicalType::Decimal.into()
        );
        assert_eq!(Some(parquet::ConvertedType::Date), LogicalType::Date.into());
        assert_eq!(
            Some(parquet::ConvertedType::TimeMillis),
            LogicalType::TimeMillis.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::TimeMicros),
            LogicalType::TimeMicros.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::TimestampMillis),
            LogicalType::TimestampMillis.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::TimestampMicros),
            LogicalType::TimestampMicros.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Uint8),
            LogicalType::Uint8.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Uint16),
            LogicalType::Uint16.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Uint32),
            LogicalType::Uint32.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Uint64),
            LogicalType::Uint64.into()
        );
        assert_eq!(Some(parquet::ConvertedType::Int8), LogicalType::Int8.into());
        assert_eq!(
            Some(parquet::ConvertedType::Int16),
            LogicalType::Int16.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Int32),
            LogicalType::Int32.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Int64),
            LogicalType::Int64.into()
        );
        assert_eq!(Some(parquet::ConvertedType::Json), LogicalType::Json.into());
        assert_eq!(Some(parquet::ConvertedType::Bson), LogicalType::Bson.into());
        assert_eq!(
            Some(parquet::ConvertedType::Interval),
            LogicalType::Interval.into()
        );
    }

    #[test]
    fn test_from_string_into_logical_type() {
        assert_eq!(
            LogicalType::None
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::None
        );
        assert_eq!(
            LogicalType::Utf8
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Utf8
        );
        assert_eq!(
            LogicalType::Map.to_string().parse::<LogicalType>().unwrap(),
            LogicalType::Map
        );
        assert_eq!(
            LogicalType::MapKeyValue
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::MapKeyValue
        );
        assert_eq!(
            LogicalType::List
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::List
        );
        assert_eq!(
            LogicalType::Enum
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Enum
        );
        assert_eq!(
            LogicalType::Decimal
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Decimal
        );
        assert_eq!(
            LogicalType::Date
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Date
        );
        assert_eq!(
            LogicalType::TimeMillis
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::TimeMillis
        );
        assert_eq!(
            LogicalType::TimeMicros
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::TimeMicros
        );
        assert_eq!(
            LogicalType::TimestampMillis
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::TimestampMillis
        );
        assert_eq!(
            LogicalType::TimestampMicros
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::TimestampMicros
        );
        assert_eq!(
            LogicalType::Uint8
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Uint8
        );
        assert_eq!(
            LogicalType::Uint16
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Uint16
        );
        assert_eq!(
            LogicalType::Uint32
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Uint32
        );
        assert_eq!(
            LogicalType::Uint64
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Uint64
        );
        assert_eq!(
            LogicalType::Int8
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Int8
        );
        assert_eq!(
            LogicalType::Int16
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Int16
        );
        assert_eq!(
            LogicalType::Int32
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Int32
        );
        assert_eq!(
            LogicalType::Int64
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Int64
        );
        assert_eq!(
            LogicalType::Json
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Json
        );
        assert_eq!(
            LogicalType::Bson
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Bson
        );
        assert_eq!(
            LogicalType::Interval
                .to_string()
                .parse::<LogicalType>()
                .unwrap(),
            LogicalType::Interval
        );
    }

    #[test]
    fn test_display_repetition() {
        assert_eq!(Repetition::Required.to_string(), "REQUIRED");
        assert_eq!(Repetition::Optional.to_string(), "OPTIONAL");
        assert_eq!(Repetition::Repeated.to_string(), "REPEATED");
    }

    #[test]
    fn test_from_repetition() {
        assert_eq!(
            Repetition::from(parquet::FieldRepetitionType::Required),
            Repetition::Required
        );
        assert_eq!(
            Repetition::from(parquet::FieldRepetitionType::Optional),
            Repetition::Optional
        );
        assert_eq!(
            Repetition::from(parquet::FieldRepetitionType::Repeated),
            Repetition::Repeated
        );
    }

    #[test]
    fn test_into_repetition() {
        assert_eq!(
            parquet::FieldRepetitionType::Required,
            Repetition::Required.into()
        );
        assert_eq!(
            parquet::FieldRepetitionType::Optional,
            Repetition::Optional.into()
        );
        assert_eq!(
            parquet::FieldRepetitionType::Repeated,
            Repetition::Repeated.into()
        );
    }

    #[test]
    fn test_from_string_into_repetition() {
        assert_eq!(
            Repetition::Required
                .to_string()
                .parse::<Repetition>()
                .unwrap(),
            Repetition::Required
        );
        assert_eq!(
            Repetition::Optional
                .to_string()
                .parse::<Repetition>()
                .unwrap(),
            Repetition::Optional
        );
        assert_eq!(
            Repetition::Repeated
                .to_string()
                .parse::<Repetition>()
                .unwrap(),
            Repetition::Repeated
        );
    }

    #[test]
    fn test_display_encoding() {
        assert_eq!(Encoding::Plain.to_string(), "PLAIN");
        assert_eq!(Encoding::PlainDictionary.to_string(), "PLAIN_DICTIONARY");
        assert_eq!(Encoding::Rle.to_string(), "RLE");
        assert_eq!(Encoding::BitPacked.to_string(), "BIT_PACKED");
        assert_eq!(
            Encoding::DeltaBinaryPacked.to_string(),
            "DELTA_BINARY_PACKED"
        );
        assert_eq!(
            Encoding::DeltaLengthByteArray.to_string(),
            "DELTA_LENGTH_BYTE_ARRAY"
        );
        assert_eq!(Encoding::DeltaByteArray.to_string(), "DELTA_BYTE_ARRAY");
        assert_eq!(Encoding::RleDictionary.to_string(), "RLE_DICTIONARY");
    }

    #[test]
    fn test_from_encoding() {
        assert_eq!(Encoding::from(parquet::Encoding::Plain), Encoding::Plain);
        assert_eq!(
            Encoding::from(parquet::Encoding::PlainDictionary),
            Encoding::PlainDictionary
        );
        assert_eq!(Encoding::from(parquet::Encoding::Rle), Encoding::Rle);
        assert_eq!(
            Encoding::from(parquet::Encoding::BitPacked),
            Encoding::BitPacked
        );
        assert_eq!(
            Encoding::from(parquet::Encoding::DeltaBinaryPacked),
            Encoding::DeltaBinaryPacked
        );
        assert_eq!(
            Encoding::from(parquet::Encoding::DeltaLengthByteArray),
            Encoding::DeltaLengthByteArray
        );
        assert_eq!(
            Encoding::from(parquet::Encoding::DeltaByteArray),
            Encoding::DeltaByteArray
        );
    }

    #[test]
    fn test_into_encoding() {
        assert_eq!(parquet::Encoding::Plain, Encoding::Plain.into());
        assert_eq!(
            parquet::Encoding::PlainDictionary,
            Encoding::PlainDictionary.into()
        );
        assert_eq!(parquet::Encoding::Rle, Encoding::Rle.into());
        assert_eq!(parquet::Encoding::BitPacked, Encoding::BitPacked.into());
        assert_eq!(
            parquet::Encoding::DeltaBinaryPacked,
            Encoding::DeltaBinaryPacked.into()
        );
        assert_eq!(
            parquet::Encoding::DeltaLengthByteArray,
            Encoding::DeltaLengthByteArray.into()
        );
        assert_eq!(
            parquet::Encoding::DeltaByteArray,
            Encoding::DeltaByteArray.into()
        );
    }

    #[test]
    fn test_display_compression() {
        assert_eq!(Compression::Uncompressed.to_string(), "UNCOMPRESSED");
        assert_eq!(Compression::Snappy.to_string(), "SNAPPY");
        assert_eq!(Compression::Gzip.to_string(), "GZIP");
        assert_eq!(Compression::Lzo.to_string(), "LZO");
        assert_eq!(Compression::Brotli.to_string(), "BROTLI");
        assert_eq!(Compression::Lz4.to_string(), "LZ4");
        assert_eq!(Compression::Zstd.to_string(), "ZSTD");
    }

    #[test]
    fn test_from_compression() {
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Uncompressed),
            Compression::Uncompressed
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Snappy),
            Compression::Snappy
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Gzip),
            Compression::Gzip
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Lzo),
            Compression::Lzo
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Brotli),
            Compression::Brotli
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Lz4),
            Compression::Lz4
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Zstd),
            Compression::Zstd
        );
    }

    #[test]
    fn test_into_compression() {
        assert_eq!(
            parquet::CompressionCodec::Uncompressed,
            Compression::Uncompressed.into()
        );
        assert_eq!(
            parquet::CompressionCodec::Snappy,
            Compression::Snappy.into()
        );
        assert_eq!(parquet::CompressionCodec::Gzip, Compression::Gzip.into());
        assert_eq!(parquet::CompressionCodec::Lzo, Compression::Lzo.into());
        assert_eq!(
            parquet::CompressionCodec::Brotli,
            Compression::Brotli.into()
        );
        assert_eq!(parquet::CompressionCodec::Lz4, Compression::Lz4.into());
        assert_eq!(parquet::CompressionCodec::Zstd, Compression::Zstd.into());
    }

    #[test]
    fn test_display_page_type() {
        assert_eq!(PageType::DataPage.to_string(), "DATA_PAGE");
        assert_eq!(PageType::IndexPage.to_string(), "INDEX_PAGE");
        assert_eq!(PageType::DictionaryPage.to_string(), "DICTIONARY_PAGE");
        assert_eq!(PageType::DataPageV2.to_string(), "DATA_PAGE_V2");
    }

    #[test]
    fn test_from_page_type() {
        assert_eq!(
            PageType::from(parquet::PageType::DataPage),
            PageType::DataPage
        );
        assert_eq!(
            PageType::from(parquet::PageType::IndexPage),
            PageType::IndexPage
        );
        assert_eq!(
            PageType::from(parquet::PageType::DictionaryPage),
            PageType::DictionaryPage
        );
        assert_eq!(
            PageType::from(parquet::PageType::DataPageV2),
            PageType::DataPageV2
        );
    }

    #[test]
    fn test_into_page_type() {
        assert_eq!(parquet::PageType::DataPage, PageType::DataPage.into());
        assert_eq!(parquet::PageType::IndexPage, PageType::IndexPage.into());
        assert_eq!(
            parquet::PageType::DictionaryPage,
            PageType::DictionaryPage.into()
        );
        assert_eq!(parquet::PageType::DataPageV2, PageType::DataPageV2.into());
    }

    #[test]
    fn test_display_sort_order() {
        assert_eq!(SortOrder::Signed.to_string(), "SIGNED");
        assert_eq!(SortOrder::Unsigned.to_string(), "UNSIGNED");
        assert_eq!(SortOrder::Undefined.to_string(), "UNDEFINED");
    }

    #[test]
    fn test_display_column_order() {
        assert_eq!(
            ColumnOrder::TypeDefinedOrder(SortOrder::Signed).to_string(),
            "TYPE_DEFINED_ORDER(SIGNED)"
        );
        assert_eq!(
            ColumnOrder::TypeDefinedOrder(SortOrder::Unsigned).to_string(),
            "TYPE_DEFINED_ORDER(UNSIGNED)"
        );
        assert_eq!(
            ColumnOrder::TypeDefinedOrder(SortOrder::Undefined).to_string(),
            "TYPE_DEFINED_ORDER(UNDEFINED)"
        );
        assert_eq!(ColumnOrder::Undefined.to_string(), "UNDEFINED");
    }

    #[test]
    fn test_column_order_get_sort_order() {
        // Helper to check the order in a list of values.
        // Only logical type is checked.
        fn check_sort_order(types: Vec<LogicalType>, expected_order: SortOrder) {
            for tpe in types {
                assert_eq!(
                    ColumnOrder::get_sort_order(tpe, Type::ByteArray),
                    expected_order
                );
            }
        }

        // Unsigned comparison (physical type does not matter)
        let unsigned = vec![
            LogicalType::Utf8,
            LogicalType::Json,
            LogicalType::Bson,
            LogicalType::Enum,
            LogicalType::Uint8,
            LogicalType::Uint16,
            LogicalType::Uint32,
            LogicalType::Uint64,
            LogicalType::Interval,
        ];
        check_sort_order(unsigned, SortOrder::Unsigned);

        // Signed comparison (physical type does not matter)
        let signed = vec![
            LogicalType::Int8,
            LogicalType::Int16,
            LogicalType::Int32,
            LogicalType::Int64,
            LogicalType::Decimal,
            LogicalType::Date,
            LogicalType::TimeMillis,
            LogicalType::TimeMicros,
            LogicalType::TimestampMillis,
            LogicalType::TimestampMicros,
        ];
        check_sort_order(signed, SortOrder::Signed);

        // Undefined comparison
        let undefined = vec![
            LogicalType::List,
            LogicalType::Map,
            LogicalType::MapKeyValue,
        ];
        check_sort_order(undefined, SortOrder::Undefined);

        // Check None logical type
        // This should return a sort order for byte array type.
        check_sort_order(vec![LogicalType::None], SortOrder::Unsigned);
    }

    #[test]
    fn test_column_order_get_default_sort_order() {
        // Comparison based on physical type
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::Boolean),
            SortOrder::Unsigned
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::Int32),
            SortOrder::Signed
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::Int64),
            SortOrder::Signed
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::Int96),
            SortOrder::Undefined
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::Float),
            SortOrder::Signed
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::Double),
            SortOrder::Signed
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::ByteArray),
            SortOrder::Unsigned
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::FixedLenByteArray),
            SortOrder::Unsigned
        );
    }

    #[test]
    fn test_column_order_sort_order() {
        assert_eq!(
            ColumnOrder::TypeDefinedOrder(SortOrder::Signed).sort_order(),
            SortOrder::Signed
        );
        assert_eq!(
            ColumnOrder::TypeDefinedOrder(SortOrder::Unsigned).sort_order(),
            SortOrder::Unsigned
        );
        assert_eq!(
            ColumnOrder::TypeDefinedOrder(SortOrder::Undefined).sort_order(),
            SortOrder::Undefined
        );
        assert_eq!(ColumnOrder::Undefined.sort_order(), SortOrder::Signed);
    }
}
