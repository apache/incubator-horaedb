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

//! Datum holds different kind of data

use std::{
    convert::TryFrom,
    fmt,
    str::{self, from_utf8, Utf8Error},
};

use arrow::{
    datatypes::{DataType, TimeUnit},
    temporal_conversions::{EPOCH_DAYS_FROM_CE, NANOSECONDS},
};
use bytes_ext::Bytes;
use chrono::{Datelike, Local, NaiveDate, NaiveTime, TimeZone, Timelike};
use datafusion::scalar::ScalarValue;
use hash_ext::hash64;
use horaedbproto::schema::DataType as DataTypePb;
use serde::ser::{Serialize, Serializer};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use sqlparser::ast::{DataType as SqlDataType, Value};

use crate::{hex, string::StringBytes, time::Timestamp};

const DATE_FORMAT: &str = "%Y-%m-%d";
const TIME_FORMAT: &str = "%H:%M:%S%.3f";
const NULL_VALUE_FOR_HASH: u128 = u128::MAX;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported SQL data type, type:{sql_type}.\nBacktrace:\n{backtrace}"))]
    UnsupportedDataType {
        sql_type: SqlDataType,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid double or float, err:{source}.\nBacktrace:\n{backtrace}"))]
    InvalidDouble {
        source: std::num::ParseFloatError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid insert value, kind:{kind}, value:{value:?}.\nBacktrace:\n{backtrace}"
    ))]
    InvalidValueType {
        kind: DatumKind,
        value: Value,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid timestamp, err:{source}.\nBacktrace:\n{backtrace}"))]
    InvalidTimestamp {
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid date, err:{source}.\nBacktrace:\n{backtrace}"))]
    InvalidDate {
        source: chrono::ParseError,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid time, err:{source}.\nBacktrace:\n{backtrace}"))]
    InvalidTimeCause {
        source: chrono::ParseError,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid time, err:{source}.\nBacktrace:\n{backtrace}"))]
    InvalidTimeHourFormat {
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid time, err:{msg}.\nBacktrace:\n{backtrace}"))]
    InvalidTimeNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display("Invalid integer, err:{source}.\nBacktrace:\n{backtrace}"))]
    InvalidInt {
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid datum byte, byte:{value}.\nBacktrace:\n{backtrace}"))]
    InvalidDatumByte { value: u8, backtrace: Backtrace },

    #[snafu(display("Invalid hex value, hex_val:{hex_val}.\nBacktrace:\n{backtrace}"))]
    InvalidHexValue {
        hex_val: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid string encoding, that are not utf-8 characters input:{msg}, err:{source}. \nBacktrace:\n{backtrace}"))]
    InvalidStringEncoding {
        msg: String,
        source: Utf8Error,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

// Float wrapper over f32/f64. Just because we cannot build std::hash::Hash for
// floats directly we have to do it through type wrapper
// Fork from datafusion:
//  https://github.com/apache/arrow-datafusion/blob/1a0542acbc01e5243471ae0fc3586c2f1f40013b/datafusion/common/src/scalar.rs#L1493
struct Fl<T>(T);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(impl std::hash::Hash for Fl<$t> {
            #[inline]
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                state.write(&<$i>::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
            }
        })+
    };
}

hash_float_value!((f64, u64), (f32, u32));

// FIXME(yingwen): How to handle timezone?

/// Data type of datum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatumKind {
    Null = 0,
    Timestamp,
    Double,
    Float,
    Varbinary,
    String,
    UInt64,
    UInt32,
    UInt16,
    UInt8,
    Int64,
    Int32,
    Int16,
    Int8,
    Boolean,
    Date,
    Time,
}

impl DatumKind {
    pub const VALUES: [Self; 17] = [
        Self::Null,
        Self::Timestamp,
        Self::Double,
        Self::Float,
        Self::Varbinary,
        Self::String,
        Self::UInt64,
        Self::UInt32,
        Self::UInt16,
        Self::UInt8,
        Self::Int64,
        Self::Int32,
        Self::Int16,
        Self::Int8,
        Self::Boolean,
        Self::Date,
        Self::Time,
    ];

    /// Return true if this is DatumKind::Timestamp
    pub fn is_timestamp(&self) -> bool {
        matches!(self, DatumKind::Timestamp)
    }

    pub fn is_f64_castable(&self) -> bool {
        matches!(
            self,
            Self::Double
                | Self::Float
                | Self::UInt64
                | Self::UInt32
                | Self::UInt16
                | Self::UInt8
                | Self::Int64
                | Self::Int32
                | Self::Int16
                | Self::Int8
        )
    }

    /// Can column of this datum kind used as key column
    pub fn is_key_kind(&self) -> bool {
        matches!(
            self,
            DatumKind::Timestamp
                | DatumKind::Varbinary
                | DatumKind::String
                | DatumKind::UInt64
                | DatumKind::UInt32
                | DatumKind::UInt16
                | DatumKind::UInt8
                | DatumKind::Int64
                | DatumKind::Int32
                | DatumKind::Int16
                | DatumKind::Int8
                | DatumKind::Boolean
                | DatumKind::Date
                | DatumKind::Time
        )
    }

    /// Can column of this datum kind used as dictionary encode column
    pub fn is_dictionary_kind(&self) -> bool {
        matches!(self, DatumKind::String)
    }

    pub fn unsign_kind(&self) -> Option<Self> {
        match self {
            Self::Int64 | Self::UInt64 => Some(Self::UInt64),
            Self::Int32 | Self::UInt32 => Some(Self::UInt32),
            Self::Int16 | Self::UInt16 => Some(Self::UInt16),
            Self::Int8 | Self::UInt8 => Some(Self::UInt8),
            _ => None,
        }
    }

    /// Get name of this kind.
    fn as_str(&self) -> &str {
        match self {
            DatumKind::Null => "null",
            DatumKind::Timestamp => "timestamp",
            DatumKind::Double => "double",
            DatumKind::Float => "float",
            DatumKind::Varbinary => "varbinary",
            DatumKind::String => "string",
            DatumKind::UInt64 => "uint64",
            DatumKind::UInt32 => "uint32",
            DatumKind::UInt16 => "uint16",
            DatumKind::UInt8 => "uint8",
            DatumKind::Int64 => "bigint",
            DatumKind::Int32 => "int",
            DatumKind::Int16 => "smallint",
            DatumKind::Int8 => "tinyint",
            DatumKind::Boolean => "boolean",
            DatumKind::Date => "date",
            DatumKind::Time => "time",
        }
    }

    /// Convert into a byte.
    #[inline]
    pub fn into_u8(self) -> u8 {
        self as u8
    }

    /// Return None for variable-length type
    pub fn size(&self) -> Option<usize> {
        let size = match self {
            DatumKind::Null => 0,
            DatumKind::Timestamp => 8,
            DatumKind::Double => 8,
            DatumKind::Float => 4,
            DatumKind::Varbinary => return None,
            DatumKind::String => return None,
            DatumKind::UInt64 => 8,
            DatumKind::UInt32 => 4,
            DatumKind::UInt16 => 2,
            DatumKind::UInt8 => 1,
            DatumKind::Int64 => 8,
            DatumKind::Int32 => 4,
            DatumKind::Int16 => 2,
            DatumKind::Int8 => 1,
            DatumKind::Boolean => 1,
            DatumKind::Date => 4,
            DatumKind::Time => 8,
        };
        Some(size)
    }
}

impl TryFrom<&SqlDataType> for DatumKind {
    type Error = Error;

    fn try_from(sql_type: &SqlDataType) -> Result<Self> {
        match sql_type {
            // TODO(yingwen): Consider timezone
            SqlDataType::Timestamp(_, _) => Ok(Self::Timestamp),
            SqlDataType::Real | SqlDataType::Float(_) => Ok(Self::Float),
            SqlDataType::Double => Ok(Self::Double),
            SqlDataType::Boolean => Ok(Self::Boolean),
            SqlDataType::BigInt(_) => Ok(Self::Int64),
            SqlDataType::Int64 => Ok(Self::Int64),
            SqlDataType::Int(_) => Ok(Self::Int32),
            SqlDataType::Int8(_) => Ok(Self::Int8),
            SqlDataType::SmallInt(_) => Ok(Self::Int16),
            SqlDataType::String(_) => Ok(Self::String),
            SqlDataType::Varbinary(_) => Ok(Self::Varbinary),
            SqlDataType::Date => Ok(Self::Date),
            SqlDataType::Time(_, _) => Ok(Self::Time),
            SqlDataType::Custom(objects, _) if objects.0.len() == 1 => {
                match objects.0[0].value.as_str() {
                    "UINT64" | "uint64" => Ok(Self::UInt64),
                    "UINT32" | "uint32" => Ok(Self::UInt32),
                    "UINT16" | "uint16" => Ok(Self::UInt16),
                    "UINT8" | "uint8" => Ok(Self::UInt8),
                    "INT64" | "int64" => Ok(Self::Int64),
                    "INT32" | "int32" => Ok(Self::Int32),
                    "INT16" | "int16" => Ok(Self::Int16),
                    "TINYINT" | "INT8" | "tinyint" | "int8" => Ok(Self::Int8),
                    _ => UnsupportedDataType {
                        sql_type: sql_type.clone(),
                    }
                    .fail(),
                }
            }

            // Unlike datafusion, Decimal is not supported now
            _ => UnsupportedDataType {
                sql_type: sql_type.clone(),
            }
            .fail(),
        }
    }
}

impl TryFrom<u8> for DatumKind {
    type Error = Error;

    fn try_from(v: u8) -> Result<Self> {
        match v {
            v if DatumKind::Null.into_u8() == v => Ok(DatumKind::Null),
            v if DatumKind::Timestamp.into_u8() == v => Ok(DatumKind::Timestamp),
            v if DatumKind::Double.into_u8() == v => Ok(DatumKind::Double),
            v if DatumKind::Float.into_u8() == v => Ok(DatumKind::Float),
            v if DatumKind::Varbinary.into_u8() == v => Ok(DatumKind::Varbinary),
            v if DatumKind::String.into_u8() == v => Ok(DatumKind::String),
            v if DatumKind::UInt64.into_u8() == v => Ok(DatumKind::UInt64),
            v if DatumKind::UInt32.into_u8() == v => Ok(DatumKind::UInt32),
            v if DatumKind::UInt16.into_u8() == v => Ok(DatumKind::UInt16),
            v if DatumKind::UInt8.into_u8() == v => Ok(DatumKind::UInt8),
            v if DatumKind::Int64.into_u8() == v => Ok(DatumKind::Int64),
            v if DatumKind::Int32.into_u8() == v => Ok(DatumKind::Int32),
            v if DatumKind::Int16.into_u8() == v => Ok(DatumKind::Int16),
            v if DatumKind::Int8.into_u8() == v => Ok(DatumKind::Int8),
            v if DatumKind::Boolean.into_u8() == v => Ok(DatumKind::Boolean),
            v if DatumKind::Date.into_u8() == v => Ok(DatumKind::Date),
            v if DatumKind::Time.into_u8() == v => Ok(DatumKind::Time),
            _ => InvalidDatumByte { value: v }.fail(),
        }
    }
}

impl From<DatumKind> for DataTypePb {
    fn from(kind: DatumKind) -> Self {
        match kind {
            DatumKind::Null => Self::Null,
            DatumKind::Timestamp => Self::Timestamp,
            DatumKind::Double => Self::Double,
            DatumKind::Float => Self::Float,
            DatumKind::Varbinary => Self::Varbinary,
            DatumKind::String => Self::String,
            DatumKind::UInt64 => Self::Uint64,
            DatumKind::UInt32 => Self::Uint32,
            DatumKind::UInt16 => Self::Uint16,
            DatumKind::UInt8 => Self::Uint8,
            DatumKind::Int64 => Self::Int64,
            DatumKind::Int32 => Self::Int32,
            DatumKind::Int16 => Self::Int16,
            DatumKind::Int8 => Self::Int8,
            DatumKind::Boolean => Self::Bool,
            DatumKind::Date => Self::Date,
            DatumKind::Time => Self::Time,
        }
    }
}

impl From<DataTypePb> for DatumKind {
    fn from(data_type: DataTypePb) -> Self {
        match data_type {
            DataTypePb::Null => DatumKind::Null,
            DataTypePb::Timestamp => DatumKind::Timestamp,
            DataTypePb::Double => DatumKind::Double,
            DataTypePb::Float => DatumKind::Float,
            DataTypePb::Varbinary => DatumKind::Varbinary,
            DataTypePb::String => DatumKind::String,
            DataTypePb::Uint64 => DatumKind::UInt64,
            DataTypePb::Uint32 => DatumKind::UInt32,
            DataTypePb::Uint16 => DatumKind::UInt16,
            DataTypePb::Uint8 => DatumKind::UInt8,
            DataTypePb::Int64 => DatumKind::Int64,
            DataTypePb::Int32 => DatumKind::Int32,
            DataTypePb::Int16 => DatumKind::Int16,
            DataTypePb::Int8 => DatumKind::Int8,
            DataTypePb::Bool => DatumKind::Boolean,
            DataTypePb::Date => DatumKind::Date,
            DataTypePb::Time => DatumKind::Time,
        }
    }
}

impl fmt::Display for DatumKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// FIXME(yingwen): Validate the length of string and varbinary.
/// A data box holds different kind of data
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Datum {
    Null,
    /// Millisecond precision
    ///
    /// Map to arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond,
    /// None)
    Timestamp(Timestamp),
    /// Map to arrow::datatypes::DataType::Float64
    Double(f64),
    /// Map to arrow::datatypes::DataType::Float32
    Float(f32),
    /// Map to arrow::datatypes::DateType::Binary
    ///
    /// No more than 2G (size of i32)
    Varbinary(Bytes),
    /// Map to arrow::datatypes::DataType::String
    ///
    /// No more than 2G (size of i32)
    String(StringBytes),
    /// Map to arrow::datatypes::DataType::UInt64
    UInt64(u64),
    UInt32(u32),
    UInt16(u16),
    UInt8(u8),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Int8(i8),
    Boolean(bool),
    /// Date represents the elapsed days since UNIX epoch.
    /// It is mapped to [`arrow::datatypes::DataType::Date32`].
    /// The supported date range is '-9999-01-01' to '9999-12-31'.
    Date(i32),
    /// Time represents the elapsed nanoseconds since midnight.
    /// It is mapped to [`arrow::datatypes::DataType::Time64`].
    /// The supported time range is '-838:59:59.000000' to '838:59:59.000000'.
    Time(i64),
}

impl Datum {
    /// Creates an empty datum by given datum kind
    pub fn empty(kind: &DatumKind) -> Self {
        match kind {
            DatumKind::Null => Self::Null,
            DatumKind::Timestamp => Self::Timestamp(Timestamp::new(0)),
            DatumKind::Double => Self::Double(0.0),
            DatumKind::Float => Self::Float(0.0),
            DatumKind::Varbinary => Self::Varbinary(Bytes::new()),
            DatumKind::String => Self::String(StringBytes::new()),
            DatumKind::UInt64 => Self::UInt64(0),
            DatumKind::UInt32 => Self::UInt32(0),
            DatumKind::UInt16 => Self::UInt16(0),
            DatumKind::UInt8 => Self::UInt8(0),
            DatumKind::Int64 => Self::Int64(0),
            DatumKind::Int32 => Self::Int32(0),
            DatumKind::Int16 => Self::Int16(0),
            DatumKind::Int8 => Self::Int8(0),
            DatumKind::Boolean => Self::Boolean(false),
            DatumKind::Date => Self::Date(0),
            DatumKind::Time => Self::Time(0),
        }
    }

    /// Return the kind of datum
    pub fn kind(&self) -> DatumKind {
        match self {
            Datum::Null => DatumKind::Null,
            Datum::Timestamp(_) => DatumKind::Timestamp,
            Datum::Double(_) => DatumKind::Double,
            Datum::Float(_) => DatumKind::Float,
            Datum::Varbinary(_) => DatumKind::Varbinary,
            Datum::String(_) => DatumKind::String,
            Datum::UInt64(_) => DatumKind::UInt64,
            Datum::UInt32(_) => DatumKind::UInt32,
            Datum::UInt16(_) => DatumKind::UInt16,
            Datum::UInt8(_) => DatumKind::UInt8,
            Datum::Int64(_) => DatumKind::Int64,
            Datum::Int32(_) => DatumKind::Int32,
            Datum::Int16(_) => DatumKind::Int16,
            Datum::Int8(_) => DatumKind::Int8,
            Datum::Boolean(_) => DatumKind::Boolean,
            Datum::Date(_) => DatumKind::Date,
            Datum::Time(_) => DatumKind::Time,
        }
    }

    // TODO: handle error
    pub fn convert_to_uint64(&self) -> u64 {
        match self {
            Datum::Null => 0,
            Datum::Timestamp(v) => v.as_i64() as u64,
            Datum::Double(v) => *v as u64,
            Datum::Float(v) => *v as u64,
            Datum::Varbinary(v) => hash64(&v[..]),
            Datum::String(v) => hash64(v.as_bytes()),
            Datum::UInt64(v) => *v,
            Datum::UInt32(v) => *v as u64,
            Datum::UInt16(v) => *v as u64,
            Datum::UInt8(v) => *v as u64,
            Datum::Int64(v) => *v as u64,
            Datum::Int32(v) => *v as u64,
            Datum::Int16(v) => *v as u64,
            Datum::Int8(v) => *v as u64,
            Datum::Boolean(v) => *v as u64,
            Datum::Date(v) => *v as u64,
            Datum::Time(v) => *v as u64,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    /// Cast datum to timestamp.
    pub fn as_timestamp(&self) -> Option<Timestamp> {
        match self {
            Datum::Time(v) => Some(Timestamp::new(*v)),
            Datum::Timestamp(v) => Some(*v),
            _ => None,
        }
    }

    /// Cast datum to &str.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Datum::String(v) => Some(v),
            _ => None,
        }
    }

    /// Cast datum to uint64.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Datum::UInt64(v) => Some(*v),
            Datum::UInt32(v) => Some(*v as u64),
            Datum::UInt16(v) => Some(*v as u64),
            Datum::UInt8(v) => Some(*v as u64),
            Datum::Int64(v) => Some(*v as u64),
            Datum::Int32(v) => Some(*v as u64),
            Datum::Int16(v) => Some(*v as u64),
            Datum::Int8(v) => Some(*v as u64),
            Datum::Boolean(v) => Some(*v as u64),
            _ => None,
        }
    }

    /// Cast datum to int64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Datum::UInt64(v) => Some(*v as i64),
            Datum::UInt32(v) => Some(*v as i64),
            Datum::UInt16(v) => Some(*v as i64),
            Datum::UInt8(v) => Some(*v as i64),
            Datum::Int64(v) => Some(*v),
            Datum::Int32(v) => Some(*v as i64),
            Datum::Int16(v) => Some(*v as i64),
            Datum::Int8(v) => Some(*v as i64),
            Datum::Boolean(v) => Some(*v as i64),
            Datum::Date(v) => Some(*v as i64),
            Datum::Time(v) => Some(*v),
            _ => None,
        }
    }

    /// Cast datum to Bytes.
    pub fn as_varbinary(&self) -> Option<&Bytes> {
        match self {
            Datum::Varbinary(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Datum::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Datum::Double(v) => Some(*v),
            Datum::Float(v) => Some(*v as f64),
            Datum::UInt64(v) => Some(*v as f64),
            Datum::UInt32(v) => Some(*v as f64),
            Datum::UInt16(v) => Some(*v as f64),
            Datum::UInt8(v) => Some(*v as f64),
            Datum::Int64(v) => Some(*v as f64),
            Datum::Int32(v) => Some(*v as f64),
            Datum::Date(v) => Some(*v as f64),
            Datum::Time(v) => Some(*v as f64),
            Datum::Int16(v) => Some(*v as f64),
            Datum::Int8(v) => Some(*v as f64),
            Datum::Boolean(_)
            | Datum::Null
            | Datum::Timestamp(_)
            | Datum::Varbinary(_)
            | Datum::String(_) => None,
        }
    }

    pub fn do_with_bytes<F>(&self, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        match self {
            Datum::Double(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            Datum::Float(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            Datum::UInt64(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            Datum::UInt32(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            Datum::UInt16(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            Datum::UInt8(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            Datum::Int64(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            Datum::Int32(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            Datum::Int16(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            Datum::Int8(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            Datum::Boolean(v) => {
                if *v {
                    f(&[1])
                } else {
                    f(&[0])
                }
            }
            Datum::Null => f(&[0]),
            Datum::Timestamp(v) => {
                let arr = v.as_i64().to_le_bytes();
                f(arr.as_slice())
            }
            Datum::Varbinary(v) => f(v.as_ref()),
            Datum::String(v) => f(v.as_bytes()),
            Datum::Date(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            Datum::Time(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Datum::Double(v) => v.to_le_bytes().to_vec(),
            Datum::Float(v) => v.to_le_bytes().to_vec(),
            Datum::UInt64(v) => v.to_le_bytes().to_vec(),
            Datum::UInt32(v) => v.to_le_bytes().to_vec(),
            Datum::UInt16(v) => v.to_le_bytes().to_vec(),
            Datum::UInt8(v) => v.to_le_bytes().to_vec(),
            Datum::Int64(v) => v.to_le_bytes().to_vec(),
            Datum::Int32(v) => v.to_le_bytes().to_vec(),
            Datum::Int16(v) => v.to_le_bytes().to_vec(),
            Datum::Int8(v) => v.to_le_bytes().to_vec(),
            Datum::Boolean(v) => {
                if *v {
                    vec![1]
                } else {
                    vec![0]
                }
            }
            Datum::Null => vec![0],
            Datum::Timestamp(ts) => ts.as_i64().to_le_bytes().to_vec(),
            Datum::Varbinary(b) => b.to_vec(),
            Datum::String(string) => string.as_bytes().to_vec(),
            Datum::Date(v) => v.to_le_bytes().to_vec(),
            Datum::Time(v) => v.to_le_bytes().to_vec(),
        }
    }

    /// Generate a negative datum if possible.
    ///
    /// It will return `None` if:
    /// - The data type has no negative value.
    /// - The negative value overflows.
    pub fn to_negative(self) -> Option<Self> {
        match self {
            Datum::Null => None,
            Datum::Timestamp(_) => None,
            Datum::Double(v) => Some(Datum::Double(-v)),
            Datum::Float(v) => Some(Datum::Float(-v)),
            Datum::Varbinary(_) => None,
            Datum::String(_) => None,
            Datum::UInt64(_) => None,
            Datum::UInt32(_) => None,
            Datum::UInt16(_) => None,
            Datum::UInt8(_) => None,
            Datum::Int64(v) => 0i64.checked_sub(v).map(Datum::Int64),
            Datum::Int32(v) => 0i32.checked_sub(v).map(Datum::Int32),
            Datum::Int16(v) => 0i16.checked_sub(v).map(Datum::Int16),
            Datum::Int8(v) => 0i8.checked_sub(v).map(Datum::Int8),
            Datum::Boolean(v) => Some(Datum::Boolean(!v)),
            Datum::Date(_) => None,
            Datum::Time(_) => None,
        }
    }

    pub fn display_string(&self) -> String {
        match self {
            Datum::Null => "null".to_string(),
            Datum::Timestamp(v) => Local.timestamp_millis_opt(v.as_i64()).unwrap().to_rfc3339(),
            Datum::Double(v) => v.to_string(),
            Datum::Float(v) => v.to_string(),
            Datum::Varbinary(v) => format!("{v:?}"),
            Datum::String(v) => v.to_string(),
            Datum::UInt64(v) => v.to_string(),
            Datum::UInt32(v) => v.to_string(),
            Datum::UInt16(v) => v.to_string(),
            Datum::UInt8(v) => v.to_string(),
            Datum::Int64(v) => v.to_string(),
            Datum::Int32(v) => v.to_string(),
            Datum::Int16(v) => v.to_string(),
            Datum::Int8(v) => v.to_string(),
            Datum::Boolean(v) => v.to_string(),
            // Display the Date(32 bits) as String.
            // Date(v) represent the days from Unix epoch(1970-01-01),
            // so it is necessary to add `EPOCH_DAYS_FROM_CE` to generate
            // `NaiveDate`.
            Datum::Date(v) => NaiveDate::from_num_days_from_ce_opt((*v) + EPOCH_DAYS_FROM_CE)
                .unwrap()
                .to_string(),

            Datum::Time(v) => Datum::format_datum_time(v),
        }
    }

    fn valid_is_utf8(s: &str) -> Result<()> {
        from_utf8(s.as_bytes()).context(InvalidStringEncoding { msg: s })?;
        Ok(())
    }

    pub fn try_from_sql_value(kind: &DatumKind, value: Value) -> Result<Datum> {
        match (kind, value) {
            (DatumKind::Null, Value::Null) => Ok(Datum::Null),
            (DatumKind::Timestamp, Value::Number(n, _long)) => {
                let n = n.parse::<i64>().context(InvalidTimestamp)?;
                Ok(Datum::Timestamp(Timestamp::new(n)))
            }
            (DatumKind::Date, Value::SingleQuotedString(s)) => {
                let date = Self::parse_datum_date_from_str(&s)?;
                Ok(date)
            }
            (DatumKind::Time, Value::SingleQuotedString(s)) => {
                let datum_time = Self::parse_datum_time_from_str(&s)?;
                Ok(datum_time)
            }
            (DatumKind::Double, Value::Number(n, _long)) => {
                let n = n.parse::<f64>().context(InvalidDouble)?;
                Ok(Datum::Double(n))
            }
            (DatumKind::Float, Value::Number(n, _long)) => {
                let n = n.parse::<f32>().context(InvalidDouble)?;
                Ok(Datum::Float(n))
            }
            // TODO(yingwen): Support hex string.
            (DatumKind::Varbinary, Value::SingleQuotedString(s)) => {
                Self::valid_is_utf8(&s)?;
                Ok(Datum::Varbinary(Bytes::from(s)))
            }
            (DatumKind::String, Value::SingleQuotedString(s)) => {
                Self::valid_is_utf8(&s)?;
                Ok(Datum::String(StringBytes::from(s)))
            }
            (DatumKind::Varbinary, Value::DoubleQuotedString(s)) => {
                Self::valid_is_utf8(&s)?;
                Ok(Datum::Varbinary(Bytes::from(s)))
            }
            (DatumKind::Varbinary, Value::HexStringLiteral(s)) => {
                let bytes = hex::try_decode(&s).context(InvalidHexValue { hex_val: s })?;
                Ok(Datum::Varbinary(Bytes::from(bytes)))
            }
            (DatumKind::String, Value::DoubleQuotedString(s)) => {
                Self::valid_is_utf8(&s)?;
                Ok(Datum::String(StringBytes::from(s)))
            }
            (DatumKind::UInt64, Value::Number(n, _long)) => {
                let n = n.parse::<u64>().context(InvalidInt)?;
                Ok(Datum::UInt64(n))
            }
            (DatumKind::UInt32, Value::Number(n, _long)) => {
                let n = n.parse::<u32>().context(InvalidInt)?;
                Ok(Datum::UInt32(n))
            }
            (DatumKind::UInt16, Value::Number(n, _long)) => {
                let n = n.parse::<u16>().context(InvalidInt)?;
                Ok(Datum::UInt16(n))
            }
            (DatumKind::UInt8, Value::Number(n, _long)) => {
                let n = n.parse::<u8>().context(InvalidInt)?;
                Ok(Datum::UInt8(n))
            }
            (DatumKind::Int64, Value::Number(n, _long)) => {
                let n = n.parse::<i64>().context(InvalidInt)?;
                Ok(Datum::Int64(n))
            }
            (DatumKind::Int32, Value::Number(n, _long)) => {
                let n = n.parse::<i32>().context(InvalidInt)?;
                Ok(Datum::Int32(n))
            }
            (DatumKind::Int16, Value::Number(n, _long)) => {
                let n = n.parse::<i16>().context(InvalidInt)?;
                Ok(Datum::Int16(n))
            }
            (DatumKind::Int8, Value::Number(n, _long)) => {
                let n = n.parse::<i8>().context(InvalidInt)?;
                Ok(Datum::Int8(n))
            }
            (DatumKind::Boolean, Value::Boolean(b)) => Ok(Datum::Boolean(b)),
            (_, value) => InvalidValueType { kind: *kind, value }.fail(),
        }
    }

    /// format the `Datum::Time`(64 bits) as String.
    /// Time represent the nanoseconds from midnight,
    /// so it is necessary to split `v` into seconds and nanoseconds to
    /// generate `NaiveTime`.
    pub fn format_datum_time(v: &i64) -> String {
        let abs_nanos = (*v).abs();
        let hours = abs_nanos / 3600 / NANOSECONDS;
        let time = NaiveTime::from_num_seconds_from_midnight_opt(
            (abs_nanos / NANOSECONDS - hours * 3600) as u32,
            (abs_nanos % NANOSECONDS) as u32,
        )
        .unwrap();
        let minute_sec = &(time.to_string())[3..];
        if *v < 0 {
            format!("-{hours:02}:{minute_sec}")
        } else {
            format!("{hours:02}:{minute_sec}")
        }
    }

    /// format the `Datum::Date`(32 bits) as String.
    fn format_datum_date(v: &i32) -> String {
        NaiveDate::from_num_days_from_ce_opt((*v) + EPOCH_DAYS_FROM_CE)
            .unwrap()
            .format(DATE_FORMAT)
            .to_string()
    }

    fn parse_datum_time_from_str(s: &str) -> Result<Datum> {
        // `NaiveTime` contains two parts: `num_seconds_from_midnight`
        // and `nanoseconds`, it is necessary to
        // calculate them into number of nanoseconds from midnight.
        if let Some(index) = s.find(':') {
            let hours: i64 = (s[..index]).parse().context(InvalidTimeHourFormat)?;
            let replace = format!("00:{}", &s[index + 1..]);
            let time =
                NaiveTime::parse_from_str(&replace, TIME_FORMAT).context(InvalidTimeCause)?;
            let sec = hours.abs() * 3600 + (time.num_seconds_from_midnight() as i64);
            let nanos = time.nanosecond() as i64 + sec * NANOSECONDS;
            let nanos = if hours < 0 { -nanos } else { nanos };
            Ok(Datum::Time(nanos))
        } else {
            InvalidTimeNoCause {
                msg: "Invalid time format".to_string(),
            }
            .fail()
        }
    }

    fn parse_datum_date_from_str(s: &str) -> Result<Datum> {
        // `NaiveDate::num_days_from_ce()` returns the elapsed time
        // since 0001-01-01 in days, it is necessary to
        // subtract `EPOCH_DAYS_FROM_CE` to generate `Datum::Date`
        let date = chrono::NaiveDate::parse_from_str(s, DATE_FORMAT).context(InvalidDate)?;
        let days = date.num_days_from_ce() - EPOCH_DAYS_FROM_CE;
        Ok(Datum::Date(days))
    }

    pub fn is_fixed_sized(&self) -> bool {
        match self {
            Datum::Null
            | Datum::Timestamp(_)
            | Datum::Double(_)
            | Datum::Float(_)
            | Datum::UInt64(_)
            | Datum::UInt32(_)
            | Datum::UInt16(_)
            | Datum::UInt8(_)
            | Datum::Int64(_)
            | Datum::Int32(_)
            | Datum::Int16(_)
            | Datum::Int8(_)
            | Datum::Boolean(_)
            | Datum::Date(_)
            | Datum::Time(_) => true,
            Datum::Varbinary(_) | Datum::String(_) => false,
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Datum::Null => 1,
            Datum::Timestamp(_) => 8,
            Datum::Double(_) => 8,
            Datum::Float(_) => 4,
            Datum::Varbinary(v) => v.len(),
            Datum::String(v) => v.len(),
            Datum::UInt64(_) => 8,
            Datum::UInt32(_) => 4,
            Datum::UInt16(_) => 2,
            Datum::UInt8(_) => 1,
            Datum::Int64(_) => 8,
            Datum::Int32(_) => 4,
            Datum::Int16(_) => 2,
            Datum::Int8(_) => 1,
            Datum::Boolean(_) => 1,
            Datum::Date(_) => 4,
            Datum::Time(_) => 8,
        }
    }

    pub fn as_view(&self) -> DatumView {
        match self {
            Datum::Null => DatumView::Null,
            Datum::Timestamp(v) => DatumView::Timestamp(*v),
            Datum::Double(v) => DatumView::Double(*v),
            Datum::Float(v) => DatumView::Float(*v),
            Datum::Varbinary(v) => DatumView::Varbinary(v),
            Datum::String(v) => DatumView::String(v),
            Datum::UInt64(v) => DatumView::UInt64(*v),
            Datum::UInt32(v) => DatumView::UInt32(*v),
            Datum::UInt16(v) => DatumView::UInt16(*v),
            Datum::UInt8(v) => DatumView::UInt8(*v),
            Datum::Int64(v) => DatumView::Int64(*v),
            Datum::Int32(v) => DatumView::Int32(*v),
            Datum::Date(v) => DatumView::Date(*v),
            Datum::Time(v) => DatumView::Time(*v),
            Datum::Int16(v) => DatumView::Int16(*v),
            Datum::Int8(v) => DatumView::Int8(*v),
            Datum::Boolean(v) => DatumView::Boolean(*v),
        }
    }
}

macro_rules! impl_from {
    ($Kind: ident, $FromType: ident) => {
        impl From<$FromType> for Datum {
            fn from(value: $FromType) -> Self {
                Self::$Kind(value)
            }
        }

        impl From<Option<$FromType>> for Datum {
            fn from(value_opt: Option<$FromType>) -> Self {
                match value_opt {
                    Some(value) => Self::$Kind(value),
                    None => Self::Null,
                }
            }
        }
    };
}

impl_from!(Timestamp, Timestamp);
impl_from!(Double, f64);
impl_from!(Float, f32);
impl_from!(Varbinary, Bytes);
impl_from!(String, StringBytes);
impl_from!(UInt64, u64);
impl_from!(UInt32, u32);
impl_from!(UInt16, u16);
impl_from!(UInt8, u8);
impl_from!(Int64, i64);
impl_from!(Int32, i32);
impl_from!(Int16, i16);
impl_from!(Int8, i8);
impl_from!(Boolean, bool);

impl From<&str> for Datum {
    fn from(value: &str) -> Datum {
        Datum::String(StringBytes::copy_from_str(value))
    }
}

impl From<Option<&str>> for Datum {
    fn from(value_opt: Option<&str>) -> Datum {
        match value_opt {
            Some(value) => Datum::String(StringBytes::copy_from_str(value)),
            None => Datum::Null,
        }
    }
}

impl From<&[u8]> for Datum {
    fn from(value: &[u8]) -> Datum {
        Datum::Varbinary(Bytes::copy_from_slice(value))
    }
}

impl From<Option<&[u8]>> for Datum {
    fn from(value_opt: Option<&[u8]>) -> Datum {
        match value_opt {
            Some(value) => Datum::Varbinary(Bytes::copy_from_slice(value)),
            None => Datum::Null,
        }
    }
}

/// impl serde serialize for Datum
impl Serialize for Datum {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Datum::Null => serializer.serialize_none(),
            Datum::Timestamp(v) => serializer.serialize_i64(v.as_i64()),
            Datum::Double(v) => serializer.serialize_f64(*v),
            Datum::Float(v) => serializer.serialize_f32(*v),
            Datum::Varbinary(v) => serializer.serialize_bytes(v),
            Datum::String(v) => serializer.serialize_str(v),
            Datum::UInt64(v) => serializer.serialize_u64(*v),
            Datum::UInt32(v) => serializer.serialize_u32(*v),
            Datum::UInt16(v) => serializer.serialize_u16(*v),
            Datum::UInt8(v) => serializer.serialize_u8(*v),
            Datum::Int64(v) => serializer.serialize_i64(*v),
            Datum::Int32(v) => serializer.serialize_i32(*v),
            Datum::Int16(v) => serializer.serialize_i16(*v),
            Datum::Int8(v) => serializer.serialize_i8(*v),
            Datum::Boolean(v) => serializer.serialize_bool(*v),
            Datum::Date(v) => serializer.serialize_str(Self::format_datum_date(v).as_ref()),
            Datum::Time(v) => serializer.serialize_str(Datum::format_datum_time(v).as_ref()),
        }
    }
}

/// A view to a datum.
///
/// Holds copy of integer like datum and reference of string like datum.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum DatumView<'a> {
    Null,
    Timestamp(Timestamp),
    Double(f64),
    Float(f32),
    Varbinary(&'a [u8]),
    String(&'a str),
    UInt64(u64),
    UInt32(u32),
    UInt16(u16),
    UInt8(u8),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Int8(i8),
    Boolean(bool),
    Date(i32),
    Time(i64),
}

impl<'a> DatumView<'a> {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, DatumView::Null)
    }

    /// Return the kind of datum
    pub fn kind(&self) -> DatumKind {
        match self {
            DatumView::Null => DatumKind::Null,
            DatumView::Timestamp(_) => DatumKind::Timestamp,
            DatumView::Double(_) => DatumKind::Double,
            DatumView::Float(_) => DatumKind::Float,
            DatumView::Varbinary(_) => DatumKind::Varbinary,
            DatumView::String(_) => DatumKind::String,
            DatumView::UInt64(_) => DatumKind::UInt64,
            DatumView::UInt32(_) => DatumKind::UInt32,
            DatumView::UInt16(_) => DatumKind::UInt16,
            DatumView::UInt8(_) => DatumKind::UInt8,
            DatumView::Int64(_) => DatumKind::Int64,
            DatumView::Int32(_) => DatumKind::Int32,
            DatumView::Int16(_) => DatumKind::Int16,
            DatumView::Int8(_) => DatumKind::Int8,
            DatumView::Boolean(_) => DatumKind::Boolean,
            DatumView::Date(_) => DatumKind::Date,
            DatumView::Time(_) => DatumKind::Time,
        }
    }

    pub fn do_with_bytes<F>(&self, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        match self {
            DatumView::Double(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::Float(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::UInt64(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::UInt32(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::UInt16(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::UInt8(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::Int64(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::Int32(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::Int16(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::Int8(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::Boolean(v) => {
                if *v {
                    f(&[1])
                } else {
                    f(&[0])
                }
            }
            DatumView::Null => f(&[0]),
            DatumView::Timestamp(v) => {
                let arr = v.as_i64().to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::Varbinary(v) => f(v),
            DatumView::String(v) => f(v.as_bytes()),
            DatumView::Date(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
            DatumView::Time(v) => {
                let arr = v.to_le_bytes();
                f(arr.as_slice())
            }
        }
    }

    pub fn to_datum(&self) -> Datum {
        match self {
            DatumView::Null => Datum::Null,
            DatumView::Timestamp(v) => Datum::Timestamp(*v),
            DatumView::Double(v) => Datum::Double(*v),
            DatumView::Float(v) => Datum::Float(*v),
            DatumView::Varbinary(v) => Datum::Varbinary(Bytes::from(v.to_vec())),
            DatumView::String(v) => Datum::String(StringBytes::copy_from_str(v)),
            DatumView::UInt64(v) => Datum::UInt64(*v),
            DatumView::UInt32(v) => Datum::UInt32(*v),
            DatumView::UInt16(v) => Datum::UInt16(*v),
            DatumView::UInt8(v) => Datum::UInt8(*v),
            DatumView::Int64(v) => Datum::Int64(*v),
            DatumView::Int32(v) => Datum::Int32(*v),
            DatumView::Int16(v) => Datum::Int16(*v),
            DatumView::Int8(v) => Datum::Int8(*v),
            DatumView::Boolean(v) => Datum::Boolean(*v),
            DatumView::Date(v) => Datum::Date(*v),
            DatumView::Time(v) => Datum::Time(*v),
        }
    }

    pub fn as_date_i32(&self) -> Option<i32> {
        match self {
            DatumView::Date(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i8(&self) -> Option<i8> {
        match self {
            DatumView::Int8(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i16(&self) -> Option<i16> {
        match self {
            DatumView::Int16(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            DatumView::Int32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            DatumView::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u8(&self) -> Option<u8> {
        match self {
            DatumView::UInt8(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u16(&self) -> Option<u16> {
        match self {
            DatumView::UInt16(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> Option<u32> {
        match self {
            DatumView::UInt32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            DatumView::UInt64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            DatumView::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<Timestamp> {
        match self {
            DatumView::Timestamp(v) => Some(*v),
            DatumView::Time(v) => Some(Timestamp::new(*v)),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            DatumView::Double(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            DatumView::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn into_str(self) -> Option<&'a str> {
        match self {
            DatumView::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_bytes(self) -> Option<&'a [u8]> {
        match self {
            DatumView::Varbinary(v) => Some(v),
            _ => None,
        }
    }
}

impl<'a> std::hash::Hash for DatumView<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            DatumView::Null => NULL_VALUE_FOR_HASH.hash(state),
            DatumView::Timestamp(v) => v.hash(state),
            DatumView::Double(v) => Fl(*v).hash(state),
            DatumView::Float(v) => Fl(*v).hash(state),
            DatumView::Varbinary(v) => v.hash(state),
            DatumView::String(v) => v.hash(state),
            DatumView::UInt64(v) => v.hash(state),
            DatumView::UInt32(v) => v.hash(state),
            DatumView::UInt16(v) => v.hash(state),
            DatumView::UInt8(v) => v.hash(state),
            DatumView::Int64(v) => v.hash(state),
            DatumView::Int32(v) => v.hash(state),
            DatumView::Int16(v) => v.hash(state),
            DatumView::Int8(v) => v.hash(state),
            DatumView::Boolean(v) => v.hash(state),
            DatumView::Date(v) => v.hash(state),
            DatumView::Time(v) => v.hash(state),
        }
    }
}

impl DatumKind {
    /// Create DatumKind from [arrow::datatypes::DataType], if
    /// the type is not supported, returns None
    pub fn from_data_type(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Null => Some(Self::Null),
            DataType::Timestamp(TimeUnit::Millisecond, None) => Some(Self::Timestamp),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => Some(Self::Timestamp),
            DataType::Float64 => Some(Self::Double),
            DataType::Float32 => Some(Self::Float),
            DataType::Binary => Some(Self::Varbinary),
            DataType::Utf8 => Some(Self::String),
            DataType::UInt64 => Some(Self::UInt64),
            DataType::UInt32 => Some(Self::UInt32),
            DataType::UInt16 => Some(Self::UInt16),
            DataType::UInt8 => Some(Self::UInt8),
            DataType::Int64 => Some(Self::Int64),
            DataType::Int32 => Some(Self::Int32),
            DataType::Int16 => Some(Self::Int16),
            DataType::Int8 => Some(Self::Int8),
            DataType::Boolean => Some(Self::Boolean),
            DataType::Date32 => Some(Self::Date),
            DataType::Time64(TimeUnit::Nanosecond) => Some(Self::Time),
            DataType::Dictionary(_, _) => Some(Self::String),
            DataType::Float16
            | DataType::LargeUtf8
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            | DataType::Struct(_)
            | DataType::Union(_, _)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Date64
            | DataType::Interval(_)
            | DataType::Duration(_)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::RunEndEncoded(_, _)
            | DataType::Map(_, _) => None,
        }
    }

    pub fn to_arrow_data_type(&self) -> DataType {
        match self {
            DatumKind::Null => DataType::Null,
            DatumKind::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, None),
            DatumKind::Double => DataType::Float64,
            DatumKind::Float => DataType::Float32,
            DatumKind::Varbinary => DataType::Binary,
            DatumKind::String => DataType::Utf8,
            DatumKind::UInt64 => DataType::UInt64,
            DatumKind::UInt32 => DataType::UInt32,
            DatumKind::UInt16 => DataType::UInt16,
            DatumKind::UInt8 => DataType::UInt8,
            DatumKind::Int64 => DataType::Int64,
            DatumKind::Int32 => DataType::Int32,
            DatumKind::Int16 => DataType::Int16,
            DatumKind::Int8 => DataType::Int8,
            DatumKind::Boolean => DataType::Boolean,
            DatumKind::Date => DataType::Date32,
            DatumKind::Time => DataType::Time64(TimeUnit::Nanosecond),
        }
    }
}

impl Datum {
    pub fn as_scalar_value(&self) -> Option<ScalarValue> {
        match self {
            Datum::Null => None,
            Datum::Timestamp(v) => {
                Some(ScalarValue::TimestampMillisecond(Some((*v).as_i64()), None))
            }
            Datum::Double(v) => Some(ScalarValue::Float64(Some(*v))),
            Datum::Float(v) => Some(ScalarValue::Float32(Some(*v))),
            Datum::Varbinary(v) => Some(ScalarValue::Binary(Some(v.to_vec()))),
            Datum::String(v) => Some(ScalarValue::Utf8(Some(v.to_string()))),
            Datum::UInt64(v) => Some(ScalarValue::UInt64(Some(*v))),
            Datum::UInt32(v) => Some(ScalarValue::UInt32(Some(*v))),
            Datum::UInt16(v) => Some(ScalarValue::UInt16(Some(*v))),
            Datum::UInt8(v) => Some(ScalarValue::UInt8(Some(*v))),
            Datum::Int64(v) => Some(ScalarValue::Int64(Some(*v))),
            Datum::Int32(v) => Some(ScalarValue::Int32(Some(*v))),
            Datum::Int16(v) => Some(ScalarValue::Int16(Some(*v))),
            Datum::Int8(v) => Some(ScalarValue::Int8(Some(*v))),
            Datum::Boolean(v) => Some(ScalarValue::Boolean(Some(*v))),
            Datum::Date(v) => Some(ScalarValue::Date32(Some(*v))),
            Datum::Time(v) => Some(ScalarValue::Time64Nanosecond(Some(*v))),
        }
    }

    pub fn from_scalar_value(val: &ScalarValue) -> Option<Self> {
        match val {
            ScalarValue::Boolean(v) => v.map(Datum::Boolean),
            ScalarValue::Float32(v) => v.map(Datum::Float),
            ScalarValue::Float64(v) => v.map(Datum::Double),
            ScalarValue::Int8(v) => v.map(Datum::Int8),
            ScalarValue::Int16(v) => v.map(Datum::Int16),
            ScalarValue::Int32(v) => v.map(Datum::Int32),
            ScalarValue::Int64(v) => v.map(Datum::Int64),
            ScalarValue::UInt8(v) => v.map(Datum::UInt8),
            ScalarValue::UInt16(v) => v.map(Datum::UInt16),
            ScalarValue::UInt32(v) => v.map(Datum::UInt32),
            ScalarValue::UInt64(v) => v.map(Datum::UInt64),
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => v
                .as_ref()
                .map(|v| Datum::String(StringBytes::copy_from_str(v.as_str()))),
            ScalarValue::Binary(v)
            | ScalarValue::FixedSizeBinary(_, v)
            | ScalarValue::LargeBinary(v) => v
                .as_ref()
                .map(|v| Datum::Varbinary(Bytes::copy_from_slice(v.as_slice()))),
            ScalarValue::TimestampMillisecond(v, _) => {
                v.map(|v| Datum::Timestamp(Timestamp::new(v)))
            }
            ScalarValue::Date32(v) => v.map(Datum::Date),
            ScalarValue::Time64Nanosecond(v) => v.map(Datum::Time),
            ScalarValue::Dictionary(_, literal) => Datum::from_scalar_value(literal),
            ScalarValue::List(_)
            | ScalarValue::Date64(_)
            | ScalarValue::Time32Second(_)
            | ScalarValue::Time32Millisecond(_)
            | ScalarValue::Time64Microsecond(_)
            | ScalarValue::TimestampSecond(_, _)
            | ScalarValue::TimestampMicrosecond(_, _)
            | ScalarValue::TimestampNanosecond(_, _)
            | ScalarValue::IntervalYearMonth(_)
            | ScalarValue::IntervalDayTime(_)
            | ScalarValue::Struct(_, _)
            | ScalarValue::Decimal128(_, _, _)
            | ScalarValue::Null
            | ScalarValue::IntervalMonthDayNano(_)
            | ScalarValue::FixedSizeList(_)
            | ScalarValue::DurationSecond(_)
            | ScalarValue::DurationMillisecond(_)
            | ScalarValue::DurationMicrosecond(_)
            | ScalarValue::Decimal256(_, _, _)
            | ScalarValue::LargeList(_)
            | ScalarValue::DurationNanosecond(_) => None,
        }
    }
}

impl<'a> DatumView<'a> {
    pub fn from_scalar_value(val: &'a ScalarValue) -> Option<Self> {
        match val {
            ScalarValue::Boolean(v) => v.map(DatumView::Boolean),
            ScalarValue::Float32(v) => v.map(DatumView::Float),
            ScalarValue::Float64(v) => v.map(DatumView::Double),
            ScalarValue::Int8(v) => v.map(DatumView::Int8),
            ScalarValue::Int16(v) => v.map(DatumView::Int16),
            ScalarValue::Int32(v) => v.map(DatumView::Int32),
            ScalarValue::Int64(v) => v.map(DatumView::Int64),
            ScalarValue::UInt8(v) => v.map(DatumView::UInt8),
            ScalarValue::UInt16(v) => v.map(DatumView::UInt16),
            ScalarValue::UInt32(v) => v.map(DatumView::UInt32),
            ScalarValue::UInt64(v) => v.map(DatumView::UInt64),
            ScalarValue::Date32(v) => v.map(DatumView::Date),
            ScalarValue::Time64Nanosecond(v) => v.map(DatumView::Time),
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => {
                v.as_ref().map(|v| DatumView::String(v.as_str()))
            }
            ScalarValue::Binary(v)
            | ScalarValue::FixedSizeBinary(_, v)
            | ScalarValue::LargeBinary(v) => v.as_ref().map(|v| DatumView::Varbinary(v.as_slice())),
            ScalarValue::TimestampMillisecond(v, _) => {
                v.map(|v| DatumView::Timestamp(Timestamp::new(v)))
            }
            ScalarValue::Dictionary(_, literal) => DatumView::from_scalar_value(literal),
            ScalarValue::List(_)
            | ScalarValue::Date64(_)
            | ScalarValue::Time32Second(_)
            | ScalarValue::Time32Millisecond(_)
            | ScalarValue::Time64Microsecond(_)
            | ScalarValue::TimestampSecond(_, _)
            | ScalarValue::TimestampMicrosecond(_, _)
            | ScalarValue::TimestampNanosecond(_, _)
            | ScalarValue::IntervalYearMonth(_)
            | ScalarValue::IntervalDayTime(_)
            | ScalarValue::Struct(_, _)
            | ScalarValue::Decimal128(_, _, _)
            | ScalarValue::Null
            | ScalarValue::IntervalMonthDayNano(_)
            | ScalarValue::FixedSizeList(_)
            | ScalarValue::DurationSecond(_)
            | ScalarValue::DurationMillisecond(_)
            | ScalarValue::DurationMicrosecond(_)
            | ScalarValue::Decimal256(_, _, _)
            | ScalarValue::LargeList(_)
            | ScalarValue::DurationNanosecond(_) => None,
        }
    }
}

impl From<DatumKind> for DataType {
    fn from(kind: DatumKind) -> Self {
        match kind {
            DatumKind::Null => DataType::Null,
            DatumKind::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, None),
            DatumKind::Double => DataType::Float64,
            DatumKind::Float => DataType::Float32,
            DatumKind::Varbinary => DataType::Binary,
            DatumKind::String => DataType::Utf8,
            DatumKind::UInt64 => DataType::UInt64,
            DatumKind::UInt32 => DataType::UInt32,
            DatumKind::UInt16 => DataType::UInt16,
            DatumKind::UInt8 => DataType::UInt8,
            DatumKind::Int64 => DataType::Int64,
            DatumKind::Int32 => DataType::Int32,
            DatumKind::Int16 => DataType::Int16,
            DatumKind::Int8 => DataType::Int8,
            DatumKind::Boolean => DataType::Boolean,
            DatumKind::Date => DataType::Date32,
            DatumKind::Time => DataType::Time64(TimeUnit::Nanosecond),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    use super::*;

    #[test]
    fn test_is_key_kind() {
        assert!(!DatumKind::Null.is_key_kind());
        assert!(DatumKind::Timestamp.is_key_kind());
        assert!(!DatumKind::Double.is_key_kind());
        assert!(!DatumKind::Float.is_key_kind());
        assert!(DatumKind::Varbinary.is_key_kind());
        assert!(DatumKind::String.is_key_kind());
        assert!(DatumKind::UInt64.is_key_kind());
        assert!(DatumKind::UInt32.is_key_kind());
        assert!(DatumKind::UInt16.is_key_kind());
        assert!(DatumKind::UInt8.is_key_kind());
        assert!(DatumKind::Int64.is_key_kind());
        assert!(DatumKind::Int32.is_key_kind());
        assert!(DatumKind::Int16.is_key_kind());
        assert!(DatumKind::Int8.is_key_kind());
        assert!(DatumKind::Boolean.is_key_kind());
        assert!(DatumKind::Date.is_key_kind());
        assert!(DatumKind::Time.is_key_kind());
    }

    #[test]
    fn test_unsign_kind() {
        assert_eq!(DatumKind::UInt64.unsign_kind(), Some(DatumKind::UInt64));
        assert_eq!(DatumKind::Int64.unsign_kind(), Some(DatumKind::UInt64));
        assert_eq!(DatumKind::UInt32.unsign_kind(), Some(DatumKind::UInt32));
        assert_eq!(DatumKind::Int32.unsign_kind(), Some(DatumKind::UInt32));
        assert_eq!(DatumKind::UInt16.unsign_kind(), Some(DatumKind::UInt16));
        assert_eq!(DatumKind::Int16.unsign_kind(), Some(DatumKind::UInt16));
        assert_eq!(DatumKind::UInt8.unsign_kind(), Some(DatumKind::UInt8));
        assert_eq!(DatumKind::Int8.unsign_kind(), Some(DatumKind::UInt8));

        assert!(DatumKind::Null.unsign_kind().is_none());
        assert!(DatumKind::Timestamp.unsign_kind().is_none());
        assert!(DatumKind::String.unsign_kind().is_none());
        assert!(DatumKind::Boolean.unsign_kind().is_none());
        assert!(DatumKind::Varbinary.unsign_kind().is_none());
        assert!(DatumKind::Double.unsign_kind().is_none());
        assert!(DatumKind::Float.unsign_kind().is_none());
    }

    #[test]
    fn test_into_u8() {
        assert_eq!(0, DatumKind::Null.into_u8());
        assert_eq!(1, DatumKind::Timestamp.into_u8());
        assert_eq!(2, DatumKind::Double.into_u8());
        assert_eq!(3, DatumKind::Float.into_u8());
        assert_eq!(4, DatumKind::Varbinary.into_u8());
        assert_eq!(5, DatumKind::String.into_u8());
        assert_eq!(6, DatumKind::UInt64.into_u8());
        assert_eq!(7, DatumKind::UInt32.into_u8());
        assert_eq!(8, DatumKind::UInt16.into_u8());
        assert_eq!(9, DatumKind::UInt8.into_u8());
        assert_eq!(10, DatumKind::Int64.into_u8());
        assert_eq!(11, DatumKind::Int32.into_u8());
        assert_eq!(12, DatumKind::Int16.into_u8());
        assert_eq!(13, DatumKind::Int8.into_u8());
        assert_eq!(14, DatumKind::Boolean.into_u8());
        assert_eq!(15, DatumKind::Date.into_u8());
        assert_eq!(16, DatumKind::Time.into_u8());
    }

    #[test]
    fn test_to_negative_value() {
        let cases = [
            (Datum::Null, None),
            (Datum::Timestamp(Timestamp::ZERO), None),
            (Datum::Date(10), None),
            (Datum::Time(10), None),
            (Datum::Double(1.0), Some(Datum::Double(-1.0))),
            (Datum::Float(1.0), Some(Datum::Float(-1.0))),
            (Datum::Varbinary(Bytes::new()), None),
            (Datum::String(StringBytes::new()), None),
            (Datum::UInt64(10), None),
            (Datum::UInt32(10), None),
            (Datum::UInt16(10), None),
            (Datum::UInt8(10), None),
            (Datum::Int64(10), Some(Datum::Int64(-10))),
            (Datum::Int32(10), Some(Datum::Int32(-10))),
            (Datum::Int16(10), Some(Datum::Int16(-10))),
            (Datum::Int8(10), Some(Datum::Int8(-10))),
        ];

        for (source, negative) in cases {
            assert_eq!(negative, source.to_negative());
        }
    }

    #[test]
    fn test_to_overflow_negative_value() {
        let cases = [
            Datum::Int64(i64::MIN),
            Datum::Int32(i32::MIN),
            Datum::Int16(i16::MIN),
            Datum::Int8(i8::MIN),
        ];

        for source in cases {
            assert!(source.to_negative().is_none());
        }
    }

    #[test]
    fn test_parse_datum_date() {
        let cases = ["-9999-01-01", "9999-12-21", "2000-01-01", "1000-02-28"];

        for case in cases {
            let datum = Datum::parse_datum_date_from_str(case).unwrap();
            assert_eq!(
                case.to_string(),
                Datum::format_datum_date(&(datum.as_i64().unwrap() as i32))
            );
        }
    }

    #[test]
    fn test_parse_datum_date_error_cases() {
        let err_cases = [
            "ab-01-01",
            "01-ab-01",
            "-9999-234-ab",
            "100099-123-01",
            "1990-01-123",
            "1999",
            "",
            "1999--00--00",
            "1999-0",
            "1999-01-01-01",
        ];

        for source in err_cases {
            assert!(Datum::parse_datum_date_from_str(source).is_err());
        }
    }

    #[test]
    fn test_parse_datum_time() {
        // '-838:59:59.000000' to '838:59:59.000000'
        let cases = [
            "-838:59:59.123",
            "830:59:59.567",
            "-23:59:59.999",
            "23:59:59.999",
            "00:59:59.567",
            "10:10:10.234",
        ];

        for case in cases {
            let datum = Datum::parse_datum_time_from_str(case).unwrap();
            assert_eq!(
                case.to_string(),
                Datum::format_datum_time(&datum.as_i64().unwrap())
            );
        }
    }

    #[test]
    fn test_parse_datum_time_error_cases() {
        let err_cases = [
            "-ab:12:59.000",
            "00:ab:59.000",
            "-12:234:59.000",
            "00:23:900.000",
            "-00:59:59.abc",
            "00",
            "",
            "00:00:00:00",
            "12:",
            ":",
        ];

        for source in err_cases {
            assert!(Datum::parse_datum_time_from_str(source).is_err());
        }
    }

    #[test]
    fn test_convert_from_sql_value() {
        let cases = vec![
            (
                Value::Boolean(false),
                DatumKind::Boolean,
                true,
                Some(Datum::Boolean(false)),
            ),
            (
                Value::Number("100.1".to_string(), false),
                DatumKind::Float,
                true,
                Some(Datum::Float(100.1)),
            ),
            (
                Value::SingleQuotedString("string_literal".to_string()),
                DatumKind::String,
                true,
                Some(Datum::String(StringBytes::from_static("string_literal"))),
            ),
            (
                Value::HexStringLiteral("c70a0b".to_string()),
                DatumKind::Varbinary,
                true,
                Some(Datum::Varbinary(Bytes::from(vec![199, 10, 11]))),
            ),
            (
                Value::EscapedStringLiteral("string_literal".to_string()),
                DatumKind::String,
                false,
                None,
            ),
        ];

        for (input, kind, succeed, expect) in cases {
            let res = Datum::try_from_sql_value(&kind, input);
            if succeed {
                assert_eq!(res.unwrap(), expect.unwrap());
            } else {
                assert!(res.is_err());
            }
        }
    }

    fn get_hash<V: Hash>(v: &V) -> u64 {
        let mut hasher = DefaultHasher::new();
        v.hash(&mut hasher);
        hasher.finish()
    }

    macro_rules! assert_datum_view_hash {
        ($v:expr, $Kind: ident) => {
            let expected = get_hash(&DatumView::$Kind($v));
            let actual = get_hash(&$v);
            assert_eq!(expected, actual);
        };
    }

    #[test]
    fn test_hash() {
        assert_datum_view_hash!(Timestamp::new(42), Timestamp);
        assert_datum_view_hash!(42_i32, Date);
        assert_datum_view_hash!(424_i64, Time);
        assert_datum_view_hash!(b"abcde", Varbinary);
        assert_datum_view_hash!("12345", String);
        assert_datum_view_hash!(42424242_u64, UInt64);
        assert_datum_view_hash!(424242_u32, UInt32);
        assert_datum_view_hash!(4242_u16, UInt16);
        assert_datum_view_hash!(42_u8, UInt8);
        assert_datum_view_hash!(-42424242_i64, Int64);
        assert_datum_view_hash!(-42424242_i32, Int32);
        assert_datum_view_hash!(-4242_i16, Int16);
        assert_datum_view_hash!(-42_i8, Int8);
        assert_datum_view_hash!(true, Boolean);

        // Null case.
        let null_expected = get_hash(&NULL_VALUE_FOR_HASH);
        let null_actual = get_hash(&DatumView::Null);
        assert_eq!(null_expected, null_actual);

        // Float case.
        let float_expected = get_hash(&Fl(42.0_f32));
        let float_actual = get_hash(&DatumView::Float(42.0));
        assert_eq!(float_expected, float_actual);

        // Double case.
        let double_expected = get_hash(&Fl(-42.0_f64));
        let double_actual = get_hash(&DatumView::Double(-42.0));
        assert_eq!(double_expected, double_actual);
    }
}
