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

//! Schema of column

use std::{collections::HashMap, convert::TryFrom, str::FromStr, sync::Arc};

use arrow::datatypes::{DataType, Field};
use fb_util::remote_engine_generated::fbprotocol::{
    ColumnDesc as FBColumnDesc, DataType as FBDataType,
};
use flatbuffers;
use horaedbproto::{remote_engine::ColumnDesc, schema as schema_pb};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use sqlparser::ast::Expr;

use crate::datum::DatumKind;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Unsupported arrow data type, type:{}.\nBacktrace:\n{}",
        data_type,
        backtrace
    ))]
    UnsupportedDataType {
        data_type: DataType,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid tag type:{}.\nBacktrace:\n{}", data_type, backtrace))]
    InvalidTagType {
        data_type: DataType,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid dictionary type:{}.\nBacktrace:\n{}", data_type, backtrace))]
    InvalidDictionaryType {
        data_type: DataType,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Arrow field meta data is missing, field name:{}.\nBacktrace:\n{}",
        field_name,
        backtrace
    ))]
    ArrowFieldMetaDataMissing {
        field_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Arrow field meta key is not found, key:{:?}.\nBacktrace:\n{}",
        key,
        backtrace
    ))]
    ArrowFieldMetaKeyNotFound {
        key: ArrowFieldMetaKey,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Arrow field meta value is invalid, key:{:?}, raw_value:{}, err:{}.\nBacktrace:\n{}",
        key,
        raw_value,
        source,
        backtrace
    ))]
    InvalidArrowFieldMetaValue {
        key: ArrowFieldMetaKey,
        raw_value: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to decode default value, encoded_val:{:?}, err:{}.\nBacktrace:\n{}",
        encoded_val,
        source,
        backtrace
    ))]
    DecodeDefaultValue {
        encoded_val: Vec<u8>,
        source: serde_json::error::Error,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Error of compatibility check
#[derive(Debug, Snafu)]
pub enum CompatError {
    #[snafu(display(
        "Incompatible data type of column, name:{}, expect:{:?}, given:{:?}.\nBacktrace:\n{}",
        name,
        expect,
        given,
        backtrace,
    ))]
    IncompatDataType {
        name: String,
        expect: DatumKind,
        given: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display("Column is not nullable, name:{}.\nBacktrace:\n{}", name, backtrace))]
    NotNullable { name: String, backtrace: Backtrace },
}

/// compatibility purpose for protobuf and flatbuffer protocol
#[derive(Debug)]
pub enum ColumnDescExt<'a> {
    Proto(&'a ColumnDesc),
    Flatbuffer(&'a FBColumnDesc),
}

impl ColumnDescExt<'_> {
    pub fn id(&self) -> u32 {
        match self {
            ColumnDescExt::Proto(v) => v.id,
            ColumnDescExt::Flatbuffer(v) => v.id(),
        }
    }

    pub fn typ_as_datumkind(&self) -> DatumKind {
        match self {
            ColumnDescExt::Proto(v) => DatumKind::from(v.typ()),
            ColumnDescExt::Flatbuffer(v) => DatumKind::from(v.typ()),
        }
    }
}

#[allow(clippy::len_without_is_empty)]
pub enum VecColumnDescExt<'a> {
    Proto(&'a [ColumnDesc]),
    Flatbuffer(flatbuffers::Vector<'a, FBColumnDesc>),
}

impl<'a> VecColumnDescExt<'_> {
    pub fn len(&self) -> usize {
        match self {
            VecColumnDescExt::Proto(v) => v.len(),
            VecColumnDescExt::Flatbuffer(v) => v.len(),
        }
    }

    pub fn iter(&'a self) -> Box<dyn Iterator<Item = ColumnDescExt> + 'a> {
        match self {
            VecColumnDescExt::Proto(v) => Box::new(v.iter().map(ColumnDescExt::Proto)),
            VecColumnDescExt::Flatbuffer(v) => Box::new(v.iter().map(ColumnDescExt::Flatbuffer)),
        }
    }
}

/// Id of column
pub type ColumnId = u32;

/// A ColumnId used to indicate that the column id is uninitialized
pub const COLUMN_ID_UNINIT: ColumnId = 0;

/// Read operation of a column
#[derive(Debug)]
pub enum ReadOp {
    /// Use the column exactly
    Exact,
    /// Fill the column by null
    FillNull,
}

/// Meta data of the arrow field.
#[derive(Clone, Debug, Default, PartialEq)]
struct ArrowFieldMeta {
    id: u32,
    is_tag: bool,
    comment: String,
    is_dictionary: bool,
}

#[derive(Copy, Clone, Debug)]
pub enum ArrowFieldMetaKey {
    Id,
    IsTag,
    IsDictionary,
    Comment,
}

impl ArrowFieldMetaKey {
    fn as_str(&self) -> &str {
        match self {
            ArrowFieldMetaKey::Id => "field::id",
            ArrowFieldMetaKey::IsTag => "field::is_tag",
            ArrowFieldMetaKey::Comment => "field::comment",
            ArrowFieldMetaKey::IsDictionary => "field::is_dictionary",
        }
    }

    // Only id,is_tag,comment are required meta keys, other keys should be optional
    // to keep backward compatible.
    fn is_required(&self) -> bool {
        matches!(self, Self::Id | Self::IsTag | Self::Comment)
    }
}

impl ToString for ArrowFieldMetaKey {
    fn to_string(&self) -> String {
        self.as_str().to_string()
    }
}

/// Schema of column
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    /// Id of column
    pub id: ColumnId,
    /// Column name
    pub name: String,
    /// Data type of the column
    pub data_type: DatumKind,
    /// Is nullable
    pub is_nullable: bool,
    /// Is tag, tag is just a hint for a column, there is no restriction that a
    /// tag column must be a part of primary key
    pub is_tag: bool,
    // Whether to use dictionary types for encoding column
    pub is_dictionary: bool,
    /// Comment of the column
    pub comment: String,
    /// Column name in response
    pub escaped_name: String,
    /// Default value expr
    pub default_value: Option<Expr>,
}

impl ColumnSchema {
    /// Check whether a type is valid tag type.
    pub fn is_valid_tag_type(typ: DatumKind) -> bool {
        match typ {
            DatumKind::Null => false,
            DatumKind::Timestamp => true,
            DatumKind::Double => false,
            DatumKind::Float => false,
            DatumKind::Varbinary => true,
            DatumKind::String => true,
            DatumKind::UInt64 => true,
            DatumKind::UInt32 => true,
            DatumKind::UInt16 => true,
            DatumKind::UInt8 => true,
            DatumKind::Int64 => true,
            DatumKind::Int32 => true,
            DatumKind::Int16 => true,
            DatumKind::Int8 => true,
            DatumKind::Boolean => true,
            DatumKind::Date => true,
            DatumKind::Time => true,
        }
    }

    /// Check whether a type is valid dictionary type.
    pub fn is_valid_dictionary_type(typ: DatumKind) -> bool {
        matches!(typ, DatumKind::String)
    }

    /// Convert `self` to [`arrow::datatypes::Field`]
    pub fn to_arrow_field(&self) -> Field {
        From::from(self)
    }

    /// Returns Ok if column with `writer_schema` can write to column with the
    /// same schema as `self`.
    pub fn compatible_for_write(
        &self,
        writer_schema: &ColumnSchema,
    ) -> std::result::Result<(), CompatError> {
        ensure!(
            self.data_type == writer_schema.data_type,
            IncompatDataType {
                name: &self.name,
                expect: writer_schema.data_type,
                given: self.data_type,
            }
        );

        // This column is not nullable but writer is nullable
        ensure!(
            self.is_nullable || !writer_schema.is_nullable,
            NotNullable { name: &self.name }
        );

        Ok(())
    }

    /// Returns `Ok` if the source schema can read by this schema, now we won't
    /// validate data type of column
    pub fn compatible_for_read(
        &self,
        source_schema: &ColumnSchema,
    ) -> std::result::Result<ReadOp, CompatError> {
        if self.is_nullable {
            // Column is nullable
            if self.id == source_schema.id {
                // Same column
                Ok(ReadOp::Exact)
            } else {
                // Not the same column, maybe dropped, fill by null.
                Ok(ReadOp::FillNull)
            }
        } else {
            // Column is not null. We consider the old column was dropped if they have
            // different column id and also try to fill by null, so we
            // also check column id.
            ensure!(
                self.id == source_schema.id && !source_schema.is_nullable,
                NotNullable {
                    name: &source_schema.name,
                }
            );

            Ok(ReadOp::Exact)
        }
    }

    /// Check whether the given `desc` is correct with self.
    pub fn is_correct_desc(&self, desc: &ColumnDescExt) -> bool {
        if self.id != desc.id() {
            return false;
        }

        desc.typ_as_datumkind() == self.data_type
    }
}

impl From<&ColumnSchema> for ColumnDesc {
    fn from(column_schema: &ColumnSchema) -> Self {
        Self {
            id: column_schema.id,
            typ: schema_pb::DataType::from(column_schema.data_type).into(),
        }
    }
}

impl From<&ColumnSchema> for FBColumnDesc {
    fn from(column_schema: &ColumnSchema) -> Self {
        FBColumnDesc::new(column_schema.id, FBDataType::from(column_schema.data_type))
    }
}

impl TryFrom<schema_pb::ColumnSchema> for ColumnSchema {
    type Error = Error;

    fn try_from(column_schema: schema_pb::ColumnSchema) -> Result<Self> {
        let escaped_name = column_schema.name.escape_debug().to_string();
        let data_type = column_schema.data_type();
        let default_value = column_schema
            .default_value
            .map(|v| match v {
                schema_pb::column_schema::DefaultValue::SerdeJson(encoded_val) => {
                    serde_json::from_slice::<Expr>(&encoded_val)
                        .context(DecodeDefaultValue { encoded_val })
                }
            })
            .transpose()?;

        Ok(Self {
            id: column_schema.id,
            name: column_schema.name,
            data_type: DatumKind::from(data_type),
            is_nullable: column_schema.is_nullable,
            is_tag: column_schema.is_tag,
            is_dictionary: column_schema.is_dictionary,
            comment: column_schema.comment,
            escaped_name,
            default_value,
        })
    }
}

impl TryFrom<&Arc<Field>> for ColumnSchema {
    type Error = Error;

    fn try_from(field: &Arc<Field>) -> Result<Self> {
        let ArrowFieldMeta {
            id,
            is_tag,
            is_dictionary,
            comment,
        } = decode_arrow_field_meta_data(field.metadata())?;
        Ok(Self {
            id,
            name: field.name().clone(),
            data_type: DatumKind::from_data_type(field.data_type()).context(
                UnsupportedDataType {
                    data_type: field.data_type().clone(),
                },
            )?,
            is_nullable: field.is_nullable(),
            is_tag,
            is_dictionary,
            comment,
            escaped_name: field.name().escape_debug().to_string(),
            default_value: None,
        })
    }
}

impl From<&ColumnSchema> for Field {
    fn from(col_schema: &ColumnSchema) -> Self {
        let metadata = encode_arrow_field_meta_data(col_schema);
        // If the column should use dictionary, create correspond dictionary type.
        let mut field = if col_schema.is_dictionary {
            Field::new_dict(
                &col_schema.name,
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                col_schema.is_nullable,
                col_schema.id.into(),
                // TODO(tanruixiang): how to use dict_is_ordered
                false,
            )
        } else {
            Field::new(
                &col_schema.name,
                col_schema.data_type.into(),
                col_schema.is_nullable,
            )
        };

        field.set_metadata(metadata);

        field
    }
}

fn parse_arrow_field_meta_value<T: Default>(
    meta: &HashMap<String, String>,
    key: ArrowFieldMetaKey,
) -> Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let raw_value = match meta.get(key.as_str()) {
        None => {
            if key.is_required() {
                return ArrowFieldMetaKeyNotFound { key }.fail();
            } else {
                return Ok(T::default());
            }
        }
        Some(v) => v,
    };

    T::from_str(raw_value.as_str())
        .map_err(|e| Box::new(e) as _)
        .context(InvalidArrowFieldMetaValue { key, raw_value })
}

fn decode_arrow_field_meta_data(meta: &HashMap<String, String>) -> Result<ArrowFieldMeta> {
    if meta.is_empty() {
        Ok(ArrowFieldMeta::default())
    } else {
        Ok(ArrowFieldMeta {
            id: parse_arrow_field_meta_value(meta, ArrowFieldMetaKey::Id)?,
            is_tag: parse_arrow_field_meta_value(meta, ArrowFieldMetaKey::IsTag)?,
            comment: parse_arrow_field_meta_value(meta, ArrowFieldMetaKey::Comment)?,
            is_dictionary: parse_arrow_field_meta_value(meta, ArrowFieldMetaKey::IsDictionary)?,
        })
    }
}

fn encode_arrow_field_meta_data(col_schema: &ColumnSchema) -> HashMap<String, String> {
    let mut meta = HashMap::new();

    meta.insert(ArrowFieldMetaKey::Id.to_string(), col_schema.id.to_string());
    meta.insert(
        ArrowFieldMetaKey::IsTag.to_string(),
        col_schema.is_tag.to_string(),
    );
    meta.insert(
        ArrowFieldMetaKey::IsDictionary.to_string(),
        col_schema.is_dictionary.to_string(),
    );
    meta.insert(
        ArrowFieldMetaKey::Comment.to_string(),
        col_schema.comment.clone(),
    );

    meta
}

/// ColumnSchema builder
#[must_use]
pub struct Builder {
    id: ColumnId,
    name: String,
    data_type: DatumKind,
    is_nullable: bool,
    is_tag: bool,
    is_dictionary: bool,
    comment: String,
    default_value: Option<Expr>,
}

impl Builder {
    /// Create a new builder
    pub fn new(name: String, data_type: DatumKind) -> Self {
        Self {
            id: COLUMN_ID_UNINIT,
            name,
            data_type,
            is_nullable: false,
            is_tag: false,
            is_dictionary: false,
            comment: String::new(),
            default_value: None,
        }
    }

    pub fn id(mut self, id: ColumnId) -> Self {
        self.id = id;
        self
    }

    /// Set this column is nullable, default is false (not nullable).
    pub fn is_nullable(mut self, is_nullable: bool) -> Self {
        self.is_nullable = is_nullable;
        self
    }

    /// Set this column is tag, default is false (not a tag).
    pub fn is_tag(mut self, is_tag: bool) -> Self {
        self.is_tag = is_tag;
        self
    }

    /// Set this column is dictionary, default is false (not a dictionary).
    pub fn is_dictionary(mut self, is_dictionary: bool) -> Self {
        self.is_dictionary = is_dictionary;
        self
    }

    pub fn comment(mut self, comment: String) -> Self {
        self.comment = comment;
        self
    }

    pub fn default_value(mut self, default_value: Option<Expr>) -> Self {
        self.default_value = default_value;
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.is_tag {
            ensure!(
                ColumnSchema::is_valid_tag_type(self.data_type),
                InvalidTagType {
                    data_type: self.data_type
                }
            );
        }

        if self.is_dictionary {
            ensure!(
                ColumnSchema::is_valid_dictionary_type(self.data_type),
                InvalidDictionaryType {
                    data_type: self.data_type
                }
            );
        }

        Ok(())
    }

    pub fn build(self) -> Result<ColumnSchema> {
        self.validate()?;
        let escaped_name = self.name.escape_debug().to_string();
        Ok(ColumnSchema {
            id: self.id,
            name: self.name,
            data_type: self.data_type,
            is_nullable: self.is_nullable,
            is_tag: self.is_tag,
            is_dictionary: self.is_dictionary,
            comment: self.comment,
            escaped_name,
            default_value: self.default_value,
        })
    }
}

impl From<ColumnSchema> for schema_pb::ColumnSchema {
    fn from(src: ColumnSchema) -> Self {
        let default_value = src.default_value.map(|v| {
            // FIXME: Maybe we should throw this error rather than panic here.
            let encoded_value = serde_json::to_vec(&v).unwrap();
            schema_pb::column_schema::DefaultValue::SerdeJson(encoded_value)
        });

        schema_pb::ColumnSchema {
            name: src.name,
            data_type: schema_pb::DataType::from(src.data_type) as i32,
            is_nullable: src.is_nullable,
            id: src.id,
            is_tag: src.is_tag,
            is_dictionary: src.is_dictionary,
            comment: src.comment,
            default_value,
        }
    }
}

#[cfg(test)]
mod tests {
    use macros::hash_map;
    use sqlparser::ast::Value;

    use super::*;

    /// Create a column schema for test, each field is filled with non-default
    /// value
    fn new_test_column_schema() -> ColumnSchema {
        Builder::new("test_column_schema".to_string(), DatumKind::String)
            .id(18)
            .is_nullable(true)
            .is_tag(true)
            .is_dictionary(true)
            .comment("Comment of this column".to_string())
            .default_value(Some(Expr::Value(Value::Boolean(true))))
            .build()
            .expect("should succeed to build column schema")
    }

    #[test]
    fn test_builder() {
        let lhs = new_test_column_schema();
        let rhs = ColumnSchema {
            id: 18,
            name: "test_column_schema".to_string(),
            data_type: DatumKind::String,
            is_nullable: true,
            is_tag: true,
            is_dictionary: true,
            comment: "Comment of this column".to_string(),
            escaped_name: "test_column_schema".escape_debug().to_string(),
            default_value: Some(Expr::Value(Value::Boolean(true))),
        };

        assert_eq!(&lhs, &rhs);
    }

    #[test]
    fn test_pb_convert() {
        let column_schema = new_test_column_schema();
        let pb_schema = schema_pb::ColumnSchema::from(column_schema.clone());
        // Check pb specific fields
        assert!(pb_schema.is_tag);
        assert!(pb_schema.is_dictionary);
        assert!(pb_schema.is_nullable);

        let schema_from_pb = ColumnSchema::try_from(pb_schema).unwrap();
        assert_eq!(&schema_from_pb, &column_schema);
    }

    #[test]
    fn test_valid_tag_type() {
        let invalid_tag_types = [DatumKind::Null, DatumKind::Float, DatumKind::Double];

        for v in &DatumKind::VALUES {
            assert_eq!(
                ColumnSchema::is_valid_tag_type(*v),
                !invalid_tag_types.contains(v)
            );
        }
    }

    #[test]
    fn test_valid_dictionary_type() {
        let valid_dictionary_types = [DatumKind::String];

        for v in &DatumKind::VALUES {
            assert_eq!(
                ColumnSchema::is_valid_dictionary_type(*v),
                valid_dictionary_types.contains(v)
            );
        }
    }

    #[test]
    fn test_decode_arrow_field_meta_data() {
        let testcases = [
            (
                hash_map! {
                    "field::id".to_string() => "1".to_string(),
                    "field::is_tag".to_string() => "true".to_string(),
                    "field::comment".to_string() => "".to_string()
                },
                ArrowFieldMeta {
                    id: 1,
                    is_tag: true,
                    comment: "".to_string(),
                    is_dictionary: false,
                },
            ),
            (
                hash_map! {
                    "field::id".to_string() => "1".to_string(),
                    "field::is_tag".to_string() => "false".to_string(),
                    "field::comment".to_string() => "abc".to_string(),
                    "field::is_dictionary".to_string() => "true".to_string()
                },
                ArrowFieldMeta {
                    id: 1,
                    is_tag: false,
                    comment: "abc".to_string(),
                    is_dictionary: true,
                },
            ),
        ];

        for (meta, expected) in &testcases {
            assert_eq!(expected, &decode_arrow_field_meta_data(meta).unwrap())
        }

        let meta = hash_map! {
            "field::id".to_string() => "1".to_string()
        };
        assert!(decode_arrow_field_meta_data(&meta).is_err());
    }
}
