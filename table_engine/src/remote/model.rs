// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Model for remote table engine

use common_types::schema::Schema;
use common_util::{
    avro,
    error::{BoxError, GenericError},
};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

use crate::table::{ReadRequest as TableReadRequest, WriteRequest as TableWriteRequest};

const ENCODE_ROWS_WITH_AVRO: u32 = 0;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to convert read request to pb, err:{}", source))]
    ReadRequestToPb { source: crate::table::Error },

    #[snafu(display(
        "Failed to convert write request to pb, table_ident:{:?}, msg:{}.\nBacktrace:\n{}",
        table_ident,
        msg,
        backtrace
    ))]
    WriteRequestToPbNoCause {
        table_ident: TableIdentifier,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to convert write request to pb, table_ident:{:?}, err:{}",
        table_ident,
        source
    ))]
    WriteRequestToPbWithCause {
        table_ident: TableIdentifier,
        source: avro::Error,
    },

    #[snafu(display("Empty table identifier.\nBacktrace:\n{}", backtrace))]
    EmptyTableIdentifier { backtrace: Backtrace },

    #[snafu(display("Empty table read request.\nBacktrace:\n{}", backtrace))]
    EmptyTableReadRequest { backtrace: Backtrace },

    #[snafu(display("Empty table schema.\nBacktrace:\n{}", backtrace))]
    EmptyTableSchema { backtrace: Backtrace },

    #[snafu(display("Empty row group.\nBacktrace:\n{}", backtrace))]
    EmptyRowGroup { backtrace: Backtrace },

    #[snafu(display("Failed to covert table read request, err:{}", source))]
    ConvertTableReadRequest { source: GenericError },

    #[snafu(display("Failed to covert table schema, err:{}", source))]
    ConvertTableSchema { source: GenericError },

    #[snafu(display("Failed to covert row group, err:{}", source))]
    ConvertRowGroup { source: GenericError },

    #[snafu(display(
        "Failed to covert row group, encoding version:{}.\nBacktrace:\n{}",
        version,
        backtrace
    ))]
    UnsupportedConvertRowGroup { version: u32, backtrace: Backtrace },
}

define_result!(Error);

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TableIdentifier {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl From<ceresdbproto::remote_engine::TableIdentifier> for TableIdentifier {
    fn from(pb: ceresdbproto::remote_engine::TableIdentifier) -> Self {
        Self {
            catalog: pb.catalog,
            schema: pb.schema,
            table: pb.table,
        }
    }
}

impl From<TableIdentifier> for ceresdbproto::remote_engine::TableIdentifier {
    fn from(table_ident: TableIdentifier) -> Self {
        Self {
            catalog: table_ident.catalog,
            schema: table_ident.schema,
            table: table_ident.table,
        }
    }
}

pub struct ReadRequest {
    pub table: TableIdentifier,
    pub read_request: TableReadRequest,
}

impl TryFrom<ceresdbproto::remote_engine::ReadRequest> for ReadRequest {
    type Error = Error;

    fn try_from(
        pb: ceresdbproto::remote_engine::ReadRequest,
    ) -> std::result::Result<Self, Self::Error> {
        let table_identifier = pb.table.context(EmptyTableIdentifier)?;
        let table_read_request = pb.read_request.context(EmptyTableReadRequest)?;
        Ok(Self {
            table: table_identifier.into(),
            read_request: table_read_request
                .try_into()
                .box_err()
                .context(ConvertTableReadRequest)?,
        })
    }
}

impl TryFrom<ReadRequest> for ceresdbproto::remote_engine::ReadRequest {
    type Error = Error;

    fn try_from(request: ReadRequest) -> std::result::Result<Self, Self::Error> {
        let table_pb = request.table.into();
        let request_pb = request.read_request.try_into().context(ReadRequestToPb)?;

        Ok(Self {
            table: Some(table_pb),
            read_request: Some(request_pb),
        })
    }
}

pub struct WriteRequest {
    pub table: TableIdentifier,
    pub write_request: TableWriteRequest,
}

impl TryFrom<ceresdbproto::remote_engine::WriteRequest> for WriteRequest {
    type Error = Error;

    fn try_from(
        pb: ceresdbproto::remote_engine::WriteRequest,
    ) -> std::result::Result<Self, Self::Error> {
        let table_identifier = pb.table.context(EmptyTableIdentifier)?;
        let row_group_pb = pb.row_group.context(EmptyRowGroup)?;
        let table_schema: Schema = row_group_pb
            .table_schema
            .context(EmptyTableSchema)?
            .try_into()
            .box_err()
            .context(ConvertTableSchema)?;
        let row_group = if row_group_pb.version == ENCODE_ROWS_WITH_AVRO {
            avro::avro_rows_to_row_group(table_schema, &row_group_pb.rows)
                .box_err()
                .context(ConvertRowGroup)?
        } else {
            UnsupportedConvertRowGroup {
                version: row_group_pb.version,
            }
            .fail()?
        };
        Ok(Self {
            table: table_identifier.into(),
            write_request: TableWriteRequest { row_group },
        })
    }
}

impl TryFrom<WriteRequest> for ceresdbproto::remote_engine::WriteRequest {
    type Error = Error;

    fn try_from(request: WriteRequest) -> std::result::Result<Self, Self::Error> {
        // Row group to pb.
        let row_group = request.write_request.row_group;
        let table_schema_pb = row_group.schema().into();
        let min_timestamp = row_group.min_timestamp().as_i64();
        let max_timestamp = row_group.max_timestmap().as_i64();
        let avro_rows =
            avro::row_group_to_avro_rows(row_group).context(WriteRequestToPbWithCause {
                table_ident: request.table.clone(),
            })?;

        let row_group_pb = ceresdbproto::remote_engine::RowGroup {
            version: ENCODE_ROWS_WITH_AVRO,
            table_schema: Some(table_schema_pb),
            rows: avro_rows,
            min_timestamp,
            max_timestamp,
        };

        // Table ident to pb.
        let table_pb = request.table.into();

        Ok(Self {
            table: Some(table_pb),
            row_group: Some(row_group_pb),
        })
    }
}
