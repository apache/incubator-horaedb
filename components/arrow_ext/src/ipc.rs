// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Utilities for `RecordBatch` serialization using Arrow IPC

use std::io::Cursor;

use arrow::{
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Arror error, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ArrowError {
        source: arrow::error::ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode record batch.\nBacktrace:\n{}", backtrace))]
    Decode { backtrace: Backtrace },
}

type Result<T> = std::result::Result<T, Error>;

pub fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let buffer: Vec<u8> = Vec::new();
    let mut stream_writer = StreamWriter::try_new(buffer, &batch.schema()).context(ArrowError)?;
    stream_writer.write(batch).context(ArrowError)?;
    stream_writer.into_inner().context(ArrowError)
}

pub fn decode_record_batch(bytes: Vec<u8>) -> Result<RecordBatch> {
    let mut stream_reader = StreamReader::try_new(Cursor::new(bytes), None).context(ArrowError)?;
    stream_reader.next().context(Decode)?.context(ArrowError)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    fn create_batch(rows: usize) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let a = Int32Array::from_iter_values(0..rows as i32);
        let b = StringArray::from_iter_values((0..rows).map(|i| i.to_string()));

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap()
    }

    #[test]
    fn test_ipc_encode_decode() {
        let batch = create_batch(1024);
        let bytes = encode_record_batch(&batch).unwrap();

        assert_eq!(batch, decode_record_batch(bytes).unwrap());
    }
}
