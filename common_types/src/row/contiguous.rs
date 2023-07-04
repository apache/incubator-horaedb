// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Contiguous row.

use std::{
    convert::{TryFrom, TryInto},
    debug_assert_eq, fmt, mem,
    ops::{Deref, DerefMut},
    str,
};

use snafu::{ensure, Backtrace, Snafu};

use crate::{
    bitset::BitSet,
    datum::{Datum, DatumKind, DatumView},
    projected_schema::RowProjector,
    row::Row,
    schema::{IndexInWriterSchema, Schema},
    time::Timestamp,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "String is too long to encode into row (max is {MAX_STRING_LEN}), len:{len}.\nBacktrace:\n{backtrace}",
    ))]
    StringTooLong { len: usize, backtrace: Backtrace },

    #[snafu(display(
        "Row is too long to encode(max is {MAX_ROW_LEN}), len:{len}.\nBacktrace:\n{backtrace}",
    ))]
    RowTooLong { len: usize, backtrace: Backtrace },

    #[snafu(display("Number of null columns is missing.\nBacktrace:\n{backtrace}"))]
    NumNullColsMissing { backtrace: Backtrace },

    #[snafu(display("The raw bytes of bit set is invalid, expect_len:{expect_len}, give_len:{given_len}.\nBacktrace:\n{backtrace}"))]
    InvalidBitSetBytes {
        expect_len: usize,
        given_len: usize,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Size to store the offset of string buffer.
type OffsetSize = u32;

/// Max allowed string length of datum to store in a contiguous row (16 MB).
const MAX_STRING_LEN: usize = 1024 * 1024 * 16;
/// Max allowed length of total bytes in a contiguous row (1 GB).
const MAX_ROW_LEN: usize = 1024 * 1024 * 1024;

/// Row encoded in a contiguous buffer.
pub trait ContiguousRow {
    /// Returns the number of datums.
    fn num_datum_views(&self) -> usize;

    /// Returns [DatumView] of column in given index, and returns null if the
    /// datum kind is unknown.
    ///
    /// Panic if index or buffer is out of bound.
    fn datum_view_at(&self, index: usize) -> DatumView;
}

pub enum ContiguousRowReader<'a, T> {
    NoNulls(ContiguousRowReaderNoNulls<'a, T>),
    WithNulls(ContiguousRowReaderWithNulls<'a, T>),
}

pub struct ContiguousRowReaderNoNulls<'a, T> {
    inner: &'a T,
    byte_offsets: &'a [usize],
    datum_offset: usize,
}

pub struct ContiguousRowReaderWithNulls<'a, T> {
    inner: &'a T,
    byte_offsets: Vec<isize>,
    datum_offset: usize,
}

impl<'a, T: Deref<Target = [u8]>> ContiguousRowReader<'a, T> {
    pub fn try_new(inner: &'a T, schema: &'a Schema) -> Result<Self> {
        let byte_offsets = schema.byte_offsets();
        ensure!(inner.len() >= 4, NumNullColsMissing);
        let num_null_cols = u32::from_ne_bytes(inner[0..4].try_into().unwrap()) as usize;
        if num_null_cols > 0 {
            ContiguousRowReaderWithNulls::try_new(inner, schema, num_null_cols).map(Self::WithNulls)
        } else {
            let reader = ContiguousRowReaderNoNulls {
                inner,
                byte_offsets,
                datum_offset: 4,
            };
            Ok(Self::NoNulls(reader))
        }
    }
}

impl<'a, T: Deref<Target = [u8]>> ContiguousRow for ContiguousRowReader<'a, T> {
    fn num_datum_views(&self) -> usize {
        match self {
            Self::NoNulls(v) => v.num_datum_views(),
            Self::WithNulls(v) => v.num_datum_views(),
        }
    }

    fn datum_view_at(&self, index: usize) -> DatumView {
        match self {
            Self::NoNulls(v) => v.datum_view_at(index),
            Self::WithNulls(v) => v.datum_view_at(index),
        }
    }
}

impl<'a, T: Deref<Target = [u8]>> ContiguousRowReaderWithNulls<'a, T> {
    fn try_new(inner: &'a T, schema: &'a Schema, num_null_cols: usize) -> Result<Self> {
        assert!(num_null_cols > 0);
        let bit_set_size = BitSet::num_bytes(num_null_cols);
        let bit_set_buf = &inner[4..];
        ensure!(
            bit_set_buf.len() >= bit_set_size,
            InvalidBitSetBytes {
                expect_len: bit_set_size,
                given_len: bit_set_buf.len()
            }
        );

        let nulls_bit_set =
            BitSet::try_from_raw(bit_set_buf[..bit_set_size].to_vec(), num_null_cols).unwrap();

        let mut col_offsets = Vec::with_capacity(schema.num_columns());
        let mut acc_null_bytes = 0;
        for (index, expect_offset) in schema.byte_offsets().iter().enumerate() {
            match nulls_bit_set.is_set(index) {
                Some(true) => col_offsets.push((expect_offset - acc_null_bytes) as isize),
                Some(false) => {
                    col_offsets.push(-1);
                    acc_null_bytes += byte_size_of_datum(&schema.column(index).data_type);
                }
                None => col_offsets.push(-1),
            }
        }

        Ok(Self {
            inner,
            byte_offsets: col_offsets,
            datum_offset: 4 + bit_set_size,
        })
    }
}

impl<'a, T: Deref<Target = [u8]>> ContiguousRow for ContiguousRowReaderWithNulls<'a, T> {
    fn num_datum_views(&self) -> usize {
        self.byte_offsets.len()
    }

    fn datum_view_at(&self, index: usize) -> DatumView<'a> {
        let offset = self.byte_offsets[index];
        if offset < 0 {
            DatumView::Null
        } else {
            let datum_offset = self.datum_offset + offset as usize;
            let datum_buf = &self.inner[datum_offset..];
            datum_view_at(datum_buf, self.inner)
        }
    }
}

impl<'a, T: Deref<Target = [u8]>> ContiguousRow for ContiguousRowReaderNoNulls<'a, T> {
    fn num_datum_views(&self) -> usize {
        self.byte_offsets.len()
    }

    fn datum_view_at(&self, index: usize) -> DatumView<'a> {
        let offset = self.byte_offsets[index];
        let datum_buf = &self.inner[self.datum_offset + offset..];
        datum_view_at(datum_buf, self.inner)
    }
}

fn datum_view_at<'a>(datum_buf: &'a [u8], string_buf: &'a [u8]) -> DatumView<'a> {
    // Get datum kind, if the datum kind is unknown, returns null.
    let datum_kind = match DatumKind::try_from(datum_buf[0]) {
        Ok(v) => v,
        Err(_) => return DatumView::Null,
    };

    // Advance 1 byte to skip the header byte.
    let datum_buf = &datum_buf[1..];
    must_read_view(&datum_kind, datum_buf, string_buf)
}

/// Contiguous row with projection information.
///
/// The caller must ensure the source schema of projector is the same as the
/// schema of source row.
pub struct ProjectedContiguousRow<'a, T> {
    source_row: T,
    projector: &'a RowProjector,
}

impl<'a, T: ContiguousRow> ProjectedContiguousRow<'a, T> {
    pub fn new(source_row: T, projector: &'a RowProjector) -> Self {
        Self {
            source_row,
            projector,
        }
    }
}

impl<'a, T: ContiguousRow> ContiguousRow for ProjectedContiguousRow<'a, T> {
    fn num_datum_views(&self) -> usize {
        self.projector.source_projection().len()
    }

    fn datum_view_at(&self, index: usize) -> DatumView {
        let p = self.projector.source_projection()[index];

        match p {
            Some(index_in_source) => self.source_row.datum_view_at(index_in_source),
            None => DatumView::Null,
        }
    }
}

impl<'a, T: ContiguousRow> fmt::Debug for ProjectedContiguousRow<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for i in 0..self.num_datum_views() {
            let view = self.datum_view_at(i);
            list.entry(&view);
        }
        list.finish()
    }
}

/// In memory buffer to hold data of a contiguous row.
pub trait RowBuffer: DerefMut<Target = [u8]> {
    /// Clear and resize the buffer size to `new_len` with given `value`.
    fn reset(&mut self, new_len: usize, value: u8);

    /// Append slice into the buffer, resize the buffer automatically.
    fn append_slice(&mut self, src: &[u8]);
}

/// A writer to build a contiguous row.
pub struct ContiguousRowWriter<'a, T> {
    inner: &'a mut T,
    /// The schema the row group need to be encoded into, the schema
    /// of the row need to be write compatible for the table schema.
    table_schema: &'a Schema,
    /// The index mapping from table schema to column in the
    /// schema of row group.
    index_in_writer: &'a IndexInWriterSchema,
}

// TODO(yingwen): Try to replace usage of row by contiguous row.
impl<'a, T: RowBuffer + 'a> ContiguousRowWriter<'a, T> {
    pub fn new(
        inner: &'a mut T,
        table_schema: &'a Schema,
        index_in_writer: &'a IndexInWriterSchema,
    ) -> Self {
        Self {
            inner,
            table_schema,
            index_in_writer,
        }
    }

    fn write_datum(
        inner: &mut T,
        datum: &Datum,
        offset: &mut usize,
        next_string_offset: &mut usize,
    ) -> Result<()> {
        match datum {
            // Already filled by null, nothing to do.
            Datum::Null => {}
            Datum::Timestamp(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::Timestamp.into_u8());
                let value_buf = v.as_i64().to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Double(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::Double.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Float(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::Float.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Varbinary(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::Varbinary.into_u8());

                ensure!(
                    *next_string_offset <= MAX_ROW_LEN,
                    StringTooLong {
                        len: *next_string_offset
                    }
                );
                // Encode the string offset as a u32.
                let value_buf = (*next_string_offset as u32).to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);

                // Encode length of string as a u32.
                *next_string_offset += mem::size_of::<u32>() + v.len();
                ensure!(v.len() <= MAX_STRING_LEN, StringTooLong { len: v.len() });
                let string_len = v.len() as u32;
                Self::write_slice_to_offset(inner, next_string_offset, &string_len.to_ne_bytes());
                Self::write_slice_to_offset(inner, next_string_offset, v);
            }
            Datum::String(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::String.into_u8());
                ensure!(
                    *next_string_offset <= MAX_ROW_LEN,
                    StringTooLong {
                        len: *next_string_offset
                    }
                );
                // Encode the string offset as a u32.
                let value_buf = (*next_string_offset as u32).to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
                // Encode length of string as a u32.
                *next_string_offset += mem::size_of::<u32>() + v.len();
                ensure!(v.len() <= MAX_STRING_LEN, StringTooLong { len: v.len() });
                let bytes_len = v.len() as u32;
                Self::write_slice_to_offset(inner, next_string_offset, &bytes_len.to_ne_bytes());
                Self::write_slice_to_offset(inner, next_string_offset, v.as_bytes());
            }
            Datum::UInt64(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::UInt64.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::UInt32(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::UInt32.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::UInt16(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::UInt16.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::UInt8(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::UInt8.into_u8());
                Self::write_slice_to_offset(inner, offset, &[*v]);
            }
            Datum::Int64(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::Int64.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Int32(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::Int32.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Int16(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::Int16.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Int8(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::Int8.into_u8());
                Self::write_slice_to_offset(inner, offset, &[*v as u8]);
            }
            Datum::Boolean(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::Boolean.into_u8());
                Self::write_slice_to_offset(inner, offset, &[*v as u8]);
            }
            Datum::Date(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::Date.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
            Datum::Time(v) => {
                Self::write_byte_to_offset(inner, offset, DatumKind::Time.into_u8());
                let value_buf = v.to_ne_bytes();
                Self::write_slice_to_offset(inner, offset, &value_buf);
            }
        }

        Ok(())
    }

    /// Write a row to the buffer, the buffer will be reset first.
    pub fn write_row(&mut self, row: &Row) -> Result<()> {
        let mut num_null_cols = 0;
        for index_in_table in 0..self.table_schema.num_columns() {
            if let Some(writer_index) = self.index_in_writer.column_index_in_writer(index_in_table)
            {
                let datum = &row[writer_index];
                if datum.is_null() {
                    num_null_cols += 1;
                }
            } else {
                num_null_cols += 1;
            }
        }

        if num_null_cols > 0 {
            self.write_row_with_nulls(row)
        } else {
            self.write_row_without_nulls(row)
        }
    }

    fn write_row_with_nulls(&mut self, row: &Row) -> Result<()> {
        let mut encoded_len = 0;
        let mut num_bytes_of_string = 0;
        for index_in_table in 0..self.table_schema.num_columns() {
            if let Some(writer_index) = self.index_in_writer.column_index_in_writer(index_in_table)
            {
                let datum = &row[writer_index];
                // No need to store null column.
                if !datum.is_null() {
                    encoded_len += byte_size_of_datum(&datum.kind());
                }

                if !datum.is_fixed_sized() {
                    // For the datum content and the length of it
                    num_bytes_of_string += datum.size() + 4;
                }
            }
        }

        let num_bits = self.table_schema.num_columns();
        let mut nulls_bit_set = BitSet::new(num_bits);
        // The flag for the BitSet, denoting the number of the columns.
        encoded_len += 4 + nulls_bit_set.as_bytes().len();

        // Pre-allocate the memory.
        self.inner.reset(encoded_len, 0);
        let mut next_string_offset = encoded_len - num_bytes_of_string;
        let mut datum_offset = 4 + nulls_bit_set.as_bytes().len();
        for index_in_table in 0..self.table_schema.num_columns() {
            if let Some(writer_index) = self.index_in_writer.column_index_in_writer(index_in_table)
            {
                let datum = &row[writer_index];
                // Write datum bytes to the buffer.
                Self::write_datum(
                    self.inner,
                    datum,
                    &mut datum_offset,
                    &mut next_string_offset,
                )?;

                nulls_bit_set.set(writer_index);
            }
        }

        // Storing the number of null columns as u32 is enough.
        Self::write_slice_to_offset(self.inner, &mut 0, &(num_bits as u32).to_ne_bytes());
        Self::write_slice_to_offset(self.inner, &mut 4, nulls_bit_set.as_bytes());

        debug_assert_eq!(datum_offset, encoded_len - num_bytes_of_string);
        debug_assert_eq!(next_string_offset, encoded_len);

        Ok(())
    }

    fn write_row_without_nulls(&mut self, row: &Row) -> Result<()> {
        let datum_buffer_len = self.table_schema.string_buffer_offset() + 4;
        let mut encoded_len = datum_buffer_len;
        for index_in_table in 0..self.table_schema.num_columns() {
            if let Some(writer_index) = self.index_in_writer.column_index_in_writer(index_in_table)
            {
                let datum = &row[writer_index];
                if !datum.is_fixed_sized() {
                    // For the datum content and the length of it
                    encoded_len += datum.size() + 4;
                }
            }
        }

        // Pre-allocate memory for row.
        self.inner.reset(encoded_len, DatumKind::Null.into_u8());

        // Offset to next string in string buffer.
        let mut next_string_offset = datum_buffer_len;
        let mut datum_offset = 4;
        for index_in_table in 0..self.table_schema.num_columns() {
            if let Some(writer_index) = self.index_in_writer.column_index_in_writer(index_in_table)
            {
                let datum = &row[writer_index];
                // Write datum bytes to the buffer.
                Self::write_datum(
                    self.inner,
                    datum,
                    &mut datum_offset,
                    &mut next_string_offset,
                )?;
            }
        }

        debug_assert_eq!(datum_offset, datum_buffer_len);
        debug_assert_eq!(next_string_offset, encoded_len);
        Ok(())
    }

    #[inline]
    fn write_byte_to_offset(inner: &mut T, offset: &mut usize, value: u8) {
        inner[*offset] = value;
        *offset += 1;
    }

    #[inline]
    fn write_slice_to_offset(inner: &mut T, offset: &mut usize, value_buf: &[u8]) {
        let dst = &mut inner[*offset..*offset + value_buf.len()];
        dst.copy_from_slice(value_buf);
        *offset += value_buf.len();
    }
}

/// The byte size to encode the datum of this kind in memory.
///
/// Returns the (datum size + 1) for header. For integer types, the datum
/// size is the memory size of the integer type. For string types, the
/// datum size is the memory size to hold the offset.
pub(crate) fn byte_size_of_datum(kind: &DatumKind) -> usize {
    let datum_size = match kind {
        DatumKind::Null => 1,
        DatumKind::Timestamp => mem::size_of::<Timestamp>(),
        DatumKind::Double => mem::size_of::<f64>(),
        DatumKind::Float => mem::size_of::<f32>(),
        // The size of offset.
        DatumKind::Varbinary | DatumKind::String => mem::size_of::<OffsetSize>(),
        DatumKind::UInt64 => mem::size_of::<u64>(),
        DatumKind::UInt32 => mem::size_of::<u32>(),
        DatumKind::UInt16 => mem::size_of::<u16>(),
        DatumKind::UInt8 => mem::size_of::<u8>(),
        DatumKind::Int64 => mem::size_of::<i64>(),
        DatumKind::Int32 => mem::size_of::<i32>(),
        DatumKind::Int16 => mem::size_of::<i16>(),
        DatumKind::Int8 => mem::size_of::<i8>(),
        DatumKind::Boolean => mem::size_of::<bool>(),
        DatumKind::Date => mem::size_of::<i32>(),
        DatumKind::Time => mem::size_of::<i64>(),
    };

    datum_size + 1
}

/// Read datum view from given datum buf, and may reference the string in
/// `string_buf`.
///
/// Panic if out of bound.
///
/// ## Safety
/// The string in buffer must be valid utf8.
fn must_read_view<'a>(
    datum_kind: &DatumKind,
    datum_buf: &'a [u8],
    string_buf: &'a [u8],
) -> DatumView<'a> {
    match datum_kind {
        DatumKind::Null => DatumView::Null,
        DatumKind::Timestamp => {
            let value_buf = datum_buf[..mem::size_of::<i64>()].try_into().unwrap();
            let ts = Timestamp::new(i64::from_ne_bytes(value_buf));
            DatumView::Timestamp(ts)
        }
        DatumKind::Double => {
            let value_buf = datum_buf[..mem::size_of::<f64>()].try_into().unwrap();
            let v = f64::from_ne_bytes(value_buf);
            DatumView::Double(v)
        }
        DatumKind::Float => {
            let value_buf = datum_buf[..mem::size_of::<f32>()].try_into().unwrap();
            let v = f32::from_ne_bytes(value_buf);
            DatumView::Float(v)
        }
        DatumKind::Varbinary => {
            let bytes = must_read_bytes(datum_buf, string_buf);
            DatumView::Varbinary(bytes)
        }
        DatumKind::String => {
            let bytes = must_read_bytes(datum_buf, string_buf);
            let v = unsafe { str::from_utf8_unchecked(bytes) };
            DatumView::String(v)
        }
        DatumKind::UInt64 => {
            let value_buf = datum_buf[..mem::size_of::<u64>()].try_into().unwrap();
            let v = u64::from_ne_bytes(value_buf);
            DatumView::UInt64(v)
        }
        DatumKind::UInt32 => {
            let value_buf = datum_buf[..mem::size_of::<u32>()].try_into().unwrap();
            let v = u32::from_ne_bytes(value_buf);
            DatumView::UInt32(v)
        }
        DatumKind::UInt16 => {
            let value_buf = datum_buf[..mem::size_of::<u16>()].try_into().unwrap();
            let v = u16::from_ne_bytes(value_buf);
            DatumView::UInt16(v)
        }
        DatumKind::UInt8 => DatumView::UInt8(datum_buf[0]),
        DatumKind::Int64 => {
            let value_buf = datum_buf[..mem::size_of::<i64>()].try_into().unwrap();
            let v = i64::from_ne_bytes(value_buf);
            DatumView::Int64(v)
        }
        DatumKind::Int32 => {
            let value_buf = datum_buf[..mem::size_of::<i32>()].try_into().unwrap();
            let v = i32::from_ne_bytes(value_buf);
            DatumView::Int32(v)
        }
        DatumKind::Int16 => {
            let value_buf = datum_buf[..mem::size_of::<i16>()].try_into().unwrap();
            let v = i16::from_ne_bytes(value_buf);
            DatumView::Int16(v)
        }
        DatumKind::Int8 => DatumView::Int8(datum_buf[0] as i8),
        DatumKind::Boolean => DatumView::Boolean(datum_buf[0] != 0),
        DatumKind::Date => {
            let value_buf = datum_buf[..mem::size_of::<i32>()].try_into().unwrap();
            let v = i32::from_ne_bytes(value_buf);
            DatumView::Date(v)
        }
        DatumKind::Time => {
            let value_buf = datum_buf[..mem::size_of::<i64>()].try_into().unwrap();
            let v = i64::from_ne_bytes(value_buf);
            DatumView::Time(v)
        }
    }
}

fn must_read_bytes<'a>(datum_buf: &'a [u8], string_buf: &'a [u8]) -> &'a [u8] {
    // Read offset of string in string buf.
    let value_buf = datum_buf[..mem::size_of::<OffsetSize>()]
        .try_into()
        .unwrap();
    let offset = OffsetSize::from_ne_bytes(value_buf) as usize;
    let string_buf = &string_buf[offset..];

    // Read len of the string.
    let len_buf = string_buf[..mem::size_of::<u32>()].try_into().unwrap();
    let string_len = u32::from_ne_bytes(len_buf) as usize;
    let string_buf = &string_buf[mem::size_of::<u32>()..];

    // Read string.
    &string_buf[..string_len]
}

impl RowBuffer for Vec<u8> {
    fn reset(&mut self, new_len: usize, value: u8) {
        self.clear();

        self.resize(new_len, value);
    }

    fn append_slice(&mut self, src: &[u8]) {
        self.extend_from_slice(src);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        projected_schema::ProjectedSchema,
        tests::{build_rows, build_schema},
    };

    fn check_contiguous_row(row: &Row, reader: impl ContiguousRow, projection: Option<Vec<usize>>) {
        let range = if let Some(projection) = projection {
            projection
        } else {
            (0..reader.num_datum_views()).collect()
        };
        for i in range {
            let datum = &row[i];
            let view = reader.datum_view_at(i);

            assert_eq!(datum.as_view(), view);
        }
    }

    #[test]
    fn test_contiguous_read_write() {
        let schema = build_schema();
        let rows = build_rows();
        let index_in_writer = IndexInWriterSchema::for_same_schema(schema.num_columns());

        let mut buf = Vec::new();
        for row in rows {
            let mut writer = ContiguousRowWriter::new(&mut buf, &schema, &index_in_writer);

            writer.write_row(&row).unwrap();

            let reader = ContiguousRowReader::try_new(&buf, &schema).unwrap();
            check_contiguous_row(&row, reader, None);
        }
    }

    #[test]
    fn test_project_contiguous_read_write() {
        let schema = build_schema();
        assert!(schema.num_columns() > 1);
        let projection: Vec<usize> = (0..schema.num_columns() - 1).collect();
        let projected_schema =
            ProjectedSchema::new(schema.clone(), Some(projection.clone())).unwrap();
        let row_projected_schema = projected_schema.try_project_with_key(&schema).unwrap();
        let rows = build_rows();
        let index_in_writer = IndexInWriterSchema::for_same_schema(schema.num_columns());

        let mut buf = Vec::new();
        for row in rows {
            let mut writer = ContiguousRowWriter::new(&mut buf, &schema, &index_in_writer);

            writer.write_row(&row).unwrap();

            let source_row = ContiguousRowReader::try_new(&buf, &schema).unwrap();
            let projected_row = ProjectedContiguousRow::new(source_row, &row_projected_schema);
            check_contiguous_row(&row, projected_row, Some(projection.clone()));
        }
    }
}
