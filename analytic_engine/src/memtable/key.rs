// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Memtable key
//!
//! Some concepts:
//! - User key (row key) is a bytes encoded from the key columns of a row
//! - Internal key contains
//!     - user key
//!     - memtable key sequence
//!         - sequence number
//!         - index

use std::{cmp::Ordering, mem};

use bytes_ext::{BufMut, BytesMut, SafeBuf, SafeBufMut};
use codec::{memcomparable::MemComparable, Decoder, Encoder};
use common_types::{row::Row, schema::Schema, SequenceNumber};
use macros::define_result;
use skiplist::KeyComparator;
use snafu::{ensure, Backtrace, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to encode key datum, err:{}", source))]
    EncodeKeyDatum { source: codec::memcomparable::Error },

    #[snafu(display("Failed to encode sequence, err:{}", source))]
    EncodeSequence { source: bytes_ext::Error },

    #[snafu(display("Failed to encode row index, err:{}", source))]
    EncodeIndex { source: bytes_ext::Error },

    #[snafu(display("Failed to decode sequence, err:{}", source))]
    DecodeSequence { source: bytes_ext::Error },

    #[snafu(display("Failed to decode row index, err:{}", source))]
    DecodeIndex { source: bytes_ext::Error },

    #[snafu(display(
        "Insufficient internal key length, len:{}.\nBacktrace:\n{}",
        len,
        backtrace
    ))]
    InternalKeyLen { len: usize, backtrace: Backtrace },
}

define_result!(Error);

// u64 + u32
const KEY_SEQUENCE_BYTES_LEN: usize = 12;

/// Row index in the batch
pub type RowIndex = u32;

/// Sequence number of row in memtable
///
/// Contains:
/// - sequence number in wal (sequence number of the write batch)
/// - unique index of the row in the write batch
///
/// Ordering:
/// 1. ordered by sequence desc
/// 2. ordered by index desc
///
/// The desc order is implemented via MAX - seq
///
/// The index is used to distinguish rows with same key of the same write batch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeySequence(SequenceNumber, RowIndex);

impl KeySequence {
    pub fn new(sequence: SequenceNumber, index: RowIndex) -> Self {
        Self(sequence, index)
    }

    #[inline]
    pub fn sequence(&self) -> SequenceNumber {
        self.0
    }

    #[inline]
    pub fn row_index(&self) -> RowIndex {
        self.1
    }
}

// TODO(yingwen): We also need opcode (PUT/DELETE), put it in key or row value
/// Comparable internal key encoder
///
/// Key order:
/// 1. ordered by user key ascend (key parts of a row)
/// 2. ordered by sequence descend
///
/// Encoding:
/// user_key + sequence
///
/// REQUIRE: The schema of row to encode matches the Self::schema
pub struct ComparableInternalKey<'a> {
    /// Sequence number of the row
    sequence: KeySequence,
    /// Schema of row
    schema: &'a Schema,
}

impl<'a> ComparableInternalKey<'a> {
    pub fn new(sequence: KeySequence, schema: &'a Schema) -> Self {
        Self { sequence, schema }
    }
}

impl<'a> Encoder<Row> for ComparableInternalKey<'a> {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, value: &Row) -> Result<()> {
        let encoder = MemComparable;
        for idx in self.schema.primary_key_indexes() {
            encoder.encode(buf, &value[*idx]).context(EncodeKeyDatum)?;
        }
        SequenceCodec.encode(buf, &self.sequence)?;

        Ok(())
    }

    fn estimate_encoded_size(&self, value: &Row) -> usize {
        let encoder = MemComparable;
        let mut total_len = 0;
        for idx in self.schema.primary_key_indexes() {
            total_len += encoder.estimate_encoded_size(&value[*idx]);
        }
        total_len += KEY_SEQUENCE_BYTES_LEN;

        total_len
    }
}

pub struct SequenceCodec;

impl Encoder<KeySequence> for SequenceCodec {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, value: &KeySequence) -> Result<()> {
        // Encode sequence number and index in descend order
        encode_sequence_number(buf, value.sequence())?;
        let reversed_index = RowIndex::MAX - value.row_index();
        buf.try_put_u32(reversed_index).context(EncodeIndex)?;

        Ok(())
    }

    fn estimate_encoded_size(&self, _value: &KeySequence) -> usize {
        KEY_SEQUENCE_BYTES_LEN
    }
}

impl Decoder<KeySequence> for SequenceCodec {
    type Error = Error;

    fn decode<B: SafeBuf>(&self, buf: &mut B) -> Result<KeySequence> {
        let sequence = buf.try_get_u64().context(DecodeSequence)?;
        // Reverse sequence
        let sequence = SequenceNumber::MAX - sequence;
        let row_index = buf.try_get_u32().context(DecodeIndex)?;
        // Reverse row index
        let row_index = RowIndex::MAX - row_index;

        Ok(KeySequence::new(sequence, row_index))
    }
}

#[inline]
fn encode_sequence_number<B: SafeBufMut>(buf: &mut B, sequence: SequenceNumber) -> Result<()> {
    // The sequence need to encode in descend order
    let reversed_sequence = SequenceNumber::MAX - sequence;
    // Encode sequence
    buf.try_put_u64(reversed_sequence).context(EncodeSequence)?;
    Ok(())
}

// TODO(yingwen): Maybe make decoded internal key a type?

/// Encode internal key from user key for seek
///
/// - user_key: the user key to encode
/// - sequence: the sequence number to encode into internal key
/// - scratch: buffer to store the encoded internal key, the scratch will be
///   clear
///
/// Returns the slice to the encoded internal key
pub fn internal_key_for_seek<'a>(
    user_key: &[u8],
    sequence: SequenceNumber,
    scratch: &'a mut BytesMut,
) -> Result<&'a [u8]> {
    scratch.clear();

    scratch.reserve(user_key.len() + mem::size_of::<SequenceNumber>());
    scratch.extend_from_slice(user_key);
    encode_sequence_number(scratch, sequence)?;

    Ok(&scratch[..])
}

/// Encode internal key from user key for seek
///
/// - user_key: the user key to encode
/// - sequence: the sequence number to encode into internal key
/// - scratch: buffer to store the encoded internal key, the scratch will be
///   clear
///
/// Returns the slice to the encoded internal key
pub fn user_key_for_seek<'a>(user_key: &[u8], scratch: &'a mut BytesMut) -> Result<&'a [u8]> {
    scratch.clear();

    scratch.reserve(user_key.len());
    scratch.extend_from_slice(user_key);

    Ok(&scratch[..])
}

/// Decode user key and sequence number from the internal key
pub fn user_key_from_internal_key(internal_key: &[u8]) -> Result<(&[u8], KeySequence)> {
    // Empty user key is meaningless
    ensure!(
        internal_key.len() > KEY_SEQUENCE_BYTES_LEN,
        InternalKeyLen {
            len: internal_key.len(),
        }
    );

    let (left, mut right) = internal_key.split_at(internal_key.len() - KEY_SEQUENCE_BYTES_LEN);
    // Decode sequence number from right part
    let sequence = SequenceCodec.decode(&mut right)?;

    Ok((left, sequence))
}

#[derive(Debug, Clone)]
pub struct BytewiseComparator;

impl KeyComparator for BytewiseComparator {
    #[inline]
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline]
    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        lhs == rhs
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sequence_codec() {
        let codec = SequenceCodec;

        let sequence = KeySequence::new(123, 456);
        assert_eq!(12, codec.estimate_encoded_size(&sequence));
        let mut buf = Vec::new();
        codec.encode(&mut buf, &sequence).unwrap();
        assert_eq!(12, buf.len());

        let mut b = &buf[..];
        let decoded_sequence = codec.decode(&mut b).unwrap();

        assert_eq!(sequence, decoded_sequence);
    }
}
