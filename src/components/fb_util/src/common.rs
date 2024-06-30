//! This module defines common request/response types as well as the JsonCodec
//! that is used by the json.helloworld.Greeter service which is defined
//! manually (instead of via proto files) by the `build_json_codec_service`
//! function in the `examples/build.rs` file.
use std::io::Read;

use bytes::{Buf, BufMut};
use flatbuffers;
use tonic::{
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    Status,
};

pub struct FlatBufferBytes(Vec<u8>);

impl FlatBufferBytes {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn serialize<'buf, T: flatbuffers::Follow<'buf> + 'buf>(
        mut builder: flatbuffers::FlatBufferBuilder<'buf>,
        root_offset: flatbuffers::WIPOffset<T>,
    ) -> Self {
        builder.finish(root_offset, None);
        let (mut data, head) = builder.collapse();
        Self(data.drain(head..).collect())
    }

    pub fn deserialize<'buf, T: flatbuffers::Follow<'buf> + flatbuffers::Verifiable + 'buf>(
        &'buf self,
    ) -> Result<T::Inner, Box<dyn std::error::Error>> {
        flatbuffers::root::<T>(self.0.as_slice())
            .map_err(|x| Box::new(x) as Box<dyn std::error::Error>)
    }
}

#[derive(Debug)]
pub struct FlatBufferEncoder();

impl Encoder for FlatBufferEncoder {
    type Error = Status;
    type Item = FlatBufferBytes;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        buf.put_slice(item.0.as_slice());
        Ok(())
    }
}

#[derive(Debug)]
pub struct FlatBufferDecoder();

impl Decoder for FlatBufferDecoder {
    type Error = Status;
    type Item = FlatBufferBytes;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        if !buf.has_remaining() {
            return Ok(None);
        }
        let mut data: Vec<u8> = Vec::new();
        buf.reader()
            .read_to_end(&mut data)
            .map_err(|e| Status::internal(e.to_string()))?;
        let item = FlatBufferBytes::new(data);
        Ok(Some(item))
    }
}

/// A [`Codec`] that implements `application/grpc+json` via the serde library.
#[derive(Debug, Clone, Default)]
pub struct FlatBufferCodec();

impl Codec for FlatBufferCodec {
    type Decode = FlatBufferBytes;
    type Decoder = FlatBufferDecoder;
    type Encode = FlatBufferBytes;
    type Encoder = FlatBufferEncoder;

    fn encoder(&mut self) -> Self::Encoder {
        FlatBufferEncoder()
    }

    fn decoder(&mut self) -> Self::Decoder {
        FlatBufferDecoder()
    }
}
