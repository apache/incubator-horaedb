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

//! Sst writer implementation based on parquet.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use common_types::{
    datum::DatumKind, record_batch::FetchedRecordBatch, request_id::RequestId, schema::Schema,
    time::TimeRange,
};
use datafusion::parquet::basic::Compression;
use futures::StreamExt;
use generic_error::BoxError;
use logger::{debug, error, info};
use object_store::{ObjectStoreRef, Path};
use parquet::data_type::AsBytes;
use snafu::{OptionExt, ResultExt};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    sst::{
        factory::ObjectStorePickerRef,
        file::Level,
        parquet::{
            encoding::{encode_sst_meta_data, ColumnEncoding, EncodeOptions, ParquetEncoder},
            meta_data::{
                filter::{ParquetFilter, RowGroupFilter, RowGroupFilterBuilder},
                ColumnValueSet, ParquetMetaData,
            },
        },
        writer::{
            self, BuildParquetFilter, EncodePbData, EncodeRecordBatch, ExpectTimestampColumn, Io,
            MetaData, PollRecordBatch, RecordBatchStream, Result, SstInfo, SstWriter, Storage,
        },
    },
    table::sst_util,
    table_options::StorageFormat,
};

const KEEP_COLUMN_VALUE_THRESHOLD: usize = 20;
/// Only the row group which contains at least
/// `MIN_NUM_ROWS_DICT_ENCODING_SAMPLE` rows can be sampling to decide whether
/// to do dictionary encoding.
const MIN_NUM_ROWS_SAMPLE_DICT_ENCODING: usize = 1024;
/// If the number of unique value exceeds
/// `total_num_values * MAX_UNIQUE_VALUE_RATIO_DICT_ENCODING`, there is no need
/// to do dictionary encoding for such column.
const MAX_UNIQUE_VALUE_RATIO_DICT_ENCODING: f64 = 0.12;

/// The implementation of sst based on parquet and object storage.
#[derive(Debug)]
pub struct ParquetSstWriter<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    store: &'a ObjectStoreRef,
    options: WriteOptions,
}

impl<'a> ParquetSstWriter<'a> {
    pub fn new(
        path: &'a Path,
        options: WriteOptions,
        store_picker: &'a ObjectStorePickerRef,
    ) -> Self {
        let store = store_picker.default_store();
        Self {
            path,
            store,
            options,
        }
    }
}

/// The writer will reorganize the record batches into row groups, and then
/// encode them to parquet file.
struct RecordBatchGroupWriter<'a> {
    request_id: RequestId,
    input: RecordBatchStream,
    meta_data: &'a MetaData,
    options: WriteOptions,

    // inner status
    input_exhausted: bool,
    // Time range of rows, not aligned to segment.
    real_time_range: Option<TimeRange>,
    // `column_values` is used to collect distinct values in each columns,
    // its order is the same with schema's columns.
    column_values: Option<Vec<Option<ColumnValueSet>>>,
}

#[derive(Clone, Debug)]
pub struct WriteOptions {
    pub num_rows_per_row_group: usize,
    pub max_buffer_size: usize,
    pub compression: Compression,
    pub sst_level: Level,
    pub column_encodings: HashMap<String, ColumnEncoding>,
}

impl WriteOptions {
    #[inline]
    pub fn need_custom_filter(&self) -> bool {
        !self.sst_level.is_min()
    }
}

impl<'a> RecordBatchGroupWriter<'a> {
    fn new(
        request_id: RequestId,
        input: RecordBatchStream,
        meta_data: &'a MetaData,
        options: WriteOptions,
    ) -> Self {
        // No need to build complex index for the min-level sst so there is no need to
        // collect the column values.
        let column_values = options.need_custom_filter().then(|| {
            meta_data
                .schema
                .columns()
                .iter()
                .map(|col| {
                    // Only keep string values now.
                    if matches!(col.data_type, DatumKind::String) {
                        Some(ColumnValueSet::StringValue(HashSet::new()))
                    } else {
                        None
                    }
                })
                .collect()
        });

        Self {
            request_id,
            input,
            meta_data,
            options,
            input_exhausted: false,
            real_time_range: None,
            column_values,
        }
    }

    /// Fetch an integral row group from the `self.input`.
    ///
    /// Except the last one, every row group is ensured to contains exactly
    /// `self.num_rows_per_row_group`. As for the last one, it will cover all
    /// the left rows.
    async fn fetch_next_row_group(
        &mut self,
        prev_record_batch: &mut Option<FetchedRecordBatch>,
    ) -> Result<Vec<FetchedRecordBatch>> {
        let mut curr_row_group = vec![];
        // Used to record the number of remaining rows to fill `curr_row_group`.
        let mut remaining = self.options.num_rows_per_row_group;

        // Keep filling `curr_row_group` until `remaining` is zero.
        while remaining > 0 {
            // Use the `prev_record_batch` to fill `curr_row_group` if possible.
            if let Some(v) = prev_record_batch {
                let total_rows = v.num_rows();
                if total_rows <= remaining {
                    // The whole record batch is part of the `curr_row_group`, and let's feed it
                    // into `curr_row_group`.
                    curr_row_group.push(prev_record_batch.take().unwrap());
                    remaining -= total_rows;
                } else {
                    // Only first `remaining` rows of the record batch belongs to `curr_row_group`,
                    // the rest should be put to `prev_record_batch` for next row group.
                    curr_row_group.push(v.slice(0, remaining));
                    *v = v.slice(remaining, total_rows - remaining);
                    remaining = 0;
                }

                continue;
            }

            if self.input_exhausted {
                break;
            }

            // Previous record batch has been exhausted, and let's fetch next record batch.
            match self.input.next().await {
                Some(v) => {
                    let v = v.context(PollRecordBatch)?;
                    debug_assert!(
                        !v.is_empty(),
                        "found empty record batch, request id:{}",
                        self.request_id
                    );

                    // Updated the exhausted `prev_record_batch`, and let next loop to continue to
                    // fill `curr_row_group`.
                    prev_record_batch.replace(v);
                }
                None => {
                    self.input_exhausted = true;
                    break;
                }
            };
        }

        Ok(curr_row_group)
    }

    fn build_column_encodings(
        &self,
        sample_row_groups: &[FetchedRecordBatch],
        column_encodings: &mut HashMap<String, ColumnEncoding>,
    ) -> Result<()> {
        let mut sampler = ColumnEncodingSampler {
            sample_row_groups,
            meta_data: self.meta_data,
            min_num_sample_rows: MIN_NUM_ROWS_SAMPLE_DICT_ENCODING,
            max_unique_value_ratio: MAX_UNIQUE_VALUE_RATIO_DICT_ENCODING,
            column_encodings,
        };
        sampler.sample()
    }

    /// Build the parquet filter for the given `row_group`.
    fn build_row_group_filter(
        &self,
        schema: &Schema,
        row_group_batch: &[FetchedRecordBatch],
    ) -> Result<RowGroupFilter> {
        let mut builder = RowGroupFilterBuilder::new(schema);

        for partial_batch in row_group_batch {
            for (col_idx, column) in partial_batch.columns().iter().enumerate() {
                for row in 0..column.num_rows() {
                    let datum_view = column.datum_view(row);
                    datum_view.do_with_bytes(|bytes| {
                        builder.add_key(col_idx, bytes);
                    });
                }
            }
        }

        builder.build().box_err().context(BuildParquetFilter)
    }

    fn update_column_values(
        column_values: &mut [Option<ColumnValueSet>],
        record_batch: &FetchedRecordBatch,
    ) {
        for (col_idx, col_values) in column_values.iter_mut().enumerate() {
            let mut too_many_values = false;
            {
                let col_values = match col_values {
                    None => continue,
                    Some(v) => v,
                };
                let rows_num = record_batch.num_rows();
                let column_block = record_batch.column(col_idx);
                for row_idx in 0..rows_num {
                    match col_values {
                        ColumnValueSet::StringValue(ss) => {
                            let datum = column_block.datum(row_idx);
                            if let Some(v) = datum.as_str() {
                                ss.insert(v.to_string());
                            }
                        }
                    }

                    if row_idx % KEEP_COLUMN_VALUE_THRESHOLD == 0
                        && col_values.len() > KEEP_COLUMN_VALUE_THRESHOLD
                    {
                        too_many_values = true;
                        break;
                    }
                }

                // Do one last check.
                if col_values.len() > KEEP_COLUMN_VALUE_THRESHOLD {
                    too_many_values = true;
                }
            }

            // When there are too many values, don't keep this column values
            // any more.
            if too_many_values {
                *col_values = None;
            }
        }
    }

    fn update_time_range(&mut self, current_range: Option<TimeRange>) {
        if let Some(current_range) = current_range {
            if let Some(real_range) = self.real_time_range {
                // Use current range to update real range,
                // We should expand range as possible as we can.
                self.real_time_range = Some(TimeRange::new_unchecked(
                    current_range
                        .inclusive_start()
                        .min(real_range.inclusive_start()),
                    current_range
                        .exclusive_end()
                        .max(real_range.exclusive_end()),
                ));
            } else {
                self.real_time_range = Some(current_range);
            }
        }
    }

    async fn write_all<W: AsyncWrite + Send + Unpin + 'static>(
        mut self,
        sink: W,
        meta_path: &Path,
    ) -> Result<(usize, ParquetMetaData, ParquetEncoder)> {
        let mut prev_record_batch: Option<FetchedRecordBatch> = None;
        let mut arrow_row_group = Vec::new();
        let mut total_num_rows = 0;

        // Build the parquet encoder.
        let mut row_group = self.fetch_next_row_group(&mut prev_record_batch).await?;
        let mut column_encodings = std::mem::take(&mut self.options.column_encodings);
        self.build_column_encodings(&row_group, &mut column_encodings)?;
        let encode_options = EncodeOptions {
            num_rows_per_row_group: self.options.num_rows_per_row_group,
            max_buffer_size: self.options.max_buffer_size,
            compression: self.options.compression,
            column_encodings,
        };
        let mut parquet_encoder =
            ParquetEncoder::try_new(sink, &self.meta_data.schema, &encode_options)
                .box_err()
                .context(EncodeRecordBatch)?;

        let mut parquet_filter = self
            .options
            .need_custom_filter()
            .then(ParquetFilter::default);
        let timestamp_index = self.meta_data.schema.timestamp_index();
        while !row_group.is_empty() {
            if let Some(filter) = &mut parquet_filter {
                filter.push_row_group_filter(
                    self.build_row_group_filter(&self.meta_data.schema, &row_group)?,
                );
            }

            let num_batches = row_group.len();
            for record_batch in row_group {
                let column_block = record_batch.column(timestamp_index);
                let ts_col = column_block.as_timestamp().context(ExpectTimestampColumn {
                    datum_kind: column_block.datum_kind(),
                })?;
                self.update_time_range(ts_col.time_range());
                if let Some(column_values) = self.column_values.as_mut() {
                    Self::update_column_values(column_values, &record_batch);
                }

                arrow_row_group.push(record_batch.into_record_batch().into_arrow_record_batch());
            }
            let num_rows = parquet_encoder
                .encode_record_batches(arrow_row_group)
                .await
                .box_err()
                .context(EncodeRecordBatch)?;

            // TODO: it will be better to use `arrow_row_group.clear()` to reuse the
            // allocated memory.
            arrow_row_group = Vec::with_capacity(num_batches);
            total_num_rows += num_rows;

            row_group = self.fetch_next_row_group(&mut prev_record_batch).await?;
        }

        let parquet_meta_data = {
            let mut parquet_meta_data = ParquetMetaData::from(self.meta_data);
            parquet_meta_data.parquet_filter = parquet_filter;
            if let Some(range) = self.real_time_range {
                parquet_meta_data.time_range = range;
            }
            // TODO: when all compaction input SST files already have column_values, we can
            // merge them from meta_data directly, calculate them here waste CPU
            // cycles.
            parquet_meta_data.column_values = self.column_values;
            parquet_meta_data
        };

        parquet_encoder
            .set_meta_data_path(Some(meta_path.to_string()))
            .box_err()
            .context(EncodeRecordBatch)?;

        Ok((total_num_rows, parquet_meta_data, parquet_encoder))
    }
}

struct ObjectStoreMultiUploadAborter<'a> {
    location: &'a Path,
    session_id: String,
    object_store: &'a ObjectStoreRef,
}

impl<'a> ObjectStoreMultiUploadAborter<'a> {
    async fn initialize_upload(
        object_store: &'a ObjectStoreRef,
        location: &'a Path,
    ) -> Result<(
        ObjectStoreMultiUploadAborter<'a>,
        Box<dyn AsyncWrite + Unpin + Send>,
    )> {
        let (session_id, upload_writer) = object_store
            .put_multipart(location)
            .await
            .context(Storage)?;
        let aborter = Self {
            location,
            session_id,
            object_store,
        };
        Ok((aborter, upload_writer))
    }

    async fn abort(self) -> Result<()> {
        self.object_store
            .abort_multipart(self.location, &self.session_id)
            .await
            .context(Storage)
    }
}

async fn write_metadata<W>(
    mut meta_sink: W,
    parquet_metadata: ParquetMetaData,
    meta_path: &object_store::Path,
) -> writer::Result<usize>
where
    W: AsyncWrite + Send + Unpin,
{
    let buf = encode_sst_meta_data(parquet_metadata).context(EncodePbData)?;
    let bytes = buf.as_bytes();
    let bytes_size = bytes.len();
    meta_sink.write_all(bytes).await.with_context(|| Io {
        file: meta_path.clone(),
    })?;

    meta_sink.shutdown().await.with_context(|| Io {
        file: meta_path.clone(),
    })?;

    Ok(bytes_size)
}

async fn multi_upload_abort(path: &Path, aborter: ObjectStoreMultiUploadAborter<'_>) {
    // The uploading file will be leaked if failed to abort. A repair command will
    // be provided to clean up the leaked files.
    if let Err(e) = aborter.abort().await {
        error!("Failed to abort multi-upload for sst:{}, err:{}", path, e);
    }
}

#[async_trait]
impl<'a> SstWriter for ParquetSstWriter<'a> {
    async fn write(
        &mut self,
        request_id: RequestId,
        meta: &MetaData,
        input: RecordBatchStream,
    ) -> writer::Result<SstInfo> {
        debug!(
            "Build parquet file, request_id:{}, meta:{:?}, num_rows_per_row_group:{}",
            request_id, meta, self.options.num_rows_per_row_group
        );

        let write_options = WriteOptions {
            num_rows_per_row_group: self.options.num_rows_per_row_group,
            max_buffer_size: self.options.max_buffer_size,
            compression: self.options.compression,
            sst_level: self.options.sst_level,
            column_encodings: std::mem::take(&mut self.options.column_encodings),
        };
        let group_writer = RecordBatchGroupWriter::new(request_id, input, meta, write_options);

        let (aborter, sink) =
            ObjectStoreMultiUploadAborter::initialize_upload(self.store, self.path).await?;

        let meta_path = Path::from(sst_util::new_metadata_path(self.path.as_ref()));

        let (total_num_rows, parquet_metadata, mut data_encoder) =
            match group_writer.write_all(sink, &meta_path).await {
                Ok(v) => v,
                Err(e) => {
                    multi_upload_abort(self.path, aborter).await;
                    return Err(e);
                }
            };
        let time_range = parquet_metadata.time_range;

        let (meta_aborter, meta_sink) =
            ObjectStoreMultiUploadAborter::initialize_upload(self.store, &meta_path).await?;
        info!("meta data begin write");
        let meta_size = match write_metadata(meta_sink, parquet_metadata, &meta_path).await {
            Ok(v) => v,
            Err(e) => {
                multi_upload_abort(self.path, aborter).await;
                multi_upload_abort(&meta_path, meta_aborter).await;
                return Err(e);
            }
        };
        info!("meta data finish write");

        data_encoder
            .set_meta_data_size(meta_size)
            .box_err()
            .context(EncodeRecordBatch)?;

        data_encoder
            .close()
            .await
            .box_err()
            .context(EncodeRecordBatch)?;

        let file_head = self.store.head(self.path).await.context(Storage)?;
        Ok(SstInfo {
            file_size: file_head.size,
            row_num: total_num_rows,
            storage_format: StorageFormat::Columnar,
            meta_path: meta_path.to_string(),
            time_range,
        })
    }
}

/// A sampler to decide the column encoding options (whether to do dictionary
/// encoding) with a bunch of sample row groups.
struct ColumnEncodingSampler<'a> {
    sample_row_groups: &'a [FetchedRecordBatch],
    meta_data: &'a MetaData,
    min_num_sample_rows: usize,
    max_unique_value_ratio: f64,
    column_encodings: &'a mut HashMap<String, ColumnEncoding>,
}

impl<'a> ColumnEncodingSampler<'a> {
    fn sample(&mut self) -> Result<()> {
        let num_total_rows: usize = self.sample_row_groups.iter().map(|v| v.num_rows()).sum();
        let ignore_sampling = num_total_rows < self.min_num_sample_rows;
        if ignore_sampling {
            self.decide_column_encodings_by_data_type();
            return Ok(());
        }

        assert!(self.max_unique_value_ratio <= 1.0 && self.max_unique_value_ratio >= 0.0);
        let max_unique_values = (num_total_rows as f64 * self.max_unique_value_ratio) as usize;
        let mut column_hashes = HashSet::with_capacity(max_unique_values);
        for (col_idx, col_schema) in self.meta_data.schema.columns().iter().enumerate() {
            if !Self::is_dictionary_type(col_schema.data_type) {
                self.column_encodings.insert(
                    col_schema.name.clone(),
                    ColumnEncoding { enable_dict: false },
                );
                continue;
            }

            if self.column_encodings.contains_key(&col_schema.name) {
                continue;
            }

            for row_group in self.sample_row_groups {
                let col_block = &row_group.columns()[col_idx];
                for idx in 0..row_group.num_rows() {
                    if column_hashes.len() >= max_unique_values {
                        break;
                    }
                    let datum_view = col_block.datum_view(idx);
                    datum_view.do_with_bytes(|val| {
                        let hash = hash_ext::hash64(val);
                        column_hashes.insert(hash);
                    })
                }
            }

            // The dictionary encoding make senses only if the number of unique values is
            // small.
            let enable_dict = column_hashes.len() < max_unique_values;
            column_hashes.clear();
            self.column_encodings
                .insert(col_schema.name.clone(), ColumnEncoding { enable_dict });
        }

        Ok(())
    }

    fn decide_column_encodings_by_data_type(&mut self) {
        for col_schema in self.meta_data.schema.columns().iter() {
            if !Self::is_dictionary_type(col_schema.data_type) {
                self.column_encodings.insert(
                    col_schema.name.clone(),
                    ColumnEncoding { enable_dict: false },
                );
            }
        }
    }

    #[inline]
    fn is_dictionary_type(data_type: DatumKind) -> bool {
        // Only do dictionary encoding for string or bytes column.
        matches!(data_type, DatumKind::String | DatumKind::Varbinary)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, task::Poll};

    use bytes_ext::Bytes;
    use common_types::{
        projected_schema::{ProjectedSchema, RowProjectorBuilder},
        tests::{build_row, build_row_for_dictionary, build_schema, build_schema_with_dictionary},
        time::{TimeRange, Timestamp},
    };
    use futures::stream;
    use object_store::LocalFileSystem;
    use runtime::{self, Runtime};
    use table_engine::predicate::Predicate;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        row_iter::tests::build_fetched_record_batch_with_key,
        sst::{
            factory::{
                Factory, FactoryImpl, ReadFrequency, ScanOptions, SstReadOptions, SstWriteOptions,
            },
            parquet::AsyncParquetReader,
            reader::{tests::check_stream, SstReader},
        },
        table_options::{self, StorageFormatHint},
    };

    // TODO(xikai): add test for reverse reader

    #[test]
    fn test_parquet_build_and_read() {
        test_util::init_log_for_test();

        let runtime = Arc::new(runtime::Builder::default().build().unwrap());
        parquet_write_and_then_read_back(runtime.clone(), 2, vec![2, 2, 2, 2, 2, 2, 2, 2, 2, 2]);
        parquet_write_and_then_read_back(runtime.clone(), 3, vec![3, 3, 3, 3, 3, 3, 2]);
        parquet_write_and_then_read_back(runtime.clone(), 4, vec![4, 4, 4, 4, 4]);
        parquet_write_and_then_read_back(runtime, 5, vec![5, 5, 5, 5]);
    }

    fn parquet_write_and_then_read_back(
        runtime: Arc<Runtime>,
        num_rows_per_row_group: usize,
        expected_num_rows: Vec<i64>,
    ) {
        runtime.block_on(async {
            let sst_factory = FactoryImpl;
            let sst_write_options = SstWriteOptions {
                storage_format_hint: StorageFormatHint::Auto,
                num_rows_per_row_group,
                compression: table_options::Compression::Uncompressed,
                max_buffer_size: 0,
                column_stats: Default::default(),
            };

            let dir = tempdir().unwrap();
            let root = dir.path();
            let store: ObjectStoreRef = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap());
            let store_picker: ObjectStorePickerRef = Arc::new(store);
            let sst_file_path = Path::from("data.par");

            let schema = build_schema_with_dictionary();
            let reader_projected_schema = ProjectedSchema::no_projection(schema.clone());
            let mut sst_meta = MetaData {
                min_key: Bytes::from_static(b"100"),
                max_key: Bytes::from_static(b"200"),
                time_range: TimeRange::new_unchecked(Timestamp::new(1), Timestamp::new(2)),
                max_sequence: 200,
                schema: schema.clone(),
            };

            let mut counter = 5;
            let record_batch_stream = Box::new(stream::poll_fn(move |_| -> Poll<Option<_>> {
                if counter == 0 {
                    return Poll::Ready(None);
                }
                counter -= 1;

                let ts = 100 + counter;
                let rows = vec![
                    build_row_for_dictionary(
                        b"a",
                        ts,
                        10.0,
                        "v4",
                        1000,
                        1_000_000,
                        Some("tagv1"),
                        "tagv2",
                    ),
                    build_row_for_dictionary(
                        b"b",
                        ts,
                        10.0,
                        "v4",
                        1000,
                        1_000_000,
                        Some("tagv2"),
                        "tagv4",
                    ),
                    build_row_for_dictionary(b"c", ts, 10.0, "v4", 1000, 1_000_000, None, "tagv2"),
                    build_row_for_dictionary(
                        b"d",
                        ts,
                        10.0,
                        "v4",
                        1000,
                        1_000_000,
                        Some("tagv3"),
                        "tagv2",
                    ),
                ];
                let batch = build_fetched_record_batch_with_key(schema.clone(), rows);
                Poll::Ready(Some(Ok(batch)))
            }));

            let mut writer = sst_factory
                .create_writer(
                    &sst_write_options,
                    &sst_file_path,
                    &store_picker,
                    Level::MAX,
                )
                .await
                .unwrap();
            let sst_info = writer
                .write(
                    RequestId::next_id(),
                    &sst_meta,
                    Box::new(record_batch_stream),
                )
                .await
                .unwrap();

            assert_eq!(20, sst_info.row_num);

            let scan_options = ScanOptions::default();
            // read sst back to test
            let row_projector_builder = RowProjectorBuilder::new(
                reader_projected_schema.to_record_schema(),
                reader_projected_schema.table_schema().clone(),
                None,
            );
            let sst_read_options = SstReadOptions {
                maybe_table_level_metrics: None,
                frequency: ReadFrequency::Frequent,
                num_rows_per_row_group: 5,
                predicate: Arc::new(Predicate::empty()),
                meta_cache: None,
                scan_options,
                runtime: runtime.clone(),
                row_projector_builder,
            };

            let mut reader: Box<dyn SstReader + Send> = {
                let mut reader = AsyncParquetReader::new(
                    &sst_file_path,
                    &sst_read_options,
                    None,
                    &store_picker,
                    None,
                );
                let mut sst_meta_readback = reader
                    .meta_data()
                    .await
                    .unwrap()
                    .as_parquet()
                    .unwrap()
                    .as_ref()
                    .clone();
                // sst filter is built insider sst writer, so overwrite to default for
                // comparison.
                sst_meta_readback.parquet_filter = Default::default();
                sst_meta_readback.column_values = None;
                // time_range is built insider sst writer, so overwrite it for
                // comparison.
                sst_meta.time_range = sst_info.time_range;
                assert_eq!(
                    sst_meta.time_range,
                    TimeRange::new_unchecked(100.into(), 105.into())
                );
                assert_eq!(&sst_meta_readback, &ParquetMetaData::from(&sst_meta));
                assert_eq!(
                    expected_num_rows,
                    reader
                        .row_groups()
                        .await
                        .iter()
                        .map(|g| g.num_rows())
                        .collect::<Vec<_>>()
                );

                Box::new(reader)
            };

            let mut stream = reader.read().await.unwrap();
            let mut expect_rows = vec![];
            for counter in &[4, 3, 2, 1, 0] {
                expect_rows.push(build_row_for_dictionary(
                    b"a",
                    100 + counter,
                    10.0,
                    "v4",
                    1000,
                    1_000_000,
                    Some("tagv1"),
                    "tagv2",
                ));
                expect_rows.push(build_row_for_dictionary(
                    b"b",
                    100 + counter,
                    10.0,
                    "v4",
                    1000,
                    1_000_000,
                    Some("tagv2"),
                    "tagv4",
                ));
                expect_rows.push(build_row_for_dictionary(
                    b"c",
                    100 + counter,
                    10.0,
                    "v4",
                    1000,
                    1_000_000,
                    None,
                    "tagv2",
                ));
                expect_rows.push(build_row_for_dictionary(
                    b"d",
                    100 + counter,
                    10.0,
                    "v4",
                    1000,
                    1_000_000,
                    Some("tagv3"),
                    "tagv2",
                ));
            }
            check_stream(&mut stream, expect_rows).await;
        });
    }

    #[tokio::test]
    async fn test_fetch_row_group() {
        // rows per group: 10
        let testcases = vec![
            // input, expected
            (10, vec![], vec![]),
            (10, vec![10, 10], vec![10, 10]),
            (10, vec![10, 10, 1], vec![10, 10, 1]),
            (10, vec![10, 10, 21], vec![10, 10, 10, 10, 1]),
            (10, vec![5, 6, 10], vec![10, 10, 1]),
            (10, vec![5, 4, 4, 30], vec![10, 10, 10, 10, 3]),
            (10, vec![20, 7, 23, 20], vec![10, 10, 10, 10, 10, 10, 10]),
            (10, vec![21], vec![10, 10, 1]),
            (10, vec![2, 2, 2, 2, 2], vec![10]),
            (4, vec![3, 3, 3, 3, 3], vec![4, 4, 4, 3]),
            (5, vec![3, 3, 3, 3, 3], vec![5, 5, 5]),
        ];

        for (num_rows_per_group, input, expected) in testcases {
            check_num_rows_of_row_group(num_rows_per_group, input, expected).await;
        }
    }

    async fn check_num_rows_of_row_group(
        num_rows_per_row_group: usize,
        input_num_rows: Vec<usize>,
        expected_num_rows: Vec<usize>,
    ) {
        test_util::init_log_for_test();
        let schema = build_schema();
        let mut poll_cnt = 0;
        let schema_clone = schema.clone();
        let record_batch_stream = Box::new(stream::poll_fn(move |_ctx| -> Poll<Option<_>> {
            if poll_cnt == input_num_rows.len() {
                return Poll::Ready(None);
            }

            let rows = (0..input_num_rows[poll_cnt])
                .map(|_| build_row(b"a", 100, 10.0, "v4", 1000, 1_000_000))
                .collect::<Vec<_>>();

            let batch = build_fetched_record_batch_with_key(schema_clone.clone(), rows);
            poll_cnt += 1;

            Poll::Ready(Some(Ok(batch)))
        }));

        let write_options = WriteOptions {
            num_rows_per_row_group,
            max_buffer_size: 0,
            compression: Compression::UNCOMPRESSED,
            sst_level: Level::default(),
            column_encodings: Default::default(),
        };
        let meta_data = MetaData {
            min_key: Default::default(),
            max_key: Default::default(),
            time_range: Default::default(),
            max_sequence: 1,
            schema,
        };
        let mut group_writer = RecordBatchGroupWriter::new(
            RequestId::next_id(),
            record_batch_stream,
            &meta_data,
            write_options,
        );

        let mut prev_record_batch = None;
        for expect_num_row in expected_num_rows {
            let batch = group_writer
                .fetch_next_row_group(&mut prev_record_batch)
                .await
                .unwrap();

            let actual_num_row: usize = batch.iter().map(|b| b.num_rows()).sum();
            assert_eq!(expect_num_row, actual_num_row);
        }
    }

    fn check_sample_column_encoding(
        mut sampler: ColumnEncodingSampler<'_>,
        expect_enable_dicts: Vec<Option<bool>>,
    ) {
        sampler.sample().unwrap();
        for (col_idx, col_schema) in sampler.meta_data.schema.columns().iter().enumerate() {
            let expect_enable_dict =
                expect_enable_dicts[col_idx].map(|v| ColumnEncoding { enable_dict: v });
            let column_encoding = sampler.column_encodings.get(&col_schema.name).cloned();
            assert_eq!(
                expect_enable_dict, column_encoding,
                "column:{}",
                col_schema.name
            );
        }
    }

    #[test]
    fn test_column_encoding_option_sample() {
        let schema = build_schema();
        let raw_rows = vec![
            (b"a", 100, 10.0, "v4", 1000, 1_000_000),
            (b"a", 100, 10.0, "v4", 1000, 1_000_000),
            (b"a", 100, 10.0, "v5", 1000, 1_000_000),
            (b"a", 100, 10.0, "v5", 1000, 1_000_000),
            (b"a", 100, 10.0, "v6", 1000, 1_000_000),
            (b"a", 100, 10.0, "v6", 1000, 1_000_000),
            (b"a", 100, 10.0, "v8", 1000, 1_000_000),
            (b"a", 100, 10.0, "v8", 1000, 1_000_000),
            (b"a", 100, 10.0, "v9", 1000, 1_000_000),
            (b"a", 100, 10.0, "v9", 1000, 1_000_000),
        ];
        let rows: Vec<_> = raw_rows
            .into_iter()
            .map(|v| build_row(v.0, v.1, v.2, v.3, v.4, v.5))
            .collect();
        let record_batch_with_key0 =
            build_fetched_record_batch_with_key(schema.clone(), rows.clone());
        let record_batch_with_key1 = build_fetched_record_batch_with_key(schema.clone(), rows);
        let meta_data = MetaData {
            min_key: Bytes::from_static(b""),
            max_key: Bytes::from_static(b""),
            time_range: TimeRange::new_unchecked(Timestamp::new(1), Timestamp::new(2)),
            max_sequence: 200,
            schema,
        };
        let record_batches_with_key = vec![record_batch_with_key0, record_batch_with_key1];

        let mut column_encodings = HashMap::new();
        let sampler = ColumnEncodingSampler {
            sample_row_groups: &record_batches_with_key,
            meta_data: &meta_data,
            min_num_sample_rows: 10,
            max_unique_value_ratio: 0.6,
            column_encodings: &mut column_encodings,
        };
        let expect_enable_dicts = vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
        ];
        check_sample_column_encoding(sampler, expect_enable_dicts);

        column_encodings.clear();
        let sampler = ColumnEncodingSampler {
            sample_row_groups: &record_batches_with_key,
            meta_data: &meta_data,
            min_num_sample_rows: 10,
            max_unique_value_ratio: 0.2,
            column_encodings: &mut column_encodings,
        };
        let expect_enable_dicts = vec![
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
        ];
        check_sample_column_encoding(sampler, expect_enable_dicts);

        column_encodings.clear();
        let sampler = ColumnEncodingSampler {
            sample_row_groups: &record_batches_with_key,
            meta_data: &meta_data,
            min_num_sample_rows: 30,
            max_unique_value_ratio: 0.2,
            column_encodings: &mut column_encodings,
        };
        let expect_enable_dicts = vec![
            None,
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(false),
        ];
        check_sample_column_encoding(sampler, expect_enable_dicts);

        column_encodings.clear();
        // `field1` is double type, it will still be changed to false even if it is set
        // as true.
        // `field2` is string type, it will be kept as the pre-set.
        column_encodings.insert("field1".to_string(), ColumnEncoding { enable_dict: true });
        column_encodings.insert("field2".to_string(), ColumnEncoding { enable_dict: true });
        let sampler = ColumnEncodingSampler {
            sample_row_groups: &record_batches_with_key,
            meta_data: &meta_data,
            min_num_sample_rows: 10,
            max_unique_value_ratio: 0.2,
            column_encodings: &mut column_encodings,
        };
        let expect_enable_dicts = vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
        ];
        check_sample_column_encoding(sampler, expect_enable_dicts);
    }
}
