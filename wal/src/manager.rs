// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! WalManager abstraction

use std::{collections::VecDeque, fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
pub use common_types::SequenceNumber;
use common_types::{table::Location, MAX_SEQUENCE_NUMBER, MIN_SEQUENCE_NUMBER};
use common_util::runtime::Runtime;
pub use error::*;
use snafu::ResultExt;

use crate::{
    kv_encoder::LogBatchEncoder,
    log_batch::{LogEntry, LogWriteBatch, PayloadDecoder},
    manager,
};

pub mod error {
    use common_types::table::Location;
    use common_util::define_result;
    use snafu::{Backtrace, Snafu};

    // Now most error from manage implementation don't have backtrace, so we add
    // backtrace here.
    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        #[snafu(display(
            "Failed to open wal, path:{}, err:{}.\nBacktrace:\n{}",
            wal_path,
            source,
            backtrace
        ))]
        Open {
            wal_path: String,
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to initialize wal, err:{}.\nBacktrace:\n{}", source, backtrace))]
        Initialization {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Region is not found, table_location:{:?}.\nBacktrace:\n{}",
            location,
            backtrace
        ))]
        RegionNotFound {
            location: Location,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to create wal encoder, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        CreateWalEncoder {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to write log entries, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        Write {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to read log entries, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        Read {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display(
            "Failed to delete log entries, err:{}.\nBacktrace:\n{}",
            source,
            backtrace
        ))]
        Delete {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to encode, err:{}.\nBacktrace:\n{}", source, backtrace))]
        Encoding {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to decode, err:{}.\nBacktrace:\n{}", source, backtrace))]
        Decoding {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to close wal, err:{}.\nBacktrace:\n{}", source, backtrace))]
        Close {
            source: Box<dyn std::error::Error + Send + Sync>,
            backtrace: Backtrace,
        },

        #[snafu(display("Failed to execute in runtime, err:{}", source))]
        RuntimeExec { source: common_util::runtime::Error },
    }

    define_result!(Error);
}

pub type RegionId = u64;
pub const MAX_REGION_ID: RegionId = u64::MAX;

#[derive(Debug, Clone)]
pub struct WriteContext {
    /// Timeout to write wal and it only takes effect when writing to a Wal on a
    /// remote machine (writing to the local disk does not have timeout).
    pub timeout: Duration,
}

impl Default for WriteContext {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadContext {
    /// Timeout to read log entries and it only takes effect when reading from a
    /// Wal on a remote machine (reading from the local disk does not have
    /// timeout).
    pub timeout: Duration,
    /// Batch size to read log entries.
    pub batch_size: usize,
}

impl Default for ReadContext {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            batch_size: 500,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ReadBoundary {
    Max,
    Min,
    Included(SequenceNumber),
    Excluded(SequenceNumber),
}

impl ReadBoundary {
    /// Convert the boundary to start sequence number.
    ///
    /// Returns `None` if the boundary is `Excluded(MAX_SEQUENCE_NUM)`
    pub fn as_start_sequence_number(&self) -> Option<SequenceNumber> {
        match *self {
            ReadBoundary::Max => Some(MAX_SEQUENCE_NUMBER),
            ReadBoundary::Min => Some(MIN_SEQUENCE_NUMBER),
            ReadBoundary::Included(n) => Some(n),
            ReadBoundary::Excluded(n) => {
                if n == MAX_SEQUENCE_NUMBER {
                    None
                } else {
                    Some(n + 1)
                }
            }
        }
    }

    /// Convert the boundary to start sequence number.
    ///
    /// Returns `None` if the boundary is `Excluded(MIN_SEQUENCE_NUM)`
    pub fn as_end_sequence_number(&self) -> Option<SequenceNumber> {
        match *self {
            ReadBoundary::Max => Some(MAX_SEQUENCE_NUMBER),
            ReadBoundary::Min => Some(MIN_SEQUENCE_NUMBER),
            ReadBoundary::Included(n) => Some(n),
            ReadBoundary::Excluded(n) => {
                if n == MIN_SEQUENCE_NUMBER {
                    None
                } else {
                    Some(n - 1)
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadRequest {
    /// Location of the wal to read
    pub location: Location,
    // TODO(yingwen): Or just rename to ReadBound?
    /// Start bound
    pub start: ReadBoundary,
    /// End bound
    pub end: ReadBoundary,
}

#[derive(Debug, Clone)]
pub struct ScanRequest {
    /// Region id of the wals to be scanned
    pub region_id: RegionId,
}

pub type ScanContext = ReadContext;

/// Blocking Iterator abstraction for log entry.
pub trait BlockingLogIterator: Send + fmt::Debug {
    /// Fetch next log entry from the iterator.
    ///
    /// NOTE that this operation may **BLOCK** caller thread now.
    fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>>;
}

/// Vectorwise log entry iterator.
#[async_trait]
pub trait AsyncLogIterator: Send + fmt::Debug {
    /// Fetch next batch of log entries from the iterator to the provided
    /// `buffer`. This iterator should clear the `buffer` before using it.
    ///
    /// Returns the entries if there are remaining log entries, or empty `Vec`
    /// if the iterator is exhausted.
    async fn next_log_entry(&mut self) -> Result<Option<LogEntry<&'_ [u8]>>>;
}

/// Management of multi-region Wals.
///
/// Every region has its own increasing (and maybe hallow) sequence number
/// space.
#[async_trait]
pub trait WalManager: Send + Sync + fmt::Debug + 'static {
    /// Get current sequence number.
    async fn sequence_num(&self, location: Location) -> Result<SequenceNumber>;

    /// Mark the entries whose sequence number is in [0, `sequence_number`] to
    /// be deleted in the future.
    async fn mark_delete_entries_up_to(
        &self,
        location: Location,
        sequence_num: SequenceNumber,
    ) -> Result<()>;

    /// Close the wal gracefully.
    async fn close_gracefully(&self) -> Result<()>;

    /// Provide iterator on necessary entries according to `ReadRequest`.
    async fn read_batch(
        &self,
        ctx: &ReadContext,
        req: &ReadRequest,
    ) -> Result<BatchLogIteratorAdapter>;

    /// Provide the encoder for encoding payloads.
    fn encoder(&self, location: Location) -> Result<LogBatchEncoder> {
        Ok(LogBatchEncoder::create(location))
    }

    /// Write a batch of log entries to log.
    ///
    /// Returns the max sequence number for the batch of log entries.
    async fn write(&self, ctx: &WriteContext, batch: &LogWriteBatch) -> Result<SequenceNumber>;

    /// Scan all logs from a `Region`.
    async fn scan(&self, ctx: &ScanContext, req: &ScanRequest) -> Result<BatchLogIteratorAdapter>;
}

/// Adapter to convert a blocking interator to a batch async iterator.
#[derive(Debug)]
pub enum InnerIterator {
    Blocking(Box<dyn BlockingLogIterator>, Arc<Runtime>),
    Async(Box<dyn AsyncLogIterator>),
}

#[derive(Debug)]
pub struct BatchLogIteratorAdapter {
    inner_iterator: Option<InnerIterator>,
    batch_size: usize,
}

impl BatchLogIteratorAdapter {
    pub fn new(inner_iterator: InnerIterator, batch_size: usize) -> Self {
        Self {
            inner_iterator: Some(inner_iterator),
            batch_size,
        }
    }
}

impl BatchLogIteratorAdapter {
    async fn simulated_async_next<D: PayloadDecoder + Send + 'static>(
        &mut self,
        decoder: D,
        runtime: Arc<Runtime>,
        blocking_iter: Box<dyn BlockingLogIterator>,
        mut buffer: VecDeque<LogEntry<D::Target>>,
    ) -> Result<(VecDeque<LogEntry<D::Target>>, Option<InnerIterator>)> {
        buffer.clear();

        let mut iter = blocking_iter;
        let batch_size = self.batch_size;
        let (log_entries, iter_opt) = runtime
            .spawn_blocking(move || {
                for _ in 0..batch_size {
                    if let Some(raw_log_entry) = iter.next_log_entry()? {
                        let mut raw_payload = raw_log_entry.payload;
                        let payload = decoder
                            .decode(&mut raw_payload)
                            .map_err(|e| Box::new(e) as _)
                            .context(manager::Decoding)?;
                        let log_entry = LogEntry {
                            sequence: raw_log_entry.sequence,
                            payload,
                        };
                        buffer.push_back(log_entry);
                    } else {
                        return Ok((buffer, None));
                    }
                }

                Ok((buffer, Some(iter)))
            })
            .await
            .context(RuntimeExec)??;

        match iter_opt {
            Some(iter) => Ok((log_entries, Some(InnerIterator::Blocking(iter, runtime)))),
            None => Ok((log_entries, None)),
        }
    }

    async fn async_next<D: PayloadDecoder + Send + 'static>(
        &mut self,
        decoder: D,
        async_iter: Box<dyn AsyncLogIterator>,
        mut buffer: VecDeque<LogEntry<D::Target>>,
    ) -> Result<(VecDeque<LogEntry<D::Target>>, Option<InnerIterator>)> {
        buffer.clear();

        let mut async_iter = async_iter;
        for _ in 0..self.batch_size {
            if let Some(raw_log_entry) = async_iter.next_log_entry().await? {
                let mut raw_payload = raw_log_entry.payload;
                let payload = decoder
                    .decode(&mut raw_payload)
                    .map_err(|e| Box::new(e) as _)
                    .context(manager::Decoding)?;
                let log_entry = LogEntry {
                    sequence: raw_log_entry.sequence,
                    payload,
                };
                buffer.push_back(log_entry);
            } else {
                return Ok((buffer, None));
            }
        }

        Ok((buffer, Some(InnerIterator::Async(async_iter))))
    }

    pub async fn next_log_entries<D: PayloadDecoder + Send + 'static>(
        &mut self,
        decoder: D,
        buffer: VecDeque<LogEntry<D::Target>>,
    ) -> Result<VecDeque<LogEntry<D::Target>>> {
        if self.inner_iterator.is_none() {
            return Ok(VecDeque::new());
        }

        let inner_iterator = self.inner_iterator.take().unwrap();
        let (log_entries, inner_iterator) = match inner_iterator {
            InnerIterator::Blocking(blocking_iter, runtime) => {
                self.simulated_async_next(decoder, runtime, blocking_iter, buffer)
                    .await?
            }
            InnerIterator::Async(async_iter) => {
                self.async_next(decoder, async_iter, buffer).await?
            }
        };

        self.inner_iterator = inner_iterator;
        Ok(log_entries)
    }
}

pub type WalManagerRef = Arc<dyn WalManager>;
