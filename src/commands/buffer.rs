#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::{mem, str, time::Duration};

use bitflags::bitflags;
use bytes::{Buf, BufMut, BytesMut};

use crate::{
    commands::field_type::FieldType,
    msgpack::Write,
    operations::{Operation, OperationBin, OperationData, OperationType},
    policy::{
        BasePolicy, BatchPolicy, CommitLevel, ConsistencyLevel, Expiration, GenerationPolicy,
        RecordExistsAction, ScanPolicy, WritePolicy,
    },
    BatchRead, Bin, Bins, Key, ResultCode, UserKey,
};

bitflags! {
    /// First set of info bits, describing read attributes.
    #[derive(Clone, Copy)]
    struct ReadAttr: u8 {
        /// Contains a read operation.
        const READ = 1;
        /// Get all bins.
        const GET_ALL = 1 << 1;
        /// Bypass monitoring, inline if data-in-memory.
        const SHORT_QUERY = 1 << 2;
        /// Batch protocol.
        const BATCH = 1 << 3;
        /// Operation is via XDR.
        const XDR = 1 << 4;
        /// Get record metadata only, no bin metadata or data.
        const GET_NO_BINS = 1 << 5;
        /// Involve all replicas in read operation.
        const CONSISTENCY_LEVEL_ALL = 1 << 6;
        /// **Enterprise only:** Compress the response data.
        const COMPRESS_RESPONSE = 1 << 7;
    }
}

bitflags! {
    /// Second set of info bits, describing write attributes.
    #[derive(Clone, Copy)]
    struct WriteAttr: u8 {
        /// Contains a write semantic.
        const WRITE = 1;
        /// Delete record.
        const DELETE = 1 << 1;
        /// Pay attention to the generation.
        const GENERATION = 1 << 2;
        /// Apply write if `new generation > old`, good for restore.
        const GENERATION_GT = 1 << 3;
        /// **Enterprise only:** Operation resulting in record deletion leaves tombstone.
        const DURABLE_DELETE = 1 << 4;
        /// Write record only if it doesn't exist.
        const CREATE_ONLY = 1 << 5;
        // Bit 6 is unused
        /// All bin operations (read, write, or modify) require a response, in request order.
        const RESPOND_ALL_OPS = 1 << 7;
    }
}

bitflags! {
    /// Third and last set of info bits, describing other attributes.
    #[derive(Clone, Copy)]
    pub(crate) struct InfoAttr: u8 {
        /// This is the last of a multi-part message.
        const LAST = 1;
        /// "Fire and forget" replica writes.
        const COMMIT_LEVEL_MASTER = 1 << 1;
        /// In query response, partition is done.
        const PARTITION_DONE = 1 << 2;
        /// Update existing record only, do not create new record.
        const UPDATE_ONLY = 1 << 3;
        /// Completely replace existing record, or create new record.
        const CREATE_OR_REPLACE = 1 << 4;
        /// Completely replace existing record, do **not** create new record.
        const REPLACE_ONLY = 1 << 5;
        /// **Enterprise only**
        const SC_READ_TYPE = 1 << 6;
        /// **Enterprise only**
        const SC_READ_RELAX = 1 << 7;
    }
}

pub const TOTAL_HEADER_SIZE: usize = ProtoHeader::SIZE + MessageHeader::SIZE;

const FIELD_HEADER_SIZE: usize = mem::size_of::<u32>() + mem::size_of::<u8>();
const OPERATION_HEADER_SIZE: usize = mem::size_of::<i32>() + mem::size_of::<[u8; 4]>();
const DIGEST_SIZE: usize = 20;

// MAX_BUFFER_SIZE protects against allocating massive memory blocks
// for buffers. Tweak this number if you are returning a lot of
// LDT elements in your queries.
const MAX_BUFFER_SIZE: usize = 1024 * 1024 + 8; // 1 MB + header

pub type Result<T, E = BufferError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("invalid size for buffer: {size} (max {max})")]
    SizeExceeded { size: usize, max: usize },
    #[error("invalid UTF-8 content encountered")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
}

// Holds data buffer for the command
#[derive(Debug, Default)]
pub struct Buffer {
    buffer: BytesMut,
    reclaim_threshold: usize,
}

impl Buffer {
    #[must_use]
    pub fn new(reclaim_threshold: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
            reclaim_threshold,
        }
    }

    pub fn advance(&mut self, cnt: usize) {
        self.buffer.advance(cnt);
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn peek(&self) -> Option<u8> {
        self.buffer.first().copied()
    }

    pub fn clear(&mut self, size: usize) -> Result<()> {
        // Corrupted data streams can result in a huge length.
        // Do a sanity check here.
        if size > MAX_BUFFER_SIZE {
            return Err(BufferError::SizeExceeded {
                size,
                max: MAX_BUFFER_SIZE,
            });
        }

        self.buffer.clear();
        self.buffer.reserve(size);

        Ok(())
    }

    pub fn resize(&mut self, size: usize) -> Result<()> {
        // Corrupted data streams can result in a huge length.
        // Do a sanity check here.
        if size > MAX_BUFFER_SIZE {
            return Err(BufferError::SizeExceeded {
                size,
                max: MAX_BUFFER_SIZE,
            });
        }

        let capacity = self.buffer.capacity();
        self.buffer.resize(size, 0);

        if self.reclaim_threshold < capacity && capacity > size {
            self.buffer = BytesMut::from(&*self.buffer);
        }

        Ok(())
    }

    // Writes the command for write operations
    pub(crate) fn set_write(
        &mut self,
        policy: &WritePolicy,
        op_type: OperationType,
        key: &Key,
        bins: &[Bin<'_>],
    ) -> Result<()> {
        let (key_size, field_count) = estimate_key_size(key, policy.as_ref().send_key);
        let op_size = bins
            .iter()
            .map(estimate_operation_size_for_bin)
            .sum::<usize>();

        self.clear(TOTAL_HEADER_SIZE + key_size + op_size)?;

        MessageHeader::for_write(
            key_size + op_size,
            policy,
            ReadAttr::empty(),
            WriteAttr::WRITE,
            field_count,
            bins.len() as u16,
        )
        .write_to(&mut self.buffer);

        self.write_key(key, policy.as_ref().send_key);

        for bin in bins {
            self.write_operation_for_bin(bin, op_type);
        }

        Ok(())
    }

    // Writes the command for write operations
    pub fn set_delete(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        let (key_size, field_count) = estimate_key_size(key, false);

        self.clear(TOTAL_HEADER_SIZE + key_size)?;

        MessageHeader::for_write(
            key_size,
            policy,
            ReadAttr::empty(),
            WriteAttr::WRITE | WriteAttr::DELETE,
            field_count,
            0,
        )
        .write_to(&mut self.buffer);

        self.write_key(key, false);

        Ok(())
    }

    // Writes the command for touch operations
    pub fn set_touch(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        let (key_size, field_count) = estimate_key_size(key, policy.as_ref().send_key);

        self.clear(TOTAL_HEADER_SIZE + key_size + OPERATION_HEADER_SIZE)?;

        MessageHeader::for_write(
            key_size + OPERATION_HEADER_SIZE,
            policy,
            ReadAttr::empty(),
            WriteAttr::WRITE,
            field_count,
            1,
        )
        .write_to(&mut self.buffer);

        self.write_key(key, policy.as_ref().send_key);

        self.write_operation_for_operation_type(OperationType::Touch);

        Ok(())
    }

    // Writes the command for exist operations
    pub fn set_exists(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        let (key_size, field_count) = estimate_key_size(key, false);

        self.clear(TOTAL_HEADER_SIZE + key_size)?;

        MessageHeader::for_read(
            key_size,
            policy.as_ref(),
            ReadAttr::READ | ReadAttr::GET_NO_BINS,
            WriteAttr::empty(),
            field_count,
            0,
        )
        .write_to(&mut self.buffer);

        self.write_key(key, false);

        Ok(())
    }

    // Writes the command for get operations
    pub fn set_read(&mut self, policy: &BasePolicy, key: &Key, bins: &Bins) -> Result<()> {
        match bins {
            Bins::None => self.set_read_header(policy, key),
            Bins::All => self.set_read_for_key_only(policy, key),
            Bins::Some(bin_names) => {
                let (key_size, field_count) = estimate_key_size(key, policy.send_key);
                let op_size = bin_names
                    .iter()
                    .map(|name| estimate_operation_size_for_bin_name(name))
                    .sum::<usize>();

                self.clear(TOTAL_HEADER_SIZE + key_size + op_size)?;

                MessageHeader::for_read(
                    key_size + op_size,
                    policy,
                    ReadAttr::READ,
                    WriteAttr::empty(),
                    field_count,
                    bin_names.len() as u16,
                )
                .write_to(&mut self.buffer);

                self.write_key(key, policy.send_key);

                for bin_name in bin_names {
                    self.write_operation_for_bin_name(bin_name, OperationType::Read);
                }

                Ok(())
            }
        }
    }

    // Writes the command for getting metadata operations
    pub fn set_read_header(&mut self, policy: &BasePolicy, key: &Key) -> Result<()> {
        let (key_size, field_count) = estimate_key_size(key, policy.send_key);
        let op_size = estimate_operation_size_for_bin_name("");

        self.clear(TOTAL_HEADER_SIZE + key_size + op_size)?;

        MessageHeader::for_read(
            key_size + op_size,
            policy,
            ReadAttr::READ | ReadAttr::GET_NO_BINS,
            WriteAttr::empty(),
            field_count,
            1,
        )
        .write_to(&mut self.buffer);

        self.write_key(key, policy.send_key);

        self.write_operation_for_bin_name("", OperationType::Read);

        Ok(())
    }

    pub fn set_read_for_key_only(&mut self, policy: &BasePolicy, key: &Key) -> Result<()> {
        let (key_size, field_count) = estimate_key_size(key, policy.send_key);

        self.clear(TOTAL_HEADER_SIZE + key_size)?;

        MessageHeader::for_read(
            key_size,
            policy,
            ReadAttr::READ | ReadAttr::GET_ALL,
            WriteAttr::empty(),
            field_count,
            0,
        )
        .write_to(&mut self.buffer);

        self.write_key(key, policy.send_key);

        Ok(())
    }

    // Writes the command for batch read operations
    pub fn set_batch_read(
        &mut self,
        policy: &BatchPolicy,
        batch_reads: &[BatchRead],
    ) -> Result<()> {
        let field_count_row = if policy.send_set_name { 2 } else { 1 };

        let field_count = 1;
        let mut field_size = FIELD_HEADER_SIZE + 5;

        let mut prev: Option<&BatchRead> = None;
        for batch_read in batch_reads {
            field_size += batch_read.key.digest.len() + 4;
            match prev {
                Some(prev) if batch_read.match_header(prev, policy.send_set_name) => {
                    field_size += 1;
                }
                _ => {
                    let key = &batch_read.key;
                    field_size += FIELD_HEADER_SIZE + 6 + key.namespace.len();
                    if policy.send_set_name {
                        field_size += FIELD_HEADER_SIZE + key.set_name.len();
                    }
                    if let Bins::Some(bin_names) = &batch_read.bins {
                        field_size += bin_names
                            .iter()
                            .map(|name| estimate_operation_size_for_bin_name(name))
                            .sum::<usize>();
                    }
                }
            }
            prev = Some(batch_read);
        }

        self.clear(TOTAL_HEADER_SIZE + field_size)?;

        MessageHeader::for_read(
            field_size,
            policy.as_ref(),
            ReadAttr::READ | ReadAttr::BATCH,
            WriteAttr::empty(),
            field_count,
            0,
        )
        .write_to(&mut self.buffer);

        self.write_field_header(
            field_size - 4,
            if policy.send_set_name {
                FieldType::BatchIndexWithSet
            } else {
                FieldType::BatchIndex
            },
        );
        self.write_u32(batch_reads.len() as u32);
        self.write_u8(u8::from(policy.allow_inline));

        prev = None;
        for (idx, batch_read) in batch_reads.iter().enumerate() {
            let key = &batch_read.key;
            self.write_u32(idx as u32);
            self.write_bytes(&key.digest);
            match prev {
                Some(prev) if batch_read.match_header(prev, policy.send_set_name) => {
                    self.write_u8(1);
                }
                _ => {
                    self.write_u8(0);
                    match &batch_read.bins {
                        Bins::None => {
                            self.write_u8((ReadAttr::READ | ReadAttr::GET_NO_BINS).bits());
                            self.write_u16(field_count_row);
                            self.write_u16(0);
                            self.write_field_string(&key.namespace, FieldType::Namespace);
                            if policy.send_set_name {
                                self.write_field_string(&key.set_name, FieldType::Table);
                            }
                        }
                        Bins::All => {
                            self.write_u8((ReadAttr::READ | ReadAttr::GET_ALL).bits());
                            self.write_u16(field_count_row);
                            self.write_u16(0);
                            self.write_field_string(&key.namespace, FieldType::Namespace);
                            if policy.send_set_name {
                                self.write_field_string(&key.set_name, FieldType::Table);
                            }
                        }
                        Bins::Some(bin_names) => {
                            self.write_u8(ReadAttr::READ.bits());
                            self.write_u16(field_count_row);
                            self.write_u16(bin_names.len() as u16);
                            self.write_field_string(&key.namespace, FieldType::Namespace);
                            if policy.send_set_name {
                                self.write_field_string(&key.set_name, FieldType::Table);
                            }
                            for bin in bin_names {
                                self.write_operation_for_bin_name(bin, OperationType::Read);
                            }
                        }
                    }
                }
            }
            prev = Some(batch_read);
        }

        Ok(())
    }

    // Writes the command for getting metadata operations
    pub fn set_operate<'a>(
        &mut self,
        policy: &WritePolicy,
        key: &Key,
        operations: &'a [Operation<'a>],
    ) -> Result<()> {
        let mut read_attr = ReadAttr::empty();
        let mut write_attr = WriteAttr::empty();

        let op_size = operations
            .iter()
            .map(|operation| {
                match *operation {
                    Operation {
                        op: OperationType::Read,
                        bin: OperationBin::None,
                        ..
                    } => read_attr |= ReadAttr::READ | ReadAttr::GET_NO_BINS,
                    Operation {
                        op: OperationType::Read,
                        bin: OperationBin::All,
                        ..
                    } => read_attr |= ReadAttr::READ | ReadAttr::GET_ALL,
                    Operation {
                        op:
                            OperationType::Read
                            | OperationType::CdtRead
                            | OperationType::BitRead
                            | OperationType::HllRead,
                        ..
                    } => read_attr |= ReadAttr::READ,
                    _ => write_attr |= WriteAttr::WRITE,
                }

                let each_op = matches!(
                    operation.data,
                    OperationData::CdtMapOp(_)
                        | OperationData::CdtBitOp(_)
                        | OperationData::HllOp(_)
                );

                if policy.respond_per_each_op || each_op {
                    write_attr |= WriteAttr::RESPOND_ALL_OPS;
                }

                OPERATION_HEADER_SIZE + operation.estimate_size()
            })
            .sum::<usize>();

        let (key_size, field_count) =
            estimate_key_size(key, policy.as_ref().send_key && !write_attr.is_empty());

        self.clear(TOTAL_HEADER_SIZE + key_size + op_size)?;

        if write_attr.is_empty() {
            MessageHeader::for_read(
                key_size + op_size,
                policy.as_ref(),
                read_attr,
                write_attr,
                field_count,
                operations.len() as u16,
            )
        } else {
            MessageHeader::for_write(
                key_size + op_size,
                policy,
                read_attr,
                write_attr,
                field_count,
                operations.len() as u16,
            )
        }
        .write_to(&mut self.buffer);

        self.write_key(key, policy.as_ref().send_key && !write_attr.is_empty());

        for operation in operations {
            operation.write_to(self);
        }

        Ok(())
    }

    pub fn set_scan(
        &mut self,
        policy: &ScanPolicy,
        namespace: &str,
        set_name: &str,
        bins: &Bins,
        task_id: u64,
        partitions: &[u16],
    ) -> Result<()> {
        let mut field_size = 0;
        let mut field_count = 0;

        if !namespace.is_empty() {
            field_size += FIELD_HEADER_SIZE + namespace.len();
            field_count += 1;
        }

        if !set_name.is_empty() {
            field_size += FIELD_HEADER_SIZE + set_name.len();
            field_count += 1;
        }

        // Estimate pid, scan timeout and task_id size
        field_size += FIELD_HEADER_SIZE
            + partitions.len() * 2
            + FIELD_HEADER_SIZE
            + 4
            + FIELD_HEADER_SIZE
            + 8;
        field_count += 3;

        let (bin_size, bin_count) = match bins {
            Bins::All | Bins::None => (0, 0),
            Bins::Some(bin_names) => (
                bin_names
                    .iter()
                    .map(|name| estimate_operation_size_for_bin_name(name))
                    .sum::<usize>(),
                bin_names.len(),
            ),
        };

        self.clear(TOTAL_HEADER_SIZE + field_size + bin_size)?;

        let mut read_attr = ReadAttr::READ;
        if *bins == Bins::None {
            read_attr |= ReadAttr::GET_NO_BINS;
        }

        MessageHeader::for_read(
            field_size + bin_size,
            policy.as_ref(),
            read_attr,
            WriteAttr::empty(),
            field_count,
            bin_count as u16,
        )
        .write_to(&mut self.buffer);

        if !namespace.is_empty() {
            self.write_field_string(namespace, FieldType::Namespace);
        }

        if !set_name.is_empty() {
            self.write_field_string(set_name, FieldType::Table);
        }

        self.write_field_header(partitions.len() * 2, FieldType::PidArray);
        for &pid in partitions {
            self.write_u16_le(pid);
        }

        // Write scan timeout
        self.write_field_header(4, FieldType::ScanTimeout);
        self.write_u32(
            policy.socket_timeout.as_secs() as u32 * 1000 + policy.socket_timeout.subsec_millis(),
        );

        self.write_field_header(8, FieldType::TranId);
        self.write_u64(task_id);

        if let Bins::Some(bin_names) = bins {
            for bin_name in bin_names {
                self.write_operation_for_bin_name(bin_name, OperationType::Read);
            }
        }

        Ok(())
    }

    pub fn set_info(&mut self, commands: &[&str]) -> Result<()> {
        let size = commands.iter().map(|cmd| cmd.len()).sum::<usize>() + commands.len();

        self.clear(ProtoHeader::SIZE + size)?;

        ProtoHeader {
            version: Version::V2,
            ty: ProtoType::Info,
            size,
        }
        .write_to(&mut self.buffer);

        for command in commands {
            self.write_str(command);
            self.write_u8(b'\n');
        }

        Ok(())
    }

    // Header write for write operations.

    fn write_key(&mut self, key: &Key, send_key: bool) {
        // Write key into buffer.
        if !key.namespace.is_empty() {
            self.write_field_string(&key.namespace, FieldType::Namespace);
        }

        if !key.set_name.is_empty() {
            self.write_field_string(&key.set_name, FieldType::Table);
        }

        self.write_field_bytes(&key.digest, FieldType::DigestRipe);

        if send_key {
            if let Some(user_key) = &key.user_key {
                self.write_user_key(user_key, FieldType::Key);
            }
        }
    }

    fn write_field_header(&mut self, size: usize, ftype: FieldType) {
        self.write_i32(size as i32 + 1);
        self.write_u8(ftype as u8);
    }

    fn write_field_string(&mut self, field: &str, ftype: FieldType) {
        self.write_field_header(field.len(), ftype);
        self.write_str(field);
    }

    fn write_field_bytes(&mut self, bytes: &[u8], ftype: FieldType) {
        self.write_field_header(bytes.len(), ftype);
        self.write_bytes(bytes);
    }

    fn write_user_key(&mut self, value: &UserKey, ftype: FieldType) {
        self.write_field_header(value.estimate_size() + 1, ftype);
        self.write_u8(value.particle_type() as u8);
        value.write_to(self);
    }

    fn write_operation_for_bin(&mut self, bin: &Bin<'_>, op_type: OperationType) {
        let name_length = bin.name.len();
        let value_length = bin.value.estimate_size();

        self.write_i32((name_length + value_length + 4) as i32);
        self.write_u8(op_type as u8);
        self.write_u8(bin.value.particle_type() as u8);
        self.write_u8(0);
        self.write_u8(name_length as u8);
        self.write_str(bin.name);
        bin.value.write_to(self);
    }

    fn write_operation_for_bin_name(&mut self, name: &str, op_type: OperationType) {
        self.write_i32(name.len() as i32 + 4);
        self.write_u8(op_type as u8);
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(name.len() as u8);
        self.write_str(name);
    }

    fn write_operation_for_operation_type(&mut self, op_type: OperationType) {
        self.write_i32(4);
        self.write_u8(op_type as u8);
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);
    }

    // Data buffer implementations

    #[must_use]
    pub fn data_offset(&self) -> usize {
        self.buffer.len()
    }

    pub fn read_bool(&mut self) -> bool {
        self.buffer.get_u8() != 0
    }

    pub fn read_u8(&mut self) -> u8 {
        self.buffer.get_u8()
    }

    pub fn read_u16(&mut self) -> u16 {
        self.buffer.get_u16()
    }

    pub fn read_u32(&mut self) -> u32 {
        self.buffer.get_u32()
    }

    pub fn read_u64(&mut self) -> u64 {
        self.buffer.get_u64()
    }

    pub fn read_i8(&mut self) -> i8 {
        self.buffer.get_i8()
    }

    pub fn read_i16(&mut self) -> i16 {
        self.buffer.get_i16()
    }

    pub fn read_i32(&mut self) -> i32 {
        self.buffer.get_i32()
    }

    pub fn read_i64(&mut self) -> i64 {
        self.buffer.get_i64()
    }

    pub fn read_f32(&mut self) -> f32 {
        self.buffer.get_f32()
    }

    pub fn read_f64(&mut self) -> f64 {
        self.buffer.get_f64()
    }

    pub fn read_msg_size(&mut self) -> usize {
        ProtoHeader::read_from(&mut self.buffer).size
    }

    pub fn read_str(&mut self, len: usize) -> Result<String> {
        let mut buf = vec![0; len];
        self.buffer.copy_to_slice(&mut buf);
        String::from_utf8(buf).map_err(Into::into)
    }

    pub fn read_bytes(&mut self, pos: usize, count: usize) -> &[u8] {
        &self.buffer[pos..pos + count]
    }

    pub fn read_slice(&mut self, count: usize) -> &[u8] {
        &self.buffer[..count]
    }

    pub fn read_blob(&mut self, len: usize) -> Vec<u8> {
        let mut buf = vec![0; len];
        self.buffer.copy_to_slice(&mut buf);
        buf
    }

    pub fn write_u16_le(&mut self, val: u16) -> usize {
        self.buffer.put_u16_le(val);
        mem::size_of::<u16>()
    }

    pub fn read_proto_header(&mut self) -> ProtoHeader {
        ProtoHeader::read_from(&mut self.buffer)
    }

    pub fn read_message_header(&mut self, proto: ProtoHeader) -> MessageHeader {
        MessageHeader::read_from(&mut self.buffer, proto)
    }

    pub fn read_stream_message_header(&mut self, proto: ProtoHeader) -> StreamMessageHeader {
        StreamMessageHeader::read_from(&mut self.buffer, proto)
    }

    pub fn read_header(&mut self) -> MessageHeader {
        let proto = ProtoHeader::read_from(&mut self.buffer);
        MessageHeader::read_from(&mut self.buffer, proto)
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

impl AsMut<[u8]> for Buffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
}

impl Write for Buffer {
    #[inline]
    fn write_u8(&mut self, v: u8) -> usize {
        self.buffer.write_u8(v)
    }

    #[inline]
    fn write_u16(&mut self, v: u16) -> usize {
        self.buffer.write_u16(v)
    }

    #[inline]
    fn write_u32(&mut self, v: u32) -> usize {
        self.buffer.write_u32(v)
    }

    #[inline]
    fn write_u64(&mut self, v: u64) -> usize {
        self.buffer.write_u64(v)
    }

    #[inline]
    fn write_i8(&mut self, v: i8) -> usize {
        self.buffer.write_i8(v)
    }

    #[inline]
    fn write_i16(&mut self, v: i16) -> usize {
        self.buffer.write_i16(v)
    }

    #[inline]
    fn write_i32(&mut self, v: i32) -> usize {
        self.buffer.write_i32(v)
    }

    #[inline]
    fn write_i64(&mut self, v: i64) -> usize {
        self.buffer.write_i64(v)
    }

    #[inline]
    fn write_f32(&mut self, v: f32) -> usize {
        self.buffer.write_f32(v)
    }

    #[inline]
    fn write_f64(&mut self, v: f64) -> usize {
        self.buffer.write_f64(v)
    }

    #[inline]
    fn write_bytes(&mut self, v: &[u8]) -> usize {
        self.buffer.write_bytes(v)
    }

    #[inline]
    fn write_str(&mut self, v: &str) -> usize {
        self.buffer.write_str(v)
    }

    #[inline]
    fn write_bool(&mut self, v: bool) -> usize {
        self.buffer.write_bool(v)
    }

    #[inline]
    fn write_geo(&mut self, v: &str) -> usize {
        self.buffer.write_geo(v)
    }
}

fn estimate_key_size(key: &Key, send_user_key: bool) -> (usize, u16) {
    let mut size = 0;
    let mut count = 0;

    if !key.namespace.is_empty() {
        size += FIELD_HEADER_SIZE + key.namespace.len();
        count += 1;
    }

    if !key.set_name.is_empty() {
        size += FIELD_HEADER_SIZE + key.set_name.len();
        count += 1;
    }

    size += FIELD_HEADER_SIZE + DIGEST_SIZE;
    count += 1;

    if let Some(user_key) = key.user_key.as_ref().filter(|_| send_user_key) {
        // field header size + key size
        size += FIELD_HEADER_SIZE + 1 + user_key.estimate_size();
        count += 1;
    }

    (size, count)
}

fn estimate_operation_size_for_bin(bin: &Bin<'_>) -> usize {
    OPERATION_HEADER_SIZE + bin.name.len() + bin.value.estimate_size()
}

fn estimate_operation_size_for_bin_name(bin_name: &str) -> usize {
    OPERATION_HEADER_SIZE + bin_name.len()
}

/// A protocol header that is present at the beginning of each message sent to or received from an
/// Aerospike instance.
///
/// The header is 8 bytes long, basically a [`u64`] integer. The first few bytes have a special
/// meaning and the rest defines the length of the data followed. Reading the bytes left to right,
/// the meanings are:
///
/// - 1 byte: Version
/// - 1 byte: Message type
/// - 6 bytes: Data size
#[derive(Clone, Copy, Debug)]
pub struct ProtoHeader {
    pub version: Version,
    pub ty: ProtoType,
    pub size: usize,
}

impl ProtoHeader {
    pub const SIZE: usize = 8;

    fn write_to(&self, buf: &mut impl BufMut) {
        buf.put_u64(
            (u64::from(self.version) << 56)
                | (u64::from(self.ty) << 48)
                | (self.size & 0xffff_ffff_ffff) as u64,
        );
    }

    fn read_from(buf: &mut impl Buf) -> Self {
        let value = buf.get_u64();

        Self {
            version: ((value >> 56 & 0xff) as u8).into(),
            ty: ((value >> 48 & 0xff) as u8).into(),
            size: (value & 0xffff_ffff_ffff) as usize,
        }
    }
}

/// Known possible protocol versions, although this implementation only supports the latest
/// [`Self::V2`].
#[derive(Clone, Copy, Debug)]
pub enum Version {
    /// Initial version.
    V0,
    /// Previous version.
    V1,
    /// Latest version.
    V2,
    /// Unknown possible future versions that are not supported yet.
    Unknown(u8),
}

impl From<u8> for Version {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::V0,
            1 => Self::V1,
            2 => Self::V2,
            _ => Self::Unknown(value),
        }
    }
}

impl From<Version> for u8 {
    fn from(value: Version) -> Self {
        match value {
            Version::V0 => 0,
            Version::V1 => 1,
            Version::V2 => 2,
            Version::Unknown(v) => v,
        }
    }
}

impl From<Version> for u64 {
    fn from(value: Version) -> Self {
        u8::from(value).into()
    }
}

/// Known message types, which define the data followed after the [`ProtoHeader`].
#[derive(Clone, Copy, Debug)]
pub enum ProtoType {
    /// Informational message.
    Info,
    /// Security related message like authentication.
    Security,
    /// Regular message.
    Message,
    /// Regular, but compressed message.
    MessageCompressed,
    /// Internal message, that should probably never been sent or received.
    InternalXdr,
    /// Unknown possible future messages that are not supported yet.
    Unknown(u8),
}

impl From<u8> for ProtoType {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Info,
            2 => Self::Security,
            3 => Self::Message,
            4 => Self::MessageCompressed,
            5 => Self::InternalXdr,
            _ => Self::Unknown(value),
        }
    }
}

impl From<ProtoType> for u8 {
    fn from(value: ProtoType) -> Self {
        match value {
            ProtoType::Info => 1,
            ProtoType::Security => 2,
            ProtoType::Message => 3,
            ProtoType::MessageCompressed => 4,
            ProtoType::InternalXdr => 5,
            ProtoType::Unknown(v) => v,
        }
    }
}

impl From<ProtoType> for u64 {
    fn from(value: ProtoType) -> Self {
        u8::from(value).into()
    }
}

/// The header for a regular message, immediately followed after the [`ProtoHeader`], if its type is
/// [`ProtoType::Message`].
///
/// This header is always 22 bytes long. Reading the bytes left to right, the meanings are:
///
/// - 1 byte: Length of this header.
/// - 1 byte: Read attributes as bit flags.
/// - 1 byte: Write attributes as bit flags.
/// - 1 byte: Info attributes as bit flags.
/// - 1 byte: _Unused_.
/// - 1 byte: Result code for the operation done.
/// - 4 bytes: Generation counter.
/// - 4 bytes: Expiration of a record (if applicable).
/// - 4 bytes: Timeout of the operation (if applicable).
/// - 2 bytes: Field count in the payload.
/// - 2 bytes: Operation count in the payload.
pub struct MessageHeader {
    /// Amount of payload data following this header.
    pub size: usize,
    header_length: u8,
    /// Attributes relevant for reading operations.
    read_attr: ReadAttr,
    /// Attributes relevant for writing operations.
    write_attr: WriteAttr,
    /// Attributes relevant for any operation.
    info_attr: InfoAttr,
    _unused: u8,
    pub result_code: ResultCode,
    pub generation: u32,
    pub expiration: u32,
    pub timeout: Duration,
    /// Amount of fields in the payload.
    pub field_count: u16,
    /// Amount of operations in the payload.
    pub operation_count: u16,
}

impl MessageHeader {
    pub const SIZE: usize = 22;

    fn write_to(&self, buf: &mut impl BufMut) {
        ProtoHeader {
            version: Version::V2,
            ty: ProtoType::Message,
            size: Self::SIZE + self.size,
        }
        .write_to(buf);

        buf.put_u8(self.header_length);
        buf.put_u8(self.read_attr.bits());
        buf.put_u8(self.write_attr.bits());
        buf.put_u8(self.info_attr.bits());
        buf.put_u8(0);
        buf.put_u8(self.result_code.into());
        buf.put_u32(self.generation);
        buf.put_u32(self.expiration);
        buf.put_u32(self.timeout.as_secs() as u32 * 1000 + self.timeout.subsec_millis());
        buf.put_u16(self.field_count);
        buf.put_u16(self.operation_count);
    }

    fn read_from(buf: &mut impl Buf, proto: ProtoHeader) -> Self {
        let ProtoHeader { version, ty, size } = proto;

        assert!(
            matches!(version, Version::V2),
            "invalid message version {version:?}",
        );
        assert!(
            matches!(ty, ProtoType::Info | ProtoType::Message),
            "invalid message type {ty:?}",
        );
        assert!(size >= Self::SIZE, "invalid message length {size}");

        Self {
            size: size - Self::SIZE,
            header_length: buf.get_u8(),
            read_attr: ReadAttr::from_bits_truncate(buf.get_u8()),
            write_attr: WriteAttr::from_bits_truncate(buf.get_u8()),
            info_attr: InfoAttr::from_bits_truncate(buf.get_u8()),
            _unused: buf.get_u8(),
            result_code: buf.get_u8().into(),
            generation: buf.get_u32(),
            expiration: buf.get_u32(),
            timeout: Duration::from_secs(buf.get_u32().into()),
            field_count: buf.get_u16(),
            operation_count: buf.get_u16(),
        }
    }

    /// Create a new header for a read operation.
    fn for_read(
        size: usize,
        policy: &BasePolicy,
        mut read_attr: ReadAttr,
        write_attr: WriteAttr,
        field_count: u16,
        operation_count: u16,
    ) -> Self {
        if policy.consistency_level == ConsistencyLevel::All {
            read_attr |= ReadAttr::CONSISTENCY_LEVEL_ALL;
        }

        Self {
            size,
            header_length: Self::SIZE as u8,
            read_attr,
            write_attr,
            info_attr: InfoAttr::empty(),
            _unused: 0,
            result_code: ResultCode::Ok,
            generation: 0,
            expiration: Expiration::NamespaceDefault.into(),
            timeout: policy.timeout,
            field_count,
            operation_count,
        }
    }

    /// Create a new header for a write operation.
    fn for_write(
        size: usize,
        policy: &WritePolicy,
        mut read_attr: ReadAttr,
        mut write_attr: WriteAttr,
        field_count: u16,
        operation_count: u16,
    ) -> Self {
        let mut generation: u32 = 0;
        let mut info_attr = InfoAttr::empty();

        match policy.record_exists_action {
            RecordExistsAction::Update => (),
            RecordExistsAction::UpdateOnly => info_attr |= InfoAttr::UPDATE_ONLY,
            RecordExistsAction::Replace => info_attr |= InfoAttr::CREATE_OR_REPLACE,
            RecordExistsAction::ReplaceOnly => info_attr |= InfoAttr::REPLACE_ONLY,
            RecordExistsAction::CreateOnly => write_attr |= WriteAttr::CREATE_ONLY,
        }

        match policy.generation_policy {
            GenerationPolicy::None => (),
            GenerationPolicy::ExpectGenEqual => {
                generation = policy.generation;
                write_attr |= WriteAttr::GENERATION;
            }
            GenerationPolicy::ExpectGenGreater => {
                generation = policy.generation;
                write_attr |= WriteAttr::GENERATION_GT;
            }
        }

        if policy.commit_level == CommitLevel::Master {
            info_attr |= InfoAttr::COMMIT_LEVEL_MASTER;
        }

        if policy.base_policy.consistency_level == ConsistencyLevel::All {
            read_attr |= ReadAttr::CONSISTENCY_LEVEL_ALL;
        }

        if policy.durable_delete {
            write_attr |= WriteAttr::DURABLE_DELETE;
        }

        Self {
            size,
            header_length: Self::SIZE as u8,
            read_attr,
            write_attr,
            info_attr,
            _unused: 0,
            result_code: ResultCode::Ok,
            generation,
            expiration: policy.expiration.into(),
            timeout: policy.as_ref().timeout,
            field_count,
            operation_count,
        }
    }
}

pub struct StreamMessageHeader {
    /// Attributes relevant for any operation.
    pub(crate) info_attr: InfoAttr,
    _unused: u8,
    pub result_code: ResultCode,
    pub generation: u32,
    pub expiration: u32,
    pub value: u32,
    /// Amount of fields in the payload.
    pub field_count: u16,
    /// Amount of operations in the payload.
    pub operation_count: u16,
}

impl StreamMessageHeader {
    pub const SIZE: usize = 22;

    fn read_from(buf: &mut impl Buf, proto: ProtoHeader) -> Self {
        let ProtoHeader { version, ty, size } = proto;

        assert!(
            matches!(version, Version::V2),
            "invalid message version {version:?}",
        );
        assert!(
            matches!(ty, ProtoType::Message),
            "invalid message type {ty:?}",
        );
        assert!(size >= Self::SIZE, "invalid message length {size}");

        // skip header length, read attrs and write attrs
        buf.advance(3);

        Self {
            info_attr: InfoAttr::from_bits_truncate(buf.get_u8()),
            _unused: buf.get_u8(),
            result_code: buf.get_u8().into(),
            generation: buf.get_u32(),
            expiration: buf.get_u32(),
            value: buf.get_u32(),
            field_count: buf.get_u16(),
            operation_count: buf.get_u16(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resize_reclaim() {
        let mut buf = Buffer::new(10);
        buf.buffer.put_bytes(1, 10);

        assert_eq!(&[1; 10], &buf.buffer[..10]);
        assert_eq!(10, buf.buffer.len());
        assert_eq!(4096, buf.buffer.capacity());

        buf.resize(15).unwrap();

        assert_eq!(15, buf.buffer.len());
        assert_eq!(15, buf.buffer.capacity());
        assert_eq!(&[1; 10], &buf.buffer[..10]);
        assert_eq!(&[0; 5], &buf.buffer[10..]);
    }
}
