#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::{str, time::Duration};

use bitflags::bitflags;

use crate::{
    commands::field_type::FieldType,
    msgpack::Write,
    operations::{Operation, OperationBin, OperationData, OperationType},
    policy::{
        BatchPolicy, CommitLevel, ConsistencyLevel, GenerationPolicy, ReadPolicy,
        RecordExistsAction, ScanPolicy, WritePolicy,
    },
    BatchRead, Bin, Bins, Key, UserKey,
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

pub const MSG_TOTAL_HEADER_SIZE: u8 = 30;
const FIELD_HEADER_SIZE: u8 = 5;
const OPERATION_HEADER_SIZE: u8 = 8;
pub const MSG_REMAINING_HEADER_SIZE: u8 = 22;
const DIGEST_SIZE: u8 = 20;
const CL_MSG_VERSION: u8 = 2;
const AS_MSG_TYPE: u8 = 3;

// MAX_BUFFER_SIZE protects against allocating massive memory blocks
// for buffers. Tweak this number if you are returning a lot of
// LDT elements in your queries.
const MAX_BUFFER_SIZE: usize = 1024 * 1024 + 8; // 1 MB + header

pub type Result<T, E = BufferError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("Invalid size for buffer: {size} (max {max})")]
    SizeExceeded { size: usize, max: usize },
    #[error("Invalid UTF-8 content ecountered")]
    InvalidUtf8(#[from] std::str::Utf8Error),
}

// Holds data buffer for the command
#[derive(Debug, Default)]
pub struct Buffer {
    pub buffer: Vec<u8>,
    pub offset: usize,
    pub reclaim_threshold: usize,
}

impl Buffer {
    #[must_use]
    pub fn new(reclaim_threshold: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(1024),
            offset: 0,
            reclaim_threshold,
        }
    }

    fn begin(&mut self) {
        self.offset = MSG_TOTAL_HEADER_SIZE as usize;
    }

    pub fn size_buffer(&mut self) -> Result<()> {
        let offset = self.offset;
        self.resize_buffer(offset)
    }

    pub fn resize_buffer(&mut self, size: usize) -> Result<()> {
        // Corrupted data streams can result in a huge length.
        // Do a sanity check here.
        if size > MAX_BUFFER_SIZE {
            return Err(BufferError::SizeExceeded {
                size,
                max: MAX_BUFFER_SIZE,
            });
        }

        let mem_size = self.buffer.capacity();
        self.buffer.resize(size, 0);
        if mem_size > self.reclaim_threshold && size < mem_size {
            self.buffer.shrink_to_fit();
        }

        Ok(())
    }

    pub fn reset_offset(&mut self) {
        // reset data offset
        self.offset = 0;
    }

    pub fn end(&mut self) {
        let size = ((self.offset - 8) as i64)
            | (i64::from(CL_MSG_VERSION) << 56)
            | (i64::from(AS_MSG_TYPE) << 48);

        // reset data offset
        self.reset_offset();
        self.write_i64(size);
    }

    // Writes the command for write operations
    pub(crate) fn set_write<'b, A: AsRef<Bin<'b>>>(
        &mut self,
        policy: &WritePolicy,
        op_type: OperationType,
        key: &Key,
        bins: &[A],
    ) -> Result<()> {
        self.begin();
        let field_count = self.estimate_key_size(key, policy.send_key);

        for bin in bins {
            self.estimate_operation_size_for_bin(bin.as_ref());
        }

        self.size_buffer()?;
        self.write_header_with_policy(
            policy,
            ReadAttr::empty(),
            WriteAttr::WRITE,
            field_count,
            bins.len() as u16,
        );
        self.write_key(key, policy.send_key);

        for bin in bins {
            self.write_operation_for_bin(bin.as_ref(), op_type);
        }

        self.end();
        Ok(())
    }

    // Writes the command for write operations
    pub fn set_delete(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        self.begin();
        let field_count = self.estimate_key_size(key, false);

        self.size_buffer()?;
        self.write_header_with_policy(
            policy,
            ReadAttr::empty(),
            WriteAttr::WRITE | WriteAttr::DELETE,
            field_count,
            0,
        );
        self.write_key(key, false);

        self.end();
        Ok(())
    }

    // Writes the command for touch operations
    pub fn set_touch(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        self.begin();
        let field_count = self.estimate_key_size(key, policy.send_key);
        self.estimate_operation_size();
        self.size_buffer()?;
        self.write_header_with_policy(policy, ReadAttr::empty(), WriteAttr::WRITE, field_count, 1);
        self.write_key(key, policy.send_key);

        self.write_operation_for_operation_type(OperationType::Touch);
        self.end();
        Ok(())
    }

    // Writes the command for exist operations
    pub fn set_exists(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        self.begin();
        let field_count = self.estimate_key_size(key, false);

        self.size_buffer()?;
        self.write_header(
            &policy.base_policy,
            ReadAttr::READ | ReadAttr::GET_NO_BINS,
            WriteAttr::empty(),
            field_count,
            0,
        );
        self.write_key(key, false);

        self.end();
        Ok(())
    }

    // Writes the command for get operations
    pub fn set_read(&mut self, policy: &ReadPolicy, key: &Key, bins: &Bins) -> Result<()> {
        match bins {
            Bins::None => self.set_read_header(policy, key),
            Bins::All => self.set_read_for_key_only(policy, key),
            Bins::Some(ref bin_names) => {
                self.begin();
                let field_count = self.estimate_key_size(key, false);
                for bin_name in bin_names {
                    self.estimate_operation_size_for_bin_name(bin_name);
                }

                self.size_buffer()?;
                self.write_header(
                    policy,
                    ReadAttr::READ,
                    WriteAttr::empty(),
                    field_count,
                    bin_names.len() as u16,
                );
                self.write_key(key, false);

                for bin_name in bin_names {
                    self.write_operation_for_bin_name(bin_name, OperationType::Read);
                }

                self.end();
                Ok(())
            }
        }
    }

    // Writes the command for getting metadata operations
    pub fn set_read_header(&mut self, policy: &ReadPolicy, key: &Key) -> Result<()> {
        self.begin();
        let field_count = self.estimate_key_size(key, false);

        self.estimate_operation_size_for_bin_name("");
        self.size_buffer()?;
        self.write_header(
            policy,
            ReadAttr::READ | ReadAttr::GET_NO_BINS,
            WriteAttr::empty(),
            field_count,
            1,
        );
        self.write_key(key, false);

        self.write_operation_for_bin_name("", OperationType::Read);
        self.end();
        Ok(())
    }

    pub fn set_read_for_key_only(&mut self, policy: &ReadPolicy, key: &Key) -> Result<()> {
        self.begin();

        let field_count = self.estimate_key_size(key, false);

        self.size_buffer()?;
        self.write_header(
            policy,
            ReadAttr::READ | ReadAttr::GET_ALL,
            WriteAttr::empty(),
            field_count,
            0,
        );
        self.write_key(key, false);

        self.end();
        Ok(())
    }

    // Writes the command for batch read operations
    pub fn set_batch_read(
        &mut self,
        policy: &BatchPolicy,
        batch_reads: &[BatchRead],
    ) -> Result<()> {
        let field_count_row = if policy.send_set_name { 2 } else { 1 };

        self.begin();
        let field_count = 1;
        self.offset += FIELD_HEADER_SIZE as usize + 5;

        let mut prev: Option<&BatchRead> = None;
        for batch_read in batch_reads {
            self.offset += batch_read.key.digest.len() + 4;
            match prev {
                Some(prev) if batch_read.match_header(prev, policy.send_set_name) => {
                    self.offset += 1;
                }
                _ => {
                    let key = &batch_read.key;
                    self.offset += key.namespace.len() + FIELD_HEADER_SIZE as usize + 6;
                    if policy.send_set_name {
                        self.offset += key.set_name.len() + FIELD_HEADER_SIZE as usize;
                    }
                    if let Bins::Some(ref bin_names) = batch_read.bins {
                        for name in bin_names {
                            self.estimate_operation_size_for_bin_name(name);
                        }
                    }
                }
            }
            prev = Some(batch_read);
        }

        self.size_buffer()?;
        self.write_header(
            &policy.base_policy,
            ReadAttr::READ | ReadAttr::BATCH,
            WriteAttr::empty(),
            field_count,
            0,
        );

        let field_size_offset = self.offset;
        let field_type = if policy.send_set_name {
            FieldType::BatchIndexWithSet
        } else {
            FieldType::BatchIndex
        };
        self.write_field_header(0, field_type);
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
                    match batch_read.bins {
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
                        Bins::Some(ref bin_names) => {
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

        let field_size = self.offset - MSG_TOTAL_HEADER_SIZE as usize - 4;
        self.buffer[field_size_offset..field_size_offset + 4]
            .copy_from_slice(&(field_size as u32).to_be_bytes());

        self.end();
        Ok(())
    }

    // Writes the command for getting metadata operations
    pub fn set_operate<'a>(
        &mut self,
        policy: &WritePolicy,
        key: &Key,
        operations: &'a [Operation<'a>],
    ) -> Result<()> {
        self.begin();

        let mut read_attr = ReadAttr::empty();
        let mut write_attr = WriteAttr::empty();

        for operation in operations {
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
                OperationData::CdtMapOp(_) | OperationData::CdtBitOp(_) | OperationData::HllOp(_)
            );

            if policy.respond_per_each_op || each_op {
                write_attr |= WriteAttr::RESPOND_ALL_OPS;
            }

            self.offset += operation.estimate_size() + OPERATION_HEADER_SIZE as usize;
        }

        let field_count = self.estimate_key_size(key, policy.send_key && !write_attr.is_empty());
        self.size_buffer()?;

        if write_attr.is_empty() {
            self.write_header(
                &policy.base_policy,
                read_attr,
                write_attr,
                field_count,
                operations.len() as u16,
            );
        } else {
            self.write_header_with_policy(
                policy,
                read_attr,
                write_attr,
                field_count,
                operations.len() as u16,
            );
        }
        self.write_key(key, policy.send_key && !write_attr.is_empty());

        for operation in operations {
            operation.write_to(self);
        }
        self.end();
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
        self.begin();

        let mut field_count = 0;

        if !namespace.is_empty() {
            self.offset += namespace.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if !set_name.is_empty() {
            self.offset += set_name.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        // // Estimate scan options size.
        // self.data_offset += 2 + FIELD_HEADER_SIZE as usize;
        // field_count += 1;

        // Estimate pid size
        self.offset += partitions.len() * 2 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        // Estimate scan timeout size.
        self.offset += 4 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        // Allocate space for task_id field.
        self.offset += 8 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        let bin_count = match *bins {
            Bins::All | Bins::None => 0,
            Bins::Some(ref bin_names) => {
                for bin_name in bin_names {
                    self.estimate_operation_size_for_bin_name(bin_name);
                }
                bin_names.len()
            }
        };

        self.size_buffer()?;

        let mut read_attr = ReadAttr::READ;
        if *bins == Bins::None {
            read_attr |= ReadAttr::GET_NO_BINS;
        }

        self.write_header(
            &policy.base_policy,
            read_attr,
            WriteAttr::empty(),
            field_count,
            bin_count as u16,
        );

        if !namespace.is_empty() {
            self.write_field_string(namespace, FieldType::Namespace);
        }

        if !set_name.is_empty() {
            self.write_field_string(set_name, FieldType::Table);
        }

        self.write_field_header(partitions.len() * 2, FieldType::PidArray);
        for pid in partitions {
            self.write_u16_little_endian(*pid);
        }

        // self.write_field_header(2, FieldType::ScanOptions);

        // let mut priority: u8 = policy.base_policy.priority.clone() as u8;
        // priority <<= 4;

        // if policy.fail_on_cluster_change {
        //     priority |= 0x08;
        // }

        // self.write_u8(priority);
        // self.write_u8(policy.scan_percent);

        // Write scan timeout
        self.write_field_header(4, FieldType::ScanTimeout);
        self.write_u32(policy.socket_timeout);

        self.write_field_header(8, FieldType::TranId);
        self.write_u64(task_id);

        if let Bins::Some(ref bin_names) = *bins {
            for bin_name in bin_names {
                self.write_operation_for_bin_name(bin_name, OperationType::Read);
            }
        }

        self.end();
        Ok(())
    }

    fn estimate_key_size(&mut self, key: &Key, send_key: bool) -> u16 {
        let mut field_count: u16 = 0;

        if !key.namespace.is_empty() {
            self.offset += key.namespace.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if !key.set_name.is_empty() {
            self.offset += key.set_name.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        self.offset += (DIGEST_SIZE + FIELD_HEADER_SIZE) as usize;
        field_count += 1;

        if send_key {
            if let Some(ref user_key) = key.user_key {
                // field header size + key size
                self.offset += user_key.estimate_size() + FIELD_HEADER_SIZE as usize + 1;
                field_count += 1;
            }
        }

        field_count
    }

    fn estimate_operation_size_for_bin(&mut self, bin: &Bin<'_>) {
        self.offset += bin.name.len() + OPERATION_HEADER_SIZE as usize;
        self.offset += bin.value.estimate_size();
    }

    fn estimate_operation_size_for_bin_name(&mut self, bin_name: &str) {
        self.offset += bin_name.len() + OPERATION_HEADER_SIZE as usize;
    }

    fn estimate_operation_size(&mut self) {
        self.offset += OPERATION_HEADER_SIZE as usize;
    }

    fn write_header(
        &mut self,
        policy: &ReadPolicy,
        read_attr: ReadAttr,
        write_attr: WriteAttr,
        field_count: u16,
        operation_count: u16,
    ) {
        let mut read_attr = read_attr;

        if policy.consistency_level == ConsistencyLevel::ConsistencyAll {
            read_attr |= ReadAttr::CONSISTENCY_LEVEL_ALL;
        }

        // Write all header data except total size which must be written last.
        self.buffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
        self.buffer[9] = read_attr.bits();
        self.buffer[10] = write_attr.bits();

        for i in 11..26 {
            self.buffer[i] = 0;
        }

        self.offset = 26;
        self.write_u16(field_count);
        self.write_u16(operation_count);

        self.offset = MSG_TOTAL_HEADER_SIZE as usize;
    }

    // Header write for write operations.
    fn write_header_with_policy(
        &mut self,
        policy: &WritePolicy,
        read_attr: ReadAttr,
        write_attr: WriteAttr,
        field_count: u16,
        operation_count: u16,
    ) {
        // Set flags.
        let mut generation: u32 = 0;
        let mut info_attr = InfoAttr::empty();
        let mut read_attr = read_attr;
        let mut write_attr = write_attr;

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

        if policy.commit_level == CommitLevel::CommitMaster {
            info_attr |= InfoAttr::COMMIT_LEVEL_MASTER;
        }

        if policy.base_policy.consistency_level == ConsistencyLevel::ConsistencyAll {
            read_attr |= ReadAttr::CONSISTENCY_LEVEL_ALL;
        }

        if policy.durable_delete {
            write_attr |= WriteAttr::DURABLE_DELETE;
        }

        // Write all header data except total size which must be written last.
        self.offset = 8;
        self.write_u8(MSG_REMAINING_HEADER_SIZE); // Message header length.
        self.write_u8(read_attr.bits());
        self.write_u8(write_attr.bits());
        self.write_u8(info_attr.bits());
        self.write_u8(0); // unused
        self.write_u8(0); // clear the result code

        self.write_u32(generation);
        self.write_u32(policy.expiration.into());

        // Initialize timeout. It will be written later.
        self.write_u32(0);

        self.write_u16(field_count);
        self.write_u16(operation_count);
        self.offset = MSG_TOTAL_HEADER_SIZE as usize;
    }

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
            if let Some(ref user_key) = key.user_key {
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
    pub const fn data_offset(&self) -> usize {
        self.offset
    }

    pub fn skip_bytes(&mut self, count: usize) {
        self.offset += count;
    }

    pub fn skip(&mut self, count: usize) {
        self.offset += count;
    }

    #[must_use]
    pub fn peek(&self) -> u8 {
        self.buffer[self.offset]
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_u8(&mut self, pos: Option<usize>) -> u8 {
        if let Some(pos) = pos {
            self.buffer[pos]
        } else {
            let res = self.buffer[self.offset];
            self.offset += 1;
            res
        }
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_i8(&mut self, pos: Option<usize>) -> i8 {
        if let Some(pos) = pos {
            self.buffer[pos] as i8
        } else {
            let res = self.buffer[self.offset] as i8;
            self.offset += 1;
            res
        }
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_u16(&mut self, pos: Option<usize>) -> u16 {
        self.read_int(pos, u16::from_be_bytes)
    }

    pub fn read_i16(&mut self, pos: Option<usize>) -> i16 {
        let val = self.read_u16(pos);
        val as i16
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_u32(&mut self, pos: Option<usize>) -> u32 {
        self.read_int(pos, u32::from_be_bytes)
    }

    pub fn read_i32(&mut self, pos: Option<usize>) -> i32 {
        let val = self.read_u32(pos);
        val as i32
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_u64(&mut self, pos: Option<usize>) -> u64 {
        self.read_int(pos, u64::from_be_bytes)
    }

    pub fn read_i64(&mut self, pos: Option<usize>) -> i64 {
        let val = self.read_u64(pos);
        val as i64
    }

    pub fn read_msg_size(&mut self, pos: Option<usize>) -> usize {
        let size = self.read_i64(pos);
        let size = size & 0xFFFF_FFFF_FFFF;
        size as usize
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_f32(&mut self, pos: Option<usize>) -> f32 {
        self.read_int(pos, f32::from_be_bytes)
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_f64(&mut self, pos: Option<usize>) -> f64 {
        self.read_int(pos, f64::from_be_bytes)
    }

    fn read_int<const LEN: usize, T>(
        &mut self,
        pos: Option<usize>,
        convert: impl FnOnce([u8; LEN]) -> T,
    ) -> T {
        let mut buf = [0; LEN];

        if let Some(pos) = pos {
            buf.copy_from_slice(&self.buffer[pos..pos + LEN]);
        } else {
            buf.copy_from_slice(&self.buffer[self.offset..self.offset + LEN]);
            self.offset += LEN;
        }

        (convert)(buf)
    }

    pub fn read_str(&mut self, len: usize) -> Result<String> {
        let s = str::from_utf8(&self.buffer[self.offset..self.offset + len])?;
        self.offset += len;
        Ok(s.to_owned())
    }

    pub fn read_bytes(&mut self, pos: usize, count: usize) -> &[u8] {
        &self.buffer[pos..pos + count]
    }

    pub fn read_slice(&mut self, count: usize) -> &[u8] {
        &self.buffer[self.offset..self.offset + count]
    }

    pub fn read_blob(&mut self, len: usize) -> Vec<u8> {
        let val = self.buffer[self.offset..self.offset + len].to_vec();
        self.offset += len;
        val
    }

    pub fn write_u16_little_endian(&mut self, val: u16) -> usize {
        self.buffer[self.offset..self.offset + 2].copy_from_slice(&val.to_le_bytes());
        self.offset += 2;
        2
    }

    pub fn write_timeout(&mut self, val: Option<Duration>) {
        if let Some(val) = val {
            let millis = (val.as_secs() * 1_000) as i32 + val.subsec_millis() as i32;
            self.buffer[22..22 + 4].copy_from_slice(&millis.to_be_bytes());
        }
    }
}

impl Write for Buffer {
    fn write_u8(&mut self, v: u8) -> usize {
        self.buffer[self.offset] = v;
        self.offset += 1;
        1
    }

    fn write_u16(&mut self, v: u16) -> usize {
        self.buffer[self.offset..self.offset + 2].copy_from_slice(&v.to_be_bytes());
        self.offset += 2;
        2
    }

    fn write_u32(&mut self, v: u32) -> usize {
        self.buffer[self.offset..self.offset + 4].copy_from_slice(&v.to_be_bytes());
        self.offset += 4;
        4
    }

    fn write_u64(&mut self, v: u64) -> usize {
        self.buffer[self.offset..self.offset + 8].copy_from_slice(&v.to_be_bytes());
        self.offset += 8;
        8
    }

    fn write_i8(&mut self, v: i8) -> usize {
        self.buffer[self.offset] = v as u8;
        self.offset += 1;
        1
    }

    fn write_i16(&mut self, v: i16) -> usize {
        self.write_u16(v as u16)
    }

    fn write_i32(&mut self, v: i32) -> usize {
        self.write_u32(v as u32)
    }

    fn write_i64(&mut self, v: i64) -> usize {
        self.write_u64(v as u64)
    }

    fn write_f32(&mut self, v: f32) -> usize {
        self.buffer[self.offset..self.offset + 4].copy_from_slice(&v.to_be_bytes());
        self.offset += 4;
        4
    }

    fn write_f64(&mut self, v: f64) -> usize {
        self.buffer[self.offset..self.offset + 8].copy_from_slice(&v.to_be_bytes());
        self.offset += 8;
        8
    }

    fn write_bytes(&mut self, v: &[u8]) -> usize {
        for b in v {
            self.write_u8(*b);
        }
        v.len()
    }

    fn write_str(&mut self, v: &str) -> usize {
        self.write_bytes(v.as_bytes())
    }

    fn write_bool(&mut self, v: bool) -> usize {
        self.write_i64(v.into())
    }

    fn write_geo(&mut self, v: &str) -> usize {
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);
        self.write_bytes(v.as_bytes());
        3 + v.len()
    }
}
