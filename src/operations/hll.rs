//! `HyperLogLog` operations on HLL items nested in lists/maps are not currently
//! supported by the server.

use bitflags::bitflags;

use super::cdt::{self, Encoder};
use crate::{
    operations::{Operation, OperationBin, OperationData, OperationType},
    Value,
};

bitflags! {
    /// `HLLWriteFlags` determines write flags for HLL
    #[derive(Clone, Copy, Debug)]
    pub struct WriteFlags: u8 {
        /// If the bin already exists, the operation will be denied.
        /// If the bin does not exist, a new bin will be created.
        const CREATE_ONLY = 1;
        /// If the bin already exists, the bin will be overwritten.
        /// If the bin does not exist, the operation will be denied.
        const UPDATE_ONLY = 2;
        /// Do not raise error if operation is denied.
        const NO_FAIL = 4;
        /// Allow the resulting set to be the minimum of provided index bits.
        /// Also, allow the usage of less precise HLL algorithms when minHash bits
        /// of all participating sets do not match.
        const ALLOW_FOLD = 8;
    }

}

/// `HLLPolicy` operation policy.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// CdtListWriteFlags
    pub flags: WriteFlags,
}

impl Policy {
    /// Use specified `HLLWriteFlags` when performing `HLL` operations
    #[must_use]
    pub const fn new(write_flags: WriteFlags) -> Self {
        Self { flags: write_flags }
    }
}

impl Default for Policy {
    /// Returns the default policy for HLL operations.
    fn default() -> Self {
        Self::new(WriteFlags::empty())
    }
}

#[derive(Clone, Copy, Debug)]
enum OpType {
    Init = 0,
    Add,
    SetUnion,
    SetCount,
    Fold,
    Count = 50,
    Union,
    UnionCount,
    IntersectCount,
    Similarity,
    Describe,
}

#[inline]
const fn write<'a>(bin: &'a str, op: OpType, args: Vec<cdt::Argument<'a>>) -> Operation<'a> {
    Operation {
        op: OperationType::HllWrite,
        ctx: &[],
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt::Operation {
            op: op as u8,
            encoder: Encoder::Hll,
            args,
        }),
    }
}

#[inline]
const fn read<'a>(bin: &'a str, op: OpType, args: Vec<cdt::Argument<'a>>) -> Operation<'a> {
    Operation {
        op: OperationType::HllRead,
        ctx: &[],
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt::Operation {
            op: op as u8,
            encoder: Encoder::Hll,
            args,
        }),
    }
}

/// Create HLL init operation.
/// Server creates a new HLL or resets an existing HLL.
/// Server does not return a value.
#[must_use]
pub fn init(policy: Policy, bin: &str, index_bit_count: i64) -> Operation<'_> {
    init_with_min_hash(policy, bin, index_bit_count, -1)
}

/// Create HLL init operation with minhash bits.
/// Server creates a new HLL or resets an existing HLL.
/// Server does not return a value.
#[must_use]
pub fn init_with_min_hash(
    policy: Policy,
    bin: &str,
    index_bit_count: i64,
    min_hash_bit_count: i64,
) -> Operation<'_> {
    write(
        bin,
        OpType::Init,
        vec![
            cdt::Argument::Int(index_bit_count),
            cdt::Argument::Int(min_hash_bit_count),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Create HLL add operation. This operation assumes HLL bin already exists.
/// Server adds values to the HLL set.
/// Server returns number of entries that caused HLL to update a register.
#[must_use]
pub fn add<'a>(policy: Policy, bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    add_with_index_and_min_hash(policy, bin, list, -1, -1)
}

/// Create HLL add operation.
/// Server adds values to HLL set. If HLL bin does not exist, use `indexBitCount` to create HLL bin.
/// Server returns number of entries that caused HLL to update a register.
#[must_use]
pub fn add_with_index<'a>(
    policy: Policy,
    bin: &'a str,
    list: &'a [Value],
    index_bit_count: i64,
) -> Operation<'a> {
    add_with_index_and_min_hash(policy, bin, list, index_bit_count, -1)
}

/// Create HLL add operation with minhash bits.
/// Server adds values to HLL set. If HLL bin does not exist, use `indexBitCount` and
/// `minHashBitCount` to create HLL bin. Server returns number of entries that caused HLL to update
/// a register.
#[must_use]
pub fn add_with_index_and_min_hash<'a>(
    policy: Policy,
    bin: &'a str,
    list: &'a [Value],
    index_bit_count: i64,
    min_hash_bit_count: i64,
) -> Operation<'a> {
    write(
        bin,
        OpType::Add,
        vec![
            cdt::Argument::List(list),
            cdt::Argument::Int(index_bit_count),
            cdt::Argument::Int(min_hash_bit_count),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Create HLL set union operation.
/// Server sets union of specified HLL objects with HLL bin.
/// Server does not return a value.
#[must_use]
pub fn set_union<'a>(policy: Policy, bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    write(
        bin,
        OpType::SetUnion,
        vec![
            cdt::Argument::List(list),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Create HLL refresh operation.
/// Server updates the cached count (if stale) and returns the count.
#[must_use]
pub fn refresh_count(bin: &str) -> Operation<'_> {
    write(bin, OpType::SetCount, vec![])
}

/// Create HLL fold operation.
/// Servers folds `indexBitCount` to the specified value.
/// This can only be applied when `minHashBitCount` on the HLL bin is 0.
/// Server does not return a value.
#[must_use]
pub fn fold(bin: &str, index_bit_count: i64) -> Operation<'_> {
    write(bin, OpType::Fold, vec![cdt::Argument::Int(index_bit_count)])
}

/// Create HLL getCount operation.
/// Server returns estimated number of elements in the HLL bin.
#[must_use]
pub fn get_count(bin: &str) -> Operation<'_> {
    read(bin, OpType::Count, vec![])
}

/// Create HLL getUnion operation.
/// Server returns an HLL object that is the union of all specified HLL objects in the list
/// with the HLL bin.
#[must_use]
pub fn get_union<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    read(bin, OpType::Union, vec![cdt::Argument::List(list)])
}

/// Create HLL `get_union_count` operation.
/// Server returns estimated number of elements that would be contained by the union of these
/// HLL objects.
#[must_use]
pub fn get_union_count<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    read(bin, OpType::UnionCount, vec![cdt::Argument::List(list)])
}

/// Create HLL `get_intersect_count` operation.
/// Server returns estimated number of elements that would be contained by the intersection of
/// these HLL objects.
#[must_use]
pub fn get_intersect_count<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    read(bin, OpType::IntersectCount, vec![cdt::Argument::List(list)])
}

/// Create HLL getSimilarity operation.
/// Server returns estimated similarity of these HLL objects. Return type is a double.
#[must_use]
pub fn get_similarity<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    read(bin, OpType::Similarity, vec![cdt::Argument::List(list)])
}

/// Create HLL describe operation.
/// Server returns `indexBitCount` and `minHashBitCount` used to create HLL bin in a list of longs.
/// The list size is 2.
#[must_use]
pub fn describe(bin: &str) -> Operation<'_> {
    read(bin, OpType::Describe, vec![])
}
