//! `HyperLogLog` operations on HLL items nested in lists/maps are not currently
//! supported by the server.

use bitflags::bitflags;

use super::cdt::OperationEncoder;
use crate::{
    operations::{
        cdt::{CdtArgument, CdtOperation},
        cdt_context::DEFAULT_CTX,
        Operation, OperationBin, OperationData, OperationType,
    },
    Value,
};

bitflags! {
    /// `HLLWriteFlags` determines write flags for HLL
    #[derive(Clone, Copy, Debug)]
    pub struct HllWriteFlags: u8 {
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
pub struct HllPolicy {
    /// CdtListWriteFlags
    pub flags: HllWriteFlags,
}

impl HllPolicy {
    /// Use specified `HLLWriteFlags` when performing `HLL` operations
    #[must_use]
    pub const fn new(write_flags: HllWriteFlags) -> Self {
        Self { flags: write_flags }
    }
}

impl Default for HllPolicy {
    /// Returns the default policy for HLL operations.
    fn default() -> Self {
        Self::new(HllWriteFlags::empty())
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum HllOpType {
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

/// Create HLL init operation.
/// Server creates a new HLL or resets an existing HLL.
/// Server does not return a value.
#[must_use]
pub fn init<'a>(policy: &HllPolicy, bin: &'a str, index_bit_count: i64) -> Operation<'a> {
    init_with_min_hash(policy, bin, index_bit_count, -1)
}

/// Create HLL init operation with minhash bits.
/// Server creates a new HLL or resets an existing HLL.
/// Server does not return a value.
#[must_use]
pub fn init_with_min_hash<'a>(
    policy: &HllPolicy,
    bin: &'a str,
    index_bit_count: i64,
    min_hash_bit_count: i64,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HllOpType::Init as u8,
        encoder: OperationEncoder::Hll,
        args: vec![
            CdtArgument::Int(index_bit_count),
            CdtArgument::Int(min_hash_bit_count),
            CdtArgument::Byte(policy.flags.bits()),
        ],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt_op),
    }
}

/// Create HLL add operation. This operation assumes HLL bin already exists.
/// Server adds values to the HLL set.
/// Server returns number of entries that caused HLL to update a register.
#[must_use]
pub fn add<'a>(policy: &HllPolicy, bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    add_with_index_and_min_hash(policy, bin, list, -1, -1)
}

/// Create HLL add operation.
/// Server adds values to HLL set. If HLL bin does not exist, use `indexBitCount` to create HLL bin.
/// Server returns number of entries that caused HLL to update a register.
#[must_use]
pub fn add_with_index<'a>(
    policy: &HllPolicy,
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
    policy: &HllPolicy,
    bin: &'a str,
    list: &'a [Value],
    index_bit_count: i64,
    min_hash_bit_count: i64,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HllOpType::Add as u8,
        encoder: OperationEncoder::Hll,
        args: vec![
            CdtArgument::List(list),
            CdtArgument::Int(index_bit_count),
            CdtArgument::Int(min_hash_bit_count),
            CdtArgument::Byte(policy.flags.bits()),
        ],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt_op),
    }
}

/// Create HLL set union operation.
/// Server sets union of specified HLL objects with HLL bin.
/// Server does not return a value.
#[must_use]
pub fn set_union<'a>(policy: &HllPolicy, bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HllOpType::SetUnion as u8,
        encoder: OperationEncoder::Hll,
        args: vec![
            CdtArgument::List(list),
            CdtArgument::Byte(policy.flags.bits()),
        ],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt_op),
    }
}

/// Create HLL refresh operation.
/// Server updates the cached count (if stale) and returns the count.
#[must_use]
pub fn refresh_count(bin: &str) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: HllOpType::SetCount as u8,
        encoder: OperationEncoder::Hll,
        args: vec![],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt_op),
    }
}

/// Create HLL fold operation.
/// Servers folds `indexBitCount` to the specified value.
/// This can only be applied when `minHashBitCount` on the HLL bin is 0.
/// Server does not return a value.
#[must_use]
pub fn fold(bin: &str, index_bit_count: i64) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: HllOpType::Fold as u8,
        encoder: OperationEncoder::Hll,
        args: vec![CdtArgument::Int(index_bit_count)],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt_op),
    }
}

/// Create HLL getCount operation.
/// Server returns estimated number of elements in the HLL bin.
#[must_use]
pub fn get_count(bin: &str) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: HllOpType::Count as u8,
        encoder: OperationEncoder::Hll,
        args: vec![],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt_op),
    }
}

/// Create HLL getUnion operation.
/// Server returns an HLL object that is the union of all specified HLL objects in the list
/// with the HLL bin.
#[must_use]
pub fn get_union<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HllOpType::Union as u8,
        encoder: OperationEncoder::Hll,
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt_op),
    }
}

/// Create HLL `get_union_count` operation.
/// Server returns estimated number of elements that would be contained by the union of these
/// HLL objects.
#[must_use]
pub fn get_union_count<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HllOpType::UnionCount as u8,
        encoder: OperationEncoder::Hll,
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt_op),
    }
}

/// Create HLL `get_intersect_count` operation.
/// Server returns estimated number of elements that would be contained by the intersection of
/// these HLL objects.
#[must_use]
pub fn get_intersect_count<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HllOpType::IntersectCount as u8,
        encoder: OperationEncoder::Hll,
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt_op),
    }
}

/// Create HLL getSimilarity operation.
/// Server returns estimated similarity of these HLL objects. Return type is a double.
#[must_use]
pub fn get_similarity<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HllOpType::Similarity as u8,
        encoder: OperationEncoder::Hll,
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt_op),
    }
}

/// Create HLL describe operation.
/// Server returns `indexBitCount` and `minHashBitCount` used to create HLL bin in a list of longs.
/// The list size is 2.
#[must_use]
pub fn describe(bin: &str) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: HllOpType::Describe as u8,
        encoder: OperationEncoder::Hll,
        args: vec![],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HllOp(cdt_op),
    }
}
