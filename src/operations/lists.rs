//! List bin operations. Create list operations used by the client's `operate()` method.
//!
//! List operations support negative indexing. If the index is negative, the resolved index starts
//! backwards from the end of the list.
//!
//! Index/Count examples:
//!
//! * Index 0: First item in list.
//! * Index 4: Fifth item in list.
//! * Index -1: Last item in list.
//! * Index -3: Third to last item in list.
//! * Index 1, Count 2: Second and third item in list.
//! * Index -3, Count 3: Last three items in list.
//! * Index -5, Count 4: Range between fifth to last item to second to last item inclusive.
//!
//! If an index is out of bounds, a parameter error will be returned. If a range is partially out of
//! bounds, the valid part of the range will be returned.

use bitflags::bitflags;

use super::cdt::OperationEncoder;
use crate::{
    operations::{
        cdt::{CdtArgument, CdtOperation},
        cdt_context::{CdtContext, DEFAULT_CTX},
        Operation, OperationBin, OperationData, OperationType,
    },
    Value,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum CdtListOpType {
    SetType = 0,
    Append,
    AppendItems,
    Insert,
    InsertItems,
    Pop,
    PopRange,
    Remove,
    RemoveRange,
    Set,
    Trim,
    Clear,
    Increment,
    Sort,
    Size = 16,
    Get,
    GetRange,
    GetByIndex,
    GetByRank = 21,
    GetByValue,
    GetByValueList,
    GetByIndexRange,
    GetByValueInterval,
    GetByRankRange,
    GetByValueRelRankRange,
    RemoveByIndex = 32,
    RemoveByRank = 34,
    RemoveByValue,
    RemoveByValueList,
    RemoveByIndexRange,
    RemoveByValueInterval,
    RemoveByRankRange,
    RemoveByValueRelRankRange,
}

/// List storage order.
#[derive(Clone, Copy, Debug)]
pub enum ListOrderType {
    /// List is not ordered. This is the default.
    Unordered = 0,
    /// List is ordered.
    Ordered,
}

/// `CdtListReturnType` determines the returned values in CDT List operations.
#[derive(Clone, Copy, Debug)]
pub enum ListReturnType {
    /// Do not return a result.
    None = 0,
    /// Return index offset order.
    /// 0 = first key
    /// N = Nth key
    /// -1 = last key
    Index,
    /// Return reverse index offset order.
    /// 0 = last key
    /// -1 = first key
    ReverseIndex,
    /// Return value order.
    /// 0 = smallest value
    /// N = Nth smallest value
    /// -1 = largest value
    Rank,
    /// Return reserve value order.
    /// 0 = largest value
    /// N = Nth largest value
    /// -1 = smallest value
    ReverseRank,
    /// Return count of items selected.
    Count,
    /// Return value for single key read and value list for range read.
    Values = 7,
    /// Invert meaning of list command and return values.
    /// With the INVERTED flag enabled, the items outside of the specified index range will be
    /// returned. The meaning of the list command can also be inverted.
    /// With the INVERTED flag enabled, the items outside of the specified index range will be
    /// removed and returned.
    Inverted = 0x10000,
}

bitflags! {
    /// `CdtListSortFlags` determines sort flags for CDT lists
    #[derive(Clone, Copy, Debug)]
    pub struct ListSortFlags: u8 {
        /// Descending will sort the contents of the list in descending order.
        const DESCENDING = 1;
        /// DropDuplicates will drop duplicate values in the results of the CDT list operation.
        const DROP_DUPLICATES = 2;
    }
}

bitflags! {
    /// `CdtListWriteFlags` determines write flags for CDT lists
    #[derive(Clone, Copy, Debug)]
    pub struct ListWriteFlags: u8 {
        /// AddUnique means: Only add unique values.
        const ADD_UNIQUE = 1;
        /// InsertBounded means: Enforce list boundaries when inserting.  Do not allow values to be
        /// inserted at index outside current list boundaries.
        const INSERT_BOUNDED = 2;
        /// NoFail means: do not raise error if a list item fails due to write flag constraints.
        const NO_FAIL = 4;
        /// Partial means: allow other valid list items to be committed if a list item fails due to
        /// write flag constraints.
        const PARTIAL = 8;
    }
}

/// `ListPolicy` directives when creating a list and writing list items.
#[derive(Clone, Debug)]
pub struct ListPolicy {
    /// CdtListOrderType
    pub attributes: ListOrderType,
    /// CdtListWriteFlags
    pub flags: ListWriteFlags,
}

impl ListPolicy {
    /// Create unique key list with specified order when list does not exist.
    /// Use specified write mode when writing list items.
    #[must_use]
    pub const fn new(order: ListOrderType, write_flags: ListWriteFlags) -> Self {
        Self {
            attributes: order,
            flags: write_flags,
        }
    }
}

impl Default for ListPolicy {
    /// Returns the default policy for CDT list operations.
    fn default() -> Self {
        Self::new(ListOrderType::Unordered, ListWriteFlags::empty())
    }
}

#[must_use]
pub(crate) const fn list_order_flag(order: ListOrderType, pad: bool) -> u8 {
    if matches!(order, ListOrderType::Ordered) {
        0xc0
    } else if pad {
        0x80
    } else {
        0x40
    }
}

/// Creates list create operation.
/// Server creates list at given context level. The context is allowed to be beyond list
/// boundaries only if pad is set to true.  In that case, nil list entries will be inserted to
/// satisfy the context position.
#[must_use]
pub fn create(bin: &str, list_order: ListOrderType, pad: bool) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::SetType as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(list_order_flag(list_order, pad)),
            CdtArgument::Byte(list_order as u8),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a set list order operation.
/// Server sets list order.  Server returns null.
#[must_use]
pub fn set_order<'a>(
    bin: &'a str,
    list_order: ListOrderType,
    ctx: &'a [CdtContext],
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::SetType as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Byte(list_order as u8)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}
/// Create list append operation. Server appends value to the end of list bin. Server returns
/// list size.
#[must_use]
pub fn append<'a>(policy: &ListPolicy, bin: &'a str, value: &'a Value) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Append as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Value(value),
            CdtArgument::Byte(policy.attributes as u8),
            CdtArgument::Byte(policy.flags.bits()),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list append items operation. Server appends each input list item to the end of list
/// bin. Server returns list size.
///
/// # Panics
/// Will panic if values is empty
#[must_use]
pub fn append_items<'a>(policy: &ListPolicy, bin: &'a str, values: &'a [Value]) -> Operation<'a> {
    assert!(!values.is_empty());

    let cdt_op = CdtOperation {
        op: CdtListOpType::AppendItems as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::List(values),
            CdtArgument::Byte(policy.attributes as u8),
            CdtArgument::Byte(policy.flags.bits()),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list insert operation. Server inserts value to the specified index of the list bin.
/// Server returns list size.
#[must_use]
pub fn insert<'a>(
    policy: &ListPolicy,
    bin: &'a str,
    index: i64,
    value: &'a Value,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Insert as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Int(index),
            CdtArgument::Value(value),
            CdtArgument::Byte(policy.flags.bits()),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list insert items operation. Server inserts each input list item starting at the
/// specified index of the list bin. Server returns list size.
///
/// # Panics
/// will panic if values is empty
#[must_use]
pub fn insert_items<'a>(
    policy: &ListPolicy,
    bin: &'a str,
    index: i64,
    values: &'a [Value],
) -> Operation<'a> {
    assert!(!values.is_empty());

    let cdt_op = CdtOperation {
        op: CdtListOpType::InsertItems as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Int(index),
            CdtArgument::List(values),
            CdtArgument::Byte(policy.flags.bits()),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list pop operation. Server returns the item at the specified index and removes the
/// item from the list bin.
#[must_use]
pub fn pop(bin: &str, index: i64) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Pop as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list pop range operation. Server returns `count` items starting at the specified
/// index and removes the items from the list bin.
#[must_use]
pub fn pop_range(bin: &str, index: i64, count: i64) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::PopRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list pop range operation. Server returns the items starting at the specified index
/// to the end of the list and removes those items from the list bin.
#[must_use]
pub fn pop_range_from(bin: &str, index: i64) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::PopRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove operation. Server removes the item at the specified index from the list
/// bin. Server returns the number of items removed.
#[must_use]
pub fn remove(bin: &str, index: i64) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Remove as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove range operation. Server removes `count` items starting at the specified
/// index from the list bin. Server returns the number of items removed.
#[must_use]
pub fn remove_range(bin: &str, index: i64, count: i64) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove range operation. Server removes the items starting at the specified
/// index to the end of the list. Server returns the number of items removed.
#[must_use]
pub fn remove_range_from(bin: &str, index: i64) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove value operation. Server removes all items that are equal to the
/// specified value. Server returns the number of items removed.
#[must_use]
pub fn remove_by_value<'a>(
    bin: &'a str,
    value: &'a Value,
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValue as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove by value list operation. Server removes all items that are equal to
/// one of the specified values. Server returns the number of items removed
#[must_use]
pub fn remove_by_value_list<'a>(
    bin: &'a str,
    values: &'a [Value],
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValueList as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::List(values),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes list items identified by value range (valueBegin inclusive, valueEnd exclusive).
/// If valueBegin is nil, the range is less than valueEnd.
/// If valueEnd is nil, the range is greater than equal to valueBegin.
/// Server returns removed data specified by returnType
#[must_use]
pub fn remove_by_value_range<'a>(
    bin: &'a str,
    return_type: ListReturnType,
    begin: &'a Value,
    end: &'a Value,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValueInterval as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(begin),
            CdtArgument::Value(end),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove by value relative to rank range operation.
/// Server removes list items nearest to value and greater by relative rank.
/// Server returns removed data specified by returnType.
///
/// Examples for ordered list \[0, 4, 5, 9, 11, 15\]:
/// ```text
/// (value,rank) = [removed items]
/// (5,0) = [5,9,11,15]
/// (5,1) = [9,11,15]
/// (5,-1) = [4,5,9,11,15]
/// (3,0) = [4,5,9,11,15]
/// (3,3) = [11,15]
/// (3,-3) = [0,4,5,9,11,15]
/// ```
#[must_use]
pub fn remove_by_value_relative_rank_range<'a>(
    bin: &'a str,
    return_type: ListReturnType,
    value: &'a Value,
    rank: i64,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValueRelRankRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove by value relative to rank range operation.
/// Server removes list items nearest to value and greater by relative rank with a count limit.
/// Server returns removed data specified by returnType.
///
/// Examples for ordered list \[0, 4, 5, 9, 11, 15\]:
/// ```text
/// (value,rank,count) = [removed items]
/// (5,0,2) = [5,9]
/// (5,1,1) = [9]
/// (5,-1,2) = [4,5]
/// (3,0,1) = [4]
/// (3,3,7) = [11,15]
/// (3,-3,2) = []
/// ```
#[must_use]
pub fn remove_by_value_relative_rank_range_count<'a>(
    bin: &'a str,
    return_type: ListReturnType,
    value: &'a Value,
    rank: i64,
    count: i64,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValueRelRankRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes list item identified by index and returns removed data specified by returnType.
#[must_use]
pub fn remove_by_index(bin: &str, index: i64, return_type: ListReturnType) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByIndex as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes list items starting at specified index to the end of list and returns removed
/// data specified by returnType.
#[must_use]
pub fn remove_by_index_range(bin: &str, index: i64, return_type: ListReturnType) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByIndexRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes "count" list items starting at specified index and returns removed data specified
/// by returnType.
#[must_use]
pub fn remove_by_index_range_count(
    bin: &str,
    index: i64,
    count: i64,
    return_type: ListReturnType,
) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByIndexRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes list item identified by rank and returns removed data specified by returnType.
#[must_use]
pub fn remove_by_rank(bin: &str, rank: i64, return_type: ListReturnType) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByRank as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes list items starting at specified rank to the last ranked item and returns removed
/// data specified by returnType.
#[must_use]
pub fn remove_by_rank_range(bin: &str, rank: i64, return_type: ListReturnType) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByRankRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes "count" list items starting at specified rank and returns removed data specified
/// by returnType.
#[must_use]
pub fn remove_by_rank_range_count(
    bin: &str,
    rank: i64,
    count: i64,
    return_type: ListReturnType,
) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByRankRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list set operation. Server sets the item value at the specified index in the list
/// bin. Server does not return a result by default.
///
/// # Panics
/// Panics if value is empty
#[must_use]
pub fn set<'a>(bin: &'a str, index: i64, value: &'a Value) -> Operation<'a> {
    assert!(*value != Value::Nil);

    let cdt_op = CdtOperation {
        op: CdtListOpType::Set as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Int(index), CdtArgument::Value(value)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list trim operation. Server removes `count` items in the list bin that do not fall
/// into the range specified by `index` and `count`. If the range is out of bounds, then all
/// items will be removed. Server returns list size after trim.
#[must_use]
pub fn trim(bin: &str, index: i64, count: i64) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Trim as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list clear operation. Server removes all items in the list bin. Server does not
/// return a result by default.
#[must_use]
pub fn clear(bin: &str) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Clear as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list increment operation. Server increments the item value at the specified index by the
/// given amount and returns the final result.
#[must_use]
pub fn increment<'a>(policy: &ListPolicy, bin: &'a str, index: i64, value: i64) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Increment as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Int(index),
            CdtArgument::Int(value),
            CdtArgument::Byte(policy.flags.bits()),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list size operation. Server returns size of the list.
#[must_use]
pub fn size(bin: &str) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Size as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list get operation. Server returns the item at the specified index in the list bin.
#[must_use]
pub fn get(bin: &str, index: i64) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Get as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list get range operation. Server returns `count` items starting at the specified
/// index in the list bin.
#[must_use]
pub fn get_range(bin: &str, index: i64, count: i64) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list get range operation. Server returns items starting at the index to the end of
/// the list.
#[must_use]
pub fn get_range_from(bin: &str, index: i64) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by value operation.
/// Server selects list items identified by value and returns selected data specified by returnType.
#[must_use]
pub fn get_by_value<'a>(
    bin: &'a str,
    value: &'a Value,
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByValue as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
        ],
    };

    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates list get by value list operation.
/// Server selects list items identified by values and returns selected data specified by
/// returnType.
#[must_use]
pub fn get_by_value_list<'a>(
    bin: &'a str,
    values: &'a [Value],
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByValueList as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::List(values),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by value range operation.
/// Server selects list items identified by value range (valueBegin inclusive, valueEnd exclusive)
/// If valueBegin is null, the range is less than valueEnd.
/// If valueEnd is null, the range is greater than equal to valueBegin.
/// Server returns selected data specified by returnType.
#[must_use]
pub fn get_by_value_range<'a>(
    bin: &'a str,
    begin: &'a Value,
    end: &'a Value,
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByValueInterval as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(begin),
            CdtArgument::Value(end),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates list get by index operation.
/// Server selects list item identified by index and returns selected data specified by returnType
#[must_use]
pub fn get_by_index(bin: &str, index: i64, return_type: ListReturnType) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByIndex as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
        ],
    };

    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates list get by index range operation.
/// Server selects list items starting at specified index to the end of list and returns selected
/// data specified by returnType.
#[must_use]
pub fn get_by_index_range(bin: &str, index: i64, return_type: ListReturnType) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByIndexRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
        ],
    };

    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates list get by index range operation.
/// Server selects "count" list items starting at specified index and returns selected data
/// specified by returnType.
#[must_use]
pub fn get_by_index_range_count(
    bin: &str,
    index: i64,
    count: i64,
    return_type: ListReturnType,
) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByIndexRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
            CdtArgument::Int(count),
        ],
    };

    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by rank operation.
/// Server selects list item identified by rank and returns selected data specified by returnType.
#[must_use]
pub fn get_by_rank(bin: &str, rank: i64, return_type: ListReturnType) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByRank as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by rank range operation.
/// Server selects list items starting at specified rank to the last ranked item and returns
/// selected data specified by returnType.
#[must_use]
pub fn get_by_rank_range(bin: &str, rank: i64, return_type: ListReturnType) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByRankRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by rank range operation.
/// Server selects "count" list items starting at specified rank and returns selected data specified
/// by returnType.
#[must_use]
pub fn get_by_rank_range_count(
    bin: &str,
    rank: i64,
    count: i64,
    return_type: ListReturnType,
) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByRankRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by value relative to rank range operation.
/// Server selects list items nearest to value and greater by relative rank.
/// Server returns selected data specified by returnType.
///
/// Examples for ordered list \[0, 4, 5, 9, 11, 15\]:
/// ```text
/// (value,rank) = [selected items]
/// (5,0) = [5,9,11,15]
/// (5,1) = [9,11,15]
/// (5,-1) = [4,5,9,11,15]
/// (3,0) = [4,5,9,11,15]
/// (3,3) = [11,15]
/// (3,-3) = [0,4,5,9,11,15]
/// ```
#[must_use]
pub fn get_by_value_relative_rank_range<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByValueRelRankRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by value relative to rank range operation.
/// Server selects list items nearest to value and greater by relative rank with a count limit.
/// Server returns selected data specified by returnType.
///
/// Examples for ordered list \[0, 4, 5, 9, 11, 15\]:
/// ```text
/// (value,rank,count) = [selected items]
/// (5,0,2) = [5,9]
/// (5,1,1) = [9]
/// (5,-1,2) = [4,5]
/// (3,0,1) = [4]
/// (3,3,7) = [11,15]
/// (3,-3,2) = []
/// ```
#[must_use]
pub fn get_by_value_relative_rank_range_count<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    count: i64,
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByValueRelRankRange as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates list sort operation.
/// Server sorts list according to sortFlags.
/// Server does not return a result by default.
#[must_use]
pub fn sort(bin: &str, sort_flags: ListSortFlags) -> Operation<'_> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Sort as u8,
        encoder: OperationEncoder::Cdt,
        args: vec![CdtArgument::Byte(sort_flags.bits())],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}
