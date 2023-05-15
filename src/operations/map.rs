//! Unique key map bin operations. Create map operations used by the client's `operate()` method.
//!
//! All maps maintain an index and a rank. The index is the item offset from the start of the map,
//! for both unordered and ordered maps. The rank is the sorted index of the value component.
//! Map supports negative indexing for indexjkj and rank.
//!
//! The default unique key map is unordered.
//!
//! Index/Count examples:
//!
//! * Index 0: First item in map.
//! * Index 4: Fifth item in map.
//! * Index -1: Last item in map.
//! * Index -3: Third to last item in map.
//! * Index 1, Count 2: Second and third items in map.
//! * Index -3, Count 3: Last three items in map.
//! * Index -5, Count 4: Range between fifth to last item to second to last item inclusive.
//!
//! Rank examples:
//!
//! * Rank 0: Item with lowest value rank in map.
//! * Rank 4: Fifth lowest ranked item in map.
//! * Rank -1: Item with highest ranked value in map.
//! * Rank -3: Item with third highest ranked value in map.
//! * Rank 1 Count 2: Second and third lowest ranked items in map.
//! * Rank -3 Count 3: Top three ranked items in map.

use std::collections::HashMap;

use super::cdt::{self, Encoder};
use crate::{
    operations::{Operation, OperationBin, OperationData, OperationType},
    value::MapKey,
    Value,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpType {
    SetType = 64,
    Add,
    AddItems,
    Put,
    PutItems,
    Replace,
    ReplaceItems,
    Increment = 73,
    Decrement,
    Clear,
    RemoveByKey,
    RemoveByIndex,
    RemoveByRank = 79,
    RemoveKeyList = 81,
    RemoveByValue,
    RemoveValueList,
    RemoveByKeyInterval,
    RemoveByIndexRange,
    RemoveByValueInterval,
    RemoveByRankRange,
    RemoveByKeyRelIndexRange,
    RemoveByValueRelRankRange,
    Size = 96,
    GetByKey,
    GetByIndex,
    GetByRank = 100,
    GetByValue = 102,
    GetByKeyInterval,
    GetByIndexRange,
    GetByValueInterval,
    GetByRankRange,
    GetByKeyList,
    GetByValueList,
    GetByKeyRelIndexRange,
    GetByValueRelRankRange,
}
/// Map storage order.
#[derive(Clone, Copy, Debug)]
pub enum OrderType {
    /// Map is not ordered. This is the default.
    Unordered = 0,
    /// Order map by key.
    KeyOrdered,
    /// Order map by key, then value.
    KeyValueOrdered = 3,
}

/// Map return type. Type of data to return when selecting or removing items from the map.
#[derive(Clone, Copy, Debug)]
pub enum ReturnType {
    /// Do not return a result.
    None = 0,
    /// Return key index order.
    ///
    /// * 0 = first key
    /// * N = Nth key
    /// * -1 = last key
    Index,
    /// Return reverse key order.
    ///
    /// * 0 = last key
    /// * -1 = first key
    ReverseIndex,
    /// Return value order.
    ///
    /// * 0 = smallest value
    /// * N = Nth smallest value
    /// * -1 = largest value
    Rank,
    /// Return reserve value order.
    ///
    /// * 0 = largest value
    /// * N = Nth largest value
    /// * -1 = smallest value
    ReverseRank,
    /// Return count of items selected.
    Count,
    /// Return key for single key read and key list for range read.
    Key,
    /// Return value for single key read and value list for range read.
    Value,
    /// Return key/value items. The possible return types are:
    ///
    /// * `Value::HashMap`: Returned for unordered maps
    /// * `Value::OrderedMap`: Returned for range results where range order needs to be preserved.
    KeyValue,
    /// Invert meaning of map command and return values.
    /// With the INVERTED flag enabled, the keys outside of the specified key range will be removed
    /// and returned.
    Inverted = 0x10000,
}

/// Unique key map write type.
#[derive(Clone, Copy, Debug)]
pub enum WriteMode {
    /// If the key already exists, the item will be overwritten.
    /// If the key does not exist, a new item will be created.
    Update,
    /// If the key already exists, the item will be overwritten.
    /// If the key does not exist, the write will fail.
    UpdateOnly,
    /// If the key already exists, the write will fail.
    /// If the key does not exist, a new item will be created.
    CreateOnly,
}

/// `MapPolicy` directives when creating a map and writing map items.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// The Order of the Map
    pub order: OrderType,
    /// The Map Write Mode
    pub write_mode: WriteMode,
}

impl Policy {
    /// Create a new map policy given the ordering for the map and the write mode.
    #[must_use]
    pub const fn new(order: OrderType, write_mode: WriteMode) -> Self {
        Self { order, write_mode }
    }
}

impl Default for Policy {
    fn default() -> Self {
        Self::new(OrderType::Unordered, WriteMode::Update)
    }
}

/// Determines the correct operation to use when setting one or more map values, depending on the
/// map policy.
const fn map_write_op(policy: Policy, multi: bool) -> OpType {
    match policy.write_mode {
        WriteMode::Update => {
            if multi {
                OpType::PutItems
            } else {
                OpType::Put
            }
        }
        WriteMode::UpdateOnly => {
            if multi {
                OpType::ReplaceItems
            } else {
                OpType::Replace
            }
        }
        WriteMode::CreateOnly => {
            if multi {
                OpType::AddItems
            } else {
                OpType::Add
            }
        }
    }
}

const fn map_order_arg(policy: Policy) -> Option<cdt::Argument<'static>> {
    match policy.write_mode {
        WriteMode::UpdateOnly => None,
        _ => Some(cdt::Argument::Byte(policy.order as u8)),
    }
}

#[must_use]
pub(super) const fn order_flag(order: OrderType) -> u8 {
    match order {
        OrderType::KeyOrdered => 0x80,
        OrderType::Unordered => 0x40,
        OrderType::KeyValueOrdered => 0xc0,
    }
}

#[inline]
const fn write<'a>(bin: &'a str, op: OpType, args: Vec<cdt::Argument<'a>>) -> Operation<'a> {
    Operation {
        op: OperationType::CdtWrite,
        ctx: &[],
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt::Operation {
            op: op as u8,
            encoder: Encoder::Cdt,
            args,
        }),
    }
}

#[inline]
const fn read<'a>(bin: &'a str, op: OpType, args: Vec<cdt::Argument<'a>>) -> Operation<'a> {
    Operation {
        op: OperationType::CdtRead,
        ctx: &[],
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt::Operation {
            op: op as u8,
            encoder: Encoder::Cdt,
            args,
        }),
    }
}

/// Create set map policy operation. Server set the map policy attributes. Server does not
/// return a result.
///
/// The required map policy attributes can be changed after the map has been created.
#[must_use]
pub fn set_order(bin: &str, map_order: OrderType) -> Operation<'_> {
    write(
        bin,
        OpType::SetType,
        vec![cdt::Argument::Byte(map_order as u8)],
    )
}

/// Create map put operation. Server writes the key/value item to the map bin and returns the
/// map size.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
#[must_use]
pub fn put<'a>(policy: Policy, bin: &'a str, key: &'a Value, val: &'a Value) -> Operation<'a> {
    let mut args = vec![cdt::Argument::Value(key)];
    if *val != Value::Nil {
        args.push(cdt::Argument::Value(val));
    }
    if let Some(arg) = map_order_arg(policy) {
        args.push(arg);
    }

    write(bin, map_write_op(policy, false), args)
}

/// Create map put items operation. Server writes each map item to the map bin and returns the
/// map size.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
#[allow(clippy::implicit_hasher)]
#[must_use]
pub fn put_items<'a>(
    policy: Policy,
    bin: &'a str,
    items: &'a HashMap<MapKey, Value>,
) -> Operation<'a> {
    let mut args = vec![cdt::Argument::Map(items)];
    if let Some(arg) = map_order_arg(policy) {
        args.push(arg);
    }

    write(bin, map_write_op(policy, true), args)
}

/// Create map increment operation. Server increments values by `incr` for all items identified
/// by the key and returns the final result. Valid only for numbers.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
#[must_use]
pub fn increment_value<'a>(
    policy: Policy,
    bin: &'a str,
    key: &'a Value,
    incr: &'a Value,
) -> Operation<'a> {
    let mut args = vec![cdt::Argument::Value(key)];
    if *incr != Value::Nil {
        args.push(cdt::Argument::Value(incr));
    }
    if let Some(arg) = map_order_arg(policy) {
        args.push(arg);
    }

    write(bin, OpType::Increment, args)
}

/// Create map decrement operation. Server decrements values by `decr` for all items identified
/// by the key and returns the final result. Valid only for numbers.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
#[must_use]
pub fn decrement_value<'a>(
    policy: Policy,
    bin: &'a str,
    key: &'a Value,
    decr: &'a Value,
) -> Operation<'a> {
    let mut args = vec![cdt::Argument::Value(key)];
    if *decr != Value::Nil {
        args.push(cdt::Argument::Value(decr));
    }
    if let Some(arg) = map_order_arg(policy) {
        args.push(arg);
    }

    write(bin, OpType::Decrement, args)
}

/// Create map clear operation. Server removes all items in the map. Server does not return a
/// result.
#[must_use]
pub fn clear(bin: &str) -> Operation<'_> {
    write(bin, OpType::Clear, vec![])
}

/// Create map remove operation. Server removes the map item identified by the key and returns
/// the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_key<'a>(bin: &'a str, key: &'a Value, return_type: ReturnType) -> Operation<'a> {
    write(
        bin,
        OpType::RemoveByKey,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(key),
        ],
    )
}

/// Create map remove operation. Server removes map items identified by keys and returns
/// removed data specified by `return_type`.
#[must_use]
pub fn remove_by_key_list<'a>(
    bin: &'a str,
    keys: &'a [Value],
    return_type: ReturnType,
) -> Operation<'a> {
    write(
        bin,
        OpType::RemoveKeyList,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::List(keys),
        ],
    )
}

/// Create map remove operation. Server removes map items identified by the key range
/// (`begin` inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is less than
/// `end`. If `end` is `Value::Nil`, the range is greater than equal to `begin`. Server returns
/// removed data specified by `return_type`.
#[must_use]
pub fn remove_by_key_range<'a>(
    bin: &'a str,
    begin: &'a Value,
    end: &'a Value,
    return_type: ReturnType,
) -> Operation<'a> {
    let mut args = vec![
        cdt::Argument::Byte(return_type as u8),
        cdt::Argument::Value(begin),
    ];
    if *end != Value::Nil {
        args.push(cdt::Argument::Value(end));
    }

    write(bin, OpType::RemoveByKeyInterval, args)
}

/// Create map remove operation. Server removes the map items identified by value and returns
/// the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_value<'a>(
    bin: &'a str,
    value: &'a Value,
    return_type: ReturnType,
) -> Operation<'a> {
    write(
        bin,
        OpType::RemoveByValue,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(value),
        ],
    )
}

/// Create map remove operation. Server removes the map items identified by values and returns
/// the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_value_list<'a>(
    bin: &'a str,
    values: &'a [Value],
    return_type: ReturnType,
) -> Operation<'a> {
    write(
        bin,
        OpType::RemoveValueList,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::List(values),
        ],
    )
}

/// Create map remove operation. Server removes map items identified by value range (`begin`
/// inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is less than `end`. If
/// `end` is `Value::Nil`, the range is greater than equal to `begin`. Server returns the
/// removed data specified by `return_type`.
#[must_use]
pub fn remove_by_value_range<'a>(
    bin: &'a str,
    begin: &'a Value,
    end: &'a Value,
    return_type: ReturnType,
) -> Operation<'a> {
    let mut args = vec![
        cdt::Argument::Byte(return_type as u8),
        cdt::Argument::Value(begin),
    ];
    if *end != Value::Nil {
        args.push(cdt::Argument::Value(end));
    }

    write(bin, OpType::RemoveByValueInterval, args)
}

/// Create map remove operation. Server removes the map item identified by the index and return
/// the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_index(bin: &str, index: i64, return_type: ReturnType) -> Operation<'_> {
    write(
        bin,
        OpType::RemoveByIndex,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(index),
        ],
    )
}

/// Create map remove operation. Server removes `count` map items starting at the specified
/// index and returns the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_index_range(
    bin: &str,
    index: i64,
    count: i64,
    return_type: ReturnType,
) -> Operation<'_> {
    write(
        bin,
        OpType::RemoveByIndexRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(index),
            cdt::Argument::Int(count),
        ],
    )
}

/// Create map remove operation. Server removes the map items starting at the specified index
/// to the end of the map and returns the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_index_range_from(bin: &str, index: i64, return_type: ReturnType) -> Operation<'_> {
    write(
        bin,
        OpType::RemoveByIndexRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(index),
        ],
    )
}

/// Create map remove operation. Server removes the map item identified by rank and returns the
/// removed data specified by `return_type`.
#[must_use]
pub fn remove_by_rank(bin: &str, rank: i64, return_type: ReturnType) -> Operation<'_> {
    write(
        bin,
        OpType::RemoveByRank,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(rank),
        ],
    )
}

/// Create map remove operation. Server removes `count` map items starting at the specified
/// rank and returns the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_rank_range(
    bin: &str,
    rank: i64,
    count: i64,
    return_type: ReturnType,
) -> Operation<'_> {
    write(
        bin,
        OpType::RemoveByRankRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(rank),
            cdt::Argument::Int(count),
        ],
    )
}

/// Create map remove operation. Server removes the map items starting at the specified rank to
/// the last ranked item and returns the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_rank_range_from(bin: &str, rank: i64, return_type: ReturnType) -> Operation<'_> {
    write(
        bin,
        OpType::RemoveByRankRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(rank),
        ],
    )
}

/// Create map size operation. Server returns the size of the map.
#[must_use]
pub fn size(bin: &str) -> Operation<'_> {
    read(bin, OpType::Size, vec![])
}

/// Create map get by key operation. Server selects the map item identified by the key and
/// returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_key<'a>(bin: &'a str, key: &'a Value, return_type: ReturnType) -> Operation<'a> {
    read(
        bin,
        OpType::GetByKey,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(key),
        ],
    )
}

/// Create map get by key range operation. Server selects the map items identified by the key
/// range (`begin` inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is less
/// than `end`. If `end` is `Value::Nil` the range is greater than equal to `begin`. Server
/// returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_key_range<'a>(
    bin: &'a str,
    begin: &'a Value,
    end: &'a Value,
    return_type: ReturnType,
) -> Operation<'a> {
    let mut args = vec![
        cdt::Argument::Byte(return_type as u8),
        cdt::Argument::Value(begin),
    ];
    if *end != Value::Nil {
        args.push(cdt::Argument::Value(end));
    }

    read(bin, OpType::GetByKeyInterval, args)
}

/// Create map get by value operation. Server selects the map items identified by value and
/// returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_value<'a>(bin: &'a str, value: &'a Value, return_type: ReturnType) -> Operation<'a> {
    read(
        bin,
        OpType::GetByValue,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(value),
        ],
    )
}

/// Create map get by value range operation. Server selects the map items identified by the
/// value range (`begin` inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is
/// less than `end`. If `end` is `Value::Nil`, the range is greater than equal to `begin`.
/// Server returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_value_range<'a>(
    bin: &'a str,
    begin: &'a Value,
    end: &'a Value,
    return_type: ReturnType,
) -> Operation<'a> {
    let mut args = vec![
        cdt::Argument::Byte(return_type as u8),
        cdt::Argument::Value(begin),
    ];
    if *end != Value::Nil {
        args.push(cdt::Argument::Value(end));
    }

    read(bin, OpType::GetByValueInterval, args)
}

/// Create map get by index operation. Server selects the map item identified by index and
/// returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_index(bin: &str, index: i64, return_type: ReturnType) -> Operation<'_> {
    read(
        bin,
        OpType::GetByIndex,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(index),
        ],
    )
}

/// Create map get by index range operation. Server selects `count` map items starting at the
/// specified index and returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_index_range(
    bin: &str,
    index: i64,
    count: i64,
    return_type: ReturnType,
) -> Operation<'_> {
    read(
        bin,
        OpType::GetByIndexRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(index),
            cdt::Argument::Int(count),
        ],
    )
}

/// Create map get by index range operation. Server selects the map items starting at the
/// specified index to the end of the map and returns the selected data specified by
/// `return_type`.
#[must_use]
pub fn get_by_index_range_from(bin: &str, index: i64, return_type: ReturnType) -> Operation<'_> {
    read(
        bin,
        OpType::GetByIndexRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(index),
        ],
    )
}

/// Create map get by rank operation. Server selects the map item identified by rank and
/// returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_rank(bin: &str, rank: i64, return_type: ReturnType) -> Operation<'_> {
    read(
        bin,
        OpType::GetByRank,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(rank),
        ],
    )
}

/// Create map get rank range operation. Server selects `count` map items at the specified
/// rank and returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_rank_range(
    bin: &str,
    rank: i64,
    count: i64,
    return_type: ReturnType,
) -> Operation<'_> {
    read(
        bin,
        OpType::GetByRankRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(rank),
            cdt::Argument::Int(count),
        ],
    )
}

/// Create map get by rank range operation. Server selects the map items starting at the
/// specified rank to the last ranked item and returns the selected data specified by
/// `return_type`.
#[must_use]
pub fn get_by_rank_range_from(bin: &str, rank: i64, return_type: ReturnType) -> Operation<'_> {
    read(
        bin,
        OpType::GetByRankRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Int(rank),
        ],
    )
}

/// Creates a map remove by key relative to index range operation.
/// Server removes map items nearest to key and greater by index.
/// Server returns removed data specified by returnType.
///
/// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
///
/// (key,index) = [removed items]
/// (5,0) = [{5=15},{9=10}]
/// (5,1) = [{9=10}]
/// (5,-1) = [{4=2},{5=15},{9=10}]
/// (3,2) = [{9=10}]
/// (3,-2) = [{0=17},{4=2},{5=15},{9=10}]
#[must_use]
pub fn remove_by_key_relative_index_range<'a>(
    bin: &'a str,
    key: &'a Value,
    index: i64,
    return_type: ReturnType,
) -> Operation<'a> {
    write(
        bin,
        OpType::RemoveByKeyRelIndexRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(key),
            cdt::Argument::Int(index),
        ],
    )
}

/// Create map remove by key relative to index range operation.
/// Server removes map items nearest to key and greater by index with a count limit.
/// Server returns removed data specified by returnType.
///
/// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
///
/// (key,index,count) = [removed items]
/// (5,0,1) = [{5=15}]
/// (5,1,2) = [{9=10}]
/// (5,-1,1) = [{4=2}]
/// (3,2,1) = [{9=10}]
/// (3,-2,2) = [{0=17}]
#[must_use]
pub fn remove_by_key_relative_index_range_count<'a>(
    bin: &'a str,
    key: &'a Value,
    index: i64,
    count: i64,
    return_type: ReturnType,
) -> Operation<'a> {
    write(
        bin,
        OpType::RemoveByKeyRelIndexRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(key),
            cdt::Argument::Int(index),
            cdt::Argument::Int(count),
        ],
    )
}

/// reates a map remove by value relative to rank range operation.
/// Server removes map items nearest to value and greater by relative rank.
/// Server returns removed data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// (value,rank) = [removed items]
/// (11,1) = [{0=17}]
/// (11,-1) = [{9=10},{5=15},{0=17}]
#[must_use]
pub fn remove_by_value_relative_rank_range<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    return_type: ReturnType,
) -> Operation<'a> {
    write(
        bin,
        OpType::RemoveByValueRelRankRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(value),
            cdt::Argument::Int(rank),
        ],
    )
}

/// Creates a map remove by value relative to rank range operation.
/// Server removes map items nearest to value and greater by relative rank with a count limit.
/// Server returns removed data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// (value,rank,count) = [removed items]
/// (11,1,1) = [{0=17}]
/// (11,-1,1) = [{9=10}]
#[must_use]
pub fn remove_by_value_relative_rank_range_count<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    count: i64,
    return_type: ReturnType,
) -> Operation<'a> {
    write(
        bin,
        OpType::RemoveByValueRelRankRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(value),
            cdt::Argument::Int(rank),
            cdt::Argument::Int(count),
        ],
    )
}

/// Creates a map get by key list operation.
/// Server selects map items identified by keys and returns selected data specified by returnType.
#[must_use]
pub fn get_by_key_list<'a>(
    bin: &'a str,
    keys: &'a [Value],
    return_type: ReturnType,
) -> Operation<'a> {
    read(
        bin,
        OpType::GetByKeyList,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::List(keys),
        ],
    )
}

/// Creates a map get by value list operation.
/// Server selects map items identified by values and returns selected data specified by returnType.
#[must_use]
pub fn get_by_value_list<'a>(
    bin: &'a str,
    values: &'a [Value],
    return_type: ReturnType,
) -> Operation<'a> {
    read(
        bin,
        OpType::GetByValueList,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::List(values),
        ],
    )
}

/// Creates a map get by key relative to index range operation.
/// Server selects map items nearest to key and greater by index.
/// Server returns selected data specified by returnType.
///
/// Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
///
/// (key,index) = [selected items]
/// (5,0) = [{5=15},{9=10}]
/// (5,1) = [{9=10}]
/// (5,-1) = [{4=2},{5=15},{9=10}]
/// (3,2) = [{9=10}]
/// (3,-2) = [{0=17},{4=2},{5=15},{9=10}]
#[must_use]
pub fn get_by_key_relative_index_range<'a>(
    bin: &'a str,
    key: &'a Value,
    index: i64,
    return_type: ReturnType,
) -> Operation<'a> {
    read(
        bin,
        OpType::GetByKeyRelIndexRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(key),
            cdt::Argument::Int(index),
        ],
    )
}

/// Creates a map get by key relative to index range operation.
/// Server selects map items nearest to key and greater by index with a count limit.
/// Server returns selected data specified by returnType.
///
/// Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
///
/// (key,index,count) = [selected items]
/// (5,0,1) = [{5=15}]
/// (5,1,2) = [{9=10}]
/// (5,-1,1) = [{4=2}]
/// (3,2,1) = [{9=10}]
/// (3,-2,2) = [{0=17}]
#[must_use]
pub fn get_by_key_relative_index_range_count<'a>(
    bin: &'a str,
    key: &'a Value,
    index: i64,
    count: i64,
    return_type: ReturnType,
) -> Operation<'a> {
    read(
        bin,
        OpType::GetByKeyRelIndexRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(key),
            cdt::Argument::Int(index),
            cdt::Argument::Int(count),
        ],
    )
}

/// Creates a map get by value relative to rank range operation.
/// Server selects map items nearest to value and greater by relative rank.
/// Server returns selected data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// (value,rank) = [selected items]
/// (11,1) = [{0=17}]
/// (11,-1) = [{9=10},{5=15},{0=17}]
#[must_use]
pub fn get_by_value_relative_rank_range<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    return_type: ReturnType,
) -> Operation<'a> {
    read(
        bin,
        OpType::GetByValueRelRankRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(value),
            cdt::Argument::Int(rank),
        ],
    )
}

/// Creates a map get by value relative to rank range operation.
/// Server selects map items nearest to value and greater by relative rank with a count limit.
/// Server returns selected data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// (value,rank,count) = [selected items]
/// (11,1,1) = [{0=17}]
/// (11,-1,1) = [{9=10}]
#[must_use]
pub fn get_by_value_relative_rank_range_count<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    count: i64,
    return_type: ReturnType,
) -> Operation<'a> {
    read(
        bin,
        OpType::GetByValueRelRankRange,
        vec![
            cdt::Argument::Byte(return_type as u8),
            cdt::Argument::Value(value),
            cdt::Argument::Int(rank),
            cdt::Argument::Int(count),
        ],
    )
}
