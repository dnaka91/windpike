use std::collections::HashMap;

use super::{list, map};
use crate::{commands::ParticleType, msgpack, value::MapKey, Value};

pub(crate) enum Argument<'a> {
    Byte(u8),
    Int(i64),
    Bool(bool),
    Value(&'a Value),
    List(&'a [Value]),
    Map(&'a HashMap<MapKey, Value>),
}

#[derive(Clone, Copy)]
pub(super) enum Encoder {
    Cdt,
    CdtBit,
    Hll,
}

impl Encoder {
    pub fn encode(self, w: &mut impl msgpack::Write, op: &Operation<'_>, ctx: &[Context]) -> usize {
        match self {
            Self::Cdt => msgpack::encoder::pack_cdt_op(w, op, ctx),
            Self::CdtBit => msgpack::encoder::pack_cdt_bit_op(w, op, ctx),
            Self::Hll => msgpack::encoder::pack_hll_op(w, op, ctx),
        }
    }
}

pub(crate) struct Operation<'a> {
    pub op: u8,
    pub(super) encoder: Encoder,
    pub args: Vec<Argument<'a>>,
}

impl<'a> Operation<'a> {
    #[must_use]
    pub const fn particle_type() -> ParticleType {
        ParticleType::Blob
    }

    pub fn estimate_size(&self, ctx: &[Context]) -> usize {
        self.encoder.encode(&mut msgpack::Sink, self, ctx)
    }

    pub fn write_to(&self, w: &mut impl msgpack::Write, ctx: &[Context]) -> usize {
        self.encoder.encode(w, self, ctx)
    }
}

enum CtxType {
    ListIndex = 0x10,
    ListRank = 0x11,
    ListValue = 0x13,
    MapIndex = 0x20,
    MapRank = 0x21,
    MapKey = 0x22,
    MapValue = 0x23,
}

/// `CdtContext` defines Nested CDT context. Identifies the location of nested list/map to apply the
/// operation. for the current level.
/// An array of CTX identifies location of the list/map on multiple
/// levels on nesting.
#[derive(Clone, Debug)]
pub struct Context {
    /// Context Type
    pub id: u8,
    /// Flags
    pub flags: u8,
    /// Context Value
    pub value: Value,
}

impl Context {
    /// Defines Lookup list by index offset.
    /// If the index is negative, the resolved index starts backwards from end of list.
    /// If an index is out of bounds, a parameter error will be returned.
    /// Examples:
    /// 0: First item.
    /// 4: Fifth item.
    /// -1: Last item.
    /// -3: Third to last item.
    #[must_use]
    pub const fn list_index(index: i64) -> Self {
        Self {
            id: CtxType::ListIndex as u8,
            flags: 0,
            value: Value::Int(index),
        }
    }

    /// list with given type at index offset, given an order and pad.
    #[must_use]
    pub const fn list_index_create(index: i64, order: list::OrderType, pad: bool) -> Self {
        Self {
            id: CtxType::ListIndex as u8,
            flags: list::order_flag(order, pad),
            value: Value::Int(index),
        }
    }

    /// Defines Lookup list by rank.
    /// 0 = smallest value
    /// N = Nth smallest value
    /// -1 = largest value
    #[must_use]
    pub const fn list_rank(rank: i64) -> Self {
        Self {
            id: CtxType::ListRank as u8,
            flags: 0,
            value: Value::Int(rank),
        }
    }

    /// Defines Lookup list by value.
    #[must_use]
    pub const fn list_value(key: Value) -> Self {
        Self {
            id: CtxType::ListValue as u8,
            flags: 0,
            value: key,
        }
    }

    /// Defines Lookup map by index offset.
    /// If the index is negative, the resolved index starts backwards from end of list.
    /// If an index is out of bounds, a parameter error will be returned.
    /// Examples:
    /// 0: First item.
    /// 4: Fifth item.
    /// -1: Last item.
    /// -3: Third to last item.
    #[must_use]
    pub const fn map_index(key: Value) -> Self {
        Self {
            id: CtxType::MapIndex as u8,
            flags: 0,
            value: key,
        }
    }

    /// Defines Lookup map by rank.
    /// 0 = smallest value
    /// N = Nth smallest value
    /// -1 = largest value
    #[must_use]
    pub const fn map_rank(rank: i64) -> Self {
        Self {
            id: CtxType::MapRank as u8,
            flags: 0,
            value: Value::Int(rank),
        }
    }

    /// Defines Lookup map by key.
    #[must_use]
    pub const fn map_key(key: Value) -> Self {
        Self {
            id: CtxType::MapKey as u8,
            flags: 0,
            value: key,
        }
    }

    /// Create map with given type at map key.
    #[must_use]
    pub const fn map_key_create(key: Value, order: map::OrderType) -> Self {
        Self {
            id: CtxType::MapKey as u8,
            flags: map::order_flag(order),
            value: key,
        }
    }

    /// Defines Lookup map by value.
    #[must_use]
    pub const fn map_value(key: Value) -> Self {
        Self {
            id: CtxType::MapValue as u8,
            flags: 0,
            value: key,
        }
    }
}
