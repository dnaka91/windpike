use std::collections::HashMap;

use crate::{
    commands::ParticleType, msgpack, operations::cdt_context::CdtContext, value::MapKey, Value,
};

pub(crate) enum CdtArgument<'a> {
    Byte(u8),
    Int(i64),
    Bool(bool),
    Value(&'a Value),
    List(&'a [Value]),
    Map(&'a HashMap<MapKey, Value>),
}

#[derive(Clone, Copy)]
pub(crate) enum OperationEncoder {
    Cdt,
    CdtBit,
    Hll,
}

impl OperationEncoder {
    pub fn encode(
        self,
        w: &mut impl msgpack::Write,
        op: &CdtOperation<'_>,
        ctx: &[CdtContext],
    ) -> usize {
        match self {
            Self::Cdt => msgpack::encoder::pack_cdt_op(w, op, ctx),
            Self::CdtBit => msgpack::encoder::pack_cdt_bit_op(w, op, ctx),
            Self::Hll => msgpack::encoder::pack_hll_op(w, op, ctx),
        }
    }
}

pub(crate) struct CdtOperation<'a> {
    pub op: u8,
    pub encoder: OperationEncoder,
    pub args: Vec<CdtArgument<'a>>,
}

impl<'a> CdtOperation<'a> {
    #[must_use]
    pub const fn particle_type() -> ParticleType {
        ParticleType::Blob
    }

    pub fn estimate_size(&self, ctx: &[CdtContext]) -> usize {
        self.encoder.encode(&mut msgpack::Sink, self, ctx)
    }

    pub fn write_to(&self, w: &mut impl msgpack::Write, ctx: &[CdtContext]) -> usize {
        self.encoder.encode(w, self, ctx)
    }
}
