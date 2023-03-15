//! Functions used to create database operations used in the client's `operate()` method.

pub mod bitwise;
#[doc(hidden)]
pub mod cdt;
pub mod cdt_context;
pub mod hll;
pub mod lists;
pub mod maps;
pub mod scalar;

use self::cdt::CdtOperation;
pub use self::{
    maps::{MapOrder, MapPolicy, MapReturnType, MapWriteMode},
    scalar::*,
};
use crate::{commands::ParticleType, msgpack, operations::cdt_context::CdtContext, Value};

#[derive(Clone, Copy)]
pub(crate) enum OperationType {
    Read = 1,
    Write,
    CdtRead,
    CdtWrite,
    Incr,
    // ExpRead = 7,
    // ExpWrite,
    Append = 9,
    Prepend,
    Touch,
    BitRead,
    BitWrite,
    Delete,
    HllRead,
    HllWrite,
}

pub(crate) enum OperationData<'a> {
    None,
    Value(&'a Value),
    CdtListOp(CdtOperation<'a>),
    CdtMapOp(CdtOperation<'a>),
    CdtBitOp(CdtOperation<'a>),
    HllOp(CdtOperation<'a>),
}

pub(crate) enum OperationBin<'a> {
    None,
    All,
    Name(&'a str),
}

/// Database operation definition. This data type is used in the client's `operate()` method.
pub struct Operation<'a> {
    // OpType determines type of operation.
    pub(crate) op: OperationType,
    // CDT context for nested types
    pub(crate) ctx: &'a [CdtContext],
    // BinName (Optional) determines the name of bin used in operation.
    pub(crate) bin: OperationBin<'a>,
    // BinData determines bin value used in operation.
    pub(crate) data: OperationData<'a>,
}

impl<'a> Operation<'a> {
    #[must_use]
    pub(crate) fn estimate_size(&self) -> usize {
        let mut size: usize = 0;
        size += match self.bin {
            OperationBin::Name(bin) => bin.len(),
            OperationBin::None | OperationBin::All => 0,
        };
        size += match &self.data {
            OperationData::None => 0,
            OperationData::Value(value) => value.estimate_size(),
            OperationData::CdtListOp(cdt_op)
            | OperationData::CdtMapOp(cdt_op)
            | OperationData::CdtBitOp(cdt_op)
            | OperationData::HllOp(cdt_op) => cdt_op.estimate_size(self.ctx),
        };

        size
    }

    pub(crate) fn write_to(&self, w: &mut impl msgpack::Write) -> usize {
        let mut size: usize = 0;

        // remove the header size from the estimate
        let op_size = self.estimate_size();

        size += w.write_u32(op_size as u32 + 4);
        size += w.write_u8(self.op as u8);

        match &self.data {
            OperationData::None => {
                size += self.write_op_header_to(w, ParticleType::Null as u8);
            }
            OperationData::Value(value) => {
                size += self.write_op_header_to(w, value.particle_type() as u8);
                size += value.write_to(w);
            }
            OperationData::CdtListOp(cdt_op)
            | OperationData::CdtMapOp(cdt_op)
            | OperationData::CdtBitOp(cdt_op)
            | OperationData::HllOp(cdt_op) => {
                size += self.write_op_header_to(w, CdtOperation::particle_type() as u8);
                size += cdt_op.write_to(w, self.ctx);
            }
        };

        size
    }

    fn write_op_header_to(&self, w: &mut impl msgpack::Write, particle_type: u8) -> usize {
        let mut size = w.write_u8(particle_type);
        size += w.write_u8(0);
        match self.bin {
            OperationBin::Name(bin) => {
                size += w.write_u8(bin.len() as u8);
                size += w.write_str(bin);
            }
            OperationBin::None | OperationBin::All => {
                size += w.write_u8(0);
            }
        }
        size
    }

    /// Set the context of the operation. Required for nested structures
    #[must_use]
    pub const fn set_context(mut self, ctx: &'a [CdtContext]) -> Operation<'a> {
        self.ctx = ctx;
        self
    }
}
