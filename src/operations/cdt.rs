// Copyright 2015-2020 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use std::collections::HashMap;

use crate::{commands::ParticleType, msgpack, operations::cdt_context::CdtContext, Value};

pub(crate) enum CdtArgument<'a> {
    Byte(u8),
    Int(i64),
    Bool(bool),
    Value(&'a Value),
    List(&'a [Value]),
    Map(&'a HashMap<Value, Value>),
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
