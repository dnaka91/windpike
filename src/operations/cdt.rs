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

use crate::{
    commands::{buffer::Buffer, ParticleType},
    operations::cdt_context::CdtContext,
    Value,
};

pub(crate) enum CdtArgument<'a> {
    Byte(u8),
    Int(i64),
    Bool(bool),
    Value(&'a Value),
    List(&'a [Value]),
    Map(&'a HashMap<Value, Value>),
}

pub(crate) type OperationEncoder = Box<
    dyn Fn(&mut Option<&mut Buffer>, &CdtOperation<'_>, &[CdtContext]) -> usize
        + Send
        + Sync
        + 'static,
>;

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
        let size: usize = (self.encoder)(&mut None, self, ctx);
        size
    }

    pub fn write_to(&self, buffer: &mut Buffer, ctx: &[CdtContext]) -> usize {
        let size: usize = (self.encoder)(&mut Some(buffer), self, ctx);
        size
    }
}
