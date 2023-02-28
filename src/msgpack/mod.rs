// Copyright 2015-2018 Aerospike, Inc.
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

pub mod decoder;
pub mod encoder;

pub type Result<T, E = MsgpackError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum MsgpackError {
    #[error("Type header with code `{0}` not recognized")]
    UnrecognizedCode(u8),
    #[error("Error unpacking value of type `{0:x}`")]
    InvalidValueType(u8),
    #[error("Buffer error")]
    Buffer(#[from] crate::commands::buffer::BufferError),
}
