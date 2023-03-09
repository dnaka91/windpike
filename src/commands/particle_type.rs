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

#[derive(Debug, Clone)]
#[repr(u8)]
pub(crate) enum ParticleType {
    // Server particle types. Unsupported types are commented out.
    NULL = 0,
    INTEGER = 1,
    FLOAT = 2,
    STRING = 3,
    BLOB = 4,
    // TIMESTAMP       = 5,
    DIGEST = 6,
    // JBLOB  = 7,
    // CSHARP_BLOB     = 8,
    // PYTHON_BLOB     = 9,
    // RUBY_BLOB       = 10,
    // PHP_BLOB        = 11,
    // ERLANG_BLOB     = 12,
    // SEGMENT_POINTER = 13,
    // RTA_LIST        = 14,
    // RTA_DICT        = 15,
    // RTA_APPEND_DICT = 16,
    // RTA_APPEND_LIST = 17,
    // LUA_BLOB        = 18,
    HLL = 18,
    MAP = 19,
    LIST = 20,
    LDT = 21,
    GEOJSON = 23,
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid particle type `{0}`")]
pub struct ParseParticleError(u8);

impl TryFrom<u8> for ParticleType {
    type Error = ParseParticleError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::NULL,
            1 => Self::INTEGER,
            2 => Self::FLOAT,
            3 => Self::STRING,
            4 => Self::BLOB,
            // 5 => ParticleType::TIMESTAMP      ,
            6 => Self::DIGEST,
            // 7 => ParticleType::JBLOB ,
            // 8 => ParticleType::CSHARP_BLOB    ,
            // 9 => ParticleType::PYTHON_BLOB    ,
            // 10 => ParticleType::RUBY_BLOB      ,
            // 11 => ParticleType::PHP_BLOB       ,
            // 12 => ParticleType::ERLANG_BLOB    ,
            // 13 => ParticleType::SEGMENT_POINTER,
            // 14 => ParticleType::RTA_LIST       ,
            // 15 => ParticleType::RTA_DICT       ,
            // 16 => ParticleType::RTA_APPEND_DICT,
            // 17 => ParticleType::RTA_APPEND_LIST,
            // 18 => ParticleType::LUA_BLOB       ,
            18 => Self::HLL,
            19 => Self::MAP,
            20 => Self::LIST,
            21 => Self::LDT,
            23 => Self::GEOJSON,
            _ => return Err(ParseParticleError(value)),
        })
    }
}
