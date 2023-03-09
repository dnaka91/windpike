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

use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    result::Result as StdResult,
    vec::Vec,
};

use ripemd::{Digest, Ripemd160};
#[cfg(feature = "serialization")]
use serde::ser::{SerializeMap, SerializeSeq};
#[cfg(feature = "serialization")]
use serde::{Serialize, Serializer};

use crate::{
    commands::{
        buffer::{Buffer, BufferError},
        particle_type::ParseParticleError,
        ParticleType,
    },
    errors::Result,
    msgpack::{decoder, encoder, MsgpackError},
};

/// Container for floating point bin values stored in the Aerospike database.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FloatValue {
    /// Container for single precision float values.
    F32(u32),
    /// Container for double precision float values.
    F64(u64),
}

impl From<f64> for FloatValue {
    fn from(val: f64) -> Self {
        let mut val = val;
        if val.is_nan() {
            val = f64::NAN;
        } // make all NaNs have the same representation
        Self::F64(val.to_bits())
    }
}

impl<'a> From<&'a f64> for FloatValue {
    fn from(val: &f64) -> Self {
        let mut val = *val;
        if val.is_nan() {
            val = f64::NAN;
        } // make all NaNs have the same representation
        Self::F64(val.to_bits())
    }
}

impl From<f32> for FloatValue {
    fn from(val: f32) -> Self {
        let mut val = val;
        if val.is_nan() {
            val = f32::NAN;
        } // make all NaNs have the same representation
        Self::F32(val.to_bits())
    }
}

impl<'a> From<&'a f32> for FloatValue {
    fn from(val: &f32) -> Self {
        let mut val = *val;
        if val.is_nan() {
            val = f32::NAN;
        } // make all NaNs have the same representation
        Self::F32(val.to_bits())
    }
}

impl fmt::Display for FloatValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> StdResult<(), fmt::Error> {
        match *self {
            Self::F32(val) => {
                let val: f32 = f32::from_bits(val);
                write!(f, "{val}")
            }
            Self::F64(val) => {
                let val: f64 = f64::from_bits(val);
                write!(f, "{val}")
            }
        }
    }
}

/// Container for bin values stored in the Aerospike database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Empty value.
    Nil,

    /// Boolean value.
    Bool(bool),

    /// Integer value. All integers are represented as 64-bit numerics in Aerospike.
    Int(i64),

    /// Unsigned integer value. The largest integer value that can be stored in a record bin is
    /// `i64::max_value()`; however the list and map data types can store integer values (and keys)
    /// up to `u64::max_value()`.
    ///
    /// # Panics
    ///
    /// Attempting to store an `u64` value as a record bin value will cause a panic. Use casting to
    /// store and retrieve `u64` values.
    UInt(u64),

    /// Floating point value. All floating point values are stored in 64-bit IEEE-754 format in
    /// Aerospike. Aerospike server v3.6.0 and later support double data type.
    Float(FloatValue),

    /// String value.
    String(String),

    /// Byte array value.
    Blob(Vec<u8>),

    /// List data type is an ordered collection of values. Lists can contain values of any
    /// supported data type. List data order is maintained on writes and reads.
    List(Vec<Value>),

    /// Map data type is a collection of key-value pairs. Each key can only appear once in a
    /// collection and is associated with a value. Map keys and values can be any supported data
    /// type.
    HashMap(HashMap<Value, Value>),

    /// Map data type where the map entries are sorted based key ordering (K-ordered maps) and may
    /// have an additional value-order index depending the namespace configuration (KV-ordered
    /// maps).
    OrderedMap(Vec<(Value, Value)>),

    /// GeoJSON data type are JSON formatted strings to encode geospatial information.
    GeoJSON(String),

    /// HLL value
    HLL(Vec<u8>),
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match *self {
            Self::Nil => {
                Option::<u8>::None.hash(state);
            }
            Self::Bool(ref val) => val.hash(state),
            Self::Int(ref val) => val.hash(state),
            Self::UInt(ref val) => val.hash(state),
            Self::Float(ref val) => val.hash(state),
            Self::String(ref val) | Self::GeoJSON(ref val) => val.hash(state),
            Self::Blob(ref val) | Self::HLL(ref val) => val.hash(state),
            Self::List(ref val) => val.hash(state),
            Self::HashMap(_) => panic!("HashMaps cannot be used as map keys."),
            Self::OrderedMap(_) => panic!("OrderedMaps cannot be used as map keys."),
        }
    }
}

impl Value {
    /// Returns true if this value is the empty value (nil).
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        matches!(*self, Self::Nil)
    }

    /// Return the particle type for the value used in the wire protocol.
    /// For internal use only.
    #[doc(hidden)]
    #[must_use]
    pub fn particle_type(&self) -> ParticleType {
        match *self {
            Self::Nil => ParticleType::NULL,
            Self::Int(_) | Self::Bool(_) => ParticleType::INTEGER,
            Self::UInt(_) => panic!(
                "Aerospike does not support u64 natively on server-side. Use casting to store and \
                 retrieve u64 values."
            ),
            Self::Float(_) => ParticleType::FLOAT,
            Self::String(_) => ParticleType::STRING,
            Self::Blob(_) => ParticleType::BLOB,
            Self::List(_) => ParticleType::LIST,
            Self::HashMap(_) => ParticleType::MAP,
            Self::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            Self::GeoJSON(_) => ParticleType::GEOJSON,
            Self::HLL(_) => ParticleType::HLL,
        }
    }

    /// Returns a string representation of the value.
    #[must_use]
    pub fn to_string(&self) -> String {
        match *self {
            Self::Nil => "<null>".to_string(),
            Self::Int(ref val) => val.to_string(),
            Self::UInt(ref val) => val.to_string(),
            Self::Bool(ref val) => val.to_string(),
            Self::Float(ref val) => val.to_string(),
            Self::String(ref val) | Self::GeoJSON(ref val) => val.to_string(),
            Self::Blob(ref val) | Self::HLL(ref val) => format!("{val:?}"),
            Self::List(ref val) => format!("{val:?}"),
            Self::HashMap(ref val) => format!("{val:?}"),
            Self::OrderedMap(ref val) => format!("{val:?}"),
        }
    }

    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(val) => Some(*val),
            _ => None,
        }
    }

    /// Calculate the size in bytes that the representation on wire for this value will require.
    /// For internal use only.
    #[doc(hidden)]
    pub fn estimate_size(&self) -> usize {
        match *self {
            Self::Nil => 0,
            Self::Int(_) | Self::Bool(_) | Self::Float(_) => 8,
            Self::UInt(_) => panic!(
                "Aerospike does not support u64 natively on server-side. Use casting to store and \
                 retrieve u64 values."
            ),
            Self::String(ref s) => s.len(),
            Self::Blob(ref b) => b.len(),
            Self::List(_) | Self::HashMap(_) => encoder::pack_value(&mut None, self),
            Self::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            Self::GeoJSON(ref s) => 1 + 2 + s.len(), // flags + ncells + jsonstr
            Self::HLL(ref h) => h.len(),
        }
    }

    /// Serialize the value into the given buffer.
    /// For internal use only.
    #[doc(hidden)]
    pub fn write_to(&self, buf: &mut Buffer) -> usize {
        match *self {
            Self::Nil => 0,
            Self::Int(ref val) => buf.write_i64(*val),
            Self::UInt(_) => panic!(
                "Aerospike does not support u64 natively on server-side. Use casting to store and \
                 retrieve u64 values."
            ),
            Self::Bool(ref val) => buf.write_bool(*val),
            Self::Float(ref val) => buf.write_f64(match *val {
                FloatValue::F32(val) => f64::from(f32::from_bits(val)),
                FloatValue::F64(val) => f64::from_bits(val),
            }),
            Self::String(ref val) => buf.write_str(val),
            Self::Blob(ref val) | Self::HLL(ref val) => buf.write_bytes(val),
            Self::List(_) | Self::HashMap(_) => encoder::pack_value(&mut Some(buf), self),
            Self::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            Self::GeoJSON(ref val) => buf.write_geo(val),
        }
    }

    /// Serialize the value as a record key.
    /// For internal use only.
    #[doc(hidden)]
    pub fn write_key_bytes(&self, h: &mut Ripemd160) -> Result<()> {
        match *self {
            Self::Int(ref val) => {
                h.update(val.to_be_bytes());
                Ok(())
            }
            Self::String(ref val) => {
                h.update(val.as_bytes());
                Ok(())
            }
            Self::Blob(ref val) => {
                h.update(val);
                Ok(())
            }
            _ => panic!("Data type is not supported as Key value."),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> StdResult<(), fmt::Error> {
        write!(f, "{}", self.to_string())
    }
}

impl From<String> for Value {
    fn from(val: String) -> Self {
        Self::String(val)
    }
}

impl From<Vec<u8>> for Value {
    fn from(val: Vec<u8>) -> Self {
        Self::Blob(val)
    }
}

impl From<Vec<Self>> for Value {
    fn from(val: Vec<Self>) -> Self {
        Self::List(val)
    }
}

impl From<HashMap<Self, Self>> for Value {
    fn from(val: HashMap<Self, Self>) -> Self {
        Self::HashMap(val)
    }
}

impl From<f32> for Value {
    fn from(val: f32) -> Self {
        Self::Float(FloatValue::from(val))
    }
}

impl From<f64> for Value {
    fn from(val: f64) -> Self {
        Self::Float(FloatValue::from(val))
    }
}

impl<'a> From<&'a f32> for Value {
    fn from(val: &'a f32) -> Self {
        Self::Float(FloatValue::from(*val))
    }
}

impl<'a> From<&'a f64> for Value {
    fn from(val: &'a f64) -> Self {
        Self::Float(FloatValue::from(*val))
    }
}

impl<'a> From<&'a String> for Value {
    fn from(val: &'a String) -> Self {
        Self::String(val.clone())
    }
}

impl<'a> From<&'a str> for Value {
    fn from(val: &'a str) -> Self {
        Self::String(val.to_string())
    }
}

impl<'a> From<&'a Vec<u8>> for Value {
    fn from(val: &'a Vec<u8>) -> Self {
        Self::Blob(val.clone())
    }
}

impl<'a> From<&'a [u8]> for Value {
    fn from(val: &'a [u8]) -> Self {
        Self::Blob(val.to_vec())
    }
}

impl From<bool> for Value {
    fn from(val: bool) -> Self {
        Self::Bool(val)
    }
}

impl From<i8> for Value {
    fn from(val: i8) -> Self {
        Self::Int(i64::from(val))
    }
}

impl From<u8> for Value {
    fn from(val: u8) -> Self {
        Self::Int(i64::from(val))
    }
}

impl From<i16> for Value {
    fn from(val: i16) -> Self {
        Self::Int(i64::from(val))
    }
}

impl From<u16> for Value {
    fn from(val: u16) -> Self {
        Self::Int(i64::from(val))
    }
}

impl From<i32> for Value {
    fn from(val: i32) -> Self {
        Self::Int(i64::from(val))
    }
}

impl From<u32> for Value {
    fn from(val: u32) -> Self {
        Self::Int(i64::from(val))
    }
}

impl From<i64> for Value {
    fn from(val: i64) -> Self {
        Self::Int(val)
    }
}

impl From<u64> for Value {
    fn from(val: u64) -> Self {
        Self::UInt(val)
    }
}

impl From<isize> for Value {
    fn from(val: isize) -> Self {
        Self::Int(val as i64)
    }
}

impl From<usize> for Value {
    fn from(val: usize) -> Self {
        Self::UInt(val as u64)
    }
}

impl<'a> From<&'a i8> for Value {
    fn from(val: &'a i8) -> Self {
        Self::Int(i64::from(*val))
    }
}

impl<'a> From<&'a u8> for Value {
    fn from(val: &'a u8) -> Self {
        Self::Int(i64::from(*val))
    }
}

impl<'a> From<&'a i16> for Value {
    fn from(val: &'a i16) -> Self {
        Self::Int(i64::from(*val))
    }
}

impl<'a> From<&'a u16> for Value {
    fn from(val: &'a u16) -> Self {
        Self::Int(i64::from(*val))
    }
}

impl<'a> From<&'a i32> for Value {
    fn from(val: &'a i32) -> Self {
        Self::Int(i64::from(*val))
    }
}

impl<'a> From<&'a u32> for Value {
    fn from(val: &'a u32) -> Self {
        Self::Int(i64::from(*val))
    }
}

impl<'a> From<&'a i64> for Value {
    fn from(val: &'a i64) -> Self {
        Self::Int(*val)
    }
}

impl<'a> From<&'a u64> for Value {
    fn from(val: &'a u64) -> Self {
        Self::UInt(*val)
    }
}

impl<'a> From<&'a isize> for Value {
    fn from(val: &'a isize) -> Self {
        Self::Int(*val as i64)
    }
}

impl<'a> From<&'a usize> for Value {
    fn from(val: &'a usize) -> Self {
        Self::UInt(*val as u64)
    }
}

impl<'a> From<&'a bool> for Value {
    fn from(val: &'a bool) -> Self {
        Self::Bool(*val)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParticleError {
    #[error("Particle type not recognized")]
    UnrecognizedParticle(#[from] ParseParticleError),
    #[error("Buffer error")]
    Buffer(#[from] BufferError),
    #[error("MessagePack error")]
    Msgpack(#[from] MsgpackError),
}

#[doc(hidden)]
pub fn bytes_to_particle(ptype: u8, buf: &mut Buffer, len: usize) -> Result<Value, ParticleError> {
    match ParticleType::try_from(ptype)? {
        ParticleType::NULL => Ok(Value::Nil),
        ParticleType::INTEGER => Ok(Value::Int(buf.read_i64(None))),
        ParticleType::FLOAT => Ok(Value::Float(buf.read_f64(None).into())),
        ParticleType::STRING => Ok(Value::String(buf.read_str(len)?)),
        ParticleType::GEOJSON => {
            buf.skip(1);
            let ncells = buf.read_u16(None) as usize;
            let header_size = ncells * 8;

            buf.skip(header_size);
            let val = buf.read_str(len - header_size - 3)?;
            Ok(Value::GeoJSON(val))
        }
        ParticleType::BLOB => Ok(Value::Blob(buf.read_blob(len))),
        ParticleType::LIST => Ok(decoder::unpack_value_list(buf)?),
        ParticleType::MAP => Ok(decoder::unpack_value_map(buf)?),
        ParticleType::DIGEST => Ok(Value::from("A DIGEST, NOT IMPLEMENTED YET!")),
        ParticleType::LDT => Ok(Value::from("A LDT, NOT IMPLEMENTED YET!")),
        ParticleType::HLL => Ok(Value::HLL(buf.read_blob(len))),
    }
}

/// Constructs a new Value from one of the supported native data types.
#[macro_export]
macro_rules! as_val {
    ($val:expr) => {{
        $crate::Value::from($val)
    }};
}

/// Constructs a new `GeoJSON` Value from one of the supported native data types.
#[macro_export]
macro_rules! as_geo {
    ($val:expr) => {{
        $crate::Value::GeoJSON($val.to_owned())
    }};
}

/// Constructs a new Blob Value from one of the supported native data types.
#[macro_export]
macro_rules! as_blob {
    ($val:expr) => {{
        $crate::Value::Blob($val)
    }};
}

/// Constructs a new List Value from a list of one or more native data types.
///
/// # Examples
///
/// Write a list value to a record bin.
///
/// ```rust
/// use aerospike::{as_bin, as_list, as_val, Client, ClientPolicy, Key, WritePolicy};
///
/// #[tokio::main]
/// async fn main() {
///     let client = Client::new(&ClientPolicy::default(), &"localhost:3000")
///         .await
///         .unwrap();
///
///     let key = Key::new("test", "test", "mykey").unwrap();
///     let list = as_list!("a", "b", "c");
///     let bin = as_bin!("list", list);
///     client
///         .put(&WritePolicy::default(), &key, &vec![bin])
///         .await
///         .unwrap();
/// }
/// ```
#[macro_export]
macro_rules! as_list {
    ( $( $v:expr),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push(as_val!($v));
            )*
            $crate::Value::List(temp_vec)
        }
    };
}

/// Constructs a vector of Values from a list of one or more native data types.
#[macro_export]
macro_rules! as_values {
    ( $( $v:expr),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push(as_val!($v));
            )*
            temp_vec
        }
    };
}

/// Constructs a Map Value from a list of key/value pairs.
///
/// # Examples
///
/// Write a map value to a record bin.
///
/// ```rust
/// use aerospike::{as_bin, Key, as_map, as_val, Client, ClientPolicy, WritePolicy};

/// #[tokio::main]
/// async fn main() {
///     let client = Client::new(&ClientPolicy::default(), &"localhost:3000")
///         .await
///         .unwrap();
///
///     let key = Key::new("test", "test", "mykey").unwrap();
///     let map = as_map!("a" => 1, "b" => 2);
///     let bin = as_bin!("map", map);
///     client
///         .put(&WritePolicy::default(), &key, &vec![bin])
///         .await
///         .unwrap();
/// }
/// ```
#[macro_export]
macro_rules! as_map {
    ( $( $k:expr => $v:expr),* ) => {
        {
            let mut temp_map = std::collections::HashMap::new();
            $(
                temp_map.insert(as_val!($k), as_val!($v));
            )*
            $crate::Value::HashMap(temp_map)
        }
    };
}

#[cfg(feature = "serialization")]
impl Serialize for Value {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> std::result::Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match &self {
            Self::Nil => serializer.serialize_none(),
            Self::Bool(b) => serializer.serialize_bool(*b),
            Self::Int(i) => serializer.serialize_i64(*i),
            Self::UInt(u) => serializer.serialize_u64(*u),
            Self::Float(f) => match f {
                FloatValue::F32(u) => serializer.serialize_f32(f32::from_bits(*u)),
                FloatValue::F64(u) => serializer.serialize_f64(f64::from_bits(*u)),
            },
            Self::String(s) | Self::GeoJSON(s) => serializer.serialize_str(s),
            Self::Blob(b) | Self::HLL(b) => serializer.serialize_bytes(&b[..]),
            Self::List(l) => {
                let mut seq = serializer.serialize_seq(Some(l.len()))?;
                for elem in l {
                    seq.serialize_element(&elem)?;
                }
                seq.end()
            }
            Self::HashMap(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (key, value) in m {
                    map.serialize_entry(&key, &value)?;
                }
                map.end()
            }
            Self::OrderedMap(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (key, value) in m {
                    map.serialize_entry(&key, &value)?;
                }
                map.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Value;

    #[test]
    fn as_string() {
        assert_eq!(Value::Nil.to_string(), String::from("<null>"));
        assert_eq!(Value::Int(42).to_string(), String::from("42"));
        assert_eq!(
            Value::UInt(9_223_372_036_854_775_808).to_string(),
            String::from("9223372036854775808")
        );
        assert_eq!(Value::Bool(true).to_string(), String::from("true"));
        assert_eq!(Value::from(4.1416).to_string(), String::from("4.1416"));
        assert_eq!(
            as_geo!(r#"{"type":"Point"}"#).to_string(),
            String::from(r#"{"type":"Point"}"#)
        );
    }

    #[test]
    fn as_geo() {
        let string = String::from(r#"{"type":"Point"}"#);
        let str = r#"{"type":"Point"}"#;
        assert_eq!(as_geo!(string), as_geo!(str));
    }

    #[test]
    #[cfg(feature = "serialization")]
    fn serializer() {
        let val: Value = as_list!("0", 9, 8, 7, 1, 2.1f64, -1, as_list!(5, 6, 7, 8, "asd"));
        let json = serde_json::to_string(&val);
        assert_eq!(
            json.unwrap(),
            "[\"0\",9,8,7,1,2.1,-1,[5,6,7,8,\"asd\"]]",
            "List Serialization failed"
        );

        let val: Value =
            as_map!("a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5, "f" => as_map!("test"=>123));
        let json = serde_json::to_string(&val);
        // We only check for the len of the String because HashMap serialization does not keep the
        // key order. Comparing like the list above is not possible.
        assert_eq!(json.unwrap().len(), 48, "Map Serialization failed");
    }
}
