use std::{collections::HashMap, fmt, result::Result as StdResult, vec::Vec};

pub use ordered_float::OrderedFloat;

use crate::{
    commands::{
        buffer::{Buffer, BufferError},
        ParseParticleError, ParticleType,
    },
    errors::Result,
    msgpack::{self, decoder, encoder, MsgpackError},
};

macro_rules! from {
    ($to:ty, $variant:ident, $($from:ty),+) => {
        $(impl From<$from> for $to {
            fn from(value: $from) -> Self {
                Self::$variant(value.into())
            }
        })+
    };
}

/// Container for floating point bin values stored in the Aerospike database.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FloatValue {
    /// Container for single precision float values.
    F32(OrderedFloat<f32>),
    /// Container for double precision float values.
    F64(OrderedFloat<f64>),
}

impl FloatValue {
    #[inline]
    #[must_use]
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Self::F32(value) => Some(value.0),
            Self::F64(_) => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::F32(_) => None,
            Self::F64(value) => Some(value.0),
        }
    }
}

from!(FloatValue, F32, f32);
from!(FloatValue, F64, f64);

impl fmt::Display for FloatValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::F32(value) => value.fmt(f),
            Self::F64(value) => value.fmt(f),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum MapKey {
    Int(i64),
    Uint(u64),
    Float(FloatValue),
    String(String),
}

impl MapKey {
    #[inline]
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Uint(value) => Some(*value),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Self::Float(value) => value.as_f32(),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(value) => value.as_f64(),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }
}

from!(MapKey, Int, i8, i16, i32, i64);
from!(MapKey, Uint, u8, u16, u32, u64);
from!(MapKey, Float, f32, f64);
from!(MapKey, String, &str, String);

impl From<isize> for MapKey {
    fn from(value: isize) -> Self {
        Self::Int(value as i64)
    }
}

impl From<usize> for MapKey {
    fn from(value: usize) -> Self {
        Self::Uint(value as u64)
    }
}

/// Container for bin values stored in the Aerospike database.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    Uint(u64),
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
    HashMap(HashMap<MapKey, Value>),
    /// GeoJSON data type are JSON formatted strings to encode geospatial information.
    GeoJson(String),
    /// HLL value
    Hll(Vec<u8>),
}

impl Value {
    /// Return the particle type for the value used in the wire protocol.
    /// For internal use only.
    #[must_use]
    pub(crate) fn particle_type(&self) -> ParticleType {
        match *self {
            Self::Nil => ParticleType::Null,
            Self::Bool(_) => ParticleType::Bool,
            Self::Int(_) => ParticleType::Integer,
            Self::Uint(_) => panic!(
                "Aerospike does not support u64 natively on server-side. Use casting to store and \
                 retrieve u64 values."
            ),
            Self::Float(_) => ParticleType::Float,
            Self::String(_) => ParticleType::String,
            Self::Blob(_) => ParticleType::Blob,
            Self::List(_) => ParticleType::List,
            Self::HashMap(_) => ParticleType::Map,
            Self::GeoJson(_) => ParticleType::GeoJson,
            Self::Hll(_) => ParticleType::Hll,
        }
    }

    #[inline]
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub const fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Uint(value) => Some(*value),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Self::Float(value) => value.as_f32(),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(value) => value.as_f64(),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_str()),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(value) => Some(value.as_slice()),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Self::List(value) => Some(value.as_slice()),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn into_string(self) -> Option<String> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn into_bytes(self) -> Option<Vec<u8>> {
        match self {
            Self::Blob(value) => Some(value),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn into_list(self) -> Option<Vec<Value>> {
        match self {
            Self::List(value) => Some(value),
            _ => None,
        }
    }

    /// Calculate the size in bytes that the representation on wire for this value will require.
    /// For internal use only.
    pub(crate) fn estimate_size(&self) -> usize {
        match self {
            Self::Nil => 0,
            Self::Bool(_) => 1,
            Self::Int(_) | Self::Float(_) => 8,
            Self::Uint(_) => panic!(
                "Aerospike does not support u64 natively on server-side. Use casting to store and \
                 retrieve u64 values."
            ),
            Self::String(s) => s.len(),
            Self::Blob(b) => b.len(),
            Self::List(_) | Self::HashMap(_) => encoder::pack_value(&mut msgpack::Sink, self),
            Self::GeoJson(s) => 1 + 2 + s.len(), // flags + ncells + jsonstr
            Self::Hll(h) => h.len(),
        }
    }

    /// Serialize the value into the given buffer.
    /// For internal use only.
    pub(crate) fn write_to(&self, w: &mut impl msgpack::Write) -> usize {
        match self {
            Self::Nil => 0,
            Self::Bool(value) => w.write_bool(*value),
            Self::Int(value) => w.write_i64(*value),
            Self::Uint(_) => panic!(
                "Aerospike does not support u64 natively on server-side. Use casting to store and \
                 retrieve u64 values."
            ),
            Self::Float(value) => match value {
                FloatValue::F32(value) => w.write_f32(value.0),
                FloatValue::F64(value) => w.write_f64(value.0),
            },
            Self::String(value) => w.write_str(value),
            Self::Blob(value) | Self::Hll(value) => w.write_bytes(value),
            Self::List(_) | Self::HashMap(_) => encoder::pack_value(w, self),
            Self::GeoJson(value) => w.write_geo(value),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> StdResult<(), fmt::Error> {
        match self {
            Self::Nil => f.write_str("<null>"),
            Self::Int(value) => value.fmt(f),
            Self::Uint(value) => value.fmt(f),
            Self::Bool(value) => value.fmt(f),
            Self::Float(value) => value.fmt(f),
            Self::String(value) | Self::GeoJson(value) => value.fmt(f),
            Self::Blob(value) | Self::Hll(value) => write!(f, "{value:?}"),
            Self::List(value) => write!(f, "{value:?}"),
            Self::HashMap(value) => write!(f, "{value:?}"),
        }
    }
}

from!(Value, Bool, bool);
from!(Value, Int, i8, i16, i32, i64);
from!(Value, Uint, u8, u16, u32, u64);
from!(Value, Float, f32, f64);
from!(Value, String, &str, String);
from!(Value, Blob, &[u8], Vec<u8>);
from!(Value, List, &[Self], Vec<Self>);
from!(Value, HashMap, HashMap<MapKey,Self>);

impl From<isize> for Value {
    fn from(value: isize) -> Self {
        Self::Int(value as i64)
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        Self::Uint(value as u64)
    }
}

impl From<FloatValue> for Value {
    fn from(value: FloatValue) -> Self {
        Self::Float(value)
    }
}

impl From<MapKey> for Value {
    fn from(value: MapKey) -> Self {
        match value {
            MapKey::Int(value) => value.into(),
            MapKey::Uint(value) => value.into(),
            MapKey::Float(value) => value.into(),
            MapKey::String(value) => value.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParticleError {
    #[error("Particle type not recognized")]
    UnrecognizedParticle(#[from] ParseParticleError),
    #[error("Particle type `{0:?}` not supported for the target type")]
    Unsupported(u8),
    #[error("Buffer error")]
    Buffer(#[from] BufferError),
    #[error("MessagePack error")]
    Msgpack(#[from] MsgpackError),
}

pub(crate) fn bytes_to_particle(
    ptype: u8,
    buf: &mut Buffer,
    len: usize,
) -> Result<Value, ParticleError> {
    match ParticleType::try_from(ptype)? {
        ParticleType::Null => Ok(Value::Nil),
        ParticleType::Integer => Ok(Value::Int(buf.read_i64())),
        ParticleType::Float => Ok(Value::Float(buf.read_f64().into())),
        ParticleType::String => Ok(Value::String(buf.read_str(len)?)),
        ParticleType::Blob => Ok(Value::Blob(buf.read_blob(len))),
        ParticleType::Bool => Ok(Value::Bool(buf.read_bool())),
        ParticleType::Hll => Ok(Value::Hll(buf.read_blob(len))),
        ParticleType::Map => Ok(decoder::unpack_value_map(buf)?),
        ParticleType::List => Ok(decoder::unpack_value_list(buf)?),
        ParticleType::GeoJson => {
            buf.advance(1);
            let ncells = buf.read_u16() as usize;
            let header_size = ncells * 8;

            buf.advance(header_size);
            let value = buf.read_str(len - header_size - 3)?;
            Ok(Value::GeoJson(value))
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
            Value::Uint(9_223_372_036_854_775_808).to_string(),
            String::from("9223372036854775808")
        );
        assert_eq!(Value::Bool(true).to_string(), String::from("true"));
        assert_eq!(Value::from(4.1416).to_string(), String::from("4.1416"));
        assert_eq!(
            Value::GeoJson(r#"{"type":"Point"}"#.to_owned()).to_string(),
            String::from(r#"{"type":"Point"}"#)
        );
    }
}
