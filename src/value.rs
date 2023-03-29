use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    result::Result as StdResult,
    vec::Vec,
};

use crate::{
    commands::{
        buffer::{Buffer, BufferError},
        ParseParticleError, ParticleType,
    },
    errors::Result,
    msgpack::{self, decoder, encoder, MsgpackError},
};

/// Container for floating point bin values stored in the Aerospike database.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FloatValue {
    /// Container for single precision float values.
    F32(u32),
    /// Container for double precision float values.
    F64(u64),
}

impl FloatValue {
    #[must_use]
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Self::F32(val) => Some(f32::from_bits(*val)),
            Self::F64(_) => None,
        }
    }

    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::F32(_) => None,
            Self::F64(val) => Some(f64::from_bits(*val)),
        }
    }
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::F32(val) => f32::from_bits(val).fmt(f),
            Self::F64(val) => f64::from_bits(val).fmt(f),
        }
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
    HashMap(HashMap<Value, Value>),
    /// Map data type where the map entries are sorted based key ordering (K-ordered maps) and may
    /// have an additional value-order index depending the namespace configuration (KV-ordered
    /// maps).
    OrderedMap(Vec<(Value, Value)>),
    /// GeoJSON data type are JSON formatted strings to encode geospatial information.
    GeoJson(String),
    /// HLL value
    Hll(Vec<u8>),
}

#[allow(clippy::derived_hash_with_manual_eq)]
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Nil => {
                Option::<u8>::None.hash(state);
            }
            Self::Bool(val) => val.hash(state),
            Self::Int(val) => val.hash(state),
            Self::Uint(val) => val.hash(state),
            Self::Float(val) => val.hash(state),
            Self::String(val) | Self::GeoJson(val) => val.hash(state),
            Self::Blob(val) | Self::Hll(val) => val.hash(state),
            Self::List(val) => val.hash(state),
            Self::HashMap(_) => panic!("HashMaps cannot be used as map keys."),
            Self::OrderedMap(_) => panic!("OrderedMaps cannot be used as map keys."),
        }
    }
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
            Self::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            Self::GeoJson(_) => ParticleType::GeoJson,
            Self::Hll(_) => ParticleType::Hll,
        }
    }

    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(val) => Some(*val),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(val) => Some(*val),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Uint(val) => Some(*val),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Self::Float(val) => val.as_f32(),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(val) => val.as_f64(),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(val) => Some(val.as_str()),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(val) => Some(val.as_slice()),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Self::List(val) => Some(val.as_slice()),
            _ => None,
        }
    }

    #[must_use]
    pub fn into_string(self) -> Option<String> {
        match self {
            Self::String(val) => Some(val),
            _ => None,
        }
    }

    #[must_use]
    pub fn into_bytes(self) -> Option<Vec<u8>> {
        match self {
            Self::Blob(val) => Some(val),
            _ => None,
        }
    }

    #[must_use]
    pub fn into_list(self) -> Option<Vec<Value>> {
        match self {
            Self::List(val) => Some(val),
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
            Self::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            Self::GeoJson(s) => 1 + 2 + s.len(), // flags + ncells + jsonstr
            Self::Hll(h) => h.len(),
        }
    }

    /// Serialize the value into the given buffer.
    /// For internal use only.
    pub(crate) fn write_to(&self, w: &mut impl msgpack::Write) -> usize {
        match self {
            Self::Nil => 0,
            Self::Bool(val) => w.write_bool(*val),
            Self::Int(val) => w.write_i64(*val),
            Self::Uint(_) => panic!(
                "Aerospike does not support u64 natively on server-side. Use casting to store and \
                 retrieve u64 values."
            ),
            Self::Float(val) => w.write_f64(match val {
                FloatValue::F32(val) => f64::from(f32::from_bits(*val)),
                FloatValue::F64(val) => f64::from_bits(*val),
            }),
            Self::String(val) => w.write_str(val),
            Self::Blob(val) | Self::Hll(val) => w.write_bytes(val),
            Self::List(_) | Self::HashMap(_) => encoder::pack_value(w, self),
            Self::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            Self::GeoJson(val) => w.write_geo(val),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> StdResult<(), fmt::Error> {
        match self {
            Self::Nil => f.write_str("<null>"),
            Self::Int(val) => val.fmt(f),
            Self::Uint(val) => val.fmt(f),
            Self::Bool(val) => val.fmt(f),
            Self::Float(val) => val.fmt(f),
            Self::String(val) | Self::GeoJson(val) => val.fmt(f),
            Self::Blob(val) | Self::Hll(val) => write!(f, "{val:?}"),
            Self::List(val) => write!(f, "{val:?}"),
            Self::HashMap(val) => write!(f, "{val:?}"),
            Self::OrderedMap(val) => write!(f, "{val:?}"),
        }
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
        Self::String(val.to_owned())
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
        Self::Uint(val)
    }
}

impl From<isize> for Value {
    fn from(val: isize) -> Self {
        Self::Int(val as i64)
    }
}

impl From<usize> for Value {
    fn from(val: usize) -> Self {
        Self::Uint(val as u64)
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
        Self::Uint(*val)
    }
}

impl<'a> From<&'a isize> for Value {
    fn from(val: &'a isize) -> Self {
        Self::Int(*val as i64)
    }
}

impl<'a> From<&'a usize> for Value {
    fn from(val: &'a usize) -> Self {
        Self::Uint(*val as u64)
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
            let val = buf.read_str(len - header_size - 3)?;
            Ok(Value::GeoJson(val))
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
