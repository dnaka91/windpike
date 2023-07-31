use std::{collections::HashMap, fmt, result::Result as StdResult, vec::Vec};

use ordered_float::OrderedFloat;

use crate::{
    commands::{buffer::BufferError, ParseParticleError, ParticleType},
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
    /// 32-bit floating point number.
    F32(OrderedFloat<f32>),
    /// 64-bit floating point number.
    F64(OrderedFloat<f64>),
}

impl FloatValue {
    /// If this value is a 32-bit floating point number, return the associated `f32`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use windpike::FloatValue;
    /// let v = FloatValue::from(5.0_f32);
    ///
    /// assert_eq!(Some(5.0), v.as_f32());
    /// assert_eq!(None, v.as_f64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Self::F32(value) => Some(value.0),
            Self::F64(_) => None,
        }
    }

    /// If this value is a 64-bit floating point number, return the associated `f64`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use windpike::FloatValue;
    /// let v = FloatValue::from(5.0_f64);
    ///
    /// assert_eq!(Some(5.0), v.as_f64());
    /// assert_eq!(None, v.as_f32());
    /// ```
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

/// Key for a [`Value::HashMap`] entry, which is a subset of the [`Value`] type, as only a limited
/// set of its variants are allowed to be used as map keys.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum MapKey {
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit unsigned integer.
    Uint(u64),
    /// Floating point number.
    Float(FloatValue),
    /// String value
    String(String),
}

impl MapKey {
    /// If this value is a 64-bit signed integer, return the associated `i64`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use windpike::MapKey;
    /// let v = MapKey::from(10_i64);
    ///
    /// assert_eq!(Some(10), v.as_i64());
    /// assert_eq!(None, v.as_u64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            _ => None,
        }
    }

    /// If this value is a 64-bit unsigned integer, return the associated `u64`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use windpike::MapKey;
    /// let v = MapKey::from(10_u64);
    ///
    /// assert_eq!(Some(10), v.as_u64());
    /// assert_eq!(None, v.as_i64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Uint(value) => Some(*value),
            _ => None,
        }
    }

    /// If this value is a 32-bit floating point number, return the associated `f32`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use windpike::MapKey;
    /// let v = MapKey::from(5.0_f32);
    ///
    /// assert_eq!(Some(5.0), v.as_f32());
    /// assert_eq!(None, v.as_f64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Self::Float(value) => value.as_f32(),
            _ => None,
        }
    }

    /// If this value is a 64-bit floating point number, return the associated `f64`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use windpike::MapKey;
    /// let v = MapKey::from(5.0_f64);
    ///
    /// assert_eq!(Some(5.0), v.as_f64());
    /// assert_eq!(None, v.as_f32());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(value) => value.as_f64(),
            _ => None,
        }
    }

    /// If this value is a string, return the associated `&str`. Return `None` oterwhise.
    ///
    /// ```
    /// # use windpike::MapKey;
    /// let v = MapKey::from("key");
    ///
    /// assert_eq!(Some("key"), v.as_str());
    /// assert_eq!(None, v.as_i64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    /// If this value is a string, return the associated `String`. Return `None` oterwhise. In
    /// contrast to [`Self::as_str`], this method consumes the value to return the owned string.
    ///
    /// ```
    /// # use windpike::MapKey;
    /// let v = MapKey::from("value");
    ///
    /// assert_eq!(Some(String::from("value")), v.into_string());
    /// ```
    #[inline]
    #[must_use]
    pub fn into_string(self) -> Option<String> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }
}

from!(MapKey, Int, i8, i16, i32, i64, u8, u16, u32);
from!(MapKey, Uint, u64);
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
    /// 64-bit signed integer. All integers are represented as 64-bit numerics in Aerospike.
    Int(i64),
    /// 64-bit unsigned integer.
    ///
    /// The biggest integer value that can be stored in a record is [`i64::MAX`]. However, the
    /// [`Self::List`] and [`Self::HashMap`] variants can store integer values up to [`u64::MAX`].
    ///
    /// # Panics
    ///
    /// Attempting to store an `u64` value as a record bin value will cause a panic. Use casting to
    /// store and retrieve `u64` values.
    Uint(u64),
    /// 32-bit or 64-bit Floating point number.
    Float(FloatValue),
    /// String value.
    String(String),
    /// Byte vector value.
    Blob(Vec<u8>),
    /// Ordered collection of values, that can contain any other value.
    List(Vec<Value>),
    /// Key-value pair collection of values. The key is limited to the variants of the [`MapKey`],
    /// as hash maps can't store every possible variant that this type represents.
    HashMap(HashMap<MapKey, Value>),
    /// String value that contains valid GeoJSON. In case the encoded content turns out to be
    /// malformed, an error will be returned by the Aerospike server.
    GeoJson(String),
    /// [HyperLogLog](https://docs.aerospike.com/server/guide/data-types/hll) value.
    Hll(Vec<u8>),
}

impl Value {
    /// Determine the particle type for the value used in the wire protocol.
    #[must_use]
    pub(crate) fn particle_type(&self) -> ParticleType {
        match self {
            Self::Nil => ParticleType::Null,
            Self::Bool(_) => ParticleType::Bool,
            Self::Int(_) => ParticleType::Integer,
            Self::Uint(_) => panic!(
                "Aerospike doesn't support 64-bit unsigned integers natively. Cast forth and back \
                 between i64 to store u64 values."
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

    /// If this value is a boolean, return the associated `bool`. Return `None` oterwhise.
    ///
    /// ```
    /// # use windpike::Value;
    /// let v = Value::from(true);
    ///
    /// assert_eq!(Some(true), v.as_bool());
    /// assert_eq!(None, v.as_i64());
    /// ```
    #[inline]
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    /// If this value is a 64-bit signed integer, return the associated `i64`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use windpike::Value;
    /// let v = Value::from(10_i64);
    ///
    /// assert_eq!(Some(10), v.as_i64());
    /// assert_eq!(None, v.as_u64());
    /// ```
    #[inline]
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            _ => None,
        }
    }

    /// If this value is a 64-bit unsigned integer, return the associated `u64`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use windpike::Value;
    /// let v = Value::from(10_u64);
    ///
    /// assert_eq!(Some(10), v.as_u64());
    /// assert_eq!(None, v.as_i64());
    /// ```
    #[inline]
    #[must_use]
    pub const fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Uint(value) => Some(*value),
            _ => None,
        }
    }

    /// If this value is a 32-bit floating point number, return the associated `f32`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use windpike::Value;
    /// let v = Value::from(5.0_f32);
    ///
    /// assert_eq!(Some(5.0), v.as_f32());
    /// assert_eq!(None, v.as_f64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Self::Float(value) => value.as_f32(),
            _ => None,
        }
    }

    /// If this value is a 64-bit floating point number, return the associated `f64`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use windpike::Value;
    /// let v = Value::from(5.0_f64);
    ///
    /// assert_eq!(Some(5.0), v.as_f64());
    /// assert_eq!(None, v.as_f32());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(value) => value.as_f64(),
            _ => None,
        }
    }

    /// If this value is a string, return the associated `&str`. Return `None` oterwhise.
    ///
    /// ```
    /// # use windpike::Value;
    /// let v = Value::from("value");
    ///
    /// assert_eq!(Some("value"), v.as_str());
    /// assert_eq!(None, v.as_i64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_str()),
            _ => None,
        }
    }

    /// If this value is a blob, return the associated `&[u8]`. Return `None` oterwhise.
    ///
    /// ```
    /// # use windpike::Value;
    /// let v = Value::from([1, 2, 3]);
    ///
    /// assert_eq!(Some(&[1, 2, 3][..]), v.as_bytes());
    /// assert_eq!(None, v.as_i64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(value) => Some(value.as_slice()),
            _ => None,
        }
    }

    /// If this value is a list, return the associated `&[Value]`. Return `None` oterwhise.
    ///
    /// ```
    /// # use windpike::Value;
    /// let v = Value::from([Value::from(1), Value::from("value")]);
    ///
    /// assert_eq!(Some(&[1.into(), "value".into()][..]), v.as_list());
    /// assert_eq!(None, v.as_i64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Self::List(value) => Some(value.as_slice()),
            _ => None,
        }
    }

    /// If this value is a hash map, return the associated `&HashMap<MapKey, Value>`. Return `None`
    /// oterwhise.
    ///
    /// ```
    /// # use std::collections::HashMap;
    /// # use windpike::{MapKey,Value};
    /// let v = Value::from([
    ///     (MapKey::from("a"), Value::from(1)),
    ///     (MapKey::from("b"), Value::from("value")),
    /// ]);
    ///
    /// assert_eq!(
    ///     Some(&HashMap::from([
    ///         ("a".into(), 1.into()),
    ///         ("b".into(), "value".into())
    ///     ])),
    ///     v.as_hash_map()
    /// );
    /// assert_eq!(None, v.as_i64());
    /// ```
    #[inline]
    #[must_use]
    pub fn as_hash_map(&self) -> Option<&HashMap<MapKey, Value>> {
        match self {
            Self::HashMap(value) => Some(value),
            _ => None,
        }
    }

    /// If this value is a string, return the associated `String`. Return `None` oterwhise. In
    /// contrast to [`Self::as_str`], this method consumes the value to return the owned string.
    ///
    /// ```
    /// # use windpike::Value;
    /// let v = Value::from("value");
    ///
    /// assert_eq!(Some(String::from("value")), v.into_string());
    /// ```
    #[inline]
    #[must_use]
    pub fn into_string(self) -> Option<String> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    /// If this value is a blob, return the associated `Vec<u8>`. Return `None` oterwhise. In
    /// contrast to [`Self::as_bytes`], this method consumes the value to return the owned vector.
    ///
    /// ```
    /// # use windpike::Value;
    /// let v = Value::from([1, 2, 3]);
    ///
    /// assert_eq!(Some(vec![1, 2, 3]), v.into_bytes());
    /// ```
    #[inline]
    #[must_use]
    pub fn into_bytes(self) -> Option<Vec<u8>> {
        match self {
            Self::Blob(value) => Some(value),
            _ => None,
        }
    }

    /// If this value is a list, return the associated `Vec<Value>`. Return `None` oterwhise. In
    /// contrast to [`Self::as_list`], this method consumes the value to return the owned vector.
    ///
    /// ```
    /// # use windpike::Value;
    /// let v = Value::from([Value::from(1), Value::from(2), Value::from(3)]);
    ///
    /// assert_eq!(Some(vec![1.into(), 2.into(), 3.into()]), v.into_list());
    /// ```
    #[inline]
    #[must_use]
    pub fn into_list(self) -> Option<Vec<Value>> {
        match self {
            Self::List(value) => Some(value),
            _ => None,
        }
    }

    /// If this value is a hash map, return the associated `HashMap<MapKey, Value>`. Return `None`
    /// oterwhise. In contrast to [`Self::as_hash_map`], this method consumes the value to
    /// return the owned hash map.
    ///
    /// ```
    /// # use std::collections::HashMap;
    /// # use windpike::{MapKey,Value};
    /// let v = Value::from([
    ///     (MapKey::from("a"), Value::from(1)),
    ///     (MapKey::from("b"), Value::from("value")),
    /// ]);
    ///
    /// assert_eq!(
    ///     Some(HashMap::from([
    ///         ("a".into(), 1.into()),
    ///         ("b".into(), "value".into())
    ///     ])),
    ///     v.into_hash_map()
    /// );
    /// ```
    #[inline]
    #[must_use]
    pub fn into_hash_map(self) -> Option<HashMap<MapKey, Value>> {
        match self {
            Self::HashMap(value) => Some(value),
            _ => None,
        }
    }

    /// Calculate the size this value requires in encoded form.
    pub(crate) fn estimate_size(&self) -> usize {
        match self {
            Self::Nil => 0,
            Self::Bool(_) => 1,
            Self::Int(_) | Self::Float(_) => 8,
            Self::Uint(_) => panic!(
                "Aerospike doesn't support 64-bit unsigned integers natively. Cast forth and back \
                 between i64 to store u64 values."
            ),
            Self::String(s) => s.len(),
            Self::Blob(b) => b.len(),
            Self::List(_) | Self::HashMap(_) => encoder::pack_value(&mut msgpack::Sink, self),
            Self::GeoJson(s) => 3 + s.len(),
            Self::Hll(h) => h.len(),
        }
    }

    /// Serialize the value into the given writer.
    pub(crate) fn write_to(&self, w: &mut impl msgpack::Write) -> usize {
        match self {
            Self::Nil => 0,
            Self::Bool(value) => w.write_bool(*value),
            Self::Int(value) => w.write_i64(*value),
            Self::Uint(_) => panic!(
                "Aerospike doesn't support 64-bit unsigned integers natively. Cast forth and back \
                 between i64 to store u64 values."
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

    /// Deserialize the value out of the given reader.
    pub(crate) fn read_from(
        r: &mut impl msgpack::Read,
        particle_type: u8,
        length: usize,
    ) -> Result<Self, ParticleError> {
        match ParticleType::try_from(particle_type)? {
            ParticleType::Null => Ok(Value::Nil),
            ParticleType::Integer => Ok(Value::Int(r.read_i64())),
            ParticleType::Float => Ok(Value::Float(r.read_f64().into())),
            ParticleType::String => Ok(Value::String(r.read_str(length)?)),
            ParticleType::Blob => Ok(Value::Blob(r.read_bytes(length))),
            ParticleType::Bool => Ok(Value::Bool(r.read_bool())),
            ParticleType::Hll => Ok(Value::Hll(r.read_bytes(length))),
            ParticleType::Map => Ok(decoder::unpack_value_map(r)?),
            ParticleType::List => Ok(decoder::unpack_value_list(r)?),
            ParticleType::GeoJson => Ok(Value::GeoJson(r.read_geo(length)?)),
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
from!(Value, Int, i8, i16, i32, i64, u8, u16, u32);
from!(Value, Uint, u64);
from!(Value, Float, f32, f64);
from!(Value, String, &str, String);
from!(Value, Blob, &[u8], Vec<u8>);
from!(Value, List, &[Self], Vec<Self>);
from!(Value, HashMap, HashMap<MapKey, Self>);

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

impl<const N: usize> From<[u8; N]> for Value {
    fn from(value: [u8; N]) -> Self {
        Self::Blob(value.into())
    }
}

impl<const N: usize> From<[Value; N]> for Value {
    fn from(value: [Value; N]) -> Self {
        Self::List(value.into())
    }
}

impl<const N: usize> From<[(MapKey, Self); N]> for Value {
    fn from(value: [(MapKey, Self); N]) -> Self {
        Self::HashMap(value.into())
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

/// Errors that can happen when parsing content markers from the wire format of an encoded value.
#[derive(Debug, thiserror::Error)]
pub enum ParticleError {
    /// The encountered particle type is not known.
    #[error("particle type not recognized")]
    UnrecognizedParticle(#[from] ParseParticleError),
    /// The encountered particle type is currently not supported.
    #[error("particle type `{0:?}` not supported for the target type")]
    Unsupported(u8),
    /// Failed to read from the data buffer.
    #[error("buffer error")]
    Buffer(#[from] BufferError),
    /// Failed to decode MessagePack encoded data.
    #[error("MessagePack error")]
    Msgpack(#[from] MsgpackError),
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
