use std::collections::HashMap;

use tracing::warn;

use super::{Marker, MsgpackError, Result};
use crate::{
    commands::{buffer::Buffer, ParticleType},
    value::{MapKey, Value},
};

pub fn unpack_value_list(buf: &mut Buffer) -> Result<Value> {
    if buf.is_empty() {
        return Ok(Value::List(Vec::new()));
    }

    let value = unpack_value(buf)?;
    assert!(matches!(value, Value::List(_)));

    Ok(value)
}

pub fn unpack_value_map(buf: &mut Buffer) -> Result<Value> {
    if buf.is_empty() {
        return Ok(Value::from(HashMap::new()));
    }

    let value = unpack_value(buf)?;
    assert!(matches!(value, Value::HashMap(_)));

    Ok(value)
}

fn unpack_array(buf: &mut Buffer, mut count: usize) -> Result<Value> {
    if count > 0 && is_ext(buf.peek()) {
        unpack_value(buf).ok();
        count -= 1;
    }

    let mut list: Vec<Value> = Vec::with_capacity(count);
    for _ in 0..count {
        let val = unpack_value(buf)?;
        list.push(val);
    }

    Ok(Value::from(list))
}

fn unpack_map(buf: &mut Buffer, mut count: usize) -> Result<Value> {
    if count > 0 && is_ext(buf.peek()) {
        unpack_value(buf).ok();
        unpack_value(buf).ok();
        count -= 1;
    }

    let mut map = HashMap::with_capacity(count);
    for _ in 0..count {
        let key = unpack_map_key(buf)?;
        let val = unpack_value(buf)?;
        map.insert(key, val);
    }

    Ok(Value::from(map))
}

fn unpack_blob(buf: &mut Buffer, count: usize) -> Result<Value> {
    let vtype = buf.read_u8();
    let count = count - 1;

    match ParticleType::try_from(vtype)? {
        ParticleType::String => {
            let val = buf.read_str(count)?;
            Ok(Value::String(val))
        }
        ParticleType::Blob => Ok(Value::Blob(buf.read_blob(count))),
        ParticleType::GeoJson => {
            let val = buf.read_str(count)?;
            Ok(Value::GeoJson(val))
        }
        _ => Err(MsgpackError::UnrecognizedCode(vtype)),
    }
}

fn unpack_string(buf: &mut Buffer, count: usize) -> Result<String> {
    let vtype = buf.read_u8();
    let count = count - 1;

    match ParticleType::try_from(vtype)? {
        ParticleType::String => buf.read_str(count).map_err(Into::into),
        _ => Err(MsgpackError::UnrecognizedCode(vtype)),
    }
}

fn unpack_map_key(buf: &mut Buffer) -> Result<MapKey> {
    let marker = buf.read_u8();

    match Marker::from(marker) {
        Marker::Pfix(value) => Ok(MapKey::from(value)),
        Marker::FixStr(len) => unpack_string(buf, len as usize).map(Into::into),
        Marker::Bin8 | Marker::Str8 => {
            let count = buf.read_u8();
            unpack_string(buf, count as usize).map(Into::into)
        }
        Marker::Bin16 | Marker::Str16 => {
            let count = buf.read_u16();
            unpack_string(buf, count as usize).map(Into::into)
        }
        Marker::Bin32 | Marker::Str32 => {
            let count = buf.read_u32();
            unpack_string(buf, count as usize).map(Into::into)
        }
        Marker::F32 => Ok(MapKey::from(buf.read_f32())),
        Marker::F64 => Ok(MapKey::from(buf.read_f64())),
        Marker::U8 => Ok(MapKey::from(buf.read_u8())),
        Marker::U16 => Ok(MapKey::from(buf.read_u16())),
        Marker::U32 => Ok(MapKey::from(buf.read_u32())),
        Marker::U64 => Ok(MapKey::from(buf.read_u64())),
        Marker::I8 => Ok(MapKey::from(buf.read_i8())),
        Marker::I16 => Ok(MapKey::from(buf.read_i16())),
        Marker::I32 => Ok(MapKey::from(buf.read_i32())),
        Marker::I64 => Ok(MapKey::from(buf.read_i64())),
        Marker::Nfix(value) => Ok(MapKey::from(value)),
        _ => Err(MsgpackError::InvalidMarker(marker)),
    }
}

fn unpack_value(buf: &mut Buffer) -> Result<Value> {
    let marker = Marker::from(buf.read_u8());

    match marker {
        Marker::Pfix(value) => Ok(Value::from(value)),
        Marker::FixMap(len) => unpack_map(buf, len as usize),
        Marker::FixArray(len) => unpack_array(buf, len as usize),
        Marker::FixStr(len) => unpack_blob(buf, len as usize),
        Marker::Nil => Ok(Value::Nil),
        Marker::Reserved => {
            warn!("skipping over reserved type");
            Ok(Value::Nil)
        }
        Marker::False => Ok(Value::from(false)),
        Marker::True => Ok(Value::from(true)),
        Marker::Bin8 | Marker::Str8 => {
            let count = buf.read_u8();
            unpack_blob(buf, count as usize)
        }
        Marker::Bin16 | Marker::Str16 => {
            let count = buf.read_u16();
            unpack_blob(buf, count as usize)
        }
        Marker::Bin32 | Marker::Str32 => {
            let count = buf.read_u32();
            unpack_blob(buf, count as usize)
        }
        Marker::Ext8 => {
            warn!("skipping over type extension with 8 bit header and bytes");
            let count = 1 + buf.read_u8() as usize;
            buf.advance(count);
            Ok(Value::Nil)
        }
        Marker::Ext16 => {
            warn!("skipping over type extension with 16 bit header and bytes");
            let count = 1 + buf.read_u16() as usize;
            buf.advance(count);
            Ok(Value::Nil)
        }
        Marker::Ext32 => {
            warn!("skipping over type extension with 32 bit header and bytes");
            let count = 1 + buf.read_u32() as usize;
            buf.advance(count);
            Ok(Value::Nil)
        }
        Marker::F32 => Ok(Value::from(buf.read_f32())),
        Marker::F64 => Ok(Value::from(buf.read_f64())),
        Marker::U8 => Ok(Value::from(buf.read_u8())),
        Marker::U16 => Ok(Value::from(buf.read_u16())),
        Marker::U32 => Ok(Value::from(buf.read_u32())),
        Marker::U64 => Ok(Value::from(buf.read_u64())),
        Marker::I8 => Ok(Value::from(buf.read_i8())),
        Marker::I16 => Ok(Value::from(buf.read_i16())),
        Marker::I32 => Ok(Value::from(buf.read_i32())),
        Marker::I64 => Ok(Value::from(buf.read_i64())),
        Marker::FixExt1 => {
            warn!("skipping over type extension with 1 byte");
            buf.advance(2);
            Ok(Value::Nil)
        }
        Marker::FixExt2 => {
            warn!("skipping over type extension with 2 bytes");
            buf.advance(3);
            Ok(Value::Nil)
        }
        Marker::FixExt4 => {
            warn!("skipping over type extension with 4 bytes");
            buf.advance(5);
            Ok(Value::Nil)
        }
        Marker::FixExt8 => {
            warn!("skipping over type extension with 8 bytes");
            buf.advance(9);
            Ok(Value::Nil)
        }
        Marker::FixExt16 => {
            warn!("skipping over type extension with 16 bytes");
            buf.advance(17);
            Ok(Value::Nil)
        }
        Marker::Array16 => {
            let count = buf.read_u16();
            unpack_array(buf, count as usize)
        }
        Marker::Array32 => {
            let count = buf.read_u32();
            unpack_array(buf, count as usize)
        }
        Marker::Map16 => {
            let count = buf.read_u16();
            unpack_map(buf, count as usize)
        }
        Marker::Map32 => {
            let count = buf.read_u32();
            unpack_map(buf, count as usize)
        }
        Marker::Nfix(value) => Ok(Value::from(value)),
    }
}

fn is_ext(marker: Option<u8>) -> bool {
    marker.map_or(false, |marker| {
        matches!(
            Marker::from(marker),
            Marker::Ext8
                | Marker::Ext16
                | Marker::Ext32
                | Marker::FixExt1
                | Marker::FixExt2
                | Marker::FixExt4
                | Marker::FixExt8
                | Marker::FixExt16
        )
    })
}
