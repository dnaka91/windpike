use std::collections::HashMap;

use tracing::warn;

use super::{Marker, MsgpackError, Result};
use crate::{
    commands::{buffer::Buffer, ParticleType},
    value::Value,
};

pub fn unpack_value_list(buf: &mut Buffer) -> Result<Value> {
    if buf.buffer.is_empty() {
        return Ok(Value::List(Vec::new()));
    }

    let value = unpack_value(buf)?;
    assert!(matches!(value, Value::List(_)));

    Ok(value)
}

pub fn unpack_value_map(buf: &mut Buffer) -> Result<Value> {
    if buf.buffer.is_empty() {
        return Ok(Value::from(HashMap::new()));
    }

    let value = unpack_value(buf)?;
    assert!(matches!(value, Value::HashMap(_)));

    Ok(value)
}

fn unpack_array(buf: &mut Buffer, mut count: usize) -> Result<Value> {
    if count > 0 && is_ext(buf.peek().into()) {
        let _uv = unpack_value(buf);
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
    if count > 0 && is_ext(buf.peek().into()) {
        let _uv = unpack_value(buf);
        let _uv = unpack_value(buf);
        count -= 1;
    }

    let mut map: HashMap<Value, Value> = HashMap::with_capacity(count);
    for _ in 0..count {
        let key = unpack_value(buf)?;
        let val = unpack_value(buf)?;
        map.insert(key, val);
    }

    Ok(Value::from(map))
}

fn unpack_blob(buf: &mut Buffer, count: usize) -> Result<Value> {
    let vtype = buf.read_u8(None);
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

fn unpack_value(buf: &mut Buffer) -> Result<Value> {
    let marker = Marker::from(buf.read_u8(None));

    match marker {
        Marker::Pfix(value) => Ok(Value::from(value)),
        Marker::FixMap(len) => unpack_map(buf, len as usize),
        Marker::FixArray(len) => unpack_array(buf, len as usize),
        Marker::FixStr(len) => unpack_blob(buf, len as usize),
        Marker::Nil => Ok(Value::Nil),
        Marker::Reserved => {
            warn!("Skipping over reserved type");
            Ok(Value::Nil)
        }
        Marker::False => Ok(Value::from(false)),
        Marker::True => Ok(Value::from(true)),
        Marker::Bin8 | Marker::Str8 => {
            let count = buf.read_u8(None);
            unpack_blob(buf, count as usize)
        }
        Marker::Bin16 | Marker::Str16 => {
            let count = buf.read_u16(None);
            unpack_blob(buf, count as usize)
        }
        Marker::Bin32 | Marker::Str32 => {
            let count = buf.read_u32(None);
            unpack_blob(buf, count as usize)
        }
        Marker::Ext8 => {
            warn!("Skipping over type extension with 8 bit header and bytes");
            let count = 1 + buf.read_u8(None) as usize;
            buf.skip_bytes(count);
            Ok(Value::Nil)
        }
        Marker::Ext16 => {
            warn!("Skipping over type extension with 16 bit header and bytes");
            let count = 1 + buf.read_u16(None) as usize;
            buf.skip_bytes(count);
            Ok(Value::Nil)
        }
        Marker::Ext32 => {
            warn!("Skipping over type extension with 32 bit header and bytes");
            let count = 1 + buf.read_u32(None) as usize;
            buf.skip_bytes(count);
            Ok(Value::Nil)
        }
        Marker::F32 => Ok(Value::from(buf.read_f32(None))),
        Marker::F64 => Ok(Value::from(buf.read_f64(None))),
        Marker::U8 => Ok(Value::from(buf.read_u8(None))),
        Marker::U16 => Ok(Value::from(buf.read_u16(None))),
        Marker::U32 => Ok(Value::from(buf.read_u32(None))),
        Marker::U64 => Ok(Value::from(buf.read_u64(None))),
        Marker::I8 => Ok(Value::from(buf.read_i8(None))),
        Marker::I16 => Ok(Value::from(buf.read_i16(None))),
        Marker::I32 => Ok(Value::from(buf.read_i32(None))),
        Marker::I64 => Ok(Value::from(buf.read_i64(None))),
        Marker::FixExt1 => {
            warn!("Skipping over type extension with 1 byte");
            buf.skip_bytes(2);
            Ok(Value::Nil)
        }
        Marker::FixExt2 => {
            warn!("Skipping over type extension with 2 bytes");
            buf.skip_bytes(3);
            Ok(Value::Nil)
        }
        Marker::FixExt4 => {
            warn!("Skipping over type extension with 4 bytes");
            buf.skip_bytes(5);
            Ok(Value::Nil)
        }
        Marker::FixExt8 => {
            warn!("Skipping over type extension with 8 bytes");
            buf.skip_bytes(9);
            Ok(Value::Nil)
        }
        Marker::FixExt16 => {
            warn!("Skipping over type extension with 16 bytes");
            buf.skip_bytes(17);
            Ok(Value::Nil)
        }
        Marker::Array16 => {
            let count = buf.read_u16(None);
            unpack_array(buf, count as usize)
        }
        Marker::Array32 => {
            let count = buf.read_u32(None);
            unpack_array(buf, count as usize)
        }
        Marker::Map16 => {
            let count = buf.read_u16(None);
            unpack_map(buf, count as usize)
        }
        Marker::Map32 => {
            let count = buf.read_u32(None);
            unpack_map(buf, count as usize)
        }
        Marker::Nfix(value) => Ok(Value::from(value)),
    }
}

const fn is_ext(marker: Marker) -> bool {
    matches!(
        marker,
        Marker::Ext8
            | Marker::Ext16
            | Marker::Ext32
            | Marker::FixExt1
            | Marker::FixExt2
            | Marker::FixExt4
            | Marker::FixExt8
            | Marker::FixExt16
    )
}
