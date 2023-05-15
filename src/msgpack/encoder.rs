use std::collections::HashMap;

use super::{Marker, Write};
use crate::{
    commands::ParticleType,
    operations::cdt,
    value::{FloatValue, MapKey, Value},
};

pub(crate) fn pack_map_key(w: &mut impl Write, val: &MapKey) -> usize {
    match val {
        MapKey::Int(val) => pack_integer(w, *val),
        MapKey::Uint(val) => pack_u64(w, *val),
        MapKey::Float(val) => match val {
            FloatValue::F64(val) => pack_f64(w, val.0),
            FloatValue::F32(val) => pack_f32(w, val.0),
        },
        MapKey::String(val) => pack_string(w, val),
    }
}

pub(crate) fn pack_value(w: &mut impl Write, val: &Value) -> usize {
    match val {
        Value::Nil => pack_nil(w),
        Value::Int(val) => pack_integer(w, *val),
        Value::Uint(val) => pack_u64(w, *val),
        Value::Bool(val) => pack_bool(w, *val),
        Value::String(val) => pack_string(w, val),
        Value::Float(val) => match val {
            FloatValue::F64(val) => pack_f64(w, val.0),
            FloatValue::F32(val) => pack_f32(w, val.0),
        },
        Value::Blob(val) | Value::Hll(val) => pack_blob(w, val),
        Value::List(val) => pack_array(w, val),
        Value::HashMap(val) => pack_map(w, val),
        Value::GeoJson(val) => pack_geo_json(w, val),
    }
}

pub(crate) fn pack_cdt_op(
    w: &mut impl Write,
    op: &cdt::Operation<'_>,
    ctx: &[cdt::Context],
) -> usize {
    let mut size: usize = 0;
    if ctx.is_empty() {
        w.write_u16(op.op.into());
        size += 2;
        if !op.args.is_empty() {
            size += pack_array_begin(w, op.args.len());
        }
    } else {
        size += pack_array_begin(w, 3);
        size += pack_integer(w, 0xff);
        size += pack_array_begin(w, ctx.len() * 2);

        for c in ctx {
            if c.id == 0 {
                size += pack_integer(w, i64::from(c.id));
            } else {
                size += pack_integer(w, i64::from(c.id | c.flags));
            }
            size += pack_value(w, &c.value);
        }

        size += pack_array_begin(w, op.args.len() + 1);
        size += pack_integer(w, i64::from(op.op));
    }

    if !op.args.is_empty() {
        for arg in &op.args {
            size += match *arg {
                cdt::Argument::Byte(byte) => pack_value(w, &Value::from(byte)),
                cdt::Argument::Int(int) => pack_value(w, &Value::from(int)),
                cdt::Argument::Value(value) => pack_value(w, value),
                cdt::Argument::List(list) => pack_array(w, list),
                cdt::Argument::Map(map) => pack_map(w, map),
                cdt::Argument::Bool(bool_val) => pack_value(w, &Value::from(bool_val)),
            }
        }
    }

    size
}

pub(crate) fn pack_hll_op(
    w: &mut impl Write,
    op: &cdt::Operation<'_>,
    _ctx: &[cdt::Context],
) -> usize {
    let mut size: usize = 0;
    size += pack_array_begin(w, op.args.len() + 1);
    size += pack_integer(w, i64::from(op.op));
    if !op.args.is_empty() {
        for arg in &op.args {
            size += match *arg {
                cdt::Argument::Byte(byte) => pack_value(w, &Value::from(byte)),
                cdt::Argument::Int(int) => pack_value(w, &Value::from(int)),
                cdt::Argument::Value(value) => pack_value(w, value),
                cdt::Argument::List(list) => pack_array(w, list),
                cdt::Argument::Map(map) => pack_map(w, map),
                cdt::Argument::Bool(bool_val) => pack_value(w, &Value::from(bool_val)),
            }
        }
    }
    size
}

pub(crate) fn pack_cdt_bit_op(
    w: &mut impl Write,
    op: &cdt::Operation<'_>,
    ctx: &[cdt::Context],
) -> usize {
    let mut size: usize = 0;
    if !ctx.is_empty() {
        size += pack_array_begin(w, 3);
        size += pack_integer(w, 0xff);
        size += pack_array_begin(w, ctx.len() * 2);

        for c in ctx {
            if c.id == 0 {
                size += pack_integer(w, i64::from(c.id));
            } else {
                size += pack_integer(w, i64::from(c.id | c.flags));
            }
            size += pack_value(w, &c.value);
        }
    }

    size += pack_array_begin(w, op.args.len() + 1);
    size += pack_integer(w, i64::from(op.op));

    if !op.args.is_empty() {
        for arg in &op.args {
            size += match *arg {
                cdt::Argument::Byte(byte) => pack_value(w, &Value::from(byte)),
                cdt::Argument::Int(int) => pack_value(w, &Value::from(int)),
                cdt::Argument::Value(value) => pack_value(w, value),
                cdt::Argument::List(list) => pack_array(w, list),
                cdt::Argument::Map(map) => pack_map(w, map),
                cdt::Argument::Bool(bool_val) => pack_value(w, &Value::from(bool_val)),
            }
        }
    }
    size
}

fn pack_array(w: &mut impl Write, values: &[Value]) -> usize {
    pack_array_begin(w, values.len()) + values.iter().map(|val| pack_value(w, val)).sum::<usize>()
}

fn pack_map(w: &mut impl Write, map: &HashMap<MapKey, Value>) -> usize {
    pack_map_begin(w, map.len())
        + map
            .iter()
            .map(|(key, val)| pack_map_key(w, key) + pack_value(w, val))
            .sum::<usize>()
}

fn pack_blob(w: &mut impl Write, value: &[u8]) -> usize {
    let mut size = value.len() + 1;

    size += pack_bytes_begin(w, size);
    w.write_u8(ParticleType::Blob as u8);
    w.write_bytes(value);

    size
}

fn pack_string(w: &mut impl Write, value: &str) -> usize {
    let mut size = value.len() + 1;

    size += pack_bytes_begin(w, size);
    w.write_u8(ParticleType::String as u8);
    w.write_str(value);

    size
}

fn pack_geo_json(w: &mut impl Write, value: &str) -> usize {
    let mut size = value.len() + 1;

    size += pack_bytes_begin(w, size);
    w.write_u8(ParticleType::GeoJson as u8);
    w.write_str(value);

    size
}

fn pack_nil(w: &mut impl Write) -> usize {
    w.write_u8(Marker::Nil.into());
    1
}

fn pack_bool(w: &mut impl Write, value: bool) -> usize {
    w.write_u8(if value { Marker::True } else { Marker::False }.into());
    1
}

fn pack_map_begin(w: &mut impl Write, len: usize) -> usize {
    assert!(
        u32::try_from(len).is_ok(),
        "map can't be larger than u32::MAX"
    );
    let len = len as u32;

    match len {
        _ if len < 16 => {
            w.write_u8(Marker::FixMap(len as u8).into());
            1
        }
        _ if len < u32::from(u16::MAX) => {
            w.write_u8(Marker::Map16.into());
            w.write_u16(len as u16);
            3
        }
        _ => {
            w.write_u8(Marker::Map32.into());
            w.write_u32(len);
            5
        }
    }
}

fn pack_array_begin(w: &mut impl Write, len: usize) -> usize {
    assert!(
        u32::try_from(len).is_ok(),
        "array can't be larger than u32::MAX"
    );
    let len = len as u32;

    match len {
        _ if len < 16 => {
            w.write_u8(Marker::FixArray(len as u8).into());
            1
        }
        _ if len < u32::from(u16::MAX) => {
            w.write_u8(Marker::Array16.into());
            w.write_u16(len as u16);
            3
        }
        _ => {
            w.write_u8(Marker::Array32.into());
            w.write_u32(len);
            5
        }
    }
}

fn pack_bytes_begin(w: &mut impl Write, len: usize) -> usize {
    assert!(
        u32::try_from(len).is_ok(),
        "bytes can't be larger than u32::MAX"
    );
    let len = len as u32;

    match len {
        _ if len < 32 => {
            w.write_u8(Marker::FixStr(len as u8).into());
            1
        }
        _ if len < u32::from(u8::MAX) => {
            w.write_u8(Marker::Str8.into());
            w.write_u8(len as u8);
            2
        }
        _ if len < u32::from(u16::MAX) => {
            w.write_u8(Marker::Str16.into());
            w.write_u16(len as u16);
            3
        }
        _ => {
            w.write_u8(Marker::Str32.into());
            w.write_u32(len);
            5
        }
    }
}

fn pack_integer(w: &mut impl Write, val: i64) -> usize {
    match val {
        _ if val > i64::from(u32::MAX) => pack_i64(w, val),
        _ if val > i64::from(u16::MAX) => pack_u32(w, val as u32),
        _ if val > i64::from(u8::MAX) => pack_u16(w, val as u16),
        _ if val > i64::from(i8::MAX) => pack_u8(w, val as u8),
        _ if val >= 0 => pack_ufix(w, val as u8),
        _ if val >= -32 => pack_ifix(w, val as i8),
        _ if val >= i64::from(i8::MIN) => pack_i8(w, val as i8),
        _ if val >= i64::from(i16::MIN) => pack_i16(w, val as i16),
        _ if val >= i64::from(i32::MIN) => pack_i32(w, val as i32),
        _ => pack_i64(w, val),
    }
}

#[inline]
fn pack_ifix(w: &mut impl Write, value: i8) -> usize {
    w.write_u8(Marker::Nfix(value).into());
    1
}

#[inline]
fn pack_i8(w: &mut impl Write, value: i8) -> usize {
    w.write_u8(Marker::I8.into());
    w.write_i8(value);
    2
}

#[inline]
fn pack_i16(w: &mut impl Write, value: i16) -> usize {
    w.write_u8(Marker::I16.into());
    w.write_i16(value);
    3
}

#[inline]
fn pack_i32(w: &mut impl Write, value: i32) -> usize {
    w.write_u8(Marker::I32.into());
    w.write_i32(value);
    5
}

#[inline]
fn pack_i64(w: &mut impl Write, value: i64) -> usize {
    w.write_u8(Marker::I64.into());
    w.write_i64(value);
    9
}

#[inline]
fn pack_ufix(w: &mut impl Write, value: u8) -> usize {
    w.write_u8(Marker::Pfix(value).into());
    1
}

#[inline]
fn pack_u8(w: &mut impl Write, value: u8) -> usize {
    w.write_u8(Marker::U8.into());
    w.write_u8(value);
    2
}

#[inline]
fn pack_u16(w: &mut impl Write, value: u16) -> usize {
    w.write_u8(Marker::U16.into());
    w.write_u16(value);
    3
}

#[inline]
fn pack_u32(w: &mut impl Write, value: u32) -> usize {
    w.write_u8(Marker::U32.into());
    w.write_u32(value);
    5
}

#[inline]
fn pack_u64(w: &mut impl Write, value: u64) -> usize {
    if let Ok(value) = i64::try_from(value) {
        pack_integer(w, value)
    } else {
        w.write_u8(Marker::U64.into());
        w.write_u64(value);
        9
    }
}

#[inline]
fn pack_f32(w: &mut impl Write, value: f32) -> usize {
    w.write_u8(Marker::F32.into());
    w.write_f32(value);
    5
}

#[inline]
fn pack_f64(w: &mut impl Write, value: f64) -> usize {
    w.write_u8(Marker::F64.into());
    w.write_f64(value);
    9
}
