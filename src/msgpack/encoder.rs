use std::{collections::HashMap, num::Wrapping};

use super::Write;
use crate::{
    commands::ParticleType,
    operations::{
        cdt::{CdtArgument, CdtOperation},
        cdt_context::CdtContext,
    },
    value::{FloatValue, Value},
};

pub(crate) fn pack_value(w: &mut impl Write, val: &Value) -> usize {
    match val {
        Value::Nil => pack_nil(w),
        Value::Int(val) => pack_integer(w, *val),
        Value::Uint(val) => pack_u64(w, *val),
        Value::Bool(val) => pack_bool(w, *val),
        Value::String(val) => pack_string(w, val),
        Value::Float(val) => match val {
            FloatValue::F64(val) => pack_f64(w, f64::from_bits(*val)),
            FloatValue::F32(val) => pack_f32(w, f32::from_bits(*val)),
        },
        Value::Blob(val) | Value::Hll(val) => pack_blob(w, val),
        Value::List(val) => pack_array(w, val),
        Value::HashMap(val) => pack_map(w, val),
        Value::OrderedMap(_) => panic!("Ordered maps are not supported in this encoder."),
        Value::GeoJson(val) => pack_geo_json(w, val),
    }
}

pub(crate) fn pack_cdt_op(
    w: &mut impl Write,
    cdt_op: &CdtOperation<'_>,
    ctx: &[CdtContext],
) -> usize {
    let mut size: usize = 0;
    if ctx.is_empty() {
        w.write_u16(cdt_op.op.into());
        size += 2;
        if !cdt_op.args.is_empty() {
            size += pack_array_begin(w, cdt_op.args.len());
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

        size += pack_array_begin(w, cdt_op.args.len() + 1);
        size += pack_integer(w, i64::from(cdt_op.op));
    }

    if !cdt_op.args.is_empty() {
        for arg in &cdt_op.args {
            size += match *arg {
                CdtArgument::Byte(byte) => pack_value(w, &Value::from(byte)),
                CdtArgument::Int(int) => pack_value(w, &Value::from(int)),
                CdtArgument::Value(value) => pack_value(w, value),
                CdtArgument::List(list) => pack_array(w, list),
                CdtArgument::Map(map) => pack_map(w, map),
                CdtArgument::Bool(bool_val) => pack_value(w, &Value::from(bool_val)),
            }
        }
    }

    size
}

pub(crate) fn pack_hll_op(
    w: &mut impl Write,
    hll_op: &CdtOperation<'_>,
    _ctx: &[CdtContext],
) -> usize {
    let mut size: usize = 0;
    size += pack_array_begin(w, hll_op.args.len() + 1);
    size += pack_integer(w, i64::from(hll_op.op));
    if !hll_op.args.is_empty() {
        for arg in &hll_op.args {
            size += match *arg {
                CdtArgument::Byte(byte) => pack_value(w, &Value::from(byte)),
                CdtArgument::Int(int) => pack_value(w, &Value::from(int)),
                CdtArgument::Value(value) => pack_value(w, value),
                CdtArgument::List(list) => pack_array(w, list),
                CdtArgument::Map(map) => pack_map(w, map),
                CdtArgument::Bool(bool_val) => pack_value(w, &Value::from(bool_val)),
            }
        }
    }
    size
}

pub(crate) fn pack_cdt_bit_op(
    w: &mut impl Write,
    cdt_op: &CdtOperation<'_>,
    ctx: &[CdtContext],
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

    size += pack_array_begin(w, cdt_op.args.len() + 1);
    size += pack_integer(w, i64::from(cdt_op.op));

    if !cdt_op.args.is_empty() {
        for arg in &cdt_op.args {
            size += match *arg {
                CdtArgument::Byte(byte) => pack_value(w, &Value::from(byte)),
                CdtArgument::Int(int) => pack_value(w, &Value::from(int)),
                CdtArgument::Value(value) => pack_value(w, value),
                CdtArgument::List(list) => pack_array(w, list),
                CdtArgument::Map(map) => pack_map(w, map),
                CdtArgument::Bool(bool_val) => pack_value(w, &Value::from(bool_val)),
            }
        }
    }
    size
}

fn pack_array(w: &mut impl Write, values: &[Value]) -> usize {
    let mut size = 0;

    size += pack_array_begin(w, values.len());
    for val in values {
        size += pack_value(w, val);
    }

    size
}

fn pack_map(w: &mut impl Write, map: &HashMap<Value, Value>) -> usize {
    let mut size = 0;

    size += pack_map_begin(w, map.len());
    for (key, val) in map.iter() {
        size += pack_value(w, key);
        size += pack_value(w, val);
    }

    size
}

/// ///////////////////////////////////////////////////////////////////

const MSGPACK_MARKER_NIL: u8 = 0xc0;
const MSGPACK_MARKER_BOOL_TRUE: u8 = 0xc3;
const MSGPACK_MARKER_BOOL_FALSE: u8 = 0xc2;

const MSGPACK_MARKER_I8: u8 = 0xcc;
const MSGPACK_MARKER_I16: u8 = 0xcd;
const MSGPACK_MARKER_I32: u8 = 0xce;
// const MSGPACK_MARKER_I64: u8 = 0xd3;

const MSGPACK_MARKER_NI8: u8 = 0xd0;
const MSGPACK_MARKER_NI16: u8 = 0xd1;
const MSGPACK_MARKER_NI32: u8 = 0xd2;
const MSGPACK_MARKER_NI64: u8 = 0xd3;

fn pack_half_byte(w: &mut impl Write, value: u8) -> usize {
    w.write_u8(value);
    1
}

fn pack_byte(w: &mut impl Write, marker: u8, value: u8) -> usize {
    w.write_u8(marker);
    w.write_u8(value);
    2
}

fn pack_nil(w: &mut impl Write) -> usize {
    w.write_u8(MSGPACK_MARKER_NIL);
    1
}

fn pack_bool(w: &mut impl Write, value: bool) -> usize {
    w.write_u8(if value {
        MSGPACK_MARKER_BOOL_TRUE
    } else {
        MSGPACK_MARKER_BOOL_FALSE
    });
    1
}

fn pack_map_begin(w: &mut impl Write, length: usize) -> usize {
    match length {
        val if val < 16 => pack_half_byte(w, 0x80 | (length as u8)),
        val if (16..(1 << 16)).contains(&val) => pack_i16(w, 0xde, length as i16),
        _ => pack_i32(w, 0xdf, length as i32),
    }
}

fn pack_array_begin(w: &mut impl Write, length: usize) -> usize {
    match length {
        val if val < 16 => pack_half_byte(w, 0x90 | (length as u8)),
        val if (16..(1 << 16)).contains(&val) => pack_i16(w, 0xdc, length as i16),
        _ => pack_i32(w, 0xdd, length as i32),
    }
}

fn pack_byte_array_begin(w: &mut impl Write, length: usize) -> usize {
    match length {
        val if val < 32 => pack_half_byte(w, 0xa0 | (length as u8)),
        val if (32..(1 << 16)).contains(&val) => pack_i16(w, 0xda, length as i16),
        _ => pack_i32(w, 0xdb, length as i32),
    }
}

fn pack_blob(w: &mut impl Write, value: &[u8]) -> usize {
    let mut size = value.len() + 1;

    size += pack_byte_array_begin(w, size);
    w.write_u8(ParticleType::Blob as u8);
    w.write_bytes(value);

    size
}

fn pack_string(w: &mut impl Write, value: &str) -> usize {
    let mut size = value.len() + 1;

    size += pack_byte_array_begin(w, size);
    w.write_u8(ParticleType::String as u8);
    w.write_str(value);

    size
}

fn pack_geo_json(w: &mut impl Write, value: &str) -> usize {
    let mut size = value.len() + 1;

    size += pack_byte_array_begin(w, size);
    w.write_u8(ParticleType::GeoJson as u8);
    w.write_str(value);

    size
}

fn pack_integer(w: &mut impl Write, val: i64) -> usize {
    match val {
        val if (0..(1 << 7)).contains(&val) => pack_half_byte(w, val as u8),
        val if val >= 1 << 7 && val < i64::from(i8::max_value()) => {
            pack_byte(w, MSGPACK_MARKER_I8, val as u8)
        }
        val if val >= i64::from(i8::max_value()) && val < i64::from(i16::max_value()) => {
            pack_i16(w, MSGPACK_MARKER_I16, val as i16)
        }
        val if val >= i64::from(i16::max_value()) && val < i64::from(i32::max_value()) => {
            pack_i32(w, MSGPACK_MARKER_I32, val as i32)
        }
        val if val >= i64::from(i32::max_value()) => pack_i64(w, MSGPACK_MARKER_I32, val),

        // Negative values
        val if (-32..0).contains(&val) => {
            pack_half_byte(w, 0xe0 | ((Wrapping(val as u8) + Wrapping(32)).0))
        }
        val if val >= i64::from(i8::min_value()) && val < -32 => {
            pack_byte(w, MSGPACK_MARKER_NI8, val as u8)
        }
        val if val >= i64::from(i16::min_value()) && val < i64::from(i8::min_value()) => {
            pack_i16(w, MSGPACK_MARKER_NI16, val as i16)
        }
        val if val >= i64::from(i32::min_value()) && val < i64::from(i16::min_value()) => {
            pack_i32(w, MSGPACK_MARKER_NI32, val as i32)
        }
        val if val < i64::from(i32::min_value()) => pack_i64(w, MSGPACK_MARKER_NI64, val),
        _ => unreachable!(),
    }
}

fn pack_i16(w: &mut impl Write, marker: u8, value: i16) -> usize {
    w.write_u8(marker);
    w.write_i16(value);
    3
}

fn pack_i32(w: &mut impl Write, marker: u8, value: i32) -> usize {
    w.write_u8(marker);
    w.write_i32(value);
    5
}

fn pack_i64(w: &mut impl Write, marker: u8, value: i64) -> usize {
    w.write_u8(marker);
    w.write_i64(value);
    9
}

fn pack_u64(w: &mut impl Write, value: u64) -> usize {
    if i64::try_from(value).is_ok() {
        return pack_integer(w, value as i64);
    }

    w.write_u8(0xcf);
    w.write_u64(value);
    9
}

fn pack_f32(w: &mut impl Write, value: f32) -> usize {
    w.write_u8(0xca);
    w.write_f32(value);
    5
}

fn pack_f64(w: &mut impl Write, value: f64) -> usize {
    w.write_u8(0xcb);
    w.write_f64(value);
    9
}
