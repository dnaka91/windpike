//! Bit operations. Create bit operations used by client operate command.
//! Offset orientation is left-to-right.  Negative offsets are supported.
//! If the offset is negative, the offset starts backwards from end of the bitmap.
//! If an offset is out of bounds, a parameter error will be returned.
//!
//! Nested CDT operations are supported by optional CTX context arguments. Example:
//!
//! ```
//! use windpike::operations::bitwise::{resize, Policy, ResizeFlags};
//! // bin = [[0b00000001, 0b01000010], [0b01011010]]
//! // Resize first bitmap (in a list of bitmaps) to 3 bytes.
//! resize("bin", 3, ResizeFlags::empty(), Policy::default());
//! // bin result = [[0b00000001, 0b01000010, 0b00000000], [0b01011010]]
//! ```

use bitflags::bitflags;

use super::cdt::{self, Encoder};
use crate::{
    operations::{Operation, OperationBin, OperationData, OperationType},
    Value,
};

#[derive(Clone, Copy, Debug)]
enum OpType {
    Resize = 0,
    Insert,
    Remove,
    Set,
    Or,
    Xor,
    And,
    Not,
    Lshift,
    Rshift,
    Add,
    Subtract,
    SetInt,
    Get = 50,
    Count,
    Lscan,
    Rscan,
    GetInt,
}

bitflags! {
    /// `CdtBitwiseResizeFlags` specifies the bitwise operation flags for resize.
    #[derive(Clone, Copy, Debug)]
    pub struct ResizeFlags: u8 {
        /// FromFront Adds/removes bytes from the beginning instead of the end.
        const FROM_FRONT = 1;
        /// GrowOnly will only allow the byte[] size to increase.
        const GROW_ONLY = 2;
        /// ShrinkOnly will only allow the byte[] size to decrease.
        const SHRINK_ONLY = 4;
    }
}

bitflags! {
    /// `CdtBitwiseWriteFlags` specify bitwise operation policy write flags.
    #[derive(Clone, Copy, Debug)]
    pub struct WriteFlags: u8 {
        /// CreateOnly specifies that:
        /// If the bin already exists, the operation will be denied.
        /// If the bin does not exist, a new bin will be created.
        const CREATE_ONLY = 1;
        /// UpdateOnly specifies that:
        /// If the bin already exists, the bin will be overwritten.
        /// If the bin does not exist, the operation will be denied.
        const UPDATE_ONLY = 2;
        /// NoFail specifies not to raise error if operation is denied.
        const NO_FAIL = 4;
        /// Partial allows other valid operations to be committed if this operations is
        /// denied due to flag constraints.
        const PARTIAL = 8;
    }

}

/// `CdtBitwiseOverflowActions` specifies the action to take when bitwise add/subtract results in
/// overflow/underflow.
#[derive(Clone, Copy, Debug)]
pub enum OverflowAction {
    /// Fail specifies to fail operation with error.
    Fail = 0,
    /// Saturate specifies that in add/subtract overflows/underflows, set to max/min value.
    /// Example: MAXINT + 1 = MAXINT
    Saturate = 2,
    /// Wrap specifies that in add/subtract overflows/underflows, wrap the value.
    /// Example: MAXINT + 1 = -1
    Wrap = 4,
}

/// `BitPolicy` determines the Bit operation policy.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// The flags determined by CdtBitwiseWriteFlags
    pub flags: WriteFlags,
}

impl Policy {
    /// Creates a new `BitPolicy` with defined `CdtBitwiseWriteFlags`
    #[must_use]
    pub const fn new(flags: WriteFlags) -> Self {
        Self { flags }
    }
}

impl Default for Policy {
    /// Returns the default `BitPolicy`
    fn default() -> Self {
        Self::new(WriteFlags::empty())
    }
}

#[inline]
const fn write<'a>(bin: &'a str, op: OpType, args: Vec<cdt::Argument<'a>>) -> Operation<'a> {
    Operation {
        op: OperationType::BitWrite,
        ctx: &[],
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt::Operation {
            op: op as u8,
            encoder: Encoder::CdtBit,
            args,
        }),
    }
}

#[inline]
const fn read<'a>(bin: &'a str, op: OpType, args: Vec<cdt::Argument<'a>>) -> Operation<'a> {
    Operation {
        op: OperationType::BitRead,
        ctx: &[],
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt::Operation {
            op: op as u8,
            encoder: Encoder::CdtBit,
            args,
        }),
    }
}

/// Creates byte "resize" operation.
/// Server resizes byte[] to byteSize according to resizeFlags.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010]
/// byteSize = 4
/// resizeFlags = 0
/// bin result = [0b00000001, 0b01000010, 0b00000000, 0b00000000]
/// ```
#[must_use]
pub fn resize(
    bin: &str,
    byte_size: i64,
    resize_flags: ResizeFlags,
    policy: Policy,
) -> Operation<'_> {
    let mut args = vec![
        cdt::Argument::Int(byte_size),
        cdt::Argument::Byte(policy.flags.bits()),
    ];
    if !resize_flags.is_empty() {
        args.push(cdt::Argument::Byte(resize_flags.bits()));
    }

    write(bin, OpType::Resize, args)
}

/// Creates byte "insert" operation.
/// Server inserts value bytes into byte[] bin at byteOffset.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// byteOffset = 1
/// value = [0b11111111, 0b11000111]
/// bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// ```
#[must_use]
pub fn insert<'a>(
    bin: &'a str,
    byte_offset: i64,
    value: &'a Value,
    policy: Policy,
) -> Operation<'a> {
    write(
        bin,
        OpType::Insert,
        vec![
            cdt::Argument::Int(byte_offset),
            cdt::Argument::Value(value),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Creates byte "remove" operation.
/// Server removes bytes from byte[] bin at byteOffset for byteSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// byteOffset = 2
/// byteSize = 3
/// bin result = [0b00000001, 0b01000010]
/// ```
#[must_use]
pub fn remove(bin: &str, byte_offset: i64, byte_size: i64, policy: Policy) -> Operation<'_> {
    write(
        bin,
        OpType::Remove,
        vec![
            cdt::Argument::Int(byte_offset),
            cdt::Argument::Int(byte_size),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Creates bit "set" operation.
/// Server sets value on byte[] bin at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 13
/// bitSize = 3
/// value = [0b11100000]
/// bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]
/// ```
#[must_use]
pub fn set<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: Policy,
) -> Operation<'a> {
    write(
        bin,
        OpType::Set,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Value(value),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Creates bit "or" operation.
/// Server performs bitwise "or" on value and byte[] bin at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 17
/// bitSize = 6
/// value = [0b10101000]
/// bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]
/// ```
#[must_use]
pub fn or<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: Policy,
) -> Operation<'a> {
    write(
        bin,
        OpType::Or,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Value(value),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Creates bit "exclusive or" operation.
/// Server performs bitwise "xor" on value and byte[] bin at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 17
/// bitSize = 6
/// value = [0b10101100]
/// bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]
/// ```
#[must_use]
pub fn xor<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: Policy,
) -> Operation<'a> {
    write(
        bin,
        OpType::Xor,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Value(value),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Creates bit "and" operation.
/// Server performs bitwise "and" on value and byte[] bin at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 23
/// bitSize = 9
/// value = [0b00111100, 0b10000000]
/// bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]
/// ```
#[must_use]
pub fn and<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: Policy,
) -> Operation<'a> {
    write(
        bin,
        OpType::And,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Value(value),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Creates bit "not" operation.
/// Server negates byte[] bin starting at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 25
/// bitSize = 6
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]
/// ```
#[must_use]
pub fn not(bin: &str, bit_offset: i64, bit_size: i64, policy: Policy) -> Operation<'_> {
    write(
        bin,
        OpType::Not,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Creates bit "left shift" operation.
/// Server shifts left byte[] bin starting at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 32
/// bitSize = 8
/// shift = 3
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]
/// ```
#[must_use]
pub fn lshift(
    bin: &str,
    bit_offset: i64,
    bit_size: i64,
    shift: i64,
    policy: Policy,
) -> Operation<'_> {
    write(
        bin,
        OpType::Lshift,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Int(shift),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Creates bit "right shift" operation.
/// Server shifts right byte[] bin starting at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 0
/// bitSize = 9
/// shift = 1
/// bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]
/// ```
#[must_use]
pub fn rshift(
    bin: &str,
    bit_offset: i64,
    bit_size: i64,
    shift: i64,
    policy: Policy,
) -> Operation<'_> {
    write(
        bin,
        OpType::Rshift,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Int(shift),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Creates bit "add" operation.
/// Server adds value to byte[] bin starting at bitOffset for bitSize. `BitSize` must be <= 64.
/// Signed indicates if bits should be treated as a signed number.
/// If add overflows/underflows, `CdtBitwiseOverflowAction` is used.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 24
/// bitSize = 16
/// value = 128
/// signed = false
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]
/// ```
#[must_use]
pub fn add(
    bin: &str,
    bit_offset: i64,
    bit_size: i64,
    value: i64,
    signed: bool,
    action: OverflowAction,
    policy: Policy,
) -> Operation<'_> {
    let mut action_flags = action as u8;
    if signed {
        action_flags |= 1;
    }

    write(
        bin,
        OpType::Add,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Int(value),
            cdt::Argument::Byte(policy.flags.bits()),
            cdt::Argument::Byte(action_flags),
        ],
    )
}

/// Creates bit "subtract" operation.
/// Server subtracts value from byte[] bin starting at bitOffset for bitSize. `bit_size` must be <=
/// 64. Signed indicates if bits should be treated as a signed number.
/// If add overflows/underflows, `CdtBitwiseOverflowAction` is used.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 24
/// bitSize = 16
/// value = 128
/// signed = false
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]
/// ```
#[must_use]
pub fn subtract(
    bin: &str,
    bit_offset: i64,
    bit_size: i64,
    value: i64,
    signed: bool,
    action: OverflowAction,
    policy: Policy,
) -> Operation<'_> {
    let mut action_flags = action as u8;
    if signed {
        action_flags |= 1;
    }

    write(
        bin,
        OpType::Subtract,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Int(value),
            cdt::Argument::Byte(policy.flags.bits()),
            cdt::Argument::Byte(action_flags),
        ],
    )
}

/// Creates bit "setInt" operation.
/// Server sets value to byte[] bin starting at bitOffset for bitSize. Size must be <= 64.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 1
/// bitSize = 8
/// value = 127
/// bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]
/// ```
#[must_use]
pub fn set_int(
    bin: &str,
    bit_offset: i64,
    bit_size: i64,
    value: i64,
    policy: Policy,
) -> Operation<'_> {
    write(
        bin,
        OpType::SetInt,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Int(value),
            cdt::Argument::Byte(policy.flags.bits()),
        ],
    )
}

/// Creates bit "get" operation.
/// Server returns bits from byte[] bin starting at bitOffset for bitSize.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 9
/// bitSize = 5
/// returns [0b1000000]
/// ```
#[must_use]
pub fn get(bin: &str, bit_offset: i64, bit_size: i64) -> Operation<'_> {
    read(
        bin,
        OpType::Get,
        vec![cdt::Argument::Int(bit_offset), cdt::Argument::Int(bit_size)],
    )
}

/// Creates bit "count" operation.
/// Server returns integer count of set bits from byte[] bin starting at bitOffset for bitSize.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 20
/// bitSize = 4
/// returns 2
/// ```
#[must_use]
pub fn count(bin: &str, bit_offset: i64, bit_size: i64) -> Operation<'_> {
    read(
        bin,
        OpType::Count,
        vec![cdt::Argument::Int(bit_offset), cdt::Argument::Int(bit_size)],
    )
}

/// Creates bit "left scan" operation.
/// Server returns integer bit offset of the first specified value bit in byte[] bin
/// starting at bitOffset for bitSize.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 24
/// bitSize = 8
/// value = true
/// returns 5
/// ```
#[must_use]
pub fn lscan(bin: &str, bit_offset: i64, bit_size: i64, value: bool) -> Operation<'_> {
    read(
        bin,
        OpType::Lscan,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Bool(value),
        ],
    )
}

/// Creates bit "right scan" operation.
/// Server returns integer bit offset of the last specified value bit in byte[] bin
/// starting at bitOffset for bitSize.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 32
/// bitSize = 8
/// value = true
/// returns 7
/// ```
#[must_use]
pub fn rscan(bin: &str, bit_offset: i64, bit_size: i64, value: bool) -> Operation<'_> {
    read(
        bin,
        OpType::Rscan,
        vec![
            cdt::Argument::Int(bit_offset),
            cdt::Argument::Int(bit_size),
            cdt::Argument::Bool(value),
        ],
    )
}

/// Creates bit "get integer" operation.
/// Server returns integer from byte[] bin starting at bitOffset for bitSize.
/// Signed indicates if bits should be treated as a signed number.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 8
/// bitSize = 16
/// signed = false
/// returns 16899
/// ```
#[must_use]
pub fn get_int(bin: &str, bit_offset: i64, bit_size: i64, signed: bool) -> Operation<'_> {
    let mut args = vec![cdt::Argument::Int(bit_offset), cdt::Argument::Int(bit_size)];
    if signed {
        args.push(cdt::Argument::Byte(1));
    }

    read(bin, OpType::GetInt, args)
}
