//! String/number bin operations. Create operations used by the client's `operate()` method.

use crate::{
    operations::{cdt_context::DEFAULT_CTX, Operation, OperationBin, OperationData, OperationType},
    Bin,
};

/// Create read all record bins database operation.
#[must_use]
pub const fn get<'a>() -> Operation<'a> {
    Operation {
        op: OperationType::Read,
        ctx: DEFAULT_CTX,
        bin: OperationBin::All,
        data: OperationData::None,
    }
}

/// Create read record header database operation.
#[must_use]
pub const fn get_header<'a>() -> Operation<'a> {
    Operation {
        op: OperationType::Read,
        ctx: DEFAULT_CTX,
        bin: OperationBin::None,
        data: OperationData::None,
    }
}

/// Create read bin database operation.
#[must_use]
pub const fn get_bin(bin_name: &str) -> Operation<'_> {
    Operation {
        op: OperationType::Read,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin_name),
        data: OperationData::None,
    }
}

/// Create set database operation.
#[must_use]
pub const fn put<'a>(bin: &'a Bin<'_>) -> Operation<'a> {
    Operation {
        op: OperationType::Write,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.name),
        data: OperationData::Value(&bin.value),
    }
}

/// Create string append database operation.
#[must_use]
pub const fn append<'a>(bin: &'a Bin<'_>) -> Operation<'a> {
    Operation {
        op: OperationType::Append,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.name),
        data: OperationData::Value(&bin.value),
    }
}

/// Create string prepend database operation.
#[must_use]
pub const fn prepend<'a>(bin: &'a Bin<'_>) -> Operation<'a> {
    Operation {
        op: OperationType::Prepend,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.name),
        data: OperationData::Value(&bin.value),
    }
}

/// Create integer add database operation.
#[must_use]
pub const fn add<'a>(bin: &'a Bin<'_>) -> Operation<'a> {
    Operation {
        op: OperationType::Incr,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.name),
        data: OperationData::Value(&bin.value),
    }
}

/// Create touch database operation.
#[must_use]
pub const fn touch<'a>() -> Operation<'a> {
    Operation {
        op: OperationType::Touch,
        ctx: DEFAULT_CTX,
        bin: OperationBin::None,
        data: OperationData::None,
    }
}

/// Create delete database operation
#[must_use]
pub const fn delete<'a>() -> Operation<'a> {
    Operation {
        op: OperationType::Delete,
        ctx: DEFAULT_CTX,
        bin: OperationBin::None,
        data: OperationData::None,
    }
}
