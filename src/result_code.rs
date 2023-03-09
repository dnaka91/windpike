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

use std::{borrow::Cow, fmt, result::Result as StdResult};

/// Database operation error codes. The error codes are defined in the server-side file proto.h.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultCode {
    /// OperationType was successful.
    Ok,
    /// Unknown server failure.
    ServerError,
    /// On retrieving, touching or replacing a record that doesn't exist.
    KeyNotFoundError,
    /// On modifying a record with unexpected generation.
    GenerationError,
    /// Bad parameter(s) were passed in database operation call.
    ParameterError,
    /// On create-only (write unique) operations on a record that already exists.
    KeyExistsError,
    /// On create-only (write unique) operations on a bin that already exists.
    BinExistsError,
    /// Expected cluster Id was not received.
    ClusterKeyMismatch,
    /// Server has run out of memory.
    ServerMemError,
    /// Client or server has timed out.
    Timeout,
    /// Xds product is not available.
    NoXds,
    /// Server is not accepting requests.
    ServerNotAvailable,
    /// OperationType is not supported with configured bin type (single-bin or multi-bin).
    BinTypeError,
    /// Record size exceeds limit.
    RecordTooBig,
    /// Too many concurrent operations on the same record.
    KeyBusy,
    /// Scan aborted by server.
    ScanAbort,
    /// Unsupported Server Feature (e.g. Scan + Udf)
    UnsupportedFeature,
    /// Specified bin name does not exist in record.
    BinNotFound,
    /// Specified bin name does not exist in record.
    DeviceOverload,
    /// Key type mismatch.
    KeyMismatch,
    /// Invalid namespace.
    InvalidNamespace,
    /// Bin name length greater than 14 characters.
    BinNameTooLong,
    /// OperationType not allowed at this time.
    FailForbidden,
    /// Returned by Map put and put_items operations when policy is REPLACE but key was not found.
    ElementNotFound,
    /// Returned by Map put and put_items operations when policy is CREATE_ONLY but key already
    /// exists.
    ElementExists,
    /// Enterprise-only feature not supported by the community edition
    EnterpriseOnly,
    /// There are no more records left for query.
    QueryEnd,
    /// Security type not supported by connected server.
    SecurityNotSupported,
    /// Administration command is invalid.
    SecurityNotEnabled,
    /// Administration field is invalid.
    SecuritySchemeNotSupported,
    /// Administration command is invalid.
    InvalidCommand,
    /// Administration field is invalid.
    InvalidField,
    /// Security protocol not followed.
    IllegalState,
    /// User name is invalid.
    InvalidUser,
    /// User was previously created.
    UserAlreadyExists,
    /// Password is invalid.
    InvalidPassword,
    /// Security credential is invalid.
    ExpiredPassword,
    /// Forbidden password (e.g. recently used)
    ForbiddenPassword,
    /// Security credential is invalid.
    InvalidCredential,
    /// Role name is invalid.
    InvalidRole,
    /// Role already exists.
    RoleAlreadyExists,
    /// Privilege is invalid.
    InvalidPrivilege,
    /// User must be authentication before performing database operations.
    NotAuthenticated,
    /// User does not posses the required role to perform the database operation.
    RoleViolation,
    /// A user defined function returned an error code.
    UdfBadResponse,
    /// The requested item in a large collection was not found.
    LargeItemNotFound,
    /// Batch functionality has been disabled.
    BatchDisabled,
    /// Batch max requests have been exceeded.
    BatchMaxRequestsExceeded,
    /// All batch queues are full.
    BatchQueuesFull,
    /// Secondary index already exists.
    IndexFound,
    /// Requested secondary index does not exist.
    IndexNotFound,
    /// Secondary index memory space exceeded.
    IndexOom,
    /// Secondary index not available.
    IndexNotReadable,
    /// Generic secondary index error.
    IndexGeneric,
    /// Index name maximum length exceeded.
    IndexNameMaxLen,
    /// Maximum number of indicies exceeded.
    IndexMaxCount,
    /// Secondary index query aborted.
    QueryAborted,
    /// Secondary index queue full.
    QueryQueueFull,
    /// Secondary index query timed out on server.
    QueryTimeout,
    /// Generic query error.
    QueryGeneric,
    /// Query NetIo error on server
    QueryNetioErr,
    /// Duplicate TaskId sent for the statement
    QueryDuplicate,
    /// Unknown server result code
    Unknown(u8),
}

impl ResultCode {
    /// Convert the result code from the server response.
    #[must_use]
    pub(crate) const fn from_u8(n: u8) -> Self {
        match n {
            0 => Self::Ok,
            1 => Self::ServerError,
            2 => Self::KeyNotFoundError,
            3 => Self::GenerationError,
            4 => Self::ParameterError,
            5 => Self::KeyExistsError,
            6 => Self::BinExistsError,
            7 => Self::ClusterKeyMismatch,
            8 => Self::ServerMemError,
            9 => Self::Timeout,
            10 => Self::NoXds,
            11 => Self::ServerNotAvailable,
            12 => Self::BinTypeError,
            13 => Self::RecordTooBig,
            14 => Self::KeyBusy,
            15 => Self::ScanAbort,
            16 => Self::UnsupportedFeature,
            17 => Self::BinNotFound,
            18 => Self::DeviceOverload,
            19 => Self::KeyMismatch,
            20 => Self::InvalidNamespace,
            21 => Self::BinNameTooLong,
            22 => Self::FailForbidden,
            23 => Self::ElementNotFound,
            24 => Self::ElementExists,
            25 => Self::EnterpriseOnly,
            50 => Self::QueryEnd,
            51 => Self::SecurityNotSupported,
            52 => Self::SecurityNotEnabled,
            53 => Self::SecuritySchemeNotSupported,
            54 => Self::InvalidCommand,
            55 => Self::InvalidField,
            56 => Self::IllegalState,
            60 => Self::InvalidUser,
            61 => Self::UserAlreadyExists,
            62 => Self::InvalidPassword,
            63 => Self::ExpiredPassword,
            64 => Self::ForbiddenPassword,
            65 => Self::InvalidCredential,
            70 => Self::InvalidRole,
            71 => Self::RoleAlreadyExists,
            72 => Self::InvalidPrivilege,
            80 => Self::NotAuthenticated,
            81 => Self::RoleViolation,
            100 => Self::UdfBadResponse,
            125 => Self::LargeItemNotFound,
            150 => Self::BatchDisabled,
            151 => Self::BatchMaxRequestsExceeded,
            152 => Self::BatchQueuesFull,
            200 => Self::IndexFound,
            201 => Self::IndexNotFound,
            202 => Self::IndexOom,
            203 => Self::IndexNotReadable,
            204 => Self::IndexGeneric,
            205 => Self::IndexNameMaxLen,
            206 => Self::IndexMaxCount,
            210 => Self::QueryAborted,
            211 => Self::QueryQueueFull,
            212 => Self::QueryTimeout,
            213 => Self::QueryGeneric,
            214 => Self::QueryNetioErr,
            215 => Self::QueryDuplicate,
            code => Self::Unknown(code),
        }
    }

    /// Convert a result code into an string.
    #[must_use]
    pub fn into_string(self) -> Cow<'static, str> {
        match self {
            Self::Ok => "ok".into(),
            Self::ServerError => "Server error".into(),
            Self::KeyNotFoundError => "Key not found".into(),
            Self::GenerationError => "Generation error".into(),
            Self::ParameterError => "Parameter error".into(),
            Self::KeyExistsError => "Key already exists".into(),
            Self::BinExistsError => "Bin already exists".into(),
            Self::ClusterKeyMismatch => "Cluster key mismatch".into(),
            Self::ServerMemError => "Server memory error".into(),
            Self::Timeout => "Timeout".into(),
            Self::NoXds => "Xds not available".into(),
            Self::ServerNotAvailable => "Server not available".into(),
            Self::BinTypeError => "Bin type error".into(),
            Self::RecordTooBig => "Record too big".into(),
            Self::KeyBusy => "Hot key".into(),
            Self::ScanAbort => "Scan aborted".into(),
            Self::UnsupportedFeature => "Unsupported Server Feature".into(),
            Self::BinNotFound => "Bin not found".into(),
            Self::DeviceOverload => "Device overload".into(),
            Self::KeyMismatch => "Key mismatch".into(),
            Self::InvalidNamespace => "Namespace not found".into(),
            Self::BinNameTooLong => "Bin name length greater than 14 characters".into(),
            Self::FailForbidden => "OperationType not allowed at this time".into(),
            Self::ElementNotFound => "Element not found".into(),
            Self::ElementExists => "Element already exists".into(),
            Self::EnterpriseOnly => {
                "Enterprise-only feature not supported by community edition".into()
            }
            Self::QueryEnd => "Query end".into(),
            Self::SecurityNotSupported => "Security not supported".into(),
            Self::SecurityNotEnabled => "Security not enabled".into(),
            Self::SecuritySchemeNotSupported => "Security scheme not supported".into(),
            Self::InvalidCommand => "Invalid command".into(),
            Self::InvalidField => "Invalid field".into(),
            Self::IllegalState => "Illegal state".into(),
            Self::InvalidUser => "Invalid user".into(),
            Self::UserAlreadyExists => "User already exists".into(),
            Self::InvalidPassword => "Invalid password".into(),
            Self::ExpiredPassword => "Expired password".into(),
            Self::ForbiddenPassword => "Forbidden password".into(),
            Self::InvalidCredential => "Invalid credential".into(),
            Self::InvalidRole => "Invalid role".into(),
            Self::RoleAlreadyExists => "Role already exists".into(),
            Self::InvalidPrivilege => "Invalid privilege".into(),
            Self::NotAuthenticated => "Not authenticated".into(),
            Self::RoleViolation => "Role violation".into(),
            Self::UdfBadResponse => "Udf returned error".into(),
            Self::LargeItemNotFound => "Large collection item not found".into(),
            Self::BatchDisabled => "Batch functionality has been disabled".into(),
            Self::BatchMaxRequestsExceeded => "Batch max requests have been exceeded".into(),
            Self::BatchQueuesFull => "All batch queues are full".into(),
            Self::IndexFound => "Index already exists".into(),
            Self::IndexNotFound => "Index not found".into(),
            Self::IndexOom => "Index out of memory".into(),
            Self::IndexNotReadable => "Index not readable".into(),
            Self::IndexGeneric => "Index error".into(),
            Self::IndexNameMaxLen => "Index name max length exceeded".into(),
            Self::IndexMaxCount => "Index count exceeds max".into(),
            Self::QueryAborted => "Query aborted".into(),
            Self::QueryQueueFull => "Query queue full".into(),
            Self::QueryTimeout => "Query timeout".into(),
            Self::QueryGeneric => "Query error".into(),
            Self::QueryNetioErr => "Query NetIo error on server".into(),
            Self::QueryDuplicate => "Duplicate TaskId sent for the statement".into(),
            Self::Unknown(code) => format!("Unknown server error code: {code}").into(),
        }
    }
}

impl From<u8> for ResultCode {
    fn from(val: u8) -> Self {
        Self::from_u8(val)
    }
}

impl fmt::Display for ResultCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> StdResult<(), fmt::Error> {
        write!(f, "{}", self.into_string())
    }
}

#[cfg(test)]
mod tests {
    use super::ResultCode;

    #[test]
    fn from_result_code() {
        assert_eq!(ResultCode::KeyNotFoundError, ResultCode::from(2u8));
    }

    #[test]
    fn from_unknown_result_code() {
        assert_eq!(ResultCode::Unknown(234), ResultCode::from(234u8));
    }

    #[test]
    fn into_string() {
        let result = ResultCode::KeyNotFoundError.into_string();
        assert_eq!("Key not found", result);
    }

    #[test]
    fn unknown_into_string() {
        let result = ResultCode::Unknown(234).into_string();
        assert_eq!("Unknown server error code: 234", result);
    }
}
