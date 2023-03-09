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

use std::{fmt, result::Result as StdResult};

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
    pub fn into_string(self) -> String {
        match self {
            Self::Ok => String::from("ok"),
            Self::ServerError => String::from("Server error"),
            Self::KeyNotFoundError => String::from("Key not found"),
            Self::GenerationError => String::from("Generation error"),
            Self::ParameterError => String::from("Parameter error"),
            Self::KeyExistsError => String::from("Key already exists"),
            Self::BinExistsError => String::from("Bin already exists"),
            Self::ClusterKeyMismatch => String::from("Cluster key mismatch"),
            Self::ServerMemError => String::from("Server memory error"),
            Self::Timeout => String::from("Timeout"),
            Self::NoXds => String::from("Xds not available"),
            Self::ServerNotAvailable => String::from("Server not available"),
            Self::BinTypeError => String::from("Bin type error"),
            Self::RecordTooBig => String::from("Record too big"),
            Self::KeyBusy => String::from("Hot key"),
            Self::ScanAbort => String::from("Scan aborted"),
            Self::UnsupportedFeature => String::from("Unsupported Server Feature"),
            Self::BinNotFound => String::from("Bin not found"),
            Self::DeviceOverload => String::from("Device overload"),
            Self::KeyMismatch => String::from("Key mismatch"),
            Self::InvalidNamespace => String::from("Namespace not found"),
            Self::BinNameTooLong => String::from("Bin name length greater than 14 characters"),
            Self::FailForbidden => String::from("OperationType not allowed at this time"),
            Self::ElementNotFound => String::from("Element not found"),
            Self::ElementExists => String::from("Element already exists"),
            Self::EnterpriseOnly => {
                String::from("Enterprise-only feature not supported by community edition")
            }
            Self::QueryEnd => String::from("Query end"),
            Self::SecurityNotSupported => String::from("Security not supported"),
            Self::SecurityNotEnabled => String::from("Security not enabled"),
            Self::SecuritySchemeNotSupported => String::from("Security scheme not supported"),
            Self::InvalidCommand => String::from("Invalid command"),
            Self::InvalidField => String::from("Invalid field"),
            Self::IllegalState => String::from("Illegal state"),
            Self::InvalidUser => String::from("Invalid user"),
            Self::UserAlreadyExists => String::from("User already exists"),
            Self::InvalidPassword => String::from("Invalid password"),
            Self::ExpiredPassword => String::from("Expired password"),
            Self::ForbiddenPassword => String::from("Forbidden password"),
            Self::InvalidCredential => String::from("Invalid credential"),
            Self::InvalidRole => String::from("Invalid role"),
            Self::RoleAlreadyExists => String::from("Role already exists"),
            Self::InvalidPrivilege => String::from("Invalid privilege"),
            Self::NotAuthenticated => String::from("Not authenticated"),
            Self::RoleViolation => String::from("Role violation"),
            Self::UdfBadResponse => String::from("Udf returned error"),
            Self::LargeItemNotFound => String::from("Large collection item not found"),
            Self::BatchDisabled => String::from("Batch functionality has been disabled"),
            Self::BatchMaxRequestsExceeded => String::from("Batch max requests have been exceeded"),
            Self::BatchQueuesFull => String::from("All batch queues are full"),
            Self::IndexFound => String::from("Index already exists"),
            Self::IndexNotFound => String::from("Index not found"),
            Self::IndexOom => String::from("Index out of memory"),
            Self::IndexNotReadable => String::from("Index not readable"),
            Self::IndexGeneric => String::from("Index error"),
            Self::IndexNameMaxLen => String::from("Index name max length exceeded"),
            Self::IndexMaxCount => String::from("Index count exceeds max"),
            Self::QueryAborted => String::from("Query aborted"),
            Self::QueryQueueFull => String::from("Query queue full"),
            Self::QueryTimeout => String::from("Query timeout"),
            Self::QueryGeneric => String::from("Query error"),
            Self::QueryNetioErr => String::from("Query NetIo error on server"),
            Self::QueryDuplicate => String::from("Duplicate TaskId sent for the statement"),
            Self::Unknown(code) => format!("Unknown server error code: {code}"),
        }
    }
}

impl From<u8> for ResultCode {
    fn from(val: u8) -> Self {
        Self::from_u8(val)
    }
}

impl From<ResultCode> for String {
    fn from(code: ResultCode) -> Self {
        code.into_string()
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
        let result: String = ResultCode::KeyNotFoundError.into();
        assert_eq!("Key not found", result);
    }

    #[test]
    fn unknown_into_string() {
        let result: String = ResultCode::Unknown(234).into();
        assert_eq!("Unknown server error code: 234", result);
    }
}
