use std::borrow::Cow;

/// Database operation error codes. The error codes are defined in the server-side file proto.h.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultCode {
    /// Operation was successful.
    Ok,
    /// Unknown server failure.
    ServerError,
    /// Retrieving, touching or replacing a record that doesn't exist.
    KeyNotFoundError,
    /// Modifying a record with unexpected generation.
    GenerationError,
    /// Bad parameter(s) were passed in database operation call.
    ParameterError,
    /// Create-only (write unique) operations on a record that already exists.
    KeyExistsError,
    /// Bin already exists on a create-only operation.
    BinExistsError,
    /// Expected cluster ID was not received.
    ClusterKeyMismatch,
    /// Server has run out of memory.
    ServerMemError,
    /// Client or server has timed out.
    Timeout,
    /// Operation not allowed in current configuration.
    AlwaysForbidden,
    /// Partition is unavailable.
    PartitionUnavailable,
    /// Operation is not supported with configured bin type (single-bin or multi-bin).
    BinTypeError,
    /// Record size exceeds limit.
    RecordTooBig,
    /// Too many concurrent operations on the same record.
    KeyBusy,
    /// Scan aborted by server.
    ScanAbort,
    /// Unsupported server feature (e.g. Scan + UDF).
    UnsupportedFeature,
    /// Bin not found on update-only operation.
    BinNotFound,
    /// Device not keeping up with writes.
    DeviceOverload,
    /// Key type mismatch.
    KeyMismatch,
    /// Invalid namespace.
    InvalidNamespace,
    /// Bin name length greater than 15 characters or maximum bins exceeded.
    BinNameTooLong,
    /// Operation not allowed at this time.
    FailForbidden,
    /// Element not found in CDT.
    ElementNotFound,
    /// Element already exists in CDT.
    ElementExists,
    /// Attempt to use an Enterprise feature on a community server or a server without the applicable feature key.
    EnterpriseOnly,
    /// The operation cannot be applied to the current bin value on the server.
    OpNotApplicable,
    /// The transaction was not performed because the filter was false.
    FilteredOut,
    /// Write command loses conflict to XDR.
    LostConflict,
    /// There are no more records left for query.
    QueryEnd,
    /// Security functionality not supported by connected server.
    SecurityNotSupported,
    /// Security functionality supported, but not enabled by connected server.
    SecurityNotEnabled,
    /// Security configuration not supported.
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
    /// Password has expired.
    ExpiredPassword,
    /// Forbidden password (e.g. recently used)
    ForbiddenPassword,
    /// Security credential is invalid.
    InvalidCredential,
    /// Login session expired.
    InvalidSession,
    /// Role name is invalid.
    InvalidRole,
    /// Role already exists.
    RoleAlreadyExists,
    /// Privilege is invalid.
    InvalidPrivilege,
    /// Invalid IP address whiltelist.
    InvalidWhitelist,
    /// Quotas not enabled on server.
    QuotasNotEnabled,
    /// Invalid quota value.
    InvalidQuota,
    /// User must be authentication before performing database operations.
    NotAuthenticated,
    /// User does not posses the required role to perform the database operation.
    RoleViolation,
    /// Command not allowed because sender IP address not whitelisted.
    NotWhitelisted,
    /// Quota exceeded.
    QuotaExceeded,
    /// A user defined function returned an error code.
    UdfBadResponse,
    /// Batch functionality has been disabled.
    BatchDisabled,
    /// Batch max requests have been exceeded.
    BatchMaxRequestsExceeded,
    /// All batch queues are full.
    BatchQueuesFull,
    /// Secondary index already exists.
    IndexAlreadyExists,
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
    /// Unknown server result code.
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
            10 => Self::AlwaysForbidden,
            11 => Self::PartitionUnavailable,
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
            26 => Self::OpNotApplicable,
            27 => Self::FilteredOut,
            28 => Self::LostConflict,
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
            66 => Self::InvalidSession,
            70 => Self::InvalidRole,
            71 => Self::RoleAlreadyExists,
            72 => Self::InvalidPrivilege,
            73 => Self::InvalidWhitelist,
            74 => Self::QuotasNotEnabled,
            75 => Self::InvalidQuota,
            80 => Self::NotAuthenticated,
            81 => Self::RoleViolation,
            82 => Self::NotWhitelisted,
            83 => Self::QuotaExceeded,
            100 => Self::UdfBadResponse,
            150 => Self::BatchDisabled,
            151 => Self::BatchMaxRequestsExceeded,
            152 => Self::BatchQueuesFull,
            200 => Self::IndexAlreadyExists,
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
            Self::AlwaysForbidden => "Operation not allowed".into(),
            Self::PartitionUnavailable => "Partitions unavailable".into(),
            Self::BinTypeError => "Bin type error".into(),
            Self::RecordTooBig => "Record too big".into(),
            Self::KeyBusy => "Hot key".into(),
            Self::ScanAbort => "Scan aborted".into(),
            Self::UnsupportedFeature => "Unsupported Server Feature".into(),
            Self::BinNotFound => "Bin not found".into(),
            Self::DeviceOverload => "Device overload".into(),
            Self::KeyMismatch => "Key mismatch".into(),
            Self::InvalidNamespace => "Namespace not found".into(),
            Self::BinNameTooLong => {
                "Bin name length greater than 15 characters or maximum bins exceeded".into()
            }
            Self::FailForbidden => "Operation not allowed at this time".into(),
            Self::ElementNotFound => "Element not found".into(),
            Self::ElementExists => "Element exists".into(),
            Self::EnterpriseOnly => "Enterprise only".into(),
            Self::OpNotApplicable => "Operation not applicable".into(),
            Self::FilteredOut => "Transaction filtered out".into(),
            Self::LostConflict => "Transaction failed due to conflict with XDR".into(),
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
            Self::InvalidSession => "Login session expired".into(),
            Self::InvalidRole => "Invalid role".into(),
            Self::RoleAlreadyExists => "Role already exists".into(),
            Self::InvalidPrivilege => "Invalid privilege".into(),
            Self::InvalidWhitelist => "Invalid whitelist".into(),
            Self::QuotasNotEnabled => "Quotas not enabled".into(),
            Self::InvalidQuota => "Invalid quota".into(),
            Self::NotAuthenticated => "Not authenticated".into(),
            Self::RoleViolation => "Role violation".into(),
            Self::NotWhitelisted => "Command not whitelisted".into(),
            Self::QuotaExceeded => "Quota exceeded".into(),
            Self::UdfBadResponse => "UDF returned error".into(),
            Self::BatchDisabled => "Batch functionality has been disabled".into(),
            Self::BatchMaxRequestsExceeded => "Batch max requests have been exceeded".into(),
            Self::BatchQueuesFull => "All batch queues are full".into(),
            Self::IndexAlreadyExists => "Index already exists".into(),
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
            Self::Unknown(code) => format!("Unknown server error code: {code}").into(),
        }
    }
}

impl From<u8> for ResultCode {
    fn from(val: u8) -> Self {
        Self::from_u8(val)
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
