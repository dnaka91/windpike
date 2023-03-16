//! Types and methods used for database queries and scans.

pub use self::{
    index_types::{CollectionIndexType, IndexType},
    recordset::Recordset,
    statement::Statement,
};

mod index_types;
mod recordset;
mod statement;
