//! Types and methods used for long running status queries.

pub use self::{
    index_task::IndexTask,
    task::{Status, Task},
};

mod index_task;
#[allow(clippy::module_inception)]
mod task;
