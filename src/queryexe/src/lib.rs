#[macro_use]
extern crate log;

pub mod mutator;
pub mod opiterator;
pub mod query;
pub use heapstore::storage_manager::StorageManager;
// pub use memstore::storage_manager::StorageManager;

pub use txn_manager::mock_tm::MockTransactionManager as TransactionManager;
