use crate::heapfile::HeapFile;
use crate::page::PageIntoIter;
use common::prelude::*;
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    pub(crate) tid: TransactionId,
    pub(crate) hf: Arc<HeapFile>,
    pub(crate) page_iter: PageIntoIter,
    pub(crate) max_pid: PageId,
    pub(crate) curr_pid: PageId,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        let page = hf.read_page_from_file(0).unwrap();
        let page_iter: PageIntoIter = page.into_iter();
        let max_pid = hf.num_pages(); // PageId uses 0-based indexing
        HeapFileIterator {
            tid,
            hf,
            page_iter,
            max_pid,
            curr_pid: 0,
        }
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        // get the item
        let mut item = self.page_iter.next();
        if item.is_none() {
            // reached the end of the page
            self.curr_pid += 1;
            if self.curr_pid >= self.max_pid {
                // invalid PageId
                return None;
            }
            self.page_iter = self
                .hf
                .read_page_from_file(self.curr_pid)
                .unwrap()
                .into_iter();
            item = self.page_iter.next();
        }
        // extract values and slot_id from item
        let values = item.clone().unwrap().0;
        let slotid: SlotId = item.unwrap().1;
        // create valueid and return item
        let valueid = ValueId::new_slot(self.hf.container_id, self.curr_pid, slotid);
        let item: Self::Item = (values, valueid);
        Some(item)
    }
}
