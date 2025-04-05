use crate::page::Page;
use common::prelude::*;
use common::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

use std::io::BufWriter;
use std::io::{Seek, SeekFrom};

/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub(crate) struct HeapFile {
    pub filename: PathBuf,         // uniquely identifies a HeapFile
    pub file: Arc<RwLock<File>>,   // to maintain persistent connection to a file struct
    pub container_id: ContainerId, // Track this HeapFile's container Id
    // pub page_count: Arc<RwLock<PageId>>, // Track the number of pages in this HeapFile
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf, container_id: ContainerId) -> Result<Self, CrustyError> {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {:?}",
                    file_path.to_string_lossy(),
                    error
                )))
            }
        };

        // let page_num: u16 = (file.metadata().unwrap().len() as usize / PAGE_SIZE) as u16;
        // let page_count: Arc<RwLock<u16>> = Arc::new(RwLock::new(page_num));
        // create a new Arc<RwLock<File>> instance
        let file = Arc::new(RwLock::new(file));

        Ok(HeapFile {
            filename: file_path,
            file,
            container_id,
            // page_count,
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        // *self.page_count.read().unwrap()
        let file = self.file.read().unwrap();
        (file.metadata().unwrap().len() as usize / PAGE_SIZE) as PageId
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    /// Note: that std::io::{Seek, SeekFrom} require Write locks on the underlying std::fs::File
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }

        // Check if the requested PageId is valid
        // if pid >= self.page_count {
        //     return Err(CrustyError::InvalidMutationError(("Invalid PageId requested for read from HeapFile").to_string()));
        // }

        // Calculate the offset in the file where the page should be read
        let offset = (pid as u64) * (PAGE_SIZE as u64);
        // Lock the file for reading
        let mut file = self.file.write().unwrap();
        // Seek to the correct offset
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| CrustyError::IOError(e.to_string()))?;
        // Read the page data from the file
        let mut page_data = vec![0; PAGE_SIZE];
        file.read_exact(&mut page_data)
            .map_err(|e| CrustyError::IOError(e.to_string()))?;
        // Deserialize the page data into a Page struct
        let page = Page::from_bytes(&page_data);

        Ok(page)
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        trace!(
            "Writing page {} to file {}",
            page.get_page_id(),
            self.container_id
        );
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        };

        // Serialize the given Page into a byte array
        let page_data = page.to_bytes();
        // Calculate the offset in the file where the page should be written
        let offset = (page.get_page_id() as u64) * (PAGE_SIZE as u64);
        // Lock the file for writing
        let mut file = self.file.write().unwrap();
        // Seek to the correct offset
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| CrustyError::IOError(e.to_string()))?;

        // Write the serialized page data to the file
        file.write_all(&page_data)
            .map_err(|e| CrustyError::IOError(e.to_string()))?;

        // let start = Instant::now();
        // update page count
        // let file_size = file.metadata().unwrap().len() as usize;
        // let mut page_count: std::sync::RwLockWriteGuard<PageId> = self.page_count.write().unwrap();
        // *page_count = (file_size / PAGE_SIZE) as PageId;
        // let elapsed = start.elapsed();
        // println!("Update page count took: {:?}", elapsed);

        // let start = Instant::now();
        // rewind to the beginning of the file
        // file.rewind().unwrap();
        // let elapsed = start.elapsed();
        // println!("Rewind took: {:?}", elapsed);

        Ok(())
    }

    pub(crate) fn find_free_space(&self, value: &[u8], page_num: PageId) -> Option<PageId> {
        // let page_count: std::sync::RwLockReadGuard<PageId> = self.page_count.read().unwrap();
        // let page_num: PageId = *page_count;
        let mut pid: PageId = 0;
        while pid < page_num {
            let page = self.read_page_from_file(pid).unwrap();
            let value_size = value.len() + 6; // 6 accounts for the slot header size
            if page.get_free_space() >= value_size {
                // this page has enough free space so we return its page_id
                return Some(pid);
            }
            pid += 1;
        }
        Some(page_num)
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_test_sm_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf(), 0).expect("Unable to create HF for test");

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.to_bytes();

        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.to_bytes();

        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.to_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
