pub mod page;
pub mod file;
pub mod mem;

pub use page::{Page, PAGE_SIZE};
pub use file::FileStorage;
pub use mem::MemStorage;
