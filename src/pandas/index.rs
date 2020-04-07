/// index and multi-index
use arrow::array::Array;
use hashbrown::hash_map::DefaultHashBuilder;

pub trait Index {
    /// set index in the columns, one or more columns
    fn create_index(columns: Vec<Box<dyn Array>>) -> Option<Vec<u64>>;
}

pub struct HashIndex {
    // hash index's count
    count: u32,

    // hash code, u64 type
    // vec's index is row number
    hasher: Vec<u64>,
}

impl Index for HashIndex {
    fn create_index(columns: Vec<Box<dyn Array>>) -> Option<Vec<u64>> {
        let iter = columns.into_iter();

        None
    }
}
