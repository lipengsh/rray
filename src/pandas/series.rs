/// series struct
use std::vec::Vec;
use arrow::buffer::MutableBuffer;

/// like pandas.series
pub struct Series {
    /// series's name
    name: String,

    /// series's type
    stype: String,

    /// data vec ,type is T
    data: Vec<MutableBuffer>,
}
