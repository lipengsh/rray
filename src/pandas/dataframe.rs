use crate::pandas::index::{HashIndex, Index};
use arrow::array;
use arrow::array::{Array, UInt32Array};
use arrow::datatypes::DataType;
use rayon::prelude::*;

/// dataframe struct and impl
pub struct Dataframe<'a> {
    /// columns's data，like
    /// data = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
    ///         'Age':[27, 24, 22, 32],
    ///         'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
    ///         'Qualification':['Msc', 'MA', 'MCA', 'Phd']}
    /// dyn trait(Index) is dynamic dispatch for polymorphism situation
    /// because trait has impl ,trait is dynamic
    data: &'a [f32],
    columns_name: &'a [String],
    row_length: u32,
    column_length: u32,
    /// index trait
    /// box is a pointer to value in heap, pointer is in stack
    /// hash index
    index: &'a [u32],
}

impl<'a> Dataframe<'a> {
    fn new(
        data: &'a [f32],
        row_length: u32,
        column_length: u32,
        columns_name: &'a [String],
    ) -> Self {
        Dataframe {
            data,
            column_length,
            row_length,
            columns_name,
            index: &[0u32],
        }
    }
}
