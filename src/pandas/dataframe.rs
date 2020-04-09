use crate::pandas::Columns;
// use crate::pandas::Index;
use arrow::array::Array;
// use std::sync::Arc;

/// dataframe struct and impl
#[warn(dead_code)]
pub struct Dataframe {
    /// columns's data，like
    /// data = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
    ///         'Age':[27, 24, 22, 32],
    ///         'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
    ///         'Qualification':['Msc', 'MA', 'MCA', 'Phd']}
    /// dyn trait(Index) is dynamic dispatch for polymorphism situation
    /// because trait has impl ,trait is dynamic
    data: Option<Vec<Box<dyn Array>>>,
    columns_name: Option<Vec<Columns>>,
    row_length: Option<u32>,
    column_length: Option<u32>,
    /// index trait
    /// box is a pointer to value in heap, pointer is in stack
    index: Option<Vec<u64>>,
}
