use crate::pandas::index::{HashIndex, Index};
use crate::pandas::Columns;
use arrow::array;
use arrow::array::Array;
use arrow::datatypes::DataType;
use rayon::prelude::*;

/// dataframe struct and impl
pub struct Dataframe {
    /// columns's data，like
    /// data = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
    ///         'Age':[27, 24, 22, 32],
    ///         'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
    ///         'Qualification':['Msc', 'MA', 'MCA', 'Phd']}
    /// dyn trait(Index) is dynamic dispatch for polymorphism situation
    /// because trait has impl ,trait is dynamic
    data: Option<Vec<Box<dyn array::Array>>>,
    columns_name: Option<Vec<Columns>>,
    row_length: u32,
    column_length: u32,
    /// index trait
    /// box is a pointer to value in heap, pointer is in stack
    /// hash index
    index: Option<Vec<u64>>,
}

impl Dataframe {
    fn new() -> Dataframe {
        Dataframe {
            data: None,
            columns_name: None,
            row_length: 0,
            column_length: 0,
            index: None,
        }
    }

    fn set_index(&self, columns: &[Columns]) -> bool {
        if self.column_length == 0 {
            // todo: set warning message log
            return false;
        }

        // set hash array
        let mut index: HashIndex = Index::new();

        if columns.len() == 1 {
            match columns[0].column_type {
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    {
                        // get column array
                        let r = &self.data.as_ref().unwrap()[columns[0].data_index as usize];
                        let u_array = r.as_any().downcast_ref::<array::UInt32Array>().unwrap();
                        // todo : Uint32Array to &[u32]
                    };
                }
                _ => {
                    {};
                }
            }
        }

        true
    }
}
