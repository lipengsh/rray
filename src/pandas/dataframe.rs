use crate::pandas::index::{HashIndex, Index};
use arrow::array;
use arrow::array::{Array, Float32Array, StringArray, UInt32Array};
use arrow::datatypes::DataType;
use rayon::prelude::*;
use std::hash::Hash;

/// dataframe struct and impl
pub struct FloatDataframe<'a> {
    /// float data and column's name
    float_data: &'a [Float32Array],
    float_columns_name: &'a [String],

    /// String data and columns's name
    string_data: &'a [Box<[String]>],
    string_columns_name: &'a [String],
    row_length: u32,
    /// hash index
    /// set from outter , todo: change allocate position
    index: Box<dyn Index>,
}

impl<'a> FloatDataframe<'a> {
    /// only set index on string columns's name
    /// columns number <=2
    fn set_index(string_columns_name: &[String]) {}

    fn set_one_index(&mut self, string_column_name: String) {
        // get this column's index on columns name strings
        let column_name_index = self
            .string_columns_name
            .into_par_iter()
            .position(|r| r.to_string() == string_column_name)
            .unwrap();

        let string_array = &self.string_data[column_name_index];
        self.index.one_string(&string_array);
    }

    fn set_two_index(string_columns_name: &[String]) {}
}
