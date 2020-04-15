use crate::pandas::index::{HashIndex, Index};
use arrow::array;
use arrow::array::{Array, Float32Array, StringArray, UInt32Array};
use arrow::datatypes::DataType;
use rayon::prelude::*;
use std::borrow::BorrowMut;
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
    /// set from trait outer , todo: change allocate position
    index: Box<dyn Index>,
}

impl<'a> FloatDataframe<'a> {
    /// only set index on string columns's name
    /// columns number <=2
    fn set_index(&mut self, string_columns_name: &[String]) -> bool {
        let length = string_columns_name.len();
        if length == 0 {
            return false;
        }
        if length == 1 {
            self.set_one_index(&string_columns_name[0]);
        }
        if length == 2 {
            self.set_two_index(string_columns_name);
        }

        true
    }

    fn set_one_index(&mut self, string_column_name: &String) {
        // get this column's index on columns name strings
        let column_name_index = self
            .string_columns_name
            .into_par_iter()
            .position(|r| r.to_string() == *string_column_name)
            .unwrap();

        let string_array = &self.string_data[column_name_index];
        self.index.one_string(&string_array);
    }

    fn set_two_index(&mut self, string_columns_name: &[String]) {
        // get this column's index on columns name strings
        let column_name_index_0 = self
            .string_columns_name
            .into_par_iter()
            .position(|r| r.to_string() == string_columns_name[0])
            .unwrap();
        let column_name_index_1 = self
            .string_columns_name
            .into_par_iter()
            .position(|r| r.to_string() == string_columns_name[1])
            .unwrap();

        let string_array_0: &[String] = &self.string_data[column_name_index_0];
        let string_array_1: &[String] = &self.string_data[column_name_index_1];
        self.index.two_str(string_array_0, string_array_1);
    }
}

#[test]
fn unique() {
    // let mut v = [(1, -5i32), (3, 4), (1, 1), (2, -3), (2, 2)];

    // v.par_sort_by_key(|k| k.0);
    use std::sync::atomic::AtomicUsize;
    //
    // let mut v: Vec<i32> = vec![1, 2, 5, 2, 9, 4, 1, 5, 2];
    // use std::cell::RefCell;
    // thread_local!(static STORE: RefCell<Vec<i32>> = RefCell::new(Vec::new()));
    //
    // v.into_par_iter().map(move |x| {
    //     STORE.with(|cell| {
    //         let mut local_store = cell.borrow_mut();
    //         *local_store.push(x);
    //         // let ls: &mut Vec<i32> = local_store.as_mut();
    //         // if !ls.contains(&x) {
    //         // ls.push(x);
    //         // }
    //     })
    // });
    //
    // println!("store");
    //
    // STORE.with(|cell| {
    //     println!("{:?}", cell.borrow_mut());
    // });
}
