#[warn(unused_imports)]
use crate::pandas::index::HashIndex;
use arrow::array::Float32Array;
use rayon::prelude::*;
use std::fmt;

/// dataframe struct and impl
pub struct FloatDataframe<'a> {
    /// float data and column's name
    pub(crate) float_data: &'a [Float32Array],
    pub(crate) float_columns_name: &'a [String],

    /// String data and columns's name
    pub(crate) string_data: &'a [Box<[String]>],
    pub(crate) string_columns_name: &'a [String],

    // row and columns length
    pub(crate) row_length: usize,
    pub(crate) columns_length: usize,

    /// hash index
    /// set from trait outer , todo: change allocate position
    pub(crate) index: HashIndex,
}


impl<'a> FloatDataframe<'a> {
    /// only set index on string columns's name
    /// columns number <=2
    pub fn set_index(&mut self, string_columns_name: &[String]) -> bool {
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

    pub fn set_one_index(&mut self, string_column_name: &String) {
        // get this column's index on columns name strings
        let column_name_index = self
            .string_columns_name
            .into_par_iter()
            .position_any(|r| r.to_string() == *string_column_name)
            .unwrap();

        let string_array = &self.string_data[column_name_index];
        self.index.one_string(&string_array);
    }

    pub fn set_two_index(&mut self, string_columns_name: &[String]) {
        // get this column's index on columns name strings
        let column_name_index_0 = self
            .string_columns_name
            .into_par_iter()
            .position_any(|r| r.to_string() == string_columns_name[0])
            .unwrap();
        let column_name_index_1 = self
            .string_columns_name
            .into_par_iter()
            .position_any(|r| r.to_string() == string_columns_name[1])
            .unwrap();

        let string_array_0: &[String] = &self.string_data[column_name_index_0];
        let string_array_1: &[String] = &self.string_data[column_name_index_1];
        self.index.two_str(string_array_0, string_array_1);
    }
}

impl<'a> fmt::Debug for FloatDataframe<'a> {
    default fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "row length:{}, columns length:{}",
            self.row_length, self.columns_length
        )
        .unwrap();

        for i in 0..self.columns_length {
            writeln!(
                f,
                "float column name:{:?}\n, float data :{:?}",
                self.float_columns_name[i], self.float_data[i],
            )
            .unwrap();

            writeln!(
                f,
                "string column name :{:?}\n, string data:{:?}",
                self.string_columns_name[i], self.string_data[i],
            )
            .unwrap();

            if i > 3 {
                writeln!(f, "...").unwrap();
                break;
            }
            writeln!(f, "----------").unwrap();
        }

        writeln!(f)
    }
}

#[cfg(test)]
mod test{
    fn unique() {
        let mut v = vec![1, 3, 4, 1, 1, 2, -3, 2, 2];
        v.sort();

        let c: Vec<i32> = Some(v[0])
            .into_iter()
            .chain(v.windows(2).filter(|w| w[0] != w[1]).map(|w| w[1]))
            .collect();

        println!("c:{:?}", c);

        use itertools::Itertools;

        let n: Vec<i32> = v.into_iter().unique().collect();
        println!("n:{:?}", n);
    }
}


