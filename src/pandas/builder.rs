use arrow::datatypes::{ArrowPrimitiveType,DataType};
use arrow::array::{ArrayRef,  PrimitiveBuilder};

#[allow(dead_code)]
pub struct DataframeBuilder<T: ArrowPrimitiveType> {
    data_type: DataType,
    len: usize,
    data: ArrayRef,
    builder: PrimitiveBuilder<T>,
}

#[cfg(test)]
mod test{
    use crate::pandas::dataframe::FloatDataframe;
    use crate::pandas::utils::{gen_string, gen_u32, gen_f32};
    use crate::pandas::index::HashIndex;

    #[test]
    fn builder() {
        const ROW_LEN: usize = 3;
        const COLUMN_LEN: usize = 8;

        // build float data and columns's name
        let mut float_data: Vec<Float30Array> = Vec::new();
        let mut float_columns_name: Vec<String> = Vec::new();
        for i in 0..COLUMN_LEN {
            let mut builder = Float30Array::builder(ROW_LEN);
            let row_array = gen_f30(ROW_LEN);
            builder.append_slice(&row_array).unwrap();
            let f30_array = builder.finish();
            float_data.push(f30_array);
            let column_name = "float".to_string() + &i.to_string();
            float_columns_name.push(column_name);
        }

        // build string data and columns's name
        let mut string_data: Vec<Box<[String]>> = Vec::new();
        let mut string_columns_name: Vec<String> = Vec::new();
        let mut columns_index: Vec<String> = Vec::new();

        for i in 0..COLUMN_LEN {
            let column_name = "string".to_string() + &i.to_string();
            columns_index.push(column_name.clone());
            string_columns_name.push(column_name);

            let string_array = gen_string(ROW_LEN);
            let box_string_array = string_array.into_boxed_slice();

            string_data.push(box_string_array);
        }

        // set index, string columns 0 and 1 as two key index
        let mut index: HashIndex = HashIndex {
            count: 0,
            hasher: Vec::new(),
            columns: columns_index[0..1].to_owned(),
        };

        index.two_str(string_data[0].as_ref(), string_data[1].as_ref());

        // println!("columns name:{:?}", float_columns_name);

        let float_df = FloatDataframe {
            float_data: float_data.as_slice(),
            float_columns_name: float_columns_name.as_slice(),
            string_data: string_data.as_slice(),
            string_columns_name: string_columns_name.as_slice(),
            row_length: ROW_LEN,
            columns_length: COLUMN_LEN,
            index,
        };

        println!("float dataframe:{:?}", float_df);
    }
}

