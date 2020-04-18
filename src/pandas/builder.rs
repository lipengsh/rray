use crate::pandas::index::HashIndex;
use crate::pandas::FloatDataframe;
use arrow::array::{ArrayRef, Float32Array, PrimitiveBuilder};
use arrow::datatypes::{ArrowPrimitiveType, DataType};
#[allow(dead_code)]
pub struct DataframeBuilder<T: ArrowPrimitiveType> {
    data_type: DataType,
    len: usize,
    data: ArrayRef,
    builder: PrimitiveBuilder<T>,
}

#[test]
#[cfg(test)]
fn builder() {
    const ROW_LEN: usize = 5;
    const COLUMN_LEN: usize = 10;

    // build float data and columns's name
    let mut float_data: Vec<Float32Array> = Vec::new();
    let mut float_columns_name: Vec<String> = Vec::new();
    for i in 0..COLUMN_LEN {
        let mut builder = Float32Array::builder(ROW_LEN);
        let row_array = gen_f32(ROW_LEN);
        builder.append_slice(&row_array).unwrap();
        let f32_array = builder.finish();
        float_data.push(f32_array);
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

    // print_fdata(float_data);
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

#[allow(dead_code)]
fn print_fdata(float_data: Vec<Float32Array>) {
    for i in 0..float_data.len() {
        println!("{:?}", float_data[i]);
    }
}

use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;

fn gen_string(size: usize) -> Vec<String> {
    let mut rand_string: Vec<String> = Vec::new();
    for _i in 0..size {
        let rand_index = thread_rng().gen_range(5, 30);
        let s: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(rand_index)
            .collect();
        rand_string.push(s);
    }
    rand_string
}

#[allow(dead_code)]
fn gen_u32(size: usize) -> Vec<u32> {
    // random u32 array
    let mut rand_array = vec![0u32; size];
    let mut rng = thread_rng();
    for i in 0..size {
        rand_array[i] = rng.gen::<u32>();
    }
    rand_array
}

fn gen_f32(size: usize) -> Vec<f32> {
    let mut rand_f32: Vec<f32> = vec![0f32; size];
    let mut rng = thread_rng();
    for i in 0..size {
        rand_f32[i] = rng.gen::<f32>();
    }
    rand_f32
}
