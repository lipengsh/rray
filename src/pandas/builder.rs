use crate::pandas::FloatDataframe;
use arrow::array::{ArrayRef, Float32Array, PrimitiveBuilder};
use arrow::datatypes::{ArrowPrimitiveType, DataType};

pub struct DataframeBuilder<T: ArrowPrimitiveType> {
    data_type: DataType,
    len: usize,
    data: ArrayRef,
    builder: PrimitiveBuilder<T>,
}

#[test]
fn builder() {
    const ROW_LEN: usize = 5;
    const COLUMN_LEN: usize = 10;
    let mut float_data: Vec<Float32Array> = Vec::new();
    for i in 0..COLUMN_LEN {
        let mut builder = Float32Array::builder(ROW_LEN);
        let row_array = gen_f32(ROW_LEN);
        builder.append_slice(&row_array).unwrap();
        let f32_array = builder.finish();
        float_data.push(f32_array);
    }
    // print_fdata(float_data);
}

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
