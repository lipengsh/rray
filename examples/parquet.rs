extern crate rray;

use parquet::basic::Repetition::REQUIRED;
use parquet::basic::{Repetition, Type};
use parquet::data_type::{ByteArray, ByteArrayType, DataType};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, RowGroupWriter, SerializedFileWriter};
use parquet::schema::types;
use rray::pandas::utils::{gen_f32, gen_string};
use std::rc::Rc;

fn main() {
    create_parquet();
}

fn create_column_schema(name: &str, ty: Type, repeat: Repetition) -> Rc<types::Type> {
    Rc::new(
        types::Type::primitive_type_builder(name, ty)
            .with_repetition(repeat)
            .build()
            .unwrap(),
    )
}

fn create_parquet() {
    let schema = Rc::new(
        types::Type::group_type_builder("schema")
            .with_fields(&mut vec![
                create_column_schema("dim", Type::BYTE_ARRAY, REQUIRED),
                create_column_schema("tag", Type::BYTE_ARRAY, REQUIRED),
                create_column_schema("ptr", Type::FLOAT, REQUIRED),
                create_column_schema("mkt", Type::FLOAT, REQUIRED),
            ])
            .build()
            .unwrap(),
    );

    let props = Rc::new(WriterProperties::builder().build());

    let file = rray::parquet::utils::get_temp_file("sample_parquet", &[]);

    // generate sample file
    let array_len = 1024;
    let row_group_size = 128;

    let dim_array = gen_string(array_len);
    let tag_array = gen_string(array_len);
    let ptr_array: Vec<f32> = gen_f32(array_len);
    let mkt_array: Vec<f32> = gen_f32(array_len);

    let dim_byte_array: Vec<ByteArray> = dim_array
        .iter()
        .map(|x| ByteArray::from(x.as_str()))
        .collect();
    let tag_byte_array: Vec<ByteArray> = tag_array
        .iter()
        .map(|x| ByteArray::from(x.as_str()))
        .collect();

    // write to parquet file
    let mut writer = SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();

    // write to parquet as row_group_size
    let mut offset = 0;
    while offset < 1024 {
        // every row group
        let mut row_group_writer = writer.next_row_group().unwrap();

        // write dim array
        write_column::<ByteArrayType>(
            &mut row_group_writer,
            &dim_byte_array[offset..offset + row_group_size],
        );
        write_column::<ByteArrayType>(
            &mut row_group_writer,
            &tag_byte_array[offset..offset + row_group_size],
        );
        write_column::<f32>(
            &mut row_group_writer,
            &ptr_array[offset..offset + row_group_size],
        );
        write_column::<f32>(
            &mut row_group_writer,
            &mkt_array[offset..offset + row_group_size],
        );

        offset += row_group_size;

        writer.close_row_group(row_group_writer).unwrap();
    }
    writer.close().unwrap();
}

fn write_column<T: DataType>(row_group_writer: &mut Box<dyn RowGroupWriter>, slice: &[T::T]) {
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        parquet::column::writer::get_typed_column_writer_mut::<T>(&mut col_writer)
            .write_batch(slice, None, None)
            .unwrap();
        row_group_writer.close_column(col_writer).unwrap();
    }
}
