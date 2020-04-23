extern crate rray;

use parquet::basic::Repetition::REQUIRED;
use parquet::basic::Type::{BYTE_ARRAY, FLOAT};
use parquet::basic::{Repetition, Type};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::schema::types;
use parquet::schema::types::PrimitiveTypeBuilder;
use rray::pandas::utils::{gen_f32, gen_string};
use std::rc::Rc;

fn main() {
    make_schema();
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
                create_column_schema("dim", BYTE_ARRAY, REQUIRED),
                create_column_schema("tag", BYTE_ARRAY, REQUIRED),
                create_column_schema("ptr", FLOAT, REQUIRED),
                create_column_schema("mkt", FLOAT, REQUIRED),
            ])
            .build()
            .unwrap(),
    );

    let props = Rc::new(WriterProperties::builder().build());

    let file =
        rray::parquet::utils::get_temp_file("test_row_group_writer_num_records_mismatch", &[]);

    // generate sample file
    let array_len = 1024;
    let row_group_size = 128;

    let mut dim_array = gen_string(len);
    let mut tag_array = gen_string(len);
    let mut ptr_array = gen_f32(len);
    let mut mkt_array = gen_f32(len);

    // write to parquet file
    let mut writer = SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();

    // write to parquet as row_group_size
    let offset = 0;
    while offset < 1024 {
        // write dim array
        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            match col_writer {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                    typed
                        .write_batch(&dim_array[offset..row_group_size], None, None)
                        .unwrap();
                }
                _ => unimplemented!(),
            }
        }

        // write tag array
        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            match col_writer {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                    typed
                        .write_batch(&tag_array[offset..row_group_size], None, None)
                        .unwrap();
                }
                _ => unimplemented!(),
            }
        }

        // write ptr array
        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            match col_writer {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                    typed
                        .write_batch(&ptr_array[offset..row_group_size], None, None)
                        .unwrap();
                }
                _ => unimplemented!(),
            }
        }

        // write mkt array
        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            match col_writer {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                    typed
                        .write_batch(&mkt_array[offset..row_group_size], None, None)
                        .unwrap();
                }
                _ => unimplemented!(),
            }
        }
    }

    writer.close_row_group().unwrap();
    writer.close().unwrap();

    println!("schema:{:?}", schema);
}

fn make_schema() {
    let column_schema = create_column_schema("dim", BYTE_ARRAY, REQUIRED);
    println!("schema: {:?}", column_schema);
}
