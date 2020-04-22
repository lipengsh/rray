extern crate rray;

use parquet::basic::Repetition::REQUIRED;
use parquet::basic::Type::{BYTE_ARRAY, FLOAT};
use parquet::basic::{Repetition, Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::schema::types;
use parquet::schema::types::PrimitiveTypeBuilder;
use std::intrinsics::rotate_right;
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

    let mut writer = SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();
    writer.close().unwrap();

    println!("schema:{:?}", schema);
}

fn make_schema() {
    let column_schema = create_column_schema("dim", BYTE_ARRAY, REQUIRED);
    println!("schema: {:?}", column_schema);
}
