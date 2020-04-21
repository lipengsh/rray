use parquet::basic::Repetition::REPEATED;
use parquet::basic::{Repetition, Type};
use parquet::schema::types;
use parquet::schema::types::PrimitiveTypeBuilder;
use std::rc::Rc;

fn main() {
    make_schema();
}

/// create_schema[
///     ("dim", Type::BYTE_ARRAY, Repetition::REQUIRED),
///     ("tag", Type::BYTE_ARRAY, Repetition::REQUIRED),
///     ("prt", Type::FLOAT, Repetition::REQUIRED),
///     ("mkt", Type::FLOAT, Repetition::REQUIRED)
/// ]
///

macro_rules! create_schema {
    ($cls: ty ,$( $e:expr ), *) => {
       let mut columns_schema = <$cls>::new();
       $(
            columns_schema.push(make_column_schema($e));
       )*
       columns_schema
    };
}

macro_rules! create_column_schema {
    ($NAME:ident, $TYPE:ident, $REPEAT:ident) => {
        // make_column_schema($NAME, $TYPE, $REPEAT)
        let result: Rc<types::Type> = Rc::new(
            types::Type::primitive_type_builder($NAME, $TYPE)
                .with_repetition($REPEAT)
                .build()
                .unwrap(),
        );
        result
    };
}

fn create_parquet() {
    let schema = Rc::new(
        types::Type::group_type_builder("schema")
            .with_fields(&mut vec![
                Rc::new(
                    types::Type::primitive_type_builder("dim", Type::BYTE_ARRAY)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                ),
                Rc::new(
                    types::Type::primitive_type_builder("tag", Type::BYTE_ARRAY)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                ),
                Rc::new(
                    types::Type::primitive_type_builder("ptr", Type::FLOAT)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                ),
                Rc::new(
                    types::Type::primitive_type_builder("mkt", Type::FLOAT)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                ),
            ])
            .build()
            .unwrap(),
    );
}

fn make_schema() {
    let schema = create_schema!(
        Vec<types::Type>,
        [
            ("dim", Type::BYTE_ARRAY, Repetition::REQUIRED),
            ("tag", Type::BYTE_ARRAY, Repetition::REQUIRED),
            ("prt", Type::FLOAT, Repetition::REQUIRED),
            ("mkt", Type::FLOAT, Repetition::REQUIRED),
        ]
    );
    println!("schema: {:?}", schema);
}
