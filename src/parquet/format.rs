use parquet::basic::Type;
use parquet::file::properties::WriterPropertiesPtr;
use parquet::schema::types::TypePtr;
use std::rc::Rc;

pub struct Format {
    pub schema: TypePtr,
    pub properties: WriterPropertiesPtr,
}

pub struct ColumnSchema {
    pub name: String,
    pub column_type: Type,
}

impl Format {
    pub fn set_schema(&self, schema_name: &str, columns_schema: Vec<ColumnSchema>) {
        let mut fields: Vec<TypePtr> = Vec::new();
        for item in &columns_schema {
            fields.push(Self::set_column_schema(item))
        }

        parquet::schema::types::Type::group_type_builder(schema_name)
            .with_fields(&mut fields)
            .build()
            .unwrap();
    }

    pub fn set_column_schema(column_schema: &ColumnSchema) -> TypePtr {
        Rc::new(
            parquet::schema::types::Type::primitive_type_builder(
                column_schema.name.as_str(),
                column_schema.column_type,
            )
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap(),
        )
    }
}
