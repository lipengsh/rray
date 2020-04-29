use crate::parquet::format::Format;
use parquet::column::writer::{get_typed_column_writer_mut, ColumnWriter};
use parquet::data_type::DataType;
use parquet::file::writer::{FileWriter, RowGroupWriter, SerializedFileWriter};
use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path;

pub struct DynamicArray<T> {
    value: Vec<T>,
}

/// parquet io struct and impl
pub struct FileHandler {
    pub file_with_path: String,
    file_handler: fs::File,
}

impl FileHandler {
    // create file and FileHandler struct
    pub fn new(file_with_path: &str) -> Self {
        // create a null temp file
        let mut file = fs::File::create(path::PathBuf::from(file_with_path)).unwrap();
        file.write_all(&[]).unwrap();
        file.sync_all().unwrap();

        // open the file, and return file handler for botyh read and write
        let file_handler = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path::PathBuf::from(file_with_path))
            .unwrap();

        Self {
            file_with_path: file_with_path.to_string(),
            file_handler,
        }
    }

    pub fn try_clone(&self) -> io::Result<fs::File> {
        self.file_handler.try_clone()
    }
}

/// parquet writer
pub struct ParquetWriter {
    writer_handler: SerializedFileWriter<File>,
}

impl ParquetWriter {
    pub fn new(file_handler: FileHandler, format: Format) -> Self {
        ParquetWriter {
            writer_handler: SerializedFileWriter::new(
                file_handler.try_clone().unwrap(),
                format.schema,
                format.properties,
            )
            .unwrap(),
        }
    }

    // // // write parquet
    // pub fn write_parquet(&mut self, data: &Vec<Vec<dyn DataType>>) {
    //     let mut row_group_writer = self.writer_handler.next_row_group().unwrap();
    //
    //     for item in &data {
    //         if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
    //             get_typed_column_writer_mut::<dyn DataType>(&mut col_writer)
    //                 .write_batch(item, None, None)
    //                 .unwrap();
    //         }
    //         row_group_writer.close_column().unwrap();
    //     }
    //     self.writer_handler.close_row_group().unwrap();
    // }

    // close writer
    pub fn close(&mut self) {
        self.writer_handler.close().unwrap();
    }
}

#[cfg(test)]
mod test {
    use crate::pandas::utils::{gen_f32, gen_string};
    use crate::parquet::file::{FileHandler, ParquetWriter};
    use crate::parquet::format::{ColumnSchema, Format};
    // use parquet::basic::Type;
    // use parquet::data_type::ByteArray;
    use arrow::datatypes::DataType;
    use parquet::basic::Type;
    use parquet::data_type::ByteArray;

    fn write_parquet() {
        // create file handler
        let file_handler = FileHandler::new("sample.parquet");

        // create file format
        let file_format = Format::new(
            "sample",
            vec![
                ColumnSchema {
                    name: "dim".to_string(),
                    column_type: Type::BYTE_ARRAY,
                },
                ColumnSchema {
                    name: "tag".to_string(),
                    column_type: Type::BYTE_ARRAY,
                },
                ColumnSchema {
                    name: "ptr".to_string(),
                    column_type: Type::BOOLEAN,
                },
                ColumnSchema {
                    name: "mkt".to_string(),
                    column_type: Type::BOOLEAN,
                },
            ],
        );

        // generate sample data

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

        let sample_data = [dim_byte_array, tag_byte_array, ptr_array, mkt_array];

        // create ParquetWriter
        ParquetWriter::new(file_handler, file_format).write_parquet(&sample_data);
    }
}
