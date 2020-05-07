use std::fs::File;
use std::io;
use std::io::Write;
use std::path;
use std::{fs, mem};

use parquet::column::writer::ColumnWriter::FloatColumnWriter;
use parquet::column::writer::{get_typed_column_writer_mut, ColumnWriter, ColumnWriterImpl};
use parquet::file::writer::{FileWriter, SerializedFileWriter};

use crate::dynamic::dynamic::Dynamic;
use crate::parquet::format::Format;

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

        // open the file, and return file handler for both read and write
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

    /// write parquet
    pub fn write_parquet(&mut self, data: &Vec<Vec<Dynamic>>) {
        let mut row_group_writer = self.writer_handler.next_row_group().unwrap();
        for item in data {
            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                // todo howto contruect datatype

                row_group_writer.close_column(col_writer).unwrap();
            }
        }
        self.writer_handler
            .close_row_group(row_group_writer)
            .unwrap();
    }

    // close writer
    pub fn close(&mut self) {
        self.writer_handler.close().unwrap();
    }
}

#[cfg(test)]
mod test {
    use parquet::basic::Type;

    use crate::dynamic::dynamic::Dynamic;
    use crate::pandas::utils::{gen_f32, gen_string};
    use crate::parquet::file::{FileHandler, ParquetWriter};
    use crate::parquet::format::{ColumnSchema, Format};

    #[test]
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
        const ARRAY_LEN: usize = 1024;

        let dim_array: Vec<String> = gen_string(ARRAY_LEN);
        let tag_array: Vec<String> = gen_string(ARRAY_LEN);
        let ptr_array: Vec<f32> = gen_f32(ARRAY_LEN);
        let mkt_array: Vec<f32> = gen_f32(ARRAY_LEN);
        //
        let dim_dynamic = make_dynamic_array::<String>(dim_array);
        let tag_dynamic = make_dynamic_array::<String>(tag_array);
        let ptr_dynamic = make_dynamic_array::<f32>(ptr_array);
        let mkt_dynamic = make_dynamic_array::<f32>(mkt_array);

        println!("dim dynamic:{}", dim_dynamic[0].native::<String>().unwrap());
        println!("mkt dynamic:{}", mkt_dynamic[0].native::<f32>().unwrap());

        let all_array = vec![dim_dynamic, tag_dynamic, ptr_dynamic, mkt_dynamic];

        ParquetWriter::new(file_handler, file_format).write_parquet(&all_array);
    }

    fn make_dynamic_array<T: 'static>(array: Vec<T>) -> Vec<Dynamic> {
        let mut result = Vec::new();
        for x in array {
            result.push(Dynamic::new(x));
        }
        result
    }
}
