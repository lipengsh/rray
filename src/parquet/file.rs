use crate::dynamic::dynamic::Dynamic;
use crate::parquet::format::Format;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path;
use std::path::PathBuf;
use std::str::FromStr;

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

    pub fn open(file_with_path: &str) -> Self {
        let file_handler =
            fs::File::open(PathBuf::from_str(file_with_path).unwrap().as_path()).unwrap();
        Self {
            file_with_path: file_with_path.to_string(),
            file_handler,
        }
    }

    pub fn try_clone(&self) -> io::Result<fs::File> {
        self.file_handler.try_clone()
    }
}

/// write_column(BoolColumnWriter , bool, typed_writer)
macro_rules! write_column {
    ($column_type:ty, $writer:ident, $item:ident) => {
        let mut native_array: Vec<$column_type> = Vec::new();
        for x in $item {
            native_array.push(x.native::<$column_type>().unwrap());
        }
        $writer
            .write_batch(native_array.as_slice(), None, None)
            .unwrap();
    };
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

    // write parquet
    pub fn write_parquet(&mut self, data: &Vec<Vec<Dynamic>>) {
        // only one row group
        let mut row_group_writer = self.writer_handler.next_row_group().unwrap();
        for item in data {
            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                // get dynamic type
                match col_writer {
                    ColumnWriter::BoolColumnWriter(ref mut typed_writer) => {
                        write_column!(bool, typed_writer, item);
                    }

                    ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                        write_column!(i32, typed_writer, item);
                    }

                    ColumnWriter::Int64ColumnWriter(ref mut typed_writer) => {
                        write_column!(i64, typed_writer, item);
                    }

                    ColumnWriter::Int96ColumnWriter(ref mut _typed_writer) => {
                        // write_column!(i128, _typed_writer, item);
                        ()
                        // TODO: add support for Int96
                    }

                    ColumnWriter::FloatColumnWriter(ref mut typed_writer) => {
                        write_column!(f32, typed_writer, item);
                    }

                    ColumnWriter::DoubleColumnWriter(ref mut typed_writer) => {
                        write_column!(f64, typed_writer, item);
                    }

                    ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                        // write_column!(String, typed_writer, item);
                        let mut native_array: Vec<ByteArray> = Vec::new();
                        for x in item {
                            native_array
                                .push(ByteArray::from(x.native::<String>().unwrap().as_str()));
                        }

                        typed_writer.write_batch(&native_array, None, None).unwrap();
                    }

                    ColumnWriter::FixedLenByteArrayColumnWriter(ref mut typed_writer) => {
                        // write_column!(String, typed_writer, item);
                        let mut native_array: Vec<ByteArray> = Vec::new();
                        for x in item {
                            native_array
                                .push(ByteArray::from(x.native::<String>().unwrap().as_str()));
                        }
                        typed_writer.write_batch(&native_array, None, None).unwrap();
                        typed_writer.write_batch(&native_array, None, None).unwrap();
                    }
                };
                row_group_writer.close_column(col_writer).unwrap();
            }
        }

        self.writer_handler
            .close_row_group(row_group_writer)
            .unwrap();

        self.writer_handler.close().unwrap();
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
    use parquet::file::reader::SerializedFileReader;
    use std::convert::TryFrom;

    #[test]
    fn write_parquet() {
        // create file handler
        let file_handler = FileHandler::new("./target/sample.parquet");

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
                    column_type: Type::FLOAT,
                },
                ColumnSchema {
                    name: "mkt".to_string(),
                    column_type: Type::FLOAT,
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

    #[test]
    fn read_parquet() {
        let file_handler = FileHandler::open("sample.parquet").try_clone().unwrap();
        // let read_result = SerializedFileReader::new(file_handler);
        let read_result = SerializedFileReader::try_from(file_handler);
        assert!(read_result.is_ok());
    }

    fn make_dynamic_array<T: 'static>(array: Vec<T>) -> Vec<Dynamic> {
        let mut result = Vec::new();
        for x in array {
            result.push(Dynamic::new::<T>(x));
        }
        result
    }
}
