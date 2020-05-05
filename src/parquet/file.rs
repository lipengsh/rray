use crate::parquet::format::Format;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path;

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
    // pub fn write_parquet(&mut self, data: &Vec<Vec<TypeTuple>>) {
    //     let mut row_group_writer = self.writer_handler.next_row_group().unwrap();
    //     for item in data {
    //         if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
    //             // get_typed_column_writer_mut::<dyn >(&mut col_writer)
    //             //     .write_batch(item, None, None)
    //             //     .unwrap();
    //             println!("next column");
    //             row_group_writer.close_column(col_writer).unwrap();
    //         }
    //     }
    //     self.writer_handler
    //         .close_row_group(row_group_writer)
    //         .unwrap();
    //
    //     // if let Some(s) = value.downcast_ref::<typeid>() {
    //     //     //Somehow downcast using typeid instead of type
    //     //     println!("{:?}", s);
    //     // }
    // }

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

    // macro_rules! make_dynamic_array {
    //     ($ty:ty, &array:expr) => {
    //         $array
    //             .iter()
    //             .map(|x| {
    //                 (
    //                     TypeId::of::<DynamicArray<&ty>>(),
    //                     Box::new(DynamicArray { value: x }),
    //                 )
    //             })
    //             .collect()
    //     };
    // }

    // fn make_dynamic_array<T: 'static>(array: &'a Vec<T>, TypeResult: &'a mut Vec<TypeTuple>) {
    //     for item in array {
    //         TypeResult.push((TypeId::of::<DynamicArray<T>>(), Box::new(&item)));
    //     }
    // }

    // #[test]
    // fn write_parquet() {
    //     // create file handler
    //     let file_handler = FileHandler::new("sample.parquet");
    //
    //     // create file format
    //     let file_format = Format::new(
    //         "sample",
    //         vec![
    //             ColumnSchema {
    //                 name: "dim".to_string(),
    //                 column_type: Type::BYTE_ARRAY,
    //             },
    //             ColumnSchema {
    //                 name: "tag".to_string(),
    //                 column_type: Type::BYTE_ARRAY,
    //             },
    //             ColumnSchema {
    //                 name: "ptr".to_string(),
    //                 column_type: Type::BOOLEAN,
    //             },
    //             ColumnSchema {
    //                 name: "mkt".to_string(),
    //                 column_type: Type::BOOLEAN,
    //             },
    //         ],
    //     );
    //
    //     // generate sample data
    //     const ARRAY_LEN: usize = 1024;
    //
    //     let dim_array: Vec<String> = gen_string(ARRAY_LEN);
    //     let tag_array: Vec<String> = gen_string(ARRAY_LEN);
    //     let ptr_array: Vec<f32> = gen_f32(ARRAY_LEN);
    //     let mkt_array: Vec<f32> = gen_f32(ARRAY_LEN);
    //
    //     let mut dim_dynamic: Vec<TypeTuple> = Vec::new();
    //     make_dynamic_array::<String>(dim_array, &mut dim_dynamic);
    //     let mut tag_dynamic: Vec<TypeTuple> = Vec::new();
    //     make_dynamic_array::<String>(tag_array, &mut tag_dynamic);
    //     let mut ptr_dynamic: Vec<TypeTuple> = Vec::new();
    //     make_dynamic_array::<f32>(ptr_array, &mut ptr_dynamic);
    //     let mut mkt_dynamic: Vec<TypeTuple> = Vec::new();
    //     make_dynamic_array::<f32>(mkt_array, &mut mkt_dynamic);
    //
    //     let mut array_gather: Vec<Vec<TypeTuple>> = Vec::new();
    //     array_gather.push(dim_dynamic);
    //     array_gather.push(tag_dynamic);
    //     array_gather.push(ptr_dynamic);
    //     array_gather.push(mkt_dynamic);
    //
    //     // ParquetWriter::new(file_handler, file_format).writer_parquet(&array_gather);
    // }
}
