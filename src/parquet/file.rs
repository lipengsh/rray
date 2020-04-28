use crate::parquet::format::Format;
use parquet::file::writer::SerializedFileWriter;
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
pub struct FileWriter {
    writer_handler: SerializedFileWriter<File>,
}

impl FileWriter {
    fn new(file_handler: FileHandler, format: Format) -> Self {
        FileWriter {
            writer_handler: SerializedFileWriter::new(
                file_handler.try_clone().unwrap(),
                format.schema,
                format.properties,
            )
            .unwrap(),
        }
    }
}
