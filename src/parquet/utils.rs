use std::{env, fs, io::Write};

/// Returns file handle for a temp file in 'target' directory with a provided content
pub fn get_temp_file(file_name: &str, content: &[u8]) -> fs::File {
    // build tmp path to a file in "target/debug/testdata"
    let mut path_buf = env::current_dir().unwrap();
    path_buf.push("target");
    path_buf.push("debug");
    path_buf.push("testdata");
    fs::create_dir_all(&path_buf).unwrap();
    path_buf.push(file_name);

    // write file content
    let mut tmp_file = fs::File::create(path_buf.as_path()).unwrap();
    tmp_file.write_all(content).unwrap();
    tmp_file.sync_all().unwrap();

    // return file handle for both read and write
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path_buf.as_path());
    file.unwrap()
}

#[cfg(test)]
mod test {
    use crate::parquet::utils::get_temp_file;
    use parquet::file::reader::Length;

    #[test]
    fn get_file() {
        let file = get_temp_file("test", &[]);
        println!("file length:{}", file.len());
    }
}
