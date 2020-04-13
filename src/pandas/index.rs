use ahash::AHasher;
use std::hash::Hasher;
// use hashbrown::hash_map::DefaultHashBuilder;
use rayon::prelude::*;

pub trait Index {
    fn new() -> Self;

    /// set index in the columns, one or more columns
    // fn create_index(columns: Vec<Box<dyn Array>>) -> Option<Vec<u32>>;

    // generate u32 type hash index
    fn one_u32(&mut self, columns: &[u32]);

    // generate string type hash index
    fn one_string(&mut self, columns: &[String]);

    // generate string and u32 type hash index
    fn two_u32_str(&mut self, columns_u32: &[u32], columns_string: &[String]);

    // get index vec
    fn index(&self) -> Vec<u32>;
}

pub struct HashIndex {
    // hash index's count
    count: u32,

    // hash code, u64 type
    // vec's index is row number
    hasher: Vec<u32>,
}

impl Index for HashIndex {
    fn new() -> HashIndex {
        HashIndex {
            count: 0,
            hasher: Vec::new(),
        }
    }

    fn index(&self) -> Vec<u32> {
        self.hasher.clone()
    }

    fn one_u32(&mut self, columns: &[u32]) {
        let result: Vec<u32> = columns
            .into_par_iter()
            .map_init(
                || AHasher::default(),
                |h, x| {
                    h.write_u32(*x);
                    h.finish() as u32
                },
            )
            .collect();
        self.hasher = Vec::from(result);
    }

    fn one_string(&mut self, columns: &[String]) {
        let result: Vec<u32> = columns
            .into_par_iter()
            .map_init(
                || AHasher::default(),
                |h, x| {
                    h.write(x.as_bytes());
                    h.finish() as u32
                },
            )
            .collect();
        self.hasher = Vec::from(result);
    }

    fn two_u32_str(&mut self, columns_u32: &[u32], columns_string: &[String]) {
        let result: Vec<u32> = (columns_u32, columns_string)
            .into_par_iter()
            .map_init(
                || AHasher::default(),
                |h, (x, s)| {
                    h.write_u32(*x);
                    h.write(s.as_bytes());
                    h.finish() as u32
                },
            )
            .collect();
        self.hasher = Vec::from(result);
    }
}

#[test]
/// 1024*100 : 6 millis
/// 1024*1000: 48 millis
/// 1024*10000: 477 millis
fn test_one_u32() {
    const SIZE: usize = 1024 * 100;

    // gen u32 array
    let mut rand_array = gen_u32(SIZE);

    // use index trait
    let mut index: HashIndex = Index::new();

    // calculate hash time
    use std::time::Instant;

    let gen_start = Instant::now();
    index.one_u32(&rand_array);
    println!(
        "test_one_u32 used time:{} millis",
        gen_start.elapsed().as_millis()
    );
}
#[test]
/// 1024*100 :23 millis
/// 1024*1000: 264 millis
/// 1024*10000: 2718 millis
fn test_string() {
    const SIZE: usize = 1024 * 100;

    // gen string array
    let mut rand_string: Vec<String> = gen_string(SIZE);

    // calculate hash time
    use std::time::Instant;

    // use index trait
    let mut index: HashIndex = Index::new();

    let gen_start = Instant::now();
    index.one_string(&rand_string);
    println!(
        "test_string used time:{} millis",
        gen_start.elapsed().as_millis()
    );
}

#[test]
/// 1024*100 : 25 millis
/// 1024*1000 : 240 millis
/// 1024*10000 : 2471 millis
fn test_string_u32() {
    const SIZE: usize = 1024 * 10000;
    // gen u32 array
    let mut rand_array = gen_u32(SIZE);

    // gen string array
    let mut rand_string: Vec<String> = gen_string(SIZE);

    // use index trait
    let mut index: HashIndex = Index::new();

    // calculate hash time
    use std::time::Instant;

    let gen_start = Instant::now();
    index.two_u32_str(&rand_array, &rand_string);
    println!(
        "test_string_u32 used time:{} millis",
        gen_start.elapsed().as_millis()
    );
}

fn gen_u32(size: usize) -> Vec<u32> {
    use rand::thread_rng;
    use rand::Rng;
    // random u32 array
    let mut rand_array = vec![0u32; size];
    let mut rng = thread_rng();
    for i in 0..size {
        rand_array[i] = rng.gen();
    }
    rand_array
}

fn gen_string(size: usize) -> Vec<String> {
    use rand::distributions::Alphanumeric;
    use rand::thread_rng;
    use rand::Rng;

    let mut rand_string: Vec<String> = Vec::new();
    for _i in 0..size {
        let rand_index = thread_rng().gen_range(5, 30);
        let s: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(rand_index)
            .collect();
        rand_string.push(s);
    }
    rand_string
}
