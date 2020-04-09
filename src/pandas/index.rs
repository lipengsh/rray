use ahash::AHasher;
use rand::Rng;
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
    fn one_str(columns: &str);

    // generate string and u32 type hash index
    fn two_u32_str(columns_u32: &[u32], columns_str: &str);
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

    fn one_u32(&mut self, columns: &[u32]) {
        // let iter = columns.into_iter();
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

    fn one_str(columns: &str) {}

    fn two_u32_str(columns_u32: &[u32], columns_str: &str) {}
}

#[test]
fn test_one_u32() {
    const SIZE: usize = 1024 * 100;
    // gen u32 array

    // random u32 array
    let mut rand_array = vec![0u32; SIZE];
    let mut rng = rand::thread_rng();
    for i in 0..SIZE {
        rand_array[i] = rng.gen();
    }

    // use index trait
    let mut index: HashIndex = Index::new();

    use std::time::Instant;

    let gen_start = Instant::now();
    index.one_u32(&rand_array);
    println!(
        "test_one_u32 used time:{} millis",
        gen_start.elapsed().as_millis()
    );
}
