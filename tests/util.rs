#![feature(test)]
extern crate test;
// use crossbeam_utils::thread;
use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;
use rayon::prelude::*;
use std::hash::Hasher;
use std::vec::Vec;
// use test::{black_box, Bencher};

struct TempStruct {
    name: String,
}

fn gen(string: &[String], array: &[u64]) {
    // par iter
    use ahash::AHasher;

    let result: Vec<_> = (array, string)
        .into_par_iter()
        .map_init(
            || AHasher::default(),
            |h, (x, s)| {
                h.write_u64(*x);
                h.write(s.as_bytes());
                h.finish()
            },
        )
        .collect();

    println!("hasher result:{:?}", &result[0..11]);
}

#[test]
fn bench_hash() {
    const SIZE: usize = 1024 * 100;

    let mut rand_string: Vec<String> = Vec::new();
    for _i in 0..SIZE {
        let rand_index = thread_rng().gen_range(5, 30);
        let s: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(rand_index)
            .collect();
        rand_string.push(s);
    }

    // random u8 slice
    let mut rand_array = vec![0u64; SIZE];
    let mut rng = rand::thread_rng();
    for i in 0..SIZE {
        rand_array[i] = rng.gen();
    }

    use std::time::Instant;

    let gen_start = Instant::now();
    gen(&rand_string, &rand_array);
    println!("gen used time:{} millis", gen_start.elapsed().as_millis());
}
