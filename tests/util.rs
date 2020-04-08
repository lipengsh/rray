#![feature(test)]
extern crate test;
use rand::Rng;
use std::hash::Hasher;
use std::vec::Vec;
use test::{black_box, Bencher};

struct TempStruct {
    name: String,
}

fn gen(string: &[String], array: &[u64]) {
    // par iter
    use ahash::AHasher;
    use rayon::prelude::*;

    let result: Vec<_> = (array.to_vec(), string.to_vec())
        .into_par_iter()
        .map_init(
            || AHasher::default(),
            |h, (x, s)| {
                h.write_u64(x);
                h.write(s.as_bytes());
                h.finish()
            },
        )
        .collect();

    // eprintln!("hasher result:{:?}", &result[0..11]);
}

#[test]
fn bench_hash() {
    use rand::distributions::Alphanumeric;
    use rand::thread_rng;

    const SIZE: usize = 1024 * 1000;
    // random string vec
    let mut rand_string: Vec<String> = Vec::new();
    for i in 0..SIZE {
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
    // rand::thread_rng().fill(&mut rand_array[..]);

    use std::time::Instant;

    let start = Instant::now();
    gen(&rand_string, &rand_array);
    eprintln!("used time:{} millis", start.elapsed().as_millis());
}
