#![feature(test)]
extern crate test;
use rand::Rng;
use std::hash::Hasher;
use std::vec::Vec;
use test::{black_box, Bencher};

struct TempStruct {
    name: String,
}

fn gen(size: u64) {
    use rand::distributions::Alphanumeric;
    use rand::thread_rng;
    // random string vec
    let mut rand_string: Vec<String> = Vec::new();
    for i in 0.. {
        let rand_index = thread_rng().gen_range(5, 30);
        let s: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(rand_index)
            .collect();
        rand_string.push(s);
    }

    // random u8 slice
    let mut rand_array = [0u64; size];
    rand::thread_rng().fill(&mut rand_array);

    println!("rand string: {:?}", rand_string.len());

    // par iter
    use ahash::AHasher;
    use rayon::prelude::*;

    let result: Vec<_> = (rand_array.to_vec(), rand_string.to_vec())
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

    eprintln!("hasher result:{:?}", &result[0..11]);
}

#[cfg(test)]
mod tests;
