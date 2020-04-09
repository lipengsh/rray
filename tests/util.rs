#![feature(test)]
extern crate crossbeam;
extern crate test;
use crossbeam::{scope, thread};
// use crossbeam_utils::thread;
use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;
use rayon::prelude::*;
use std::hash::Hasher;
use std::vec::Vec;
use test::{black_box, Bencher};

struct TempStruct {
    name: String,
}

fn gen(string: &[String], array: &[u64]) {
    // par iter
    use ahash::AHasher;

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
    const SIZE: usize = 1024 * 1000;
    // random string vec
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
    // rand::thread_rng().fill(&mut rand_array[..]);

    use std::time::Instant;

    let start = Instant::now();
    gen(&rand_string, &rand_array);
    eprintln!("used time:{} millis", start.elapsed().as_millis());
}

#[test]
fn rand_string() {
    const SIZE: usize = 128;
    const thread_chunks: usize = 36;
    // parallel generate string
    // let mut rand_string: Vec<String> = Vec::with_capacity(SIZE);
    let mut rand_string = vec!["a".to_string(); SIZE];

    // thread create chunks

    for (n, chunk) in rand_string.chunks_mut(thread_chunks).enumerate() {
        thread::scope(|s| {
            s.spawn(|_| {
                for i in 0..chunk.len() {
                    let rand_index = thread_rng().gen_range(5, 30);
                    chunk[i] = thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(rand_index)
                        .collect();
                }
            });
        })
        .unwrap();
    }

    println!("string:{:?}", rand_string);

    // println!("string length:{}", rand_string.len());
    // let result = rand_string
    //     .par_iter_mut()
    //     .map(|x| {
    //         let rand_index = thread_rng().gen_range(5, 30);
    //         *x = thread_rng()
    //             .sample_iter(&Alphanumeric)
    //             .take(rand_index)
    //             .collect();
    //     })
    //     .collect();
    // println!("string:{:?}", rand_string);
    // println!("string length:{}", rand_string.len());
}

#[test]
fn cb_scope() {
    use crossbeam::thread as cb_thread;
    let mut result = [0; 1000];
    for (n, chunk) in result.chunks_mut(300).enumerate() {
        cb_thread::scope(|scope| {
            scope.spawn(|_| {
                println!("Thread {}", n);
                for i in 0..chunk.len() {
                    chunk[i] = i;
                }
            });
        });
    }
}
