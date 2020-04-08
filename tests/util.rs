use rand::Rng;
use std::hash::Hasher;
use std::vec::Vec;

struct TempStruct {
    name: String,
}

#[test]
fn new_vec() {
    let mut v: Vec<TempStruct> = Vec::new();
    v.push(TempStruct {
        name: "hello".to_string(),
    });
    println!("{:?}", v[0].name);
}

#[test]
fn hash_insert() {
    use std::collections::hash_map::HashMap;
    let mut map = HashMap::new();

    map.insert(1, 2);
    map.insert(2, 1);
    map.insert(3, 4);
    map.insert(1, 3);
    map.insert(1, 5);

    assert_eq!(map[&2], 1);
}

#[test]
fn hash_dup() {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    7920.hash(&mut hasher);
    println!("Hash is {:x}!", hasher.finish());

    let mut hasher1 = DefaultHasher::new();
    7920.hash(&mut hasher1);
    println!("Hash is {:x}!", hasher.finish());

    let mut hasher2 = DefaultHasher::new();
    7920.hash(&mut hasher2);
    println!("Hash is {:x}!", hasher.finish());
}

#[test]
fn ahaser() {
    use ahash::AHasher;
    use std::hash::Hasher;

    let mut hasher = AHasher::new_with_keys(123, 456);

    hasher.write_u32(1989);
    hasher.write_u8(11);
    hasher.write_u8(9);
    hasher.write(b"Huh?");

    // let mut v = vec![5, 1, 8, 22, 0, 44];

    println!("Hash is {:x}!", hasher.finish());
}

#[test]
fn chunks() {
    use rand::distributions::Alphanumeric;
    use rand::prelude::*;
    use rand::thread_rng;
    let mut rng = thread_rng();
    // let y: char = rng.gen();
    // println!("gen:{:?}", y);

    // let mut nums: Vec<i32> = (1..100).collect();
    // nums.shuffle(&mut rng);
    // println!("num:{:?}", nums);

    // if rand::random() {
    //     // generates a boolean
    //     // Try printing a random unicode code point (probably a bad idea)!
    //     println!("char: {}", rand::random::<char>());
    // }

    // get some random data:
    let mut rand_u8 = [0u8; 128];
    rand::thread_rng().fill(&mut rand_u8);
    // println!("rand_u8:{:?}", &rand_u8[..]);

    // random string vec
    let mut rand_string: Vec<String> = Vec::new();
    for i in 0..12800 {
        let rand_index = rng.gen_range(5, 30);
        let s: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(rand_index)
            .collect();
        rand_string.push(s);
    }

    println!("rand string: {:?}", rand_string.len());

    //chunks, step 16
    let size = 10;
    let result = rand_u8.chunks(size);
    // println!("result's length:{}", result.len());
    // println!("result:{:?}", result);

    // par iter
    use ahash::AHasher;
    use rayon::prelude::*;

    let mut rand_array = [0u64; 12800];
    rand::thread_rng().fill(&mut rand_array[..]);

    // let result: Vec<_> = rand_array
    //     .into_par_iter()
    //     .map(|&i| {
    //         let mut hasher = AHasher::default();
    //         hasher.write_u64(i);
    //         hasher.finish()
    //     })
    //     .collect();

    // let result: Vec<_> = rand_array
    //     .into_par_iter()
    //     .map_init(
    //         || AHasher::default(),
    //         |h, x| {
    //             h.write_u64(*x);
    //             h.finish()
    //         },
    //     )
    //     .collect();

    let result: Vec<_> = (rand_array.to_vec(), &rand_string)
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

    println!("hasher result:{:?}", &result[0..10]);

    //
}
