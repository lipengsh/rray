use rayon::prelude::*;
use std::collections::{hash_map, HashMap};

fn main(){
    reduce_with();
    sort_hashmap();
}

fn reduce_with(){
    let _sums = [(0, 1), (5, 6), (16, 2), (8, 9)]
        .par_iter()        // iterating over &(i32, i32)
        .cloned()          // iterating over (i32, i32)
        .reduce_with(|a, b| (a.0 + b.0, a.1 + b.1))
        .unwrap();

    // println!("sums:{:?}", sums);
    // assert_eq!(sums, (0 + 5 + 16 + 8, 1 + 6 + 2 + 9));
}

fn sort_hashmap(){
    let mut h = HashMap::new();

    h.insert(1,101);
    h.insert(2,33);
    h.insert(3,1231);
    h.insert(4,33);
    h.insert(5,45);
    h.insert(6,1231);

    for (key, val) in h.iter() {
        println!("key: {} val: {}", key, val);
    }
    println!("------------------");
    let mut b:Vec<_> = h.par_iter().collect();
    println!("b:{:?}", b);
    b.par_sort_by(|a,b|a.1.cmp(b.1));
    println!("b after sort :{:?}", b);


}

