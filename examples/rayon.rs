use rayon::prelude::*;

fn main(){
    reduce_with();
}

fn reduce_with(){
    let sums = [(0, 1), (5, 6), (16, 2), (8, 9)]
        .par_iter()        // iterating over &(i32, i32)
        .cloned()          // iterating over (i32, i32)
        .reduce_with(|a, b| (a.0 + b.0, a.1 + b.1))
        .unwrap();

    println!("sums:{:?}", sums);
    assert_eq!(sums, (0 + 5 + 16 + 8, 1 + 6 + 2 + 9));
}

