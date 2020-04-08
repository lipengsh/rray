```
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

    // let mut rand_string = [
    //     "abc", "abc", "abc", "abc", "abc", "abc", "abcd", "abcd", "abcd", "abcd", "abc", "abc",
    // ];
    //
    // let mut rand_array = [8u64; 12];
```