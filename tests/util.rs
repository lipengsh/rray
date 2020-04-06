use std::vec::Vec;

struct TempStruct {
    name: String,
}

#[test]
fn new_vec() {
    let mut v: Vec<TempStruct> = Vec::new();
    v.push(TempStruct { name: "hello".to_string() });
    println!("{:?}", v[0].name);
}

#[test]
fn hash_insert() {
    use std::collections::hash_map::HashMap;
    let mut map = HashMap::new();

    map.insert(1, 2);
    map.insert(2, 1);
    map.insert(3, 4);
    map.insert(1,3);
    map.insert(1,5);

    assert_eq!(map[&2], 1);
}