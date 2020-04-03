use std::vec::Vec;

struct TempStruct {
    name: String,
}

#[test]
fn new_vec() {
    let mut v:Vec<TempStruct> = Vec::new();
    v.push(TempStruct{name:"hello".to_string()});
    println!("{:?}", v[0].name);
}
