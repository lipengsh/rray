use itertools::Itertools;

#[test]
fn group_by(){
    let mut  data = vec![0, 0, 0, 1, 1, 0, 0, 1, 1, 2, 2];
    data.sort_by(|a, b| Ord::cmp(&a,&b));

    let grouper = data.iter().group_by(|elt| *elt);
    let mut last = None;
    for (key, group) in &grouper {
        if let Some(gr) = last.take() {
            for elt in gr {
                println!("elt:{}, key :{}", elt, key);
                println!("one group");
                assert!(elt != key && i32::abs(elt - key) == 1);
            }
        }
        last = Some(group);
    }
}