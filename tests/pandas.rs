use csv::ReaderBuilder;
use itertools::Itertools;
use serde::Deserialize;
use std::error::Error;
use time::Instant;

struct DataFrame {
    header: csv::StringRecord,
    index: Vec<String>,
    dimension: Vec<String>,
    tag: Vec<String>,
    prf: Vec<f32>,
    mkt: Vec<f32>,
}

impl DataFrame {
    fn new() -> DataFrame {
        DataFrame {
            header: csv::StringRecord::new(),
            index: Vec::new(),
            dimension: Vec::new(),
            tag: Vec::new(),
            prf: Vec::new(),
            mkt: Vec::new(),
        }
    }

    fn push(&mut self, row: Row, index_value: String) {
        self.dimension.push(row.dimension);
        self.index.push(index_value);
        self.tag.push(row.tag);
        self.prf.push(row.prf);
        self.mkt.push(row.mkt);
    }
}

#[derive(Debug, Deserialize, PartialEq)]
struct Row {
    dimension: String,
    tag: String,
    #[serde(rename = "return_ptf_plus1")]
    prf: f32,
    #[serde(rename = "return_mkt_plus1")]
    mkt: f32,
}

#[test]
fn read_csv() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    // read csv file
    let mut file = ReaderBuilder::new()
        .has_headers(true)
        .from_path("tests/files/df.csv")?;

    let mut iter = file.into_deserialize();

    let mut dataframe = DataFrame::new();

    while let Some(result) = iter.next() {
        let record: Row = result?;
        // println!("row {:?}", record);
        let index_value: String = record.dimension.clone() + record.tag.clone().as_str();
        dataframe.push(record, index_value);
    }

    println!("time elapse:{}", start.elapsed().as_seconds_f32());
    println!("dataframe: {}", dataframe.dimension.len());

    // for (key, group) in &dataframe.index.into_iter().group_by() {
    //     println!("key:{},group:{}", key, group);
    // }

    Ok(())
}

#[test]
fn csv_2vec() -> Result<(), Box<dyn Error>> {
    Ok(())
}

#[test]
fn iter_groupby() -> Result<(), Box<dyn Error>> {
    Ok(())
}

#[test]
fn ndarray_test() {
    use ndarray::{arr2, ArrayView, Axis};

    let a = arr2(&[
        [1., 2.], // ... axis 0, row 0
        [4., 3.], // --- axis 0, row 1
        [5., 6.],
    ]); // ... axis 0, row 2

    let view = a.index_axis(Axis(0), 1);
    println!("{:?}", view);
}

#[test]
fn test_map_axis() {
    use ndarray::Axis;
    use ndarray::{arr1, arr2, Array3};
    let a = arr2(&[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]);

    let b = a.map_axis(Axis(0), |view| view.product());
    println!("{:?}", b);
    // let answer1 = arr1(&[22, 26, 30]);
    // assert_eq!(b, answer1);
    // let c = a.map_axis(Axis(1), |view| view.sum());
    // let answer2 = arr1(&[6, 15, 24, 33]);
    // assert_eq!(c, answer2);
    //
    // // Test zero-length axis case
    // let arr = Array3::<f32>::zeros((3, 0, 4));
    // let mut counter = 0;
    // let result = arr.map_axis(Axis(1), |x| {
    //     assert_eq!(x.shape(), &[0]);
    //     counter += 1;
    //     counter
    // });
    // assert_eq!(result.shape(), &[3, 4]);
    // itertools::assert_equal(result.iter().cloned().sorted(), 1..=3 * 4);
    //
    // let mut arr = Array3::<f32>::zeros((3, 0, 4));
    // let mut counter = 0;
    // let result = arr.map_axis_mut(Axis(1), |x| {
    //     assert_eq!(x.shape(), &[0]);
    //     counter += 1;
    //     counter
    // });
    // assert_eq!(result.shape(), &[3, 4]);
    // itertools::assert_equal(result.iter().cloned().sorted(), 1..=3 * 4);
}

#[test]
fn enumerate() {
    use itertools::enumerate;
    use itertools::Itertools;
    use ndarray::Array;

    let mut  data = vec![0, 0, 0, 1, 1, 0, 0, 1, 1, 2, 2];
    data.sort_by(|a, b| Ord::cmp(&a,&b));
    for (ch1, sub) in &data.into_iter().group_by(|&x| x) {
        for ch2 in sub {
            println!("{}", ch2);
            assert_eq!(ch1, ch2);
        }
        println!("one group");
    }


    // for (ch1, sub) in &"ABABBCACC".chars().group_by(|&x| x) {
    //     for ch2 in sub {
    //         println!("{}", ch2);
    //         assert_eq!(ch1, ch2);
    //     }
    //     println!("one group");
    // }
}

