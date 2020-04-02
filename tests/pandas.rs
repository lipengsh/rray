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

    for (key, group) in &dataframe.index.into_iter().group_by() {
        println!("key:{},group:{}", key, group);
    }

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
