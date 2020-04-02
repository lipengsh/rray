use csv::ReaderBuilder;
use serde::Deserialize;
use std::error::Error;
use time::Instant;

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
fn groupby() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    // read csv file
    let mut file = ReaderBuilder::new()
        .has_headers(true)
        .from_path("tests/files/df.csv")?;

    let mut iter = file.into_deserialize();

    if let Some(result) = iter.next() {
        let record: Row = result?;
        println!("row {:?}", record);
    }

    println!("time elapse:{}", start.elapsed().as_seconds_f32());
    Ok(())
}
