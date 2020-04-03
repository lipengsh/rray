/// dataframe struct and impl

use crate::pandas::series;


pub struct Dataframe{
    data: Vec<series::Series>,
}