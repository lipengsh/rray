/// dataframe struct and impl

use crate::pandas::series;
use crate::pandas::Columns;

pub struct Dataframe{
    data: Option<Vec<series::Series>>,
    columns: Option<Vec<Columns>>,
    index : Option<series::Series>
}