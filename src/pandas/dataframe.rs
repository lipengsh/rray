/// dataframe struct and impl

use crate::pandas::Series;
use crate::pandas::Columns;
use crate::pandas::Index;

pub struct Dataframe {
    data: Option<Vec<Series>>,
    columns: Option<Vec<Columns>>,
    /// index trait
    /// box is a pointer to value in heap, pointer is in stack
    /// dyn trait(Index) is dynamic dispatch for polymorphism situation
    /// because trait has impl ,trait is dynamic
    index: Option<Box<dyn Index>>,
}