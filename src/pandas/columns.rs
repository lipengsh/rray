/// columns's struct
use arrow::datatypes::DataType;

#[warn(unused_variables)]
pub struct Columns {
    pub name: String,
    pub column_type: DataType,
    /// column in data,
    /// data = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
    ///         'Age':[27, 24, 22, 32],
    ///         'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
    ///         'Qualification':['Msc', 'MA', 'MCA', 'Phd']}
    /// column name : Age, column data_index:1
    pub data_index: u32,
}
