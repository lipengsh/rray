use parquet::basic::Type;

fn main() {
    let _b = ByteArrayTypeTest {
        x: ByteArrayTest { x: 21 as u8 },
    };
}

pub trait DataTypeTest<T> {
    /// Returns Parquet physical type.
    fn get_physical_type(&self) -> &T;

    /// Returns size in bytes for Rust representation of the physical type.
    fn get_type_size() -> usize;
}
pub struct ByteArrayTypeTest<T> {
    x: T,
}

impl<T> DataTypeTest<T> for ByteArrayTypeTest<T> {
    fn get_physical_type(&self) -> &T {
        &self.x
    }

    fn get_type_size() -> usize {
        100
    }
}

pub struct ByteArrayTest {
    x: u8,
}
