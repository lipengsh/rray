use super::list::Types as RustTypes;
use std::any::{Any, TypeId};

pub struct DynamicType<T> {
    type_name: RustTypes,
    data: T,
}

impl<T: 'static> DynamicType<T> {
    pub fn new(value: T) -> Self {
        DynamicType {
            type_name: rust_type(&value),
            data: value,
        }
    }
}

pub fn rust_type<T: 'static>(value: &T) -> RustTypes {
    let mut result = RustTypes::NONE;
    let type_t = TypeId::of::<T>();
    if type_t == TypeId::of::<String>() {
        result = RustTypes::STRING;
    } else if type_t == TypeId::of::<bool>() {
        result = RustTypes::BOOLEAN;
    } else if type_t == TypeId::of::<u8>() {
        result = RustTypes::U8;
    }
    result
}

#[cfg(test)]
mod test {
    use std::any::TypeId;

    fn generic_any<T: 'static>() {
        println!("type:{:?}", TypeId::of::<T>());
        assert_eq!(TypeId::of::<T>(), TypeId::of::<String>())
    }

    #[test]
    fn any_check() {
        // println!("type:{:?}", TypeId::of::<i8>());
        // println!("type name:{:?}", std::any::type_name::<Option<String>>());
        generic_any::<String>();
    }
}
