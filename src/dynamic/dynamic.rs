use super::types::Types as RustTypes;
use std::any::{Any, TypeId};
use std::fmt;

pub struct Dynamic {
    type_name: RustTypes,
    data: Box<dyn Any>,
}

impl Dynamic {
    pub fn new<T: 'static>(value: T) -> Self {
        Dynamic {
            type_name: rust_type(&value),
            data: Box::new(value),
        }
    }
}

// native_rust_type(dynamic_data ,RustTypes::BOOLEAN)
// return option,like Some(32), or None
macro_rules! native_rust_type {
    ($data:ident, $rray_type:ident, $native_type:ty) => {
        if let Some(value) = $data.downcast_ref::<$native_type>() {
            Some(value)
        } else {
            None
        }
    };
}

impl fmt::Display for Dynamic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data = &self.data;
        let type_name = self.type_name;
        match type_name {
            RustTypes::BOOLEAN => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<bool>().unwrap()
            ),
            RustTypes::STRING => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<String>().unwrap()
            ),
            RustTypes::U8 => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<u8>().unwrap()
            ),
            // todo , add more types
            _ => write!(f, "wrong types or not supported types"),
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
    } else if type_t == TypeId::of::<u16>() {
        result = RustTypes::U16;
    } else if type_t == TypeId::of::<u32>() {
        result = RustTypes::U32;
    } else if type_t == TypeId::of::<u64>() {
        result = RustTypes::U64;
    } else if type_t == TypeId::of::<u128>() {
        result = RustTypes::U128;
    } else if type_t == TypeId::of::<i8>() {
        result = RustTypes::I8;
    } else if type_t == TypeId::of::<i16>() {
        result = RustTypes::I16;
    } else if type_t == TypeId::of::<i32>() {
        result = RustTypes::I32;
    } else if type_t == TypeId::of::<i64>() {
        result = RustTypes::I64;
    } else if type_t == TypeId::of::<i128>() {
        result = RustTypes::I128;
    } else if type_t == TypeId::of::<char>() {
        result = RustTypes::CHAR;
    } else if type_t == TypeId::of::<String>() {
        result = RustTypes::STRING;
    }
    // not all types , no Sequence, user-defined,function,pointer,trait
    result
}

#[cfg(test)]
mod test {
    use crate::dynamic::dynamic::{rust_type, Dynamic};
    use std::any::TypeId;

    fn generic_any<T: 'static>() {
        println!("type:{:?}", TypeId::of::<T>());
        assert_eq!(TypeId::of::<T>(), TypeId::of::<String>())
    }

    #[test]
    fn any_check() {
        // check type
        generic_any::<String>();

        // check rust_type function
        let check_type: String = "hello".to_string();
        let result = rust_type::<String>(&check_type);
        println!("types:{:?}", result);

        // check Dynamic
        let result = Dynamic::new::<String>("hello".to_string());

        // print dynamic
        println!("result:: {}", result);
    }
}
