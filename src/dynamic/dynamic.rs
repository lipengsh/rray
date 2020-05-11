use super::types::Types as RustTypes;
use std::any::{Any, TypeId};
use std::fmt;

pub struct Dynamic {
    type_name: RustTypes,
    data: Box<dyn Any>,
}

impl Dynamic {
    pub fn new<T>(value: T) -> Self
    where
        T: 'static,
    {
        Dynamic {
            type_name: rust_type::<T>(),
            data: Box::new(value),
        }
    }

    pub fn native<T: Clone>(&self) -> Result<T, &'static str>
    where
        T: 'static,
    {
        if rust_type::<T>() != self.type_name {
            println!("rust type:{:?}", rust_type::<T>());
            println!("self type name:{:?}", self.type_name);
            return Err("wrong type at 26");
        }

        let result = self.data.downcast_ref::<T>().unwrap() as &T;

        Ok(result.clone())
    }

    pub fn type_name(&self) -> RustTypes {
        self.type_name
    }
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
            RustTypes::U16 => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<u16>().unwrap()
            ),
            RustTypes::U32 => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<u32>().unwrap()
            ),
            RustTypes::U64 => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<u64>().unwrap()
            ),
            RustTypes::U128 => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<u128>().unwrap()
            ),
            RustTypes::I8 => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<i8>().unwrap()
            ),
            RustTypes::I16 => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<i16>().unwrap()
            ),
            RustTypes::I32 => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<i32>().unwrap()
            ),
            RustTypes::I64 => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<i64>().unwrap()
            ),
            RustTypes::I128 => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<i128>().unwrap()
            ),
            RustTypes::CHAR => write!(
                f,
                "type:{:?}, data:{}",
                self.type_name,
                data.downcast_ref::<char>().unwrap()
            ),
            _ => write!(f, "wrong types or not supported types"),
        }
    }
}

pub fn rust_type<T: 'static>() -> RustTypes {
    let type_t = TypeId::of::<T>();

    if type_t == TypeId::of::<String>() {
        return RustTypes::STRING;
    } else if type_t == TypeId::of::<bool>() {
        return RustTypes::BOOLEAN;
    } else if type_t == TypeId::of::<u8>() {
        return RustTypes::U8;
    } else if type_t == TypeId::of::<u16>() {
        return RustTypes::U16;
    } else if type_t == TypeId::of::<u32>() {
        return RustTypes::U32;
    } else if type_t == TypeId::of::<u64>() {
        return RustTypes::U64;
    } else if type_t == TypeId::of::<u128>() {
        return RustTypes::U128;
    } else if type_t == TypeId::of::<i8>() {
        return RustTypes::I8;
    } else if type_t == TypeId::of::<i16>() {
        return RustTypes::I16;
    } else if type_t == TypeId::of::<i32>() {
        return RustTypes::I32;
    } else if type_t == TypeId::of::<i64>() {
        return RustTypes::I64;
    } else if type_t == TypeId::of::<i128>() {
        return RustTypes::I128;
    } else if type_t == TypeId::of::<f32>() {
        return RustTypes::F32;
    } else if type_t == TypeId::of::<f64>() {
        return RustTypes::F64;
    } else if type_t == TypeId::of::<char>() {
        return RustTypes::CHAR;
    } else {
        return RustTypes::NONE;
    }
}

#[cfg(test)]
mod test {
    use crate::dynamic::dynamic::{rust_type, Dynamic};
    use crate::dynamic::types::Types as RustTypes;
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
        let _check_type: String = "hello".to_string();
        let result = rust_type::<String>();
        println!("types:{:?}", result);

        // check Dynamic
        let result = Dynamic::new::<String>("hello".to_string());

        // print dynamic
        println!("result:: {}", result);

        // print native
        println!("native result:: {}", result.native::<String>().unwrap());

        // print native from type name
        if result.type_name == RustTypes::STRING {
            println!(
                "native type name result:: {}",
                result.native::<String>().unwrap()
            );
        }
    }
}
