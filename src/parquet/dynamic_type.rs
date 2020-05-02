use std::any::Any;

pub enum TypeName {
    STRING,
}

pub struct DynamicType {
    type_name: String,
    data: Box<dyn Any>,
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
