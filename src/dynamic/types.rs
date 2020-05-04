//! from https://doc.rust-lang.org/reference/types.html
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Types {
    NONE,
    BOOLEAN,
    // Numeric
    U8,
    U16,
    U32,
    U64,
    U128,
    I8,
    I16,
    I32,
    I64,
    I128,

    // Textual
    CHAR,
    STRING,

    NEVER,

    // Sequence types
    TUPLE,
    ARRAY,
    SLICE,

    // User-defined types
    STRUCT,
    ENUM,
    UNION,

    // Function types
    FUNCTIONS,
    CLOSURES,

    // Pointer types
    REFERENCES,
    RawPointers,
    FunctionPointers,

    // Trait types
    TRAIT,
    ImplTrait,
}
