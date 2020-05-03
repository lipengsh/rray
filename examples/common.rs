struct Yoo;
struct Xoo;

pub trait Zoo {
    fn zoo(&self) {
        println!(" zoo ");
    }
}

impl<T> Zoo for T {}

fn main() {
    let z = Yoo;
    z.zoo();

    let x = Xoo;
    x.zoo();

    let m: i32 = 21;
    m.zoo();
}
