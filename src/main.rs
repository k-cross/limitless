fn main() {
    const SIZE: usize = 20;
    let a: Box<[i32]> = Box::new([0; SIZE]);

    for i in a {
        println!("{i}");
    }
}
