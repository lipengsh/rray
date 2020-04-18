use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;

fn gen_string(size: usize) -> Vec<String> {
    let mut rand_string: Vec<String> = Vec::new();
    for _i in 0..size {
        let rand_index = thread_rng().gen_range(5, 30);
        let s: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(rand_index)
            .collect();
        rand_string.push(s);
    }
    rand_string
}

fn gen_u32(size: usize) -> Vec<u32> {
    // random u32 array
    let mut rand_array = vec![0u32; size];
    let mut rng = thread_rng();
    for i in 0..size {
        rand_array[i] = rng.gen::<u32>();
    }
    rand_array
}

fn gen_f32(size: usize) -> Vec<f32> {
    let mut rand_f32: Vec<f32> = vec![0f32; size];
    let mut rng = thread_rng();
    for i in 0..size {
        rand_array[i] = rng.gen::<f32>();
    }
    rand_array
}
