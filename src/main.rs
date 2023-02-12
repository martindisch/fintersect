use env_logger::Env;
use eyre::Result;
use log::info;
use std::{
    fs::File,
    io::{BufReader, Read},
};

const CHUNK_SIZE: usize = 2_usize.pow(30); // 1 GiB

fn main() -> Result<()> {
    let env = Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let mut reader = BufReader::new(File::open("1.bin")?);
    let mut num_buffer = [0; 4];
    let mut chunk_buffer = Vec::<u32>::with_capacity(CHUNK_SIZE);

    info!("Reading chunk");

    for _ in 0..CHUNK_SIZE {
        if reader.read_exact(&mut num_buffer).is_ok() {
            let number = u32::from_le_bytes(num_buffer);
            chunk_buffer.push(number);
        } else {
            break;
        }
    }

    info!("Sorting chunk");

    chunk_buffer.sort_unstable();

    info!("Done");

    Ok(())
}
