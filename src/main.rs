use env_logger::Env;
use eyre::Result;
use log::info;
use rayon::slice::ParallelSliceMut;
use std::{
    fs::File,
    io::{BufReader, Read},
};

const CHUNK_SIZE: usize = 2_usize.pow(30); // 2^30 * 4 bytes = 4 GiB

fn main() -> Result<()> {
    let env = Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let mut reader = BufReader::new(File::open("1.bin")?);
    let mut num_buffer = [0; 4];
    let mut chunk_buffer = Vec::<u32>::with_capacity(CHUNK_SIZE);

    info!("Reading chunk");

    while chunk_buffer.len() < CHUNK_SIZE
        && reader.read_exact(&mut num_buffer).is_ok()
    {
        let number = u32::from_le_bytes(num_buffer);
        chunk_buffer.push(number);
    }

    info!("Sorting chunk");

    chunk_buffer.par_sort_unstable();

    info!("Done");

    Ok(())
}
