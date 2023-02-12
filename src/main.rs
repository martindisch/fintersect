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
    let mut buffer = Vec::<u32>::with_capacity(CHUNK_SIZE);

    while read_chunk(&mut reader, &mut buffer) > 0 {
        info!("Sorting chunk");
        buffer.par_sort_unstable();
        buffer.clear();
    }

    info!("Done");

    Ok(())
}

fn read_chunk(reader: &mut impl Read, buffer: &mut Vec<u32>) -> usize {
    info!("Reading chunk");

    let mut num_buffer = [0; 4];
    while buffer.len() < CHUNK_SIZE
        && reader.read_exact(&mut num_buffer).is_ok()
    {
        let number = u32::from_le_bytes(num_buffer);
        buffer.push(number);
    }

    buffer.len()
}
