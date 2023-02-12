use env_logger::Env;
use eyre::Result;
use log::info;
use rayon::slice::ParallelSliceMut;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

const CHUNK_SIZE: usize = 4 * 2_usize.pow(30); // 4 * 2^30 * 4 bytes = 16 GiB

fn main() -> Result<()> {
    let env = Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let mut reader = BufReader::new(File::open("1.bin")?);
    let mut buffer = Vec::<u32>::with_capacity(CHUNK_SIZE);
    let mut chunks = Vec::new();

    while read_chunk(&mut reader, &mut buffer) > 0 {
        let chunk_name = format!("1_{}.bin", chunks.len());

        info!("Sorting chunk");
        buffer.par_sort_unstable();

        info!("Writing chunk {chunk_name}");
        write_chunk(&buffer, &chunk_name)?;

        chunks.push(chunk_name);
        buffer.clear();
    }

    info!("Done");

    Ok(())
}

fn read_chunk(reader: &mut impl Read, buffer: &mut Vec<u32>) -> usize {
    info!("Reading chunk");

    let mut num_buffer = [0; 4];
    while buffer.len() < CHUNK_SIZE && reader.read_exact(&mut num_buffer).is_ok() {
        let number = u32::from_le_bytes(num_buffer);
        buffer.push(number);
    }

    buffer.len()
}

fn write_chunk(buffer: &[u32], file_name: impl AsRef<Path>) -> Result<()> {
    let mut writer = BufWriter::new(File::create(file_name)?);
    let mut previous = None;

    for &number in buffer {
        if previous.map(|previous| number != previous).unwrap_or(true) {
            writer.write_all(&number.to_le_bytes())?;
        }

        previous = Some(number);
    }

    Ok(())
}
