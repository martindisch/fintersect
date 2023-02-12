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

    let mut integers = Integers::new(File::open("1.bin")?);
    let mut buffer = Vec::<u32>::with_capacity(CHUNK_SIZE);
    let mut chunks = Vec::new();

    while read_chunk(&mut integers, &mut buffer) > 0 {
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

fn read_chunk(integers: &mut Integers, buffer: &mut Vec<u32>) -> usize {
    info!("Reading chunk");

    let chunk = integers.take(CHUNK_SIZE);
    buffer.extend(chunk);

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

/// An iterator that reads a bunch of `u32` from a file of little endian bytes.
struct Integers {
    reader: BufReader<File>,
    buffer: [u8; 4],
}

impl Integers {
    fn new(file: File) -> Self {
        Self {
            reader: BufReader::new(file),
            buffer: [0; 4],
        }
    }
}

impl Iterator for Integers {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        self.reader
            .read_exact(&mut self.buffer)
            .ok()
            .map(|_| u32::from_le_bytes(self.buffer))
    }
}
