use env_logger::Env;
use eyre::Result;
use log::info;
use rayon::slice::ParallelSliceMut;
use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    iter::Peekable,
    path::Path,
};

const CHUNK_SIZE: usize = 4 * 2_usize.pow(30); // 4 * 2^30 * 4 bytes = 16 GiB

fn main() -> Result<()> {
    let env = Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    external_sort_distinct("1.bin", "1_sorted_distinct.bin")?;
    external_sort_distinct("2.bin", "2_sorted_distinct.bin")?;

    merge_shared(
        "1_sorted_distinct.bin",
        "1_sorted_distinct.bin",
        "result.bin",
    )?;

    info!("Done");

    Ok(())
}

fn external_sort_distinct(in_file: &str, out_file: &str) -> Result<()> {
    info!("Starting external sort of {in_file}");

    let mut buffer = Vec::<u32>::with_capacity(CHUNK_SIZE);
    let mut integers = Integers::new(File::open(in_file)?);
    let mut chunks = Vec::new();

    while read_chunk(&mut integers, &mut buffer) > 0 {
        let chunk_name = format!("{}_{in_file}", chunks.len());

        info!("Sorting chunk");
        buffer.par_sort_unstable();

        info!("Writing chunk {chunk_name}");
        write_chunk(&buffer, &chunk_name)?;

        chunks.push(chunk_name);
        buffer.clear();
    }

    drop(buffer);

    info!("Merging chunks");
    merge_distinct(&chunks, out_file)?;

    info!("Deleting chunks");
    for chunk in &chunks {
        fs::remove_file(chunk)?;
    }

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

fn merge_distinct(inputs: &[impl AsRef<Path>], output: impl AsRef<Path>) -> Result<()> {
    let mut writer = BufWriter::new(File::create(output)?);
    let mut readers = inputs
        .iter()
        .map(|file_name| Integers::new(File::open(file_name).unwrap()).peekable())
        .collect::<Vec<Peekable<Integers>>>();
    let mut last_write: Option<u32> = None;

    // Determine the next number to write by going through all reader iterators and...
    while let Some(&next_write) = readers
        .iter_mut()
        .filter_map(|reader| {
            // ...as long as the next number of the iterator is one we've already written...
            while last_write
                .map(|last_written| {
                    if let Some(&next) = reader.peek() {
                        next <= last_written
                    } else {
                        false
                    }
                })
                .unwrap_or(false)
            {
                // ...advance the iterator and check again...
                reader.next();
            }

            // ...then finally once we have a new number (or have reached the iterator's end),
            // return what's next...
            reader.peek()
        })
        // ...and the next number to write is then the smallest of the ones we haven't written yet.
        .min()
    {
        writer.write_all(&next_write.to_le_bytes())?;
        last_write = Some(next_write);
    }

    Ok(())
}

fn merge_shared(
    input_1: impl AsRef<Path>,
    input_2: impl AsRef<Path>,
    output: impl AsRef<Path>,
) -> Result<()> {
    info!("Combining files into set of shared values");

    let mut writer = BufWriter::new(File::create(output)?);
    let mut first = Integers::new(File::open(input_1)?).peekable();
    let mut second = Integers::new(File::open(input_2)?).peekable();

    loop {
        match (first.peek(), second.peek()) {
            (Some(next_1), Some(next_2)) if next_1 == next_2 => {
                writer.write_all(&next_1.to_le_bytes())?;
                first.next();
                second.next();
            }
            (Some(next_1), Some(next_2)) if next_1 > next_2 => {
                second.next();
            }
            (Some(next_1), Some(next_2)) if next_1 < next_2 => {
                first.next();
            }
            _ => break,
        }
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
