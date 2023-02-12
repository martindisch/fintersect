use std::{
    fs::File,
    io::{BufReader, Read},
};

use eyre::Result;

fn main() -> Result<()> {
    let mut reader = BufReader::new(File::open("1.bin")?);
    let mut buffer = [0; 4];

    let mut count = 0_usize;
    while reader.read_exact(&mut buffer).is_ok() {
        let _number = u32::from_le_bytes(buffer);
        count += 1;
    }

    println!("{count}");

    Ok(())
}
