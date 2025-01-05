use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            
            // Read file header (100 bytes should be enough for our needs)
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            // Get page size from header (bytes 16-17, big-endian)
            let page_size = u16::from_be_bytes([header[16], header[17]]);
            
            // The number of cells (tables) is stored in the page header of the sqlite_schema page
            // The page header starts after the file header (100 bytes)
            // The number of cells is a 2-byte big-endian value at offset 3 in the page header
            let mut cell_count_bytes = [0; 2];
            file.seek(std::io::SeekFrom::Start(103))?; // 100 bytes + 3 bytes offset
            file.read_exact(&mut cell_count_bytes)?;
            let number_of_tables = u16::from_be_bytes(cell_count_bytes);

            println!("database page size: {}", page_size);
            println!("number of tables: {}", number_of_tables);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }
    Ok(())
}