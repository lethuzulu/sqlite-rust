use anyhow::{bail, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn read_varint(buffer: &[u8], start: usize) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut bytes_read = 0;

    for &byte in buffer[start..].iter() {
        bytes_read += 1;
        result = (result << 7) | (byte & 0x7F) as u64;
        if (byte & 0x80) == 0 {
            break;
        }
    }

    (result, bytes_read)
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 3 {
        bail!("Usage: <database path> .tables");
    }

    let command = &args[2];
    if command != ".tables" {
        bail!("Invalid command. Use .tables.");
    }

    let mut file = File::open(&args[1])?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;
    eprintln!("Page size: {}", page_size); // Debug

    let mut page = vec![0; page_size];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut page)?;

    let cell_count = u16::from_be_bytes([page[3], page[4]]) as usize;
    eprintln!("Cell count: {}", cell_count); // Debug

    if cell_count == 0 {
        bail!("No cells found on the page.");
    }

    // Read cell pointer array
    let mut cell_pointers = Vec::with_capacity(cell_count);
    for i in 0..cell_count {
        let offset = 8 + i * 2; // Start of cell pointer array
        if offset + 1 >= page_size {
            bail!("Invalid cell pointer offset at index {}", i);
        }
        let pointer = u16::from_be_bytes([page[offset], page[offset + 1]]) as usize;
        eprintln!("Cell pointer {}: {}", i, pointer); // Debug

        if pointer >= page_size {
            bail!("Cell pointer out of bounds at index {}", i);
        }
        cell_pointers.push(pointer);
    }

    let mut table_names = Vec::new();

    for (i, &pointer) in cell_pointers.iter().enumerate() {
        let cell_start = pointer;
        if cell_start >= page_size {
            bail!("Cell start out of bounds for cell index {}", i);
        }

        let (payload_size, payload_size_len) = read_varint(&page, cell_start);
        let pos = cell_start + payload_size_len;

        // Skip rowid
        let (_, rowid_size) = read_varint(&page, pos);
        let mut pos = pos + rowid_size;

        // Read header size
        let (header_size, header_size_len) = read_varint(&page, pos);
        pos += header_size_len;

        let header_end = pos + header_size as usize;
        if header_end > page_size {
            bail!("Header end out of bounds for cell index {}", i);
        }

        let mut serial_types = Vec::new();
        while pos < header_end {
            let (serial_type, size) = read_varint(&page, pos);
            serial_types.push(serial_type);
            pos += size;
        }

        pos = header_end;

        // Extract tbl_name (second column)
        if let Some(&second_type) = serial_types.get(1) {
            if second_type >= 13 && second_type % 2 == 1 {
                let str_len = ((second_type - 13) / 2) as usize;
                if pos + str_len > page_size {
                    bail!("String length out of bounds for cell index {}", i);
                }
                let table_name = String::from_utf8_lossy(&page[pos..pos + str_len]).to_string();
                if !table_name.starts_with("sqlite_") {
                    table_names.push(table_name);
                }
            }
        }
    }

    // Sort and print table names
    table_names.sort_unstable();
    println!("{}", table_names.join(" "));

    Ok(())
}
