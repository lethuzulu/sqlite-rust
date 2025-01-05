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
    if args.len() != 3 {
        bail!("Usage: <database path> \"SELECT COUNT(*) FROM <table>\"");
    }

    let db_path = &args[1];
    let query = &args[2];

    // Extract table name from query
    let query_parts: Vec<&str> = query.split_whitespace().collect();
    if query_parts.len() < 4 || query_parts[0].to_uppercase() != "SELECT" || query_parts[1] != "COUNT(*)" || query_parts[2].to_uppercase() != "FROM" {
        bail!("Invalid query. Use: SELECT COUNT(*) FROM <table>");
    }
    let table_name = query_parts[3];

    let mut file = File::open(db_path)?;
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

    // Read cell pointer array and find table row in sqlite_schema
    let mut table_root_page = None;
    for i in 0..cell_count {
        let offset = 8 + i * 2;
        let pointer = u16::from_be_bytes([page[offset], page[offset + 1]]) as usize;
        let (payload_size, payload_size_len) = read_varint(&page, pointer);
        let pos = pointer + payload_size_len;

        // Skip rowid
        let (_, rowid_size) = read_varint(&page, pos);
        let mut pos = pos + rowid_size;

        // Read header size
        let (header_size, header_size_len) = read_varint(&page, pos);
        pos += header_size_len;

        let header_end = pos + header_size as usize;
        let mut serial_types = Vec::new();
        while pos < header_end {
            let (serial_type, size) = read_varint(&page, pos);
            serial_types.push(serial_type);
            pos += size;
        }

        pos = header_end;

        // Skip to tbl_name column (2nd column)
        let mut column_idx = 0;
        let mut current_pos = pos;
        for &serial_type in &serial_types {
            if column_idx == 1 {
                if serial_type >= 13 && serial_type % 2 == 1 {
                    let str_len = ((serial_type - 13) / 2) as usize;
                    let name = String::from_utf8_lossy(&page[current_pos..current_pos + str_len]);
                    if name == table_name {
                        // Rootpage is in the 4th column (index 3)
                        if let Some(&rootpage_type) = serial_types.get(3) {
                            if rootpage_type == 6 {
                                let rootpage = u32::from_be_bytes([
                                    page[current_pos],
                                    page[current_pos + 1],
                                    page[current_pos + 2],
                                    page[current_pos + 3],
                                ]);
                                table_root_page = Some(rootpage);
                                break;
                            }
                        }
                    }
                }
            }
            if serial_type >= 13 && serial_type % 2 == 1 {
                current_pos += ((serial_type - 13) / 2) as usize;
            } else if serial_type == 6 {
                current_pos += 4;
            }
            column_idx += 1;
        }

        if table_root_page.is_some() {
            break;
        }
    }

    let rootpage = table_root_page.ok_or_else(|| anyhow::anyhow!("Table '{}' not found in sqlite_schema", table_name))?;
    eprintln!("Root page for table '{}': {}", table_name, rootpage);

    // Read root page to count rows
    file.seek(SeekFrom::Start((rootpage as usize - 1) * page_size as u64))?;
    file.read_exact(&mut page)?;

    let row_count = u16::from_be_bytes([page[3], page[4]]) as usize;
    println!("{}", row_count);

    Ok(())
}
