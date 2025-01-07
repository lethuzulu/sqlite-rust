use anyhow::{bail, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use regex::Regex;

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
        bail!("Usage: <database path> \"SELECT <column> FROM <table>\"");
    }

    let db_path = &args[1];
    let query = &args[2];

    // Extract column and table name
    let query_parts: Vec<&str> = query.split_whitespace().collect();
    if query_parts.len() < 4 || query_parts[0].to_uppercase() != "SELECT" || query_parts[2].to_uppercase() != "FROM" {
        bail!("Invalid query. Use: SELECT <column> FROM <table>");
    }
    let column_name = query_parts[1];
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

    // Read sqlite_schema to find table and its CREATE TABLE statement
    let mut rootpage = None;
    let mut create_table_stmt = None;

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

        // Extract columns from sqlite_schema: tbl_name (2nd column), rootpage (4th), and sql (5th)
        let mut column_idx = 0;
        let mut current_pos = pos;
        for &serial_type in &serial_types {
            if column_idx == 1 {
                if serial_type >= 13 && serial_type % 2 == 1 {
                    let str_len = ((serial_type - 13) / 2) as usize;
                    let name = String::from_utf8_lossy(&page[current_pos..current_pos + str_len]);
                    if name == table_name {
                        // Rootpage (4th column)
                        let rootpage_type = serial_types.get(3).ok_or_else(|| anyhow::anyhow!("Missing rootpage"))?;
                        if *rootpage_type == 6 {
                            rootpage = Some(u32::from_be_bytes([
                                page[current_pos + str_len],
                                page[current_pos + str_len + 1],
                                page[current_pos + str_len + 2],
                                page[current_pos + str_len + 3],
                            ]));
                        }

                        // SQL column (5th column)
                        let sql_pos = current_pos + str_len + 4;
                        let sql_serial_type = serial_types.get(4).ok_or_else(|| anyhow::anyhow!("Missing SQL column"))?;
                        if *sql_serial_type >= 13 && *sql_serial_type % 2 == 1 {
                            let sql_len = ((*sql_serial_type - 13) / 2) as usize;
                            create_table_stmt = Some(String::from_utf8_lossy(&page[sql_pos..sql_pos + sql_len]).to_string());
                        }
                        break;
                    }
                }
            }
            current_pos += if serial_type >= 13 && serial_type % 2 == 1 {
                ((serial_type - 13) / 2) as usize
            } else if serial_type == 6 {
                4
            } else {
                0
            };
            column_idx += 1;
        }

        if rootpage.is_some() && create_table_stmt.is_some() {
            break;
        }
    }

    let rootpage = rootpage.ok_or_else(|| anyhow::anyhow!("Table '{}' not found in sqlite_schema", table_name))?;
    let create_table_stmt = create_table_stmt.ok_or_else(|| anyhow::anyhow!("CREATE TABLE statement not found for '{}'", table_name))?;
    eprintln!("Root page for table '{}': {}", table_name, rootpage);
    eprintln!("CREATE TABLE statement: {}", create_table_stmt);

    // Parse CREATE TABLE statement to determine column index
    let re = Regex::new(r"CREATE TABLE \w+ \((.*?)\)").unwrap();
    let captures = re.captures(&create_table_stmt).ok_or_else(|| anyhow::anyhow!("Failed to parse CREATE TABLE statement"))?;
    let columns = captures[1].split(',').map(|s| s.trim()).collect::<Vec<_>>();
    let column_index = columns.iter().position(|&col| col.starts_with(column_name)).ok_or_else(|| anyhow::anyhow!("Column '{}' not found in '{}'", column_name, table_name))?;

    eprintln!("Column index for '{}': {}", column_name, column_index);

    // Read root page to extract rows
    file.seek(SeekFrom::Start((rootpage as usize - 1) * page_size as u64))?;
    file.read_exact(&mut page)?;

    let row_count = u16::from_be_bytes([page[3], page[4]]) as usize;
    let mut results = Vec::new();

    for i in 0..row_count {
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

        // Skip columns before the target column
        for idx in 0..column_index {
            let serial_type = serial_types.get(idx).ok_or_else(|| anyhow::anyhow!("Invalid column index"))?;
            if *serial_type >= 13 && *serial_type % 2 == 1 {
                pos += ((*serial_type - 13) / 2) as usize;
            }
        }

        // Read the target column
        let serial_type = serial_types.get(column_index).ok_or_else(|| anyhow::anyhow!("Invalid column index"))?;
        if *serial_type >= 13 && *serial_type % 2 == 1 {
            let str_len = ((*serial_type - 13) / 2) as usize;
            let value = String::from_utf8_lossy(&page[pos..pos + str_len]).to_string();
            results.push(value);
        }
    }

    // Print results
    for result in results {
        println!("{}", result);
    }

    Ok(())
}
