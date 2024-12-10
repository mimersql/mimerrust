# Mimer SQL Rust API
The `mimerrust` crate implements an API for interacting with [Mimer SQL](https://www.mimer.com) databases from Rust. 

The Mimer SQL Rust API is built as a wrapper around the Mimer C API. It consists of two crates:
1. `mimerrust`: This crate implement the Mimer SQL Rust API. It uses the wrappers from [mimerrust-sys](https://crates.io/crates/mimerrust-sys) to create a high level, safe interface.
2. `mimerrust-sys`: This crate is responsible for the low-level wrapping of the C library into compatible Rust concepts. 
It is not meant for direct use, but rather as an intermediary wrapping step. To reduce build time and avoid requirements on LLVM and Clang on Windows a pre-generated binding is used by default. To generate and use a new binding, pass the `--features run_bindgen` when building.


A small example of how to use Mimer SQL Rust API can look like this:
```Rust
use mimerrust::{Connection, ToSql, CursorMode};

fn main() {
    print!("Connecting to database\n");
    let mut conn =
        Connection::open("", "RUSTUSER", "RUSTPASSWORD").unwrap_or_else(|ec| panic!("{}", ec));

    conn.execute_statement("DROP TABLE test_table").ok();
    println!("Creating table");
    conn.execute_statement("CREATE TABLE test_table (id INT primary key, text NVARCHAR(30))")
        .expect("Error creating table");
    println!("Inserting rows");
    let insert_stmt = conn.prepare("INSERT INTO test_table (id, text) VALUES(:id, :text)", 
        CursorMode::Forward).expect("Error preparing statement");

    let mut text = "Hello";
    let mut id = 1;
    let params: &[&dyn ToSql] = &[&id,&text];
    insert_stmt.execute_bind(params).expect("Error inserting first row"); 

    text = "World!";
    id = 2;
    let params: &[&dyn ToSql] = &[&id,&text];
    insert_stmt.execute_bind(params).expect("Error inserting second row");  

    let stmt = conn
        .prepare("SELECT * from test_table", CursorMode::Forward)
        .unwrap();
    let mut cursor = stmt.open_cursor().unwrap();
    println!("Fetching all rows");
    while let Some(row) = cursor.next_row().unwrap() {
        let id: i32 = row.get(1).unwrap().unwrap();
        let text: String = row.get(2).unwrap().unwrap();
        println!("id: {}, text: {}", id, text);
    }
}
```

## Resources
- [Documentation](https://docs.rs/mimerrust/latest/mimerrust/)
- [Mimer Information Technology](https://www.mimer.com)
- [Mimer SQL Developer site](https://developer.mimer.com)

## Credits
The following persons have contributed to the initial version of Mimer SQL Rust API:
- Edvard Axelman
- Edvin Bruce
- Simin Eriksson
- William Forslund
- Fredrik Hammarberg
- Viktor Wallsten

