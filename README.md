# Mimer SQL Rust API
This project aims to create an API for interacting with [Mimer SQL](https://www.mimer.com) databases from Rust. 

The Mimer SQL Rust API is built as a wrapper around the Mimer C API. It consists of two crates:
1. `mimerrust`: Iimplements the Mimer SQL Rust API. It uses the wrappers from `mimerrust-sys` to create a high level, safe interface.
2. `mimerrust-sys`: Handles low-level wrapping of the C library into Rust-compatible concepts. 
It is not intended for direct use, but rather as an intermediary wrapping step. To reduce build time and avoid requiring LLVM and Clang on Windows a pre-generated binding is used by default. To generate and use a new binding, pass the `--features run_bindgen` flag when building.


Example usage:
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

## How to Build, Test, and Run Examples
This project uses the `cargo` build tool and is only a library, 
so no main.rs file is provided, and the `cargo run` command cannot be used directly.

### User Setup
All examples and tests use an ident called `RUSTUSER` with the password `RUSTPASSWORD`. To create the user in Mimer SQL, run `bsql` or DbVisualizer as SYSADM, or an other ident with proper privileges, and create the ident as follows:
```SQL
create ident RUSTUSER as user using 'RUSTPASSWORD';
grant databank to RUSTUSER;
```
To run the examples you need a databank as well. Log into Mimer SQL using `bsql` or DbVisualizer as `RUSTUSER` and run:
```SQL
create datank rustdb
```
The tests will create the necessary databanks needed.

### Build the Project
Run: 
```
cargo build
```

### Run Tests
Run: 
```
cargo test -- --test-threads=1
```
Tests must be run serially on a single thread to avoid concurrency issues, as different tests access and modify the same database schema.

### Generate Documentation
Run: 
```
cargo doc --no-deps
```
To automatically open the generated html file, use: 
```
cargo doc --no-deps --open
```

Documenation is also available online at [docs.rs](https://docs.rs/mimerrust/latest/mimerrust)

### Run the Example Program
Navigate to the example directory and run:
```
cargo run
```

For additional commands, see the cargo documentation

## Resources
- [Documentation](https://docs.rs/mimerrust/latest/mimerrust/)
- [Mimer Information Technology](https://www.mimer.com)
- [Mimer SQL Developer site](https://developer.mimer.com)

## Credits
The following contributors worked on the initial version of the Mimer SQL Rust API:
- [Edvard Axelman](https://github.com/popfumo)
- [Edvin Bruce](https://github.com/Bruce1887)
- [Simon Eriksson](https://github.com/sier8161)
- [William Forslund](https://github.com/Forslund16)
- [Fredrik Hammarberg](https://github.com/efreham1)
- [Viktor Wallsten](https://github.com/viwa3399)

