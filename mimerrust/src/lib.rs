/* *********************************************************************
* Copyright (c) 2024 Mimer Information Technology
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*
* See license for more details.
* *********************************************************************/

//! This create implements an API for interacting with Mimer SQL databases from Rust. 
//! 
//! The Mimer SQL Rust API is built as a wrapper around the Mimer C API. It consists of two crates:
//! 1. `mimerrust`: This crate implement the Mimer SQL Rust API. It uses the wrappers from [mimerrust-sys](https://crates.io/crates/mimerrust-sys) to create a high level, safe interface.
//! 2. `mimerrust-sys`: This crate is responsible for the low-level wrapping of the C library into compatible Rust concepts. 
//! It is not meant for direct use, but rather as an intermediary wrapping step. To reduce build time and avoid requirements on LLVM and Clang on Windows a pre-generated binding is used by default. To generate and use a new binding, pass the `--features run_bindgen` when building.
//! 
//! A small example of how to use Mimer SQL Rust API can look like this:
//! ```
//! use mimerrust::{Connection, ToSql, CursorMode};
//! 
//! fn main() {
//!     print!("Connecting to database\n");
//!     let mut conn =
//!         Connection::open("", "RUSTUSER", "RUSTPASSWORD").unwrap_or_else(|ec| panic!("{}", ec));
//! 
//!     conn.execute_statement("DROP TABLE test_table").ok();
//!     println!("Creating table");
//!     conn.execute_statement("CREATE TABLE test_table (id INT primary key, text NVARCHAR(30))")
//!         .expect("Error creating table");
//!     println!("Inserting rows");
//!     let insert_stmt = conn.prepare("INSERT INTO test_table (id, text) VALUES(:id, :text)", 
//!         CursorMode::Forward).expect("Error preparing statement");
//! 
//!     let mut text = "Hello";
//!     let mut id = 1;
//!     let params: &[&dyn ToSql] = &[&id,&text];
//!     insert_stmt.execute_bind(params).expect("Error inserting first row"); 
//! 
//!     text = "World!";
//!     id = 2;
//!     let params: &[&dyn ToSql] = &[&id,&text];
//!     insert_stmt.execute_bind(params).expect("Error inserting second row");  
//! 
//!     let stmt = conn
//!         .prepare("SELECT * from test_table", CursorMode::Forward)
//!         .unwrap();
//!     let mut cursor = stmt.open_cursor().unwrap();
//!     println!("Fetching all rows");
//!     while let Some(row) = cursor.next_row().unwrap() {
//!         let id: i32 = row.get(1).unwrap().unwrap();
//!         let text: String = row.get(2).unwrap().unwrap();
//!         println!("id: {}, text: {}", id, text);
//!     }
//! }
//! ```
//! All examples and tests uses an ident called `RUSTUSER` with the password `RUSTPASSWORD`. To create the user in Mimer SQL, run `bsql` or DbVisualizer as SYSADM, or an other ident with proper privileges, and create the ident as follows:
//! ```SQL
//! create ident RUSTUSER as user using 'RUSTPASSWORD';
//! grant databank to RUSTUSER;
//! ```
//! To run the examples you need a databank as well. Log into Mimer SQL using `bsql` or DbVisualizer as `RUSTUSER` and run:
//! ```SQL
//! create datank rustdb
//! ```
//! The tests create the databanks needed.
//! 
//! # Requirements
//! This API requires the Mimer SQL C API to be installed on the system. The API is tested with Mimer SQL 11.0.8D.
//! Furthermore, bindings to the Mimer SQL C API are generated at compile time using [bindgen](https://docs.rs/bindgen/latest/bindgen/), which requires [clang](https://clang.llvm.org/docs/index.html) to be installed on the system.
//! The bindings are not re-built automatically, instead a pre-generated binding is used. This is to avoid requirements on having Clang on for example Windows.
//! To generate new bindings, go into the `mimerrust-bindings` and run `cargo build`.
//!

pub(crate) mod common;
pub(crate) mod connection;
pub(crate) mod cursor;
pub(crate) mod inner_connection;
pub(crate) mod inner_statement;
pub(crate) mod mimer_error;
pub(crate) mod row;
pub(crate) mod statement;
pub(crate) mod testing;
pub(crate) mod transaction;

/// Handles datatypes and their conversions between Rust and Mimer SQL.
///
/// This module contains definitions for the [ToSql] and [FromSql] traits, which defines how a rust datatype should be converted to a Mimer SQL type and vice versa.
/// These traits are implemented for a variety of types as described in the documentation for each trait, but also allows for custom implementations by the user.
/// Below follows an example of how this can be done:
///
/// 1. Define a custom type (e.g. a struct):
/// ```
/// #[derive(Debug, PartialEq)]
/// struct CustomType {
///     first_value: i32,
///     second_value: i32,
/// }
/// ```
///
/// 2. Implement the [ToSql] and [FromSql] trait for the custom type:
/// The ToSql trait defines how the custom datatype should be stored in the database (e.g. if the data is to be stored as a string-variant or integer-variant),
/// whereas the FromSql trait defines from what Mimerdatatype the custom datatype can be converted from.
///
/// ```
/// impl ToSql for CustomType {
///     fn to_sql(&self) -> MimerDatatype {
///         let mut bytes: [u8; 8] = [0; 8];
///         bytes[..4].copy_from_slice(&self.first_value.to_le_bytes());
///         bytes[4..].copy_from_slice(&self.second_value.to_le_bytes());
///         MimerDatatype::BinaryArray(bytes.to_vec())
///     }
/// }
/// # use mimerrust::*;
/// # #[derive(Debug, PartialEq)]
/// # struct CustomType {
/// #     first_value: i32,
/// #     second_value: i32,
/// # }
/// impl FromSql for CustomType {
///     fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
///         match value {
///                 MimerDatatype::BinaryArray(v) => {
///                     if v.len() != 8 {
///                         return Err(-26200); // Mimer Rust API error code for unsupported type conversion.
///                     }
///                     Ok(CustomType {
///                         first_value: i32::from_le_bytes(v[0..4].try_into().unwrap()),
///                         second_value: i32::from_le_bytes(v[4..8].try_into().unwrap())
///                     }
///                     )
///                 }
///                 _ => Err(-26200), // Mimer Rust API error code for unsupported type conversion.
///             }
///         }    
/// }
/// ```
///
/// 3. Once both the ToSql and FromSql traits are implemented, the custom type can be used in the API. An example of this is shown below:
/// ```
/// # use mimerrust::*;
/// # #[derive(Debug, PartialEq)]
/// # struct CustomType {
/// #     first_value: i32,
/// #     second_value: i32,
/// # }
/// # impl ToSql for CustomType {
/// #     fn to_sql(&self) -> MimerDatatype {
/// #         let mut bytes: [u8; 8] = [0; 8];
/// #         bytes[..4].copy_from_slice(&self.first_value.to_le_bytes());
/// #         bytes[4..].copy_from_slice(&self.second_value.to_le_bytes());
/// #         MimerDatatype::BinaryArray(bytes.to_vec())
/// #     }
/// # }
/// # impl FromSql for CustomType {
/// #     fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
/// #         match value {
/// #                 MimerDatatype::BinaryArray(v) => {
/// #                     if v.len() != 8 {
/// #                         return Err(-26200); // Mimer Rust API error code for unsupported type conversion.
/// #                     }
/// #                     Ok(CustomType {
/// #                         first_value: i32::from_le_bytes(v[0..4].try_into().unwrap()),
/// #                         second_value: i32::from_le_bytes(v[4..8].try_into().unwrap())
/// #                     }
/// #                     )
/// #                 }
/// #                 _ => Err(-26200), // Mimer Rust API error code for unsupported type conversion.
/// #             }
/// #         }    
/// # }
/// # let db = &std::env::var("MIMER_DATABASE").unwrap();
/// # let ident = "RUSTUSER";
/// # let pass = "RUSTPASSWORD";
/// let mut conn = Connection::open(db, ident, pass).unwrap();
/// _ = conn.execute_statement("DROP TABLE my_table");
/// conn.execute_statement("CREATE TABLE my_table (my_custom_column BINARY(8))").unwrap();
///
/// let custom_type = CustomType {
///     first_value: 1,
///     second_value: 2,
/// };
///
/// let stmnt = conn.prepare("INSERT INTO my_table (my_custom_column) VALUES(:param)", CursorMode::Forward).unwrap();
/// stmnt.execute_bind(&[&custom_type]).unwrap();
///
/// let stmnt = conn.prepare("SELECT * FROM my_table", CursorMode::Forward).unwrap();
/// let mut cursor = stmnt.open_cursor().unwrap();
/// let row = cursor.next_row().unwrap().unwrap();
/// let fetched_custom_type = row.get::<CustomType>(1).unwrap().unwrap();
///
/// assert_eq!(custom_type, fetched_custom_type);
/// ```
pub mod types;

pub use common::mimer_options::*;
pub use common::return_codes::*;
pub use connection::Connection;
pub use cursor::Cursor;
pub use mimer_error::MimerError;
pub use row::Row;
pub use statement::Statement;
pub use transaction::Transaction;
pub use types::*;
