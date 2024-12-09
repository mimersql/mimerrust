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

#![allow(dead_code)]

use crate::connection::*;

#[doc(hidden)]
use std::{
    env,
    fs::File,
    io::Read,
    process::{Command, Output},
};

pub const PASSWORD: &str = "RUSTPASSWORD";
pub const IDENT: &str = "RUSTUSER";
pub const EXAMPLE_TABLE_COLUMNS: &str = "(column_1 VARCHAR(30), column_2 INT)";
pub const EXAMPLE_TABLE: &str = "test_table";
pub const EXAMPLE_TABLE_COLUMN_NAMES: &str = "(column_1, column_2)";
pub const EXAMPLE_TABLE_EXAMPLE_VALUES: &str = "VALUES('the number one ÅÄÖ',1)";
pub const EXAMPLE_VALUE_1: &str = "the number one ÅÄÖ";
pub const EXAMPLE_VALUE_2: i32 = 1;

pub const EXAMPLE_TABLE_2: &str = "test_table_2";
pub const EXAMPLE_TABLE_2_COLUMNS: &str = "(column_1 NATIONAL CHAR(30))";
pub const EXAMPLE_TABLE_2_COLUMN_NAMES: &str = "(column_1)";

pub const BIG_TABLE: &str = "big_table";
pub const BIG_TABLE_COLUMN_NAMES: &str =
    "(column_1, column_2, column_3, column_4, column_5, column_6, column_7, column_8, column_UUID)";
pub const BIG_TABLE_COLUMNS: &str = "(column_1 VARCHAR(30), column_2 VARCHAR(30), column_3 INT, column_4 BIGINT, column_5 BOOLEAN, column_6 DOUBLE PRECISION, column_7 REAL, column_8 BINARY(4), column_UUID BINARY(16))";

pub const NULLABLE_TABLE: &str = "nullable_table";
pub const NULLABLE_TABLE_COLUMN_NAMES: &str = "(column_1, column_2, column_3)";
pub const NULLABLE_TABLE_COLUMNS: &str =
    "(column_1 INT, column_2 VARCHAR(30), column_3 VARCHAR(30))";

pub const BLOB_TABLE_1024: &str = "blob_table_1024";
pub const BLOB_TABLE_1024_COLUMN_NAMES: &str = "(column1)";
pub const BLOB_TABLE_1024_COLUMNS: &str = "(column1 BLOB(1024))"; // 1024 bytes. Suffix with K, M or G for kilo, mega or giga bytes.

pub const BLOB_TABLE_GIGA: &str = "blob_table_GIGA";
pub const BLOB_TABLE_GIGA_COLUMN_NAMES: &str = "(column1)";
pub const BLOB_TABLE_GIGA_COLUMNS: &str = "(column1 BLOB(1024G))"; // 1024 Gigabytes. Suffix with K, M or G for kilo, mega or giga bytes.

pub const CLOB_TABLE: &str = "clob_table";
pub const CLOB_TABLE_COLUMN_NAMES: &str = "(column1)";
pub const CLOB_TABLE_COLUMNS: &str = "(column1 CLOB(1024M))"; // 1024 Megabytes. Suffix with K, M or G for kilo, mega or giga bytes.

pub const CLOB_TABLE_GIGA: &str = "clob_table";
pub const CLOB_TABLE_GIGA_COLUMN_NAMES: &str = "(column1)";
pub const CLOB_TABLE_GIGA_COLUMNS: &str = "(column1 CLOB(1024G))"; // 1024 Gigabytes. Suffix with K, M or G for kilo, mega or giga bytes.

pub const TEMPORAL_TABLE: &str = "temporal_table";
pub const TEMPORAL_TABLE_COLUMN_NAMES: &str = "(column1, column2, column3)";
pub const TEMPORAL_TABLE_COLUMNS: &str = "(column1 DATE, column2 TIME(0), column3 TIMESTAMP(0))";

pub const INTERVAL_TABLE: &str = "interval_table";
pub const INTERVAL_TABLE_COLUMN_NAMES: &str = "(column1, column2, column3, column4, column5, column6, column7, column8, column9, column10, column11, column12, column13)";
pub const INTERVAL_TABLE_COLUMNS: &str =
    "(
    column1 INTERVAL YEAR(7), column2 INTERVAL MONTH(7), column3 INTERVAL YEAR(7) TO MONTH,
    column4 INTERVAL DAY(7), column5 INTERVAL HOUR(7), column6 INTERVAL MINUTE(7), column7 INTERVAL SECOND(7),
    column8 INTERVAL DAY(7) TO HOUR, column9 INTERVAL DAY(7) TO MINUTE, column10 INTERVAL DAY(7) TO SECOND(7),
    column11 INTERVAL HOUR(7) TO MINUTE, column12 INTERVAL HOUR(7) TO SECOND(7), 
    column13 INTERVAL MINUTE(7) TO SECOND(7)
    )";

pub const BIGINT_TABLE: &str = "bigint_table";
pub const BIGINT_TABLE_COLUMN_NAMES: &str = "(column1)";
pub const BIGINT_TABLE_COLUMNS: &str = "(column1 BIGINT)";

pub const BINARY_TABLE: &str = "binary_table";
pub const BINARY_TABLE_COLUMN_NAMES: &str = "(column1)";
pub const BINARY_TABLE_COLUMNS: &str = "(column1 BINARY(4))";

pub const VARBINARY_TABLE: &str = "varbinary_table";
pub const VARBINARY_TABLE_COLUMN_NAMES: &str = "(column1)";
pub const VARBINARY_TABLE_COLUMNS: &str = "(column1 VARBINARY(14))";

pub const UUID_TABLE: &str = "UUID_table";
pub const UUID_TABLE_COLUMN_NAMES: &str = "(column1)";
pub const UUID_TABLE_COLUMNS: &str = "(column1 BINARY(16))";

pub const SPATIAL_TABLE: &str = "spatial_table";
pub const SPATIAL_TABLE_COLUMN_NAMES: &str = "(column1, column2, column3, column4)";
pub const SPATIAL_TABLE_COLUMNS: &str = "(column1 BUILTIN.GIS_COORDINATE, column2 BUILTIN.GIS_LATITUDE, column3 BUILTIN.GIS_LONGITUDE, column4 BUILTIN.GIS_LOCATION)";

pub const RESULT_TABLE: &str = "result_table";
pub const RESULT_TABLE_COLUMN_NAMES: &str = "(column1, column2, column3)";
pub const RESULT_TABLE_COLUMNS: &str = "(column1 INTEGER, column2 INTEGER, column3 INTEGER)";

pub const PROCEDURE_MATHMAGIC_DEF: &str = // a procedure for doing nonsense math.
    "
CREATE PROCEDURE MATHMAGIC(IN x INTEGER, OUT y INTEGER, INOUT z INTEGER)
BEGIN
    IF x > 0 THEN 
        SET y = -1; 
    END IF;
    
    IF x < 0 THEN 
        SET y = 1; 
    END IF;
    
    IF x = 0 THEN 
        SET y = 0; 
    END IF;

    SET z = x + z;
END;
";

///  Creates a local database definition in the sqlhosts file (usually/always /etc/sqlhosts on Unix)
///  The database definition has name DB and home directory DB_HOMEDIR
pub fn create_database_definition(db_name: &str, db_homerdir: &str) -> std::io::Result<Output> {
    // TODO: add support for windows and other OS. Or verify that this function is fine for other OS aswell.
    if !cfg!(target_os = "linux") {
        panic!("Support for this function is only implemeted for Linux.")
    }

    let args = ["mimsqlhosts", "-a", db_name, db_homerdir];
    Command::new("pkexec").args(args).output()
}

// Reads from file and writes to buffer. Path should be relative to current directory
pub fn read_from_file(file_path: &str, mut buffer: String) -> String {
    let file_path = format!(
        "{}{}",
        std::env::current_dir().unwrap().to_str().unwrap(),
        file_path
    );

    let fpclone = file_path.clone();
    // Attempt to open the file
    let mut file = match File::open(file_path) {
        Ok(file) => file,
        Err(e) => {
            panic!("Error opening file: {}, filepath: {}", e, fpclone);
        }
    };

    // Read the file's contents into a String
    if let Err(e) = file.read_to_string(&mut buffer) {
        panic!("Error reading file: {}", e);
    }
    buffer
}

/// Creates system databanks for db definition DB in directory DB_HOMEDIR
pub fn create_system_databanks(password: &str, db_name: &str) -> std::io::Result<Output> {
    if !cfg!(target_os = "linux") {
        panic!("Support for this function is only implemeted for Linux.")
    }

    let args = [
        "-p",
        password,
        db_name,
        "4000",
        "transaction_databank.dbf",
        "1000",
        "log_databank.dbf",
        "1000",
        "sql_databank.dbf",
        "1000",
    ];
    let output = Command::new("sdbgen").args(args).output();
    output
}

/// Creates a databank for for user RUSTUSER
/// The databank will be used for performing various queries for testing purposes.
pub fn create_user_databank(
    db: &str,
    ident: &str,
    pass: &str,
    databank_name: &str,
) -> Result<i32, i32> {
    let conn =
        Connection::open(db, ident, pass).unwrap_or_else(|ec| panic!("Connection failed: {ec}"));

    conn.execute_statement(&format!("CREATE DATABANK {} ", databank_name))
}

/// Sets up connection to db defined by the environment variable MIMER_DATABASE as IDENT with PASSWORD.
pub fn establish_connection() -> Connection {
    let db = env::var("MIMER_DATABASE").expect("Environment variable MIMER_DATABASE not set.");
    let rustuser_pass = env::var("RUSTPASSWORD").unwrap_or(String::from(PASSWORD));

    let mut conn = Connection::open(&db, IDENT, &rustuser_pass)
        .unwrap_or_else(|ec| panic!("Connection failed: {ec}"));

    // check that the user has a databank, or create one if they dont.
    let stmnt = conn
        .prepare(
            "select * from INFORMATION_SCHEMA.EXT_DATABANKS",
            crate::CursorMode::Forward,
        )
        .unwrap();
    match stmnt.open_cursor().unwrap().next_row().unwrap() {
        Some(_) => (),
        None => {
            conn.execute_statement("CREATE DATABANK test_databank")
                .unwrap();
        }
    }
    return conn;
}

/// Drops table and creates it again after with columns specified in function arguments. Used for starting tests from a clean slate.
pub fn drop_create_table(conn: &Connection, table: &str, table_columns: &str) {
    if let Err(ec) = conn.execute_statement(&format!("DROP TABLE {}", table)) {
        assert!(ec == -12501 || ec == -12517); // Mimer SQL Error: Table does not exist or Object does not exist respectively.
    };

    match conn.execute_statement(&format!("CREATE TABLE {} {}", table, table_columns)) {
        Ok(_) => (),
        Err(ec) => {
            dbg!(conn.get_error(ec));
            panic!("Execute statement failed, errorcode: {ec}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DB: &str = "test_db";
    const TEST_DB_HOMEDIR: &str = "/usr/local/MimerSQL/test_db";

    #[ignore = "This test is ignored as it requires root access (annoying)."]
    #[test]
    fn create_db_def() {
        create_database_definition(TEST_DB, TEST_DB_HOMEDIR)
            .unwrap_or_else(|err| panic!("Failed to create database definition: {err}"));
    }

    #[ignore = "We dont want to run this everytime"]
    #[test]
    fn create_sys_databank() {
        let output = create_system_databanks(PASSWORD, TEST_DB)
            .unwrap_or_else(|err| panic!("failed to create system databanks: {err}"));
        dbg!(output);
    }
}
