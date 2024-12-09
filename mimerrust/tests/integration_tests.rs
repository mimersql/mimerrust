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

use mimerrust::*;

#[test]
fn integration_test_1() {
    let mut conn =
        Connection::open("", "RUSTUSER", "RUSTPASSWORD").unwrap_or_else(|ec| panic!("{}", ec));

    conn.execute_statement("DROP TABLE test_table").ok();


    let rc = conn.execute_statement("CREATE TABLE test_table (column_1 VARCHAR(30), column_2 INT)");
    match rc {
        Ok(_) => (),
        Err(ec) => panic!("{}", conn.get_error(ec)),
    }

    let mut trans = conn.begin_transaction(TransactionMode::ReadWrite).unwrap();

    let rc = trans.execute_statement(
        "INSERT INTO test_table (column_1, column_2) VALUES('the number one',1)",
    );
    match rc {
        Ok(_) => (),
        Err(ec) => panic!("{}", trans.get_error(ec)),
    }

    let option = CursorMode::Forward;

    let stmt = trans
        .prepare(&format!("SELECT * FROM test_table"), option)
        .unwrap();
    let mut cursor = stmt.open_cursor().unwrap();
    cursor
        .next_row()
        .unwrap()
        .expect("Select didn't find any entries");

    let stmnt = trans
        .prepare(
            "INSERT INTO test_table (column_1, column_2) VALUES('the number two',2)",
            option,
        )
        .unwrap();

    stmnt.execute_bind(&[]).unwrap();

    trans.commit().unwrap();
}

#[test]
fn integration_test_2() {
    let rc = Connection::open("test_db123", "RUSTUSER", "RUSTPASSWORD");
    match rc {
        Ok(_) => panic!("test_db123 should not exist"),
        Err(error) => {
            println!("{error}");
            error
        }
    };

    let rc = Connection::open("", "RUSTUSER", "RUSTPASSWORD");
    let mut conn = match rc {
        Ok(connection) => connection,
        Err(error) => panic!("Error connecting to db {error}"),
    };

    conn.execute_statement("DROP TABLE test_table").ok();

    let rc = conn.execute_statement("CREATE TABLE test_table (column_1 VARCHAR(30), column_2 INT)");
    match rc {
        Ok(_) => (),
        Err(ec) => panic!("{}", conn.get_error(ec)),
    }

    let option = CursorMode::Forward;

    let stmt = conn
        .prepare(&format!("SELECT COUNT(*) FROM test_table"), option)
        .unwrap();
    let mut cursor = stmt.open_cursor().unwrap();
    cursor
        .next_row()
        .unwrap()
        .expect("Select didn't find any entries when transaction was commited");

    let stmnt = conn
        .prepare(
            "INSERT INTO test_table (column_1, column_2) VALUES('the number two',2)",
            option,
        )
        .unwrap();

    stmnt.execute_bind(&[]).unwrap();
}
