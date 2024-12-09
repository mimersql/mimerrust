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

const DB: &str = "";
const IDENT: &str = "RUSTUSER";
const PASS: &str = "RUSTPASSWORD";

#[test]
fn test_main(){
    let mut conn = Connection::open(DB, IDENT, PASS).unwrap();

    let rc = conn.execute_statement("DROP TABLE temp_integration");
    match  rc {
        Ok(_) => (),
        Err(err) => assert!(err == -12517 || err == -12501), // Mimer SQL Error: Object does not exist.
    }

    let rc = conn.execute_statement("CREATE TABLE temp_integration (column_1 BIGINT, column_2 INT)");
    match rc {
        Ok(_) => (),
        Err(ec) => panic!("{}", conn.get_error(ec)),
    }

    let stmnt = conn.prepare("INSERT INTO temp_integration (column_1, column_2) VALUES(:i1, :i2)", CursorMode::Forward).unwrap();
    stmnt.execute_bind(&[&1, &2]).unwrap();

    // dbg!(stmnt.get_parameter_type(1).unwrap());
    // dbg!(stmnt.get_parameter_type(2).unwrap());

    let stmnt = conn.prepare("SELECT * FROM temp_integration", CursorMode::Forward).unwrap();
    let mut cursor = stmnt.open_cursor().unwrap();
    let row = cursor.next_row().unwrap().unwrap();

    match row.get::<i32>(1) {
        Ok(val) =>{dbg!(val.unwrap()); ()},
        Err(err) => {dbg!(stmnt.get_error(err)); ()},
    }
    match row.get::<i32>(2) {
        Ok(val) =>{dbg!(val.unwrap()); ()},
        Err(err) => {dbg!(stmnt.get_error(err)); ()},
    }
}