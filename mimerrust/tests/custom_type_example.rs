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

#[derive(Debug, PartialEq)]
struct CustomType {
    first_value: i32,
    second_value: i32,
}

impl ToSql for CustomType {
    fn to_sql(&self) -> MimerDatatype {
        let mut bytes: [u8; 8] = [0; 8];
        bytes[..4].copy_from_slice(&self.first_value.to_le_bytes());
        bytes[4..].copy_from_slice(&self.second_value.to_le_bytes());
        MimerDatatype::BinaryArray(bytes.to_vec())
    }
}

impl FromSql for CustomType {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
                MimerDatatype::BinaryArray(v) => {
                    if v.len() != 8 {
                        return Err(-26200); // Mimer Rust API error code for unsupported type conversion.
                    }
                    Ok(CustomType {
                        first_value: i32::from_le_bytes(v[0..4].try_into().unwrap()), 
                        second_value: i32::from_le_bytes(v[4..8].try_into().unwrap())
                    }
                    )
                }
                _ => Err(-26200), // Mimer Rust API error code for unsupported type conversion.
            }
        }    
}

#[test]
fn example_main(){
    let db = &std::env::var("MIMER_DATABASE").unwrap();
    let ident = "RUSTUSER";
    let pass = "RUSTPASSWORD";
    let mut conn = Connection::open(db, ident, pass).unwrap();
    _ = conn.execute_statement("DROP TABLE my_table");
    conn.execute_statement("CREATE TABLE my_table (my_custom_column BINARY(8))").unwrap();

    let custom_type = CustomType {
        first_value: 1,
        second_value: 2,
    };

    let stmnt = conn.prepare("INSERT INTO my_table (my_custom_column) VALUES(:i1)", CursorMode::Forward).unwrap();
    stmnt.execute_bind(&[&custom_type]).unwrap();

    let stmnt = conn.prepare("SELECT * FROM my_table", CursorMode::Forward).unwrap();
    let mut cursor = stmnt.open_cursor().unwrap();
    let row = cursor.next_row().unwrap().unwrap();
    let fetched_custom_type = row.get::<CustomType>(1).unwrap().unwrap();

    assert_eq!(custom_type, fetched_custom_type);
}

