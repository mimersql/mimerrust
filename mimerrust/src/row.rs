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

use crate::{common::return_codes::MIMER_SUCCESS, common::traits::*, inner_statement::*, types::*};
use crate::{
    match_mimer_BINARY, match_mimer_BLOB, match_mimer_CLOB, match_mimer_big_ints,
    match_mimer_booleans, match_mimer_doubles, match_mimer_real, match_mimer_small_ints,
    match_mimer_spatial, match_mimer_strings, match_mimer_temporal,
};
use mimerrust_sys as ffi;

#[doc(hidden)]
use std::{cmp::Ordering, ffi::CString, ptr::null_mut, sync::Weak};

#[derive(Clone)]
/// Represents a row in a result set.
pub struct Row {
    pub(crate) inner_statement: Weak<InnerStatement>,
}

impl Row {
    /// Gets the content from a specified index and returns a [MimerDataType](crate::types::MimerDatatype) if successful.
    ///
    /// # Errors
    /// Returns [Err] when the column type couldn't be determined.
    ///
    /// # Examples
    /// ```
    /// # use mimerrust::*;
    /// # let db = &std::env::var("MIMER_DATABASE").unwrap();
    /// # let ident = "RUSTUSER";
    /// # let pass = "RUSTPASSWORD";
    /// let mut conn = Connection::open(db, ident, pass).unwrap();
    /// # conn.execute_statement("drop table test_table").ok();
    /// # conn.execute_statement("create table test_table (column_1 VARCHAR(30), column_2 INT)").unwrap();
    /// # conn.execute_statement("INSERT INTO test_table VALUES('the number one',1)").unwrap();
    /// let stmnt = conn.prepare("SELECT * FROM test_table", CursorMode::Forward).unwrap();
    /// let mut cursor = stmnt.open_cursor().unwrap();
    ///
    /// let row = cursor.next_row().unwrap().expect("Nothing was found on this row");
    /// let data_type = row.get_type(1).unwrap();
    /// ```
    pub fn get_type(&self, idx: i16) -> Result<MimerDatatype, i32> {
        let strong_inner_statement = self.inner_statement.upgrade().ok_or(-26004)?;
        let handle = strong_inner_statement.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        strong_inner_statement.check_connection()?;
        let column_type: i32;

        unsafe {
            column_type = ffi::MimerColumnType(*handle, idx);
        }

        if column_type < 0 {
            return Err(column_type);
        }

        match column_type as u32 {
            match_mimer_big_ints!() => {
                let mut val: i64 = 0;
                unsafe {
                    let err = ffi::MimerGetInt64(*handle, idx, &mut val);
                    match err {
                        0 => Ok(MimerDatatype::BigInt(val)),
                        ffi::MIMER_SQL_NULL_VALUE => Ok(MimerDatatype::Null),
                        _ => Err(err),
                    }
                }
            }
            match_mimer_small_ints!() => {
                let mut val: i32 = 0;
                unsafe {
                    let err = ffi::MimerGetInt32(*handle, idx, &mut val);
                    match err {
                        0 => Ok(MimerDatatype::Int(val)),
                        ffi::MIMER_SQL_NULL_VALUE => Ok(MimerDatatype::Null),
                        _ => Err(err),
                    }
                }
            }
            match_mimer_strings!() => unsafe {
                let mut size = ffi::MimerGetString8(*handle, idx, std::ptr::null_mut(), 0);

                if size < 0 {
                    return Err(size);
                } else {
                    size += 1;
                }

                let buffer = vec![0u8; size as usize];
                let c_str = CString::from_vec_unchecked(buffer);
                let c_str_ptr = c_str.into_raw();

                let rc = ffi::MimerGetString8(*handle, idx, c_str_ptr, size as usize);

                // retake pointer to free memory
                let maybe_string = CString::from_raw(c_str_ptr).into_string();

                match maybe_string {
                    Ok(s) => match rc {
                        _ if rc + 1 == size as i32 => Ok(MimerDatatype::String(s)),
                        ffi::MIMER_SQL_NULL_VALUE => Ok(MimerDatatype::Null),
                        _ => Err(size),
                    },
                    Err(_) => Err(-26001),
                }
            },
            match_mimer_real!() => {
                let mut val: f32 = 0.0;
                unsafe {
                    let err = ffi::MimerGetFloat(*handle, idx, &mut val);
                    match err {
                        0 => Ok(MimerDatatype::Real(val)),
                        ffi::MIMER_SQL_NULL_VALUE => Ok(MimerDatatype::Null),
                        _ => Err(err),
                    }
                }
            }
            match_mimer_doubles!() => {
                let mut val: f64 = 0.0;
                unsafe {
                    let err = ffi::MimerGetDouble(*handle, idx, &mut val);
                    match err {
                        0 => Ok(MimerDatatype::Double(val)),
                        ffi::MIMER_SQL_NULL_VALUE => Ok(MimerDatatype::Null),
                        _ => Err(err),
                    }
                }
            }
            match_mimer_booleans!() => {
                let val: i32;
                unsafe {
                    val = ffi::MimerGetBoolean(*handle, idx);
                    match val {
                        1 => Ok(MimerDatatype::Bool(true)),
                        0 => Ok(MimerDatatype::Bool(false)),
                        ffi::MIMER_SQL_NULL_VALUE => Ok(MimerDatatype::Null),
                        code => Err(code),
                    }
                }
            }
            match_mimer_BINARY!() | match_mimer_spatial!() => {
                let bytes = unsafe { ffi::MimerGetBinary(*handle, idx, null_mut(), 0) };
                if bytes < 0 {
                    return Err(bytes);
                };

                let mut vec: Vec<u8> = Vec::new();
                vec.resize(bytes as usize, 0);

                let ptr = vec.as_ptr() as *mut std::ffi::c_void;
                let rc: i32 = unsafe { ffi::MimerGetBinary(*handle, idx, ptr, bytes as usize) };

                match rc.cmp(MIMER_SUCCESS) {
                    Ordering::Less => Err(rc),
                    _ => Ok(MimerDatatype::BinaryArray(vec)),
                }
            }

            match_mimer_BLOB!() => {
                let mut blob_len: usize = 0;
                let mut blob_handle: ffi::MimerLob = std::ptr::null_mut();
                let mut val: Vec<u8> = Vec::new();
                unsafe {
                    let err = ffi::MimerGetLob(*handle, idx, &mut blob_len, &mut blob_handle);
                    if err < 0 {
                        return Err(err);
                    }
                    let mut left_to_return = blob_len;
                    val.resize(blob_len, 0);
                    let blob_idx = 0;
                    while left_to_return > 0 {
                        let to_recieve = std::cmp::min(left_to_return, LOB_CHUNK_MAXSIZE_SET);
                        let err = ffi::MimerGetBlobData(
                            &mut blob_handle,
                            val.as_mut_ptr().add(blob_idx) as *mut std::ffi::c_void,
                            to_recieve,
                        );
                        if err < 0 {
                            return Err(err);
                        }
                        left_to_return -= to_recieve;
                    }
                    Ok(MimerDatatype::BinaryArray(val))
                }
            }
            match_mimer_CLOB!() => {
                let mut clob_len: usize = 0;
                let mut clob_handle: ffi::MimerLob = std::ptr::null_mut();
                let mut val: Vec<i8> = Vec::new();
                unsafe {
                    let err = ffi::MimerGetLob(*handle, idx, &mut clob_len, &mut clob_handle);
                    if err < 0 {
                        return Err(err);
                    }
                    let mut left_to_return = clob_len * 4 + 1;
                    val.resize(clob_len * 4 + 1, 0);
                    let mut clob_idx = 0;
                    while left_to_return > 0 {
                        let to_recieve = std::cmp::min(left_to_return, LOB_CHUNK_MAXSIZE_SET);
                        let err = ffi::MimerGetNclobData8(
                            &mut clob_handle,
                            val.as_mut_ptr().add(clob_idx),
                            to_recieve,
                        );
                        if err < 0 {
                            return Err(err);
                        }
                        left_to_return -= to_recieve;
                        clob_idx += LOB_CHUNK_MAXSIZE_SET;
                    }
                    Ok(MimerDatatype::String(
                        String::from_utf8(
                            val.iter().filter(|&&c| c != 0).map(|&c| c as u8).collect(),
                        )
                        .or_else(|_| Err(-26999))?,
                    ))
                }
            }
            match_mimer_temporal!() => unsafe {
                //TODO: when bug is fixed, get size with nullptr instead of dummy buffer
                let c_str_dummy = CString::new(vec![255u8; 20]).unwrap();
                let dummy_ptr = c_str_dummy.into_raw();

                // getting the size with a nullpointer here instead of val as ptr causes a segfault. This is only the case for temporal columns, and not for others string columns.
                let mut size = ffi::MimerGetString8(*handle, idx, dummy_ptr, 0);

                // retake pointer to free memory
                let _ = CString::from_raw(dummy_ptr);

                if size < 0 {
                    return Err(size);
                }

                size += 1;

                let buffer = vec![0u8; size as usize];
                let c_str = CString::from_vec_unchecked(buffer);
                let c_str_ptr = c_str.into_raw();

                let rc = ffi::MimerGetString8(*handle, idx, c_str_ptr, size as usize);

                // retake pointer to free memory
                let maybe_string = CString::from_raw(c_str_ptr).into_string();

                match maybe_string {
                    Ok(s) => match rc {
                        _ if rc + 1 == size as i32 => Ok(MimerDatatype::String(s)),
                        ffi::MIMER_SQL_NULL_VALUE => Ok(MimerDatatype::Null),
                        _ => Err(size),
                    },
                    Err(_) => Err(-26001),
                }
            },
            _ => Err(-26201),
        }
    }

    /// Gets the content from a specified index in a row using polymorphism.
    /// Returns a [Result] of either [`Ok<Option<T>>`] or [`Err<i32>`] where T is the type specified and i32 is the error code.
    /// If a null value is fetched, the return value will be [`Ok<None>`].
    ///
    /// # Errors
    /// Returns [Err] when conversion to the specified type fails.
    ///
    /// # Examples
    /// ```
    /// # use mimerrust::*;
    /// # let db = &std::env::var("MIMER_DATABASE").unwrap();
    /// # let ident = "RUSTUSER";
    /// # let pass = "RUSTPASSWORD";
    /// let mut conn = Connection::open(db, ident, pass).unwrap();
    /// # conn.execute_statement("drop table test_table").unwrap();
    /// # conn.execute_statement("create table test_table (column_1 VARCHAR(30), column_2 INT)").unwrap();
    /// # conn.execute_statement("INSERT INTO test_table VALUES('the number one',1)").unwrap();
    /// let stmnt = conn.prepare("SELECT * FROM test_table", CursorMode::Forward).unwrap();
    /// let mut cursor = stmnt.open_cursor().unwrap();
    ///
    /// let row = cursor.next_row().unwrap().unwrap();
    /// let str:String = row.get(1).unwrap().unwrap();
    /// ```
    pub fn get<T: FromSql>(&self, idx: i16) -> Result<Option<T>, i32> {
        let val = self.get_type(idx);
        match val {
            Ok(val) => match T::from_sql(val) {
                Ok(val) => Ok(Some(val)),
                Err(err) => Err(err),
            },
            Err(err) => match err.cmp(&ffi::MIMER_SQL_NULL_VALUE) {
                Ordering::Equal => Ok(None),
                _ => Err(err),
            },
        }
    }

    /// Checks if the value at the specified index is null.
    ///
    /// # Examples
    /// ```
    /// # use mimerrust::*;
    /// # let db = &std::env::var("MIMER_DATABASE").unwrap();
    /// # let ident = "RUSTUSER";
    /// # let pass = "RUSTPASSWORD";
    /// let mut conn = Connection::open(db, ident, pass).unwrap();
    /// # conn.execute_statement("drop table test_table").ok();
    /// # conn.execute_statement("create table test_table (column_1 VARCHAR(30), column_2 INT)").unwrap();
    /// let stmnt = conn.prepare("INSERT INTO test_table (column_1) VALUES(?)", CursorMode::Forward).unwrap();
    ///
    /// stmnt.execute_bind(&[&None::<String>]).unwrap(); // insert a null value
    /// stmnt.execute_bind(&[&Some("Hello, World!".to_string())]).unwrap(); // insert a value that is not null
    ///
    /// let stmnt = conn.prepare("SELECT * FROM test_table", CursorMode::Forward).unwrap();
    /// let mut cursor = stmnt.open_cursor().unwrap();
    ///
    /// let mut row = cursor.next_row().unwrap().unwrap();
    /// assert!(row.is_null(1).unwrap()); // assert that the first value is null
    ///
    /// row = cursor.next_row().unwrap().unwrap();
    /// assert!(!row.is_null(1).unwrap()); // assert that the second value is not null
    ///
    pub fn is_null(&self, idx: i16) -> Result<bool, i32> {
        let strong_inner_statement = self.inner_statement.upgrade().ok_or(-26004)?;
        let handle = strong_inner_statement.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        strong_inner_statement.check_connection()?;

        unsafe {
            let rc = ffi::MimerIsNull(*handle, idx);
            match rc.cmp(&0) {
                Ordering::Greater => Ok(true),
                Ordering::Equal => Ok(false),
                Ordering::Less => Err(rc),
            }
        }
    }
}

#[cfg(test)]
mod row_tests {
    use super::*;
    use crate::common::mimer_options::CursorMode;
    use crate::testing::*;

    #[test]
    fn row_get_type() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
        conn.execute_statement(&format!(
            "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
        ))
        .unwrap();

        let mut cursor;
        let row;

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        cursor = stmt.open_cursor().unwrap();

        row = cursor.next_row().unwrap().unwrap();

        let val = row.get_type(1).unwrap();
        match val {
            MimerDatatype::String(s) => assert_eq!(s, EXAMPLE_VALUE_1),
            _ => panic!("Expected string"),
        }
        let val = row.get_type(2).unwrap();
        match val {
            MimerDatatype::Int(s) => assert_eq!(s, EXAMPLE_VALUE_2),
            _ => panic!("Expected small int"),
        }
    }
    #[test]
    fn row_get() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
        conn.execute_statement(&format!(
            "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
        ))
        .unwrap();

        let mut cursor;
        let row;

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        cursor = stmt.open_cursor().unwrap();
        row = cursor.next_row().unwrap().unwrap();

        let val: String = row.get(1).unwrap().unwrap();
        assert_eq!(val, EXAMPLE_VALUE_1);
        let val: i32 = row.get(2).unwrap().unwrap();
        assert_eq!(val, EXAMPLE_VALUE_2);
    }

    #[test]
    fn row_get_fail() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
        conn.execute_statement(&format!(
            "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
        ))
        .unwrap();

        let mut cursor;
        let row;

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        cursor = stmt.open_cursor().unwrap();
        row = cursor.next_row().unwrap().unwrap();

        let val: Result<Option<i64>, i32> = row.get(1);
        assert!(val.is_err());

        let val: Result<Option<f32>, i32> = row.get(2);
        assert!(val.is_err());
    }
    #[test]
    fn row_get_blob() {
        let mut conn = establish_connection();

        drop_create_table(&conn, BLOB_TABLE_1024, BLOB_TABLE_1024_COLUMNS);

        let option = CursorMode::Forward;
        let stmnt = conn
            .prepare(
                &format!(
                    "INSERT INTO {BLOB_TABLE_1024} {BLOB_TABLE_1024_COLUMN_NAMES} VALUES(:BLOB)"
                ),
                option,
            )
            .unwrap();

        let blob = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let params: &[&dyn ToSql] = &[&blob];
        stmnt.execute_bind(params).unwrap();

        let stmnt = conn
            .prepare(&format!("SELECT * FROM {BLOB_TABLE_1024}"), option)
            .unwrap();
        let mut cursor = stmnt.open_cursor().unwrap();
        let row = cursor.next_row().unwrap().unwrap();
        let val: Vec<u8> = row.get(1).unwrap().unwrap();
        assert_eq!(val, blob);
    }

    #[test]
    fn row_get_clob_small() {
        let mut conn = establish_connection();

        drop_create_table(&conn, CLOB_TABLE, CLOB_TABLE_COLUMNS);

        let option = CursorMode::Forward;
        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {CLOB_TABLE} {CLOB_TABLE_COLUMN_NAMES} VALUES(:CLOB)"),
                option,
            )
            .unwrap();

        let clob = String::from("Hello, this is ÖÄÅ clob");
        let params: &[&dyn ToSql] = &[&clob];
        stmnt.execute_bind(params).unwrap();

        let stmnt = conn
            .prepare(&format!("SELECT * FROM {CLOB_TABLE}"), option)
            .unwrap();
        let mut cursor = stmnt.open_cursor().unwrap();
        let row = cursor.next_row().unwrap().unwrap();
        let val: String = row.get(1).unwrap().unwrap();
        assert_eq!(val, clob);
    }
    #[test]
    fn row_get_clob_big() {
        let mut conn = establish_connection();

        drop_create_table(&conn, CLOB_TABLE, CLOB_TABLE_COLUMNS);

        let option = CursorMode::Forward;
        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {CLOB_TABLE} {CLOB_TABLE_COLUMN_NAMES} VALUES(:CLOB)"),
                option,
            )
            .unwrap();

        let clob = String::from("a").repeat(25_000);
        let params: &[&dyn ToSql] = &[&clob];
        stmnt.execute_bind(params).unwrap();

        let stmnt = conn
            .prepare(&format!("SELECT * FROM {CLOB_TABLE}"), option)
            .unwrap();
        let mut cursor = stmnt.open_cursor().unwrap();
        let row = cursor.next_row().unwrap().unwrap();
        let val: String = row.get(1).unwrap().unwrap();
        assert_eq!(val, clob);
    }

    #[test]
    fn check_statement_get() {
        let row;
        let mut cursor;
        {
            let mut conn = establish_connection();

            drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
            conn.execute_statement(&format!(
                "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
            ))
            .unwrap();
            let stmt = conn
                .prepare("SELECT * FROM test_table", CursorMode::Forward)
                .unwrap();
            cursor = stmt.open_cursor().unwrap();
            row = cursor.next_row().unwrap().unwrap();
        }
        match row.get::<String>(1) {
            Ok(_) => panic!("Cursor went to next row when it shouldn't have!"),
            Err(ec) => {
                assert_eq!(ec, -26004); // statement has been dropped
            }
        }
    }

    #[test]
    fn check_connection_get() {
        let stmt;
        let mut cursor;
        let row;
        {
            let mut conn = establish_connection();

            drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
            conn.execute_statement(&format!(
                "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
            ))
            .unwrap();
            stmt = conn
                .prepare("SELECT * FROM test_table", CursorMode::Forward)
                .unwrap();
            cursor = stmt.open_cursor().unwrap();
            row = cursor.next_row().unwrap().unwrap();
        }
        match row.get::<String>(1) {
            Ok(_) => panic!("Cursor went to next row when it shouldn't have!"),
            Err(ec) => {
                assert_eq!(ec, -26003); // connection has been dropped
            }
        }
    }

    #[test]
    fn test_get_binary() {
        let mut cursor;
        let mut row;

        let mut conn = establish_connection();
        drop_create_table(&conn, BINARY_TABLE, BINARY_TABLE_COLUMNS);

        let mut stmnt = conn
            .prepare(
                &format!("INSERT INTO {BINARY_TABLE} VALUES(:b)"),
                CursorMode::Forward,
            )
            .unwrap();

        let binary: Vec<u8> = vec![b't', b'e', b's', b't'];
        stmnt.execute_bind(&[&binary]).unwrap();

        let mut shorter_binary: Vec<u8> = vec![1, 2, 3];
        stmnt.execute_bind(&[&shorter_binary]).unwrap();

        match stmnt.execute_bind(&[&vec![b't', b'o', b'o', b' ', b'l', b'o', b'n', b'g']]) {
            Ok(_) => panic!("Should have failed. Binary is to long!"),
            Err(rc) => assert_eq!(rc, ffi::MIMER_TRUNCATION_ERROR),
        }

        stmnt = conn
            .prepare(
                &format!("SELECT * FROM {BINARY_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        cursor = stmnt.open_cursor().unwrap();
        row = cursor.next_row().unwrap().unwrap();

        let vec = row.get::<Vec<u8>>(1).unwrap().unwrap();
        assert_eq!(vec, binary);

        row = cursor.next_row().unwrap().unwrap();

        let vec = row.get::<Vec<u8>>(1).unwrap().unwrap();

        shorter_binary.push(0); // since this binary is shorter than 4 bytes, it is "padded" prior to insertion in the table. Thus we should compare it with a padded version.
        assert_eq!(vec, shorter_binary);
    }

    #[test]
    fn test_get_varbinary() {
        let mut row;
        let mut vec: Vec<u8>;

        let mut conn = establish_connection();
        drop_create_table(&conn, VARBINARY_TABLE, VARBINARY_TABLE_COLUMNS);
        let binary_test: Vec<u8> = vec![b't', b'e', b's', b't'];
        let binary_msg: Vec<u8> = vec![
            b'M', b'i', b'm', b'e', b'r', b' ', b'R', b'u', b's', b't', b' ', b'A', b'P', b'I',
        ];

        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {VARBINARY_TABLE} VALUES(:b)"),
                CursorMode::Forward,
            )
            .unwrap();

        stmnt.execute_bind(&[&binary_test]).unwrap(); // these should work
        stmnt.execute_bind(&[&binary_msg]).unwrap(); // these should work

        match stmnt.execute_bind(&[&[20; 1024]]) {
            Ok(_) => panic!("Should have failed. Binary is to long!"),
            Err(rc) => assert_eq!(rc, ffi::MIMER_TRUNCATION_ERROR),
        }

        let stmnt = conn
            .prepare(
                &format!("SELECT * FROM {VARBINARY_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();

        let mut cursor = stmnt.open_cursor().unwrap();

        row = cursor.next_row().unwrap().unwrap();
        vec = row.get::<Vec<u8>>(1).unwrap().unwrap();
        assert_eq!(vec, binary_test);

        row = cursor.next_row().unwrap().unwrap();
        vec = row.get::<Vec<u8>>(1).unwrap().unwrap();
        assert_eq!(vec, binary_msg);
    }

    #[test]
    fn test_get_uuid() {
        let mut row: &Row;

        let mut conn = establish_connection();
        drop_create_table(&conn, UUID_TABLE, UUID_TABLE_COLUMNS);

        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {UUID_TABLE} VALUES(:b)"),
                CursorMode::Forward,
            )
            .unwrap();

        let u1 = uuid::Uuid::new_v4();
        stmnt.execute_bind(&[&u1]).unwrap();
        let u2 = uuid::Uuid::new_v4();
        stmnt.execute_bind(&[&u2]).unwrap();

        let stmnt = conn
            .prepare(&format!("SELECT * FROM {UUID_TABLE}"), CursorMode::Forward)
            .unwrap();

        let mut cursor = stmnt.open_cursor().unwrap();
        row = cursor.next_row().unwrap().unwrap();
        let u1_fetched = row.get::<uuid::Uuid>(1).unwrap();
        row = cursor.next_row().unwrap().unwrap();
        let u2_fetched = row.get::<uuid::Uuid>(1).unwrap();

        assert_eq!(u1_fetched.unwrap(), u1);
        assert_eq!(u2_fetched.unwrap(), u2);
    }

    #[test]
    fn test_get_null() {
        let mut conn = establish_connection();

        drop_create_table(&conn, NULLABLE_TABLE, NULLABLE_TABLE_COLUMNS);

        let int = Some(1);
        let string1: Option<String> = None;
        let string2 = Some(String::from("test"));
        let params: &[&dyn ToSql] = &[&int, &string1, &string2];

        let option = CursorMode::Forward;
        let mut stmnt = conn.prepare(&format!("INSERT INTO {NULLABLE_TABLE} {NULLABLE_TABLE_COLUMN_NAMES} VALUES(:INT,?,:STRING2)"), option).unwrap();

        match stmnt.execute_bind(params) {
            Ok(_) => (),
            Err(err) => panic!("Failed to set execute statement: {err}"),
        }

        stmnt = conn
            .prepare(&format!("SELECT * FROM {NULLABLE_TABLE}"), option)
            .unwrap();

        let mut cursor = stmnt.open_cursor().unwrap();
        let row = cursor.next_row().unwrap().unwrap();

        let fetched_int = row.get::<i32>(1).unwrap().unwrap();
        assert_eq!(fetched_int, 1);

        assert!(row.get::<String>(2).unwrap().is_none());

        let fetched_str2 = row.get::<String>(3).unwrap().unwrap();
        assert_eq!(fetched_str2, string2.unwrap());
    }

    #[test]
    fn test_row_singleton() {
        let mut conn = establish_connection();

        drop_create_table(&conn, NULLABLE_TABLE, NULLABLE_TABLE_COLUMNS);

        let int = Some(1);
        let string1: Option<String> = None;
        let string2 = String::from("test");
        let params: &[&dyn ToSql] = &[&int, &string1, &string2];

        let mut stmnt = conn.prepare(&format!("INSERT INTO {NULLABLE_TABLE} {NULLABLE_TABLE_COLUMN_NAMES} VALUES(:INT,?,:STRING2)"), CursorMode::Forward).unwrap();

        match stmnt.execute_bind(params) {
            Ok(_) => (),
            Err(err) => panic!("Failed to set execute statement: {err}"),
        }

        stmnt = conn
            .prepare(
                &format!("SELECT * FROM {NULLABLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();

        let mut cursor = stmnt.open_cursor().unwrap();
        let row = cursor.next_row().unwrap().unwrap();

        assert_eq!(int.unwrap(), row.get::<i32>(1).unwrap().unwrap());
        assert!(row.get::<String>(2).unwrap().is_none());
        assert_eq!(string2, row.get::<String>(3).unwrap().unwrap());
    }

    #[test]
    fn test_row_multibyte_chars() {
        let mut conn = establish_connection();

        drop_create_table(&conn, EXAMPLE_TABLE_2, EXAMPLE_TABLE_2_COLUMNS);

        let mut stmnt = conn
            .prepare(
                &format!("INSERT INTO {EXAMPLE_TABLE_2} (column_1) VALUES(:vchar30)"),
                CursorMode::Forward,
            )
            .unwrap();

        let multibyte = "😀😀😀😀";
        match stmnt.execute_bind(&[&multibyte]) {
            Ok(_) => (),
            Err(err) => panic!("Failed to set execute statement: {err}"),
        }

        stmnt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE_2}"),
                CursorMode::Forward,
            )
            .unwrap();

        let mut cursor = stmnt.open_cursor().unwrap();
        let row = cursor.next_row().unwrap().unwrap();
        let fetched_string = row.get::<String>(1).unwrap().unwrap();
        assert_eq!(fetched_string.trim(), multibyte)
    }
}
