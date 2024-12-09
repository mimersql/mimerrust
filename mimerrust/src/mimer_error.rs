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

use crate::common::traits::*;
use crate::common::{return_codes::MIMER_SUCCESS, traits::GetHandle};
use mimerrust_sys as ffi;

#[doc(hidden)]
use core::cmp::Ordering;
#[doc(hidden)]
use std::{
    ffi::{c_void, CString},
    fmt,
};

/// Represents an error occurring during communication with a MimerSQL database.
#[derive(Debug)]
pub struct MimerError {
    error_code: i32,
    error_message: String,
}

impl MimerError {
    /// Creates a [MimerError].
    pub(crate) fn new<T>(handle: &T, error_code: i32) -> MimerError
    where
        T: GetHandle,
    {
        if -26999 <= error_code && error_code <= -26000 {
            return MimerError::mimer_error_from_code(error_code);
        }
        let mut ec = -1;
        unsafe {
            let handle = match handle.get_handle() {
                Ok(MimerHandle::Session(session)) => *session as *const _ as *mut c_void,
                Ok(MimerHandle::Statement(statement)) => *statement as *const _ as *mut c_void,
                Err(ec) => return MimerError::mimer_error_from_code(ec),
            };

            // let null_ptr: *mut i8 = std::ptr::null_mut();
            let c_str_dummy = CString::new("").unwrap();
            let dummy_ptr = c_str_dummy.into_raw();

            let buffer_size: i32 = ffi::MimerGetError8(handle, &mut ec, dummy_ptr, 0);

            // retake pointer to free memory
            let _ = CString::from_raw(dummy_ptr);

            match buffer_size.cmp(MIMER_SUCCESS) {
                Ordering::Greater => (),
                Ordering::Equal => (),
                Ordering::Less => return MimerError::mimer_error_from_code(buffer_size),
            }

            let c_buffer_size: usize = (buffer_size + 1) as usize;

            //let mut buffer: Vec<c_char> = Vec::with_capacity(c_buffer_size);
            let mut buffer = vec![255u8; c_buffer_size];
            buffer[c_buffer_size - 1] = 0;
            let c_str = CString::from_vec_unchecked(buffer);
            let c_str_ptr = c_str.into_raw();
            //let error_message = buffer.as_mut_ptr();

            let rc = ffi::MimerGetError8(handle, &mut ec, c_str_ptr, c_buffer_size);
            //assert!(false);
            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Greater => (),
                Ordering::Equal => (),
                Ordering::Less => return MimerError::mimer_error_from_code(rc),
            }

            // retake pointer to free memory
            let maybe_string = CString::from_raw(c_str_ptr).into_string();

            return match maybe_string {
                Ok(s) => MimerError {
                    error_code: ec,
                    error_message: s,
                },
                Err(_) => MimerError::mimer_error_from_code(-26001),
            };
        };
    }

    /// Returns a [MimerError] given a program dependent error code.
    /// Mainly used when connecting to the database fails.
    pub(crate) fn mimer_error_from_code(ec: i32) -> MimerError {
        let em = match ec {
            // TODO: Would be nice to have macros for these RustApi-Errors, as in the C API?
            -14006 => String::from("Login failure"),
            -18500 => String::from("Database name not found in SQLHOSTS file"),
            -24101 => String::from("An illegal sequence of API calls was detected"),
            -21028 => {
                String::from("Failed to do a LOCAL connection to the server for database <%>")
            } // TODO: should we bother displaying the database name here? This would mean implementing a way for types that implement the trait GetHandle to also fetch name of database.
            -26001 => String::from("Error converting from utf-8 vector of bytes to String"),
            -26002 => String::from("Invalid session pointer was returned from C API"),
            -26003 => String::from("Connection is dropped"),
            -26004 => String::from("Statement is dropped"),
            -26005 => String::from("Handle is NULL"),
            -26006 => String::from("Wrong number of parameters"),
            -26007 => String::from("Could not convert UTF-8 string to CString"),
            -26100 => String::from("Failed to get handle, handle is not a connection or statement"),
            -26200 => {
                String::from("Unsupported type conversion between MimerDatatype and Rust type")
            }
            -26201 => String::from("Unsupported type in Row::get_type()"),
            -26203 => String::from("Invalid parameter type for MimerDatatype-variant"),
            -26999 => String::from("Rust error"),
            _ => String::from("Unknown error"),
        };

        MimerError {
            error_code: ec,
            error_message: em,
        }
    }

    /// Gets the error code from a [MimerError] struct.
    ///
    /// # Examples
    /// ```
    /// # use mimerrust::Connection;
    /// # use mimerrust::MimerError;
    /// # let db = &std::env::var("MIMER_DATABASE").unwrap();
    /// # let ident = "RUSTUSER";
    /// # let pass = "RUSTPASSWORD";
    /// let conn = Connection::open(db, ident, pass).unwrap();
    ///
    /// let err = match conn.execute_statement(&format!("DROP TABLE {}", "non_existing_table")) {
    ///     Ok(_) => panic!("Execute statement succeded when it should have failed."),
    ///     Err(ec) => conn.get_error(ec),
    /// };
    ///
    /// println!("Error code: {}", err.get_error_code());
    /// ```
    pub fn get_error_code(&self) -> i32 {
        self.error_code
    }

    /// Gets the error message from a [MimerError] struct.
    ///
    /// # Examples
    /// ```
    /// # use mimerrust::Connection;
    /// # use mimerrust::MimerError;
    /// # let db = &std::env::var("MIMER_DATABASE").unwrap();
    /// # let ident = "RUSTUSER";
    /// # let pass = "RUSTPASSWORD";
    /// let conn = Connection::open(db, ident, pass).unwrap();
    ///
    /// let err = match conn.execute_statement(&format!("DROP TABLE {}", "non_existing_table")) {
    ///     Ok(_) => panic!("Execute statement succeded when it should have failed."),
    ///     Err(ec) => conn.get_error(ec),
    /// };
    ///
    /// println!("Error message: {}", err.get_error_message());
    /// ```
    pub fn get_error_message(&self) -> &String {
        &self.error_message
    }
}

impl fmt::Display for MimerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MimerError: {}, {}", self.error_code, self.error_message)
    }
}

#[cfg(test)]
mod error_tests {
    use super::*;
    use crate::common::mimer_options::*;
    use crate::statement::*;
    use crate::testing::*;

    #[test]
    fn error_dropping_table() {
        let conn = establish_connection();

        let err = match conn.execute_statement(&format!("DROP TABLE {}", "non_existing_table")) {
            Ok(_) => panic!("Execute statement succeded when it should have failed."),
            Err(ec) => MimerError::new(&conn, ec),
        };
        let ec = err.get_error_code();
        assert!(ec == -12501 || ec == -12517); // Mimer SQL Error: Table does not exist or Object does not exist respectively.
        println!("dropping non existing table: : {}", err);
    }

    #[test]
    fn error_dropping_table_utf8() {
        let conn = establish_connection();

        let err = match conn.execute_statement(&format!("DROP TABLE ÄÄÄÄ")) {
            Ok(_) => panic!("Execute statement succeded when it should have failed."),
            Err(ec) => MimerError::new(&conn, ec),
        };
        let ec = err.get_error_code();
        assert!(ec == -12501 || ec == -12517); // Mimer SQL Error: Table does not exist or Object does not exist respectively.
        println!("dropping non existing table: : {}", err);
    }

    #[test]
    fn error_creating_table_utf8() {
        let conn = establish_connection();

        drop_create_table(&conn, "ÄÄÄÄÄÄÄÄ", EXAMPLE_TABLE_COLUMNS);
        let err = match conn
            .execute_statement(&format!("CREATE TABLE ÄÄÄÄÄÄÄÄ {}", EXAMPLE_TABLE_COLUMNS))
        {
            Ok(_) => panic!("Execute statement succeded when it should have failed."),
            Err(ec) => MimerError::new(&conn, ec),
        };
        assert_eq!(err.get_error_code(), -12560); // Mimer SQL Error: Table, view, synonym, index or constraint named <%> already exists
        println!("error creating table test: {}", err);
    }

    #[test]
    fn error_creating_table() {
        let conn = establish_connection();

        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
        let err = match conn.execute_statement(&format!(
            "CREATE TABLE {} {}",
            EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS
        )) {
            Ok(_) => panic!("Execute statement succeded when it should have failed."),
            Err(ec) => MimerError::new(&conn, ec),
        };
        assert_eq!(err.get_error_code(), -12560); // Mimer SQL Error: Table, view, synonym, index or constraint named <%> already exists
        println!("error creating table test: {}", err);
    }

    #[test]
    fn error_prepare() {
        let mut conn = establish_connection();

        let stmnt: Result<Statement, i32>;
        let option = CursorMode::Forward;

        stmnt = conn.prepare("[an invalid query]", option);
        let err = match stmnt {
            Ok(_) => panic!("Prepare method succeeded when it should have failed."),
            Err(ec) => MimerError::new(&conn, ec),
        };
        let ec = err.get_error_code();
        assert!(ec == -12102 || ec == -12103); // Mimer SQL Error: Syntax error, <%> ignored or Syntax error, <%> assumed to mean <%>
        println!("error prepare test: {}", err);
    }
}
