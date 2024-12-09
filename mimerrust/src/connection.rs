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

use crate::{
    common::{mimer_options::*, return_codes::*, traits::*},
    inner_connection::InnerConnection,
    MimerError, Statement, Transaction,
};
use mimerrust_sys as ffi;

#[doc(hidden)]
use parking_lot::MappedMutexGuard;
#[doc(hidden)]
use std::{
    cmp::Ordering,
    ffi::CString,
    result::Result::{Err, Ok},
    sync::Arc,
};

/// Represents a connection to a MimerSQL database.
pub struct Connection {
    inner_connection: Arc<InnerConnection>,
}

impl GetHandle for Connection {
    fn get_handle(&self) -> Result<MimerHandle, i32> {
        self.inner_connection.get_handle()
    }
    fn get_session_handle(&self) -> Result<Option<MappedMutexGuard<ffi::MimerSession>>, i32> {
        self.inner_connection.get_session_handle()
    }
}

impl Connection {
    /// Opens a connection to a MimerSQL database.
    ///
    /// # Errors
    /// Returns [Err] holding a [MimerError] when a connection failed to open.
    ///
    /// # Examples
    /// ```
    /// # use mimerrust::Connection;
    /// # let db = &std::env::var("MIMER_DATABASE").unwrap();
    /// # let ident = "RUSTUSER";
    /// # let pass = "RUSTPASSWORD";
    /// let conn = Connection::open(db, ident, pass).unwrap();
    /// ```
    pub fn open(database: &str, ident: &str, password: &str) -> Result<Connection, MimerError> {
        let inner = InnerConnection::open(database, ident, password)?;
        Ok(Connection {
            inner_connection: Arc::new(inner),
        })
    }

    /// Returns a MimerError given a [Connection] and a return code.
    /// This can be errors from the Mimer database itself, or errors from the Mimer Rust API.
    ///
    /// # Errors
    /// Returns [Err] when this method fails. It will still return a MimerError explaining what failed in this method.
    ///
    /// # Examples
    /// ```
    /// # use mimerrust::*;
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
    /// println!("{}", err);
    /// ```
    pub fn get_error(&self, error_code: i32) -> MimerError {
        MimerError::new(self, error_code)
    }

    /// Executes an SQL statement on the database. Mainly used for DDL statements.
    /// The query needs to be defined with parameter values inline, and can't contain named parameters.
    ///
    /// # Errors
    /// Returns [`Err`] when a statement can't be executed, e.g. if the query contained a syntax error or if the database server is stopped.
    ///
    /// # Examples
    /// ```
    /// # use mimerrust::*;
    /// # let db = &std::env::var("MIMER_DATABASE").unwrap();
    /// # let ident = "RUSTUSER";
    /// # let pass = "RUSTPASSWORD";
    /// let conn = Connection::open(db, ident, pass).unwrap();
    /// # conn.execute_statement("drop table test_table").ok();
    /// # conn.execute_statement("create table test_table (column_1 VARCHAR(30), column_2 INT)").unwrap();
    ///
    /// conn.execute_statement("INSERT INTO test_table VALUES('the number one',1)").unwrap();
    /// ```
    pub fn execute_statement(&self, sqlstatement: &str) -> Result<i32, i32> {
        let stmnt_char_ptr = CString::new(sqlstatement)
            .or_else(|_| Err(-26999))?
            .into_raw();

        unsafe {
            let rc =
                ffi::MimerExecuteStatement8(*self.get_session_handle()?.unwrap(), stmnt_char_ptr); //Ok unwrap since we know the session is a session

            // retake pointer to free memory
            let _ = std::ffi::CString::from_raw(stmnt_char_ptr);
            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Less => Err(rc),
                Ordering::Equal => Ok(rc),
                Ordering::Greater => {
                    // i suppose this is a reasonable panic?
                    panic!("Return code is positive from C API function which doesn't return a positive value");
                }
            }
        }
    }

    /// Prepares a SQL statement and creates a [Statement].
    ///
    /// # Errors
    /// Returns [Err] when a statement can't be prepared, e.g. if the query contained invalid syntax.
    ///
    /// # Examples
    /// ```
    /// # use mimerrust::*;
    /// # let db = &std::env::var("MIMER_DATABASE").unwrap();
    /// # let ident = "RUSTUSER";
    /// # let pass = "RUSTPASSWORD";
    /// let mut conn = Connection::open(db, ident, pass).unwrap();
    ///
    /// # conn.execute_statement("drop table test_table").ok();
    /// # conn.execute_statement("create table test_table (column_1 VARCHAR(30), column_2 INT)").unwrap();
    /// let stmnt = conn.prepare("INSERT INTO test_table VALUES(:column_1,:column_2)", CursorMode::Forward).unwrap();
    /// ```
    pub fn prepare(&mut self, sqlstatement: &str, option: CursorMode) -> Result<Statement, i32> {
        let (inner, stmt) =
            Statement::new(Arc::downgrade(&self.inner_connection), sqlstatement, option)?;
        self.inner_connection.push_statement(inner);
        Ok(stmt)
    }

    /// Initiates a database transaction.
    /// This method only needs to be called if two or more database operations should participate in the transaction.
    ///
    /// # Errors
    /// Returns [Err] when a transaction can't be started on the connection.
    ///
    /// # Examples
    /// ```
    /// # use mimerrust::*;
    /// # let db = &std::env::var("MIMER_DATABASE").unwrap();
    /// # let ident = "RUSTUSER";
    /// # let pass = "RUSTPASSWORD";
    /// let mut conn = Connection::open(db, ident, pass).unwrap();
    /// let trans_option = TransactionMode::ReadWrite;
    /// let trans = conn.begin_transaction(trans_option).unwrap();
    ///
    /// // Do some actions on the database like inserting or removing values in table(s)
    ///
    /// trans.commit().unwrap();
    /// ```
    pub fn begin_transaction(&mut self, trans_option: TransactionMode) -> Result<Transaction, i32> {
        Transaction::new(self, trans_option)
    }
    /// Obtains server statistics information.
    /// Statistics is returned in the form of counters.
    /// Counters may either be an absolute value representing the current status or a monotonically increasing value representing the number of occurred events since the server started.
    /// An example of the former is current number of users and an example of the latter is number of server page requests.
    /// The available counter values are:
    ///
    ///
    /// - BSI_4K : The number of 4K pages available in the system.
    /// - BSI_32K : The number of 32K pages available in the system.
    /// - BSI_128K : The number of 128K pages available in the system.
    /// - BSI_PAGES_USED : The total number of pages in use.
    /// - BSI_4K_USED : The number of 4K pages in use.
    /// - BSI_32K_USED : The number of 32K pages in use.
    /// - BSI_128K_USED : The number of 128K pages in use.
    ///
    /// # Errors
    /// Returns [Err] on invalid counter parameter in "counters" argument, or if failed to connect to server.
    ///
    /// # Examples
    ///
    /// ```
    /// # use mimerrust::*;
    /// # let db = &std::env::var("MIMER_DATABASE").unwrap();
    /// # let ident = "RUSTUSER";
    /// # let pass = "RUSTPASSWORD";
    /// let conn = Connection::open(db, ident, pass).unwrap();
    ///
    /// // Specify counters
    /// let mut counters = vec![BSI_4K,BSI_4K_USED,BSI_32K,BSI_32K_USED,BSI_128K,BSI_128K_USED,BSI_PAGES_USED];
    ///
    /// // Get statistics
    /// conn.get_statistics(&mut counters).unwrap();
    ///
    /// // counters now holds statistics from the database
    /// counters.iter().for_each(|c| assert!(c >= MIMER_SUCCESS));
    /// ```

    pub fn get_statistics(&self, counters: &mut Vec<i32>) -> Result<i32, i32> {
        let num_counters = counters.len() as i16;
        let counters_arr = counters.as_mut_ptr();
        let rc: i32;
        unsafe {
            rc = ffi::MimerGetStatistics(
                *self.get_session_handle()?.unwrap(),
                counters_arr,
                num_counters,
            );
        }
        match rc.cmp(MIMER_SUCCESS) {
            Ordering::Equal => {
                // replace each element in counters with the corresponding element in counters_arr
                unsafe {
                    let _ = counters
                        .iter_mut()
                        .enumerate()
                        .map(|(idx, c)| *c = *counters_arr.offset(idx as isize));
                }
                Ok(*MIMER_SUCCESS)
            }
            _ => Err(rc),
        }
    }
}

#[cfg(test)]
mod connection_tests {
    use super::*;
    use crate::testing::*;

    #[test]
    fn connection_new_fail() {
        let db: &str = "invalid_database_name";
        match Connection::open(db, IDENT, PASSWORD) {
            Err(_) => (),
            _ => panic!("This test should fail, and it did not fail! :("),
        }
    }

    #[test]
    fn error_login() {
        if let Ok(db) = std::env::var("MIMER_DATABASE") {
            match Connection::open(&db, IDENT, "wrong_password") {
                Ok(_) => panic!("Created a connection with the wrong password"),
                Err(ec) => assert_eq!(-14006, ec.get_error_code()),
            }
        } else {
            panic!("Environment variable MIMER_DATABASE not set.")
        }
    }

    #[test]
    fn create_transaction() {
        let mut conn = establish_connection();
        let trans_option = TransactionMode::ReadWrite;
        let _trans = match conn.begin_transaction(trans_option) {
            Ok(t) => t,
            Err(ec) => panic!("Could not create transaction: {}", ec),
        };
    }

    #[test]
    fn statement_list_decreasing() {
        let mut conn = establish_connection();
        let stmt = conn
            .prepare("SELECT * FROM test_table", CursorMode::Forward)
            .unwrap();
        assert_eq!(1, conn.inner_connection.statements.lock().len());
        drop(stmt);
        assert_eq!(0, conn.inner_connection.statements.lock().len());
    }
}

#[cfg(test)]
mod execute_tests {
    use std::vec;

    use super::*;
    use crate::testing::*;
    #[test]
    fn various_executes() {
        let conn = establish_connection();

        match conn.execute_statement(&format!("DROP TABLE {}", "non_existing_table")) {
            Ok(_) => panic!("Execute statement succeded when it should have failed."),
            Err(ec) => assert!(ec == -12501 || ec == -12517), // Mimer SQL Error: Table does not exist or Object does not exist respectively.
        }

        match conn.execute_statement(&format!("Invalid sql statemen")) {
            Ok(_) => panic!("Execute statement succeded when it should have failed."),
            Err(ec) => assert_eq!(ec, -12103), // Mimer SQL Error: Syntax error.
        }
    }
    #[test]
    fn get_error_execute() {
        let conn = establish_connection();

        let err = match conn.execute_statement(&format!("DROP TABLE {}", "non_existing_table")) {
            Ok(_) => panic!("Execute statement succeded when it should have failed."),
            Err(ec) => conn.get_error(ec),
        };
        let ec = err.get_error_code();
        assert!(ec == -12501 || ec == -12517); // Mimer SQL Error: Table does not exist or Object does not exist respectively.
        println!("dropping non existing table: {}", err);
    }

    #[test]
    fn prepare_new_fail() {
        let mut conn = establish_connection();

        let stmnt: Result<Statement, i32>;
        let option = CursorMode::Forward;

        stmnt = conn.prepare("[an invalid query]", option);
        match stmnt {
            Ok(_) => panic!("Prepare method succeeded when it should have failed."),
            Err(_) => (),
        }
    }

    #[test]
    fn prepare_new_succeed() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let _stmnt: Statement;
        let option = CursorMode::Forward;

        _stmnt = conn
            .prepare(&format!("SELECT * FROM {}", EXAMPLE_TABLE), option)
            .expect("Prepare method failed when it should have succeeded.");
    }

    #[test]
    fn prepare_execute_with_params_set() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let stmnt: Statement;
        let option = CursorMode::Forward;
        stmnt = conn
            .prepare(
                &format!(
                    "INSERT INTO {} {} {}",
                    EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMN_NAMES, EXAMPLE_TABLE_EXAMPLE_VALUES
                ),
                option,
            )
            .expect("Prepare method failed when it should have succeeded.");

        match stmnt.execute_bind(&[]) {
            Ok(rc) => assert_eq!(1, rc),
            Err(ec) => panic!("error executing statment {}", ec),
        }
    }

    #[test]
    fn execute_with_params_unset() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let stmnt: Statement;
        let option = CursorMode::Forward;
        let foo = String::from("hello world");
        let bar = 1;
        println!("foo: {}, bar: {}", foo, bar);
        stmnt = conn
            .prepare(
                &format!(
                    "INSERT INTO {} {} VALUES(:string,:id)",
                    EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMN_NAMES
                ),
                option,
            )
            .expect("Prepare method failed when it should have succeeded.");
        println!("foo: {}, bar: {}", foo, bar);
        match stmnt.execute_bind(&[&foo, &bar]) {
            Ok(rc) => assert_eq!(1, rc),
            Err(ec) => panic!("error executing statment {}", ec),
        };
        println!("foo: {}, bar: {}", foo, bar);
    }

    #[test]
    fn test_get_statistics() {
        let conn = establish_connection();
        let mut counters = vec![
            BSI_4K,
            BSI_4K_USED,
            BSI_32K,
            BSI_32K_USED,
            BSI_128K,
            BSI_128K_USED,
            BSI_PAGES_USED,
        ];
        dbg!(&counters);
        conn.get_statistics(&mut counters).unwrap();
        counters.iter().for_each(|c| assert!(c >= MIMER_SUCCESS));
        dbg!(counters);
    }
}
