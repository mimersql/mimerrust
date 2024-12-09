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
    common::{
        mimer_options::*,
        return_codes::MIMER_SUCCESS,
        traits::{GetHandle, MimerHandle},
    },
    connection::Connection,
};
use mimerrust_sys as ffi;

#[doc(hidden)]
use std::{
    cmp::Ordering,
    ops::{Deref, DerefMut},
    result::Result::{Err, Ok},
};

/// Represents a transaction on a database connection. A Transaction will roll back by default if the object is dropped.
/// Use the `commit` method to commit the changes made in the transaction.
pub struct Transaction<'a> {
    connection: &'a mut Connection,
}

impl GetHandle for Transaction<'_> {
    fn get_handle(&self) -> Result<MimerHandle, i32> {
        self.connection.get_handle()
    }

    fn get_session_handle(
        &self,
    ) -> Result<Option<parking_lot::MappedMutexGuard<mimerrust_sys::MimerSession>>, i32> {
        self.connection.get_session_handle()
    }
}

impl Transaction<'_> {
    /// Creates a Transaction struct
    pub(crate) fn new(conn: &mut Connection, toption: TransactionMode) -> Result<Transaction, i32> {
        unsafe {
            let rc = ffi::MimerBeginTransaction(
                *conn.get_session_handle()?.unwrap(), //Ok unwrap since we know the connection is a connection
                toption as i32,
            );
            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Greater => {
                    // i suppose this is a reasonable panic?
                    panic!("Return code is positive from C API function which doesn't return a positive value")
                }
                Ordering::Equal => Ok(Transaction { connection: conn }),
                Ordering::Less => Err(rc),
            }
        }
    }

    /// Commits a [Transaction] into the database, returns 0 if successful and a negative number if unsuccessful.
    /// This function consumes the transaction, meaning that the transaction object will be dropped after being called.
    ///
    /// # Errors
    /// Returns [`Err`] when a transaction can't be commited.
    ///
    /// # Examples
    ///
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
    pub fn commit(mut self) -> Result<i32, i32> {
        self.end_transaction(EndTransactionMode::Commit)
    }

    /// Rolls back a [Transaction] to the state of the database before transaction was created, returns 0 if successful and a negative number if unsuccessful.
    /// This function consumes the transaction, meaning that the transaction object will be dropped after call.
    ///
    /// # Errors
    /// Returns [Err] when a transaction can't be rolled back.
    ///
    /// # Examples
    ///
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
    /// trans.rollback().unwrap();
    /// ```
    pub fn rollback(mut self) -> Result<i32, i32> {
        self.end_transaction(EndTransactionMode::Rollback)
    }

    /// Ends a transaction
    fn end_transaction(&mut self, trans_option: EndTransactionMode) -> Result<i32, i32> {
        let handle = self.get_session_handle()?.unwrap(); //Ok unwrap since we know the connection is a connection
        unsafe {
            let rc = ffi::MimerEndTransaction(*handle, trans_option as i32);
            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Greater => {
                    // i suppose this is a reasonable panic?
                    panic!("Return code is positive from C API function which doesn't return a positive value")
                }
                Ordering::Equal => Ok(rc),
                Ordering::Less => Err(rc),
            }
        }
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        self.end_transaction(EndTransactionMode::Rollback).ok();
    }
}

impl Deref for Transaction<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.connection
    }
}

impl DerefMut for Transaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection
    }
}

#[cfg(test)]
mod transaction_tests {
    use super::*;
    use crate::testing::*;
    use core::panic;

    #[test]
    fn create_transaction() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
        let trans_option = TransactionMode::ReadWrite;
        match Transaction::new(&mut conn, trans_option) {
            Ok(_) => (),
            Err(ec) => panic!("Could not create transaction: {}", ec),
        };
    }

    #[test]
    fn transaction_drop() {
        let mut conn = establish_connection();
        let trans_option = TransactionMode::ReadWrite;
        match Transaction::new(&mut conn, trans_option) {
            Ok(t) => t,
            Err(ec) => panic!("Could not create transaction: {ec}"),
        };
        match Transaction::new(&mut conn, trans_option) {
            Ok(_) => (),
            Err(ec) => panic!("Could not create transaction: {ec}"),
        };
    }

    #[test]
    fn rollback_on_drop() {
        let mut conn = establish_connection();
        let trans_option = TransactionMode::ReadWrite;
        let mut trans = match Transaction::new(&mut conn, trans_option) {
            Ok(t) => t,
            Err(ec) => panic!("Could not create transaction: {ec}"),
        };
        match trans.end_transaction(EndTransactionMode::Rollback) {
            Ok(_rc) => (),
            Err(ec) => panic!("Could not end transaction: {ec}"),
        }
        match trans.end_transaction(EndTransactionMode::Rollback) {
            Ok(_rc) => panic!("Should not be able to end transaction twice"),
            Err(ec) => assert_eq!(-24101, ec),
        };
    }

    #[test]
    fn execute_while_transaction() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let trans_option = TransactionMode::ReadWrite;
        let trans = Transaction::new(&mut conn, trans_option).unwrap();

        trans
            .execute_statement(&format!(
                "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_COLUMN_NAMES} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
            ))
            .unwrap();

        match trans.rollback() {
            Ok(rc) => rc,
            Err(ec) => panic!("Ending transaction failed: {}", ec),
        };
    }

    #[test]
    fn transaction_rollback() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let trans_option = TransactionMode::ReadWrite;
        let trans = Transaction::new(&mut conn, trans_option).unwrap();

        trans
            .execute_statement(&format!(
                "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_COLUMN_NAMES} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
            ))
            .unwrap();

        match trans.rollback() {
            Ok(rc) => rc,
            Err(ec) => panic!("Ending transaction failed: {}", ec),
        };

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        let mut cursor = stmt.open_cursor().unwrap();
        match cursor.next_row().unwrap() {
            Some(_) => panic!("Select found entries when transaction rolled back"),
            None => (),
        }
    }

    #[test]
    fn transaction_commit() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let trans_option = TransactionMode::ReadWrite;
        let trans = Transaction::new(&mut conn, trans_option).unwrap();

        let _ = trans.execute_statement(
            "INSERT INTO test_table (column_1, column_2) VALUES ('some value', 42)",
        );

        match trans.commit() {
            Ok(rc) => rc,
            Err(ec) => panic!("Ending transaction failed: {}", ec),
        };

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        let mut cursor = stmt.open_cursor().unwrap();
        cursor
            .next_row()
            .unwrap()
            .expect("Select didn't find any entries when transaction was commited");
    }

    #[test]
    fn transaction_read_only() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let trans_option = TransactionMode::ReadOnly;
        let trans = Transaction::new(&mut conn, trans_option).unwrap();

        let _ = trans.execute_statement(
            "INSERT INTO test_table (column_1, column_2) VALUES ('some value', 42)",
        );

        match trans.commit() {
            Ok(rc) => rc,
            Err(ec) => panic!("Ending transaction failed: {}", ec),
        };

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        let mut cursor = stmt.open_cursor().unwrap();
        match cursor.next_row().unwrap() {
            Some(_) => panic!("Select found entries when transaction was in read only mode"),
            None => (),
        }
    }

    #[test]
    fn transaction_deref() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let trans_option = TransactionMode::ReadWrite;
        let trans = Transaction::new(&mut conn, trans_option).unwrap();

        let _ = trans.execute_statement(
            "INSERT INTO test_table (column_1, number) VALUES ('some value', 42)",
        );

        match trans.commit() {
            Ok(rc) => rc,
            Err(ec) => panic!("Ending transaction failed: {}", ec),
        };
    }

    #[test]
    fn transaction_begin_deref() {
        let mut conn = establish_connection();

        let trans_option = TransactionMode::ReadWrite;
        let mut trans = Transaction::new(&mut conn, trans_option).unwrap();

        match trans.begin_transaction(trans_option) {
            Ok(_) => panic!("Should not be able to create another transaction"),
            Err(ec) => assert_eq!(-14011, ec),
        };
    }
}
