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

use crate::common::mimer_options::*;
use crate::common::return_codes::MIMER_SUCCESS;
use crate::common::traits::GetHandle;
use crate::inner_statement::*;
use crate::row::Row;
use mimerrust_sys as ffi;

#[doc(hidden)]
use fallible_streaming_iterator::FallibleStreamingIterator;
#[doc(hidden)]
use std::{
    cmp::Ordering,
    sync::{Arc, Weak},
};

/// An iterator for result sets from MimerSQL databases.
pub struct Cursor {
    mode: CursorMode,
    pub(crate) inner_statement: Weak<InnerStatement>,
    pub(crate) scroll_option: ScrollOption,
    row: Option<Row>, // To store the current row
}

impl Cursor {
    pub(crate) fn open(
        inner_statement: Arc<InnerStatement>,
        mode: CursorMode,
    ) -> Result<Cursor, i32> {
        let handle = inner_statement.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        inner_statement.check_connection()?;
        let code: i32;
        unsafe {
            code = ffi::MimerOpenCursor(*handle);
        }

        match code.cmp(MIMER_SUCCESS) {
            Ordering::Less => Err(code),
            Ordering::Equal => Ok(Cursor {
                inner_statement: Arc::downgrade(&inner_statement),
                mode,
                scroll_option: ScrollOption::NEXT,
                row: None,
            }),
            Ordering::Greater => {
                // i suppose this is a reasonable panic?
                panic!("Return code is positive from C API function which doesn't return a positive value")
            }
        }
    }

    /// Closes the cursor.
    fn close_cursor(&self) -> Result<i32, i32> {
        let strong_inner_statement = self.inner_statement.upgrade().ok_or(-26004)?;
        let handle = strong_inner_statement.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        strong_inner_statement.check_connection()?;

        let code: i32;
        unsafe {
            code = ffi::MimerCloseCursor(*handle);
        }
        match code.cmp(MIMER_SUCCESS) {
            Ordering::Greater => {
                // i suppose this is a reasonable panic?
                panic!("Return code is positive from C API function which doesn't return a positive value")
            }
            Ordering::Equal => Ok(code),
            Ordering::Less => Err(code),
        }
    }

    /// Sets the scroll option for the cursor. Only avaiable for cursors on statements prepared as (scrollable)[CursorMode::Scrollable].
    /// Valid options are [ScrollOption::NEXT], [ScrollOption::PREVIOUS], [ScrollOption::FIRST], [ScrollOption::LAST], [ScrollOption::ABSOLUTE], [ScrollOption::RELATIVE].
    ///
    /// The options are configured as follows
    /// - [ScrollOption::NEXT] - Moves the cursor to the next row in the result set.
    /// - [ScrollOption::PREVIOUS] - Moves the cursor to the previous row in the result set.
    /// - [ScrollOption::FIRST] - Moves the cursor to the first row in the result set.
    /// - [ScrollOption::LAST] - Moves the cursor to the last row in the result set.
    /// - [ScrollOption::ABSOLUTE] - Moves the cursor to the specified row in the result set. The row number is specified as a parameter to the [scroll](crate::cursor::Cursor::scroll) method.
    /// - [ScrollOption::RELATIVE] - Moves the cursor to the specified row relative to the current row. The row number is specified as a parameter to the [scroll](crate::cursor::Cursor::scroll) method.
    ///
    pub fn set_scroll_option(&mut self, option: ScrollOption) {
        self.scroll_option = option;
    }

    /// Moves cursor to specified row index.
    /// Takes into consideration the [ScrollOption](crate::common::mimer_options::ScrollOption) set for the cursor.
    /// The default scroll option is [ScrollOption::NEXT].
    /// To change the scroll option use [set_scroll_option](crate::cursor::Cursor::set_scroll_option).
    ///
    /// # Errors
    /// Returns [Err] when the cursor could not be moved to the specified row, e.g. if the specified index is out of bounds.
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
    /// # conn.execute_statement("INSERT INTO test_table VALUES('the number one',1)").unwrap();
    /// # conn.execute_statement("INSERT INTO test_table VALUES('the number one',1)").unwrap();
    /// let stmnt = conn.prepare("SELECT * FROM test_table", CursorMode::Scrollable).unwrap();
    ///
    /// let mut cursor = stmnt.open_cursor().unwrap();
    /// let row = cursor.scroll(2).unwrap().expect("Nothing was found on the specified index");
    /// ```
    pub fn scroll(&mut self, idx: i32) -> Result<Option<&Row>, i32> {
        let strong_inner_statement = self.inner_statement.upgrade().ok_or(-26004)?;
        let handle = strong_inner_statement.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        strong_inner_statement.check_connection()?;
        let code: i32;
        unsafe {
            code = ffi::MimerFetchScroll(*handle, self.scroll_option.to_c_int(), idx);
        }
        match code.try_into() {
            Ok(ffi::MIMER_SUCCESS) => {
                self.row = Some(Row {
                    inner_statement: self.inner_statement.clone(),
                });
                Ok(self.row.as_ref())
            }
            Ok(ffi::MIMER_NO_DATA) => {
                self.row = None;
                Ok(self.row.as_ref())
            }
            _ => Err(code),
        }
    }

    /// Moves cursor to the next row in the result set and returns its contents.
    /// On success, returns either Some([Row](crate::row::Row)) or [None] if there is no more data to fetch.
    ///
    /// # Errors
    /// Returns [Err] when cursor couldn't advance.
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
    ///
    /// let mut cursor = stmnt.open_cursor().unwrap();
    /// let row = cursor.next_row().unwrap().expect("Nothing was found on this row");
    /// ```
    pub fn next_row(&mut self) -> Result<Option<&Row>, i32> {
        self.next()
    }

    /// Returns the [CursorMode] of the Cursor.
    pub fn get_mode(&self) -> CursorMode {
        self.mode
    }

    /// Checks if the cursor's mode matches a given [CursorMode].
    pub fn check_is_mode(&self, mode: CursorMode) -> bool {
        self.mode == mode
    }

    /// Returns current index
    pub fn current_row(&self) -> Result<i32, i32> {
        let strong_inner_statement = self.inner_statement.upgrade().ok_or(-26004)?;
        let handle = strong_inner_statement.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        strong_inner_statement.check_connection()?;
        let rc: i32;
        unsafe {
            rc = ffi::MimerCurrentRow(*handle);
            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Less => Err(rc),
                _ => Ok(rc),
            }
        }
    }
    /// Returns the maximum number of bytes required to hold one row of data.
    /// This method might be used to calculate the maximum number of rows allowed in an array fetching scenario under certain memory restrictions.
    pub fn get_row_size(&self) -> Result<i32, i32> {
        let strong_inner_statement = self.inner_statement.upgrade().ok_or(-26004)?;
        let handle = strong_inner_statement.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        unsafe {
            let rc = ffi::MimerRowSize(*handle);
            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Less => Err(rc),
                _ => Ok(rc),
            }
        }
    }
}
impl FallibleStreamingIterator for Cursor {
    type Error = i32;
    type Item = Row;

    fn advance(&mut self) -> Result<(), Self::Error> {
        let strong_inner_statement = self.inner_statement.upgrade().ok_or(-26004)?;
        let handle = strong_inner_statement.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        strong_inner_statement.check_connection()?;
        let code: i32;
        if self.mode == CursorMode::Scrollable {
            unsafe {
                code = ffi::MimerFetchScroll(*handle, ffi::MIMER_NEXT as i32, 0);
            }
        } else {
            unsafe {
                code = ffi::MimerFetch(*handle);
            }
        }
        match code.try_into() {
            Ok(ffi::MIMER_SUCCESS) => {
                self.row = Some(Row {
                    inner_statement: self.inner_statement.clone(),
                });
                Ok(())
            }
            Ok(ffi::MIMER_NO_DATA) => {
                self.row = None;
                Ok(())
            }
            _ => Err(code),
        }
    }

    fn get(&self) -> Option<&Self::Item> {
        self.row.as_ref()
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        match self.close_cursor() {
            Ok(_) => (),
            Err(-26003) => (), // Mimer Rust API error : Connection is dropped
            Err(-26004) => (), // Mimer Rust API error : Statement is dropped
            // is this is a reasonable panic?
            Err(ec) => panic!("Failed to close cursor: {ec}"),
        }
    }
}

#[cfg(test)]
mod cursor_tests {
    use super::*;
    use crate::common::mimer_options::CursorMode;
    use crate::testing::*;

    #[test]
    fn cursor_open_close() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();

        let _cursor = stmt.open_cursor().unwrap();
    }

    #[test]
    fn cursor_fetch_empty() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        let mut cursor = stmt.open_cursor().unwrap();
        //let mut cursor = Cursor::open(Cursor::new(&stmt)).unwrap();
        let row = cursor.next_row().unwrap();
        if row.is_some() {
            panic!("Expected no data")
        }
    }

    #[test]
    fn cursor_fetch() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
        conn.execute_statement(&format!(
            "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
        ))
        .unwrap();

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        let mut cursor = stmt.open_cursor().unwrap();
        let row = cursor.next_row().unwrap();
        if row.is_none() {
            panic!("Expected data")
        }
    }

    #[test]
    fn cursor_iter_get_once() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
        conn.execute_statement(&format!(
            "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
        ))
        .unwrap();

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        let mut cursor = stmt.open_cursor().unwrap();
        //let mut cursor = Cursor::open(Cursor::new(&stmt)).unwrap();
        let mut count = 0;
        while let Some(row) = cursor.next_row().unwrap() {
            count += 1;
            assert_eq!(row.get::<String>(1).unwrap().unwrap(), EXAMPLE_VALUE_1);
            assert_eq!(row.get::<i32>(2).unwrap().unwrap(), EXAMPLE_VALUE_2);
        }
        assert_eq!(count, 1);
    }

    #[test]
    fn cursor_iter_map() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
        conn.execute_statement(&format!(
            "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
        ))
        .unwrap();

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        let cursor = stmt.open_cursor().unwrap();
        let mut map_iter = cursor.map(|row| {
            assert_eq!(row.get::<String>(1).unwrap().unwrap(), EXAMPLE_VALUE_1);
            assert_eq!(row.get::<i32>(2).unwrap().unwrap(), EXAMPLE_VALUE_2);
            Ok(1)
        });
        assert!(map_iter
            .all(|int: &Result<i32, i32>| { int.unwrap() == 1 })
            .unwrap());
    }
    #[test]
    fn cursor_iter_map_many() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
        for _ in 0..10 {
            conn.execute_statement(&format!(
                "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
            ))
            .unwrap();
        }

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        let cursor = stmt.open_cursor().unwrap();
        let mut map_iter = cursor.map(|row| {
            assert_eq!(row.get::<String>(1).unwrap().unwrap(), EXAMPLE_VALUE_1);
            assert_eq!(row.get::<i32>(2).unwrap().unwrap(), EXAMPLE_VALUE_2);
            Ok(1)
        });
        assert!(map_iter
            .all(|int: &Result<i32, i32>| { int.unwrap() == 1 })
            .unwrap());

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        let cursor = stmt.open_cursor().unwrap();
        let map_iter = cursor.map(|row| {
            assert_eq!(row.get::<String>(1).unwrap().unwrap(), EXAMPLE_VALUE_1);
            assert_eq!(row.get::<i32>(2).unwrap().unwrap(), EXAMPLE_VALUE_2);
        });
        assert_eq!(map_iter.count().unwrap(), 10);
    }

    #[test]
    fn cursor_scroll() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
        for _ in 0..10 {
            conn.execute_statement(&format!(
                "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_EXAMPLE_VALUES}"
            ))
            .unwrap();
        }

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Scrollable,
            )
            .unwrap();
        let mut cursor = stmt.open_cursor().unwrap();
        let row = cursor.scroll(1).unwrap();
        assert!(row.is_some());
        let row = cursor.scroll(2).unwrap();
        assert!(row.is_some());
        let row = cursor.scroll(3).unwrap();
        assert!(row.is_some());
        let row = cursor.scroll(4).unwrap();
        assert!(row.is_some());
        let row = cursor.scroll(5).unwrap();
        assert!(row.is_some());
        let row = cursor.scroll(6).unwrap();
        assert!(row.is_some());
        let row = cursor.scroll(7).unwrap();
        assert!(row.is_some());
        let row = cursor.scroll(8).unwrap();
        assert!(row.is_some());
        let row = cursor.scroll(9).unwrap();
        assert!(row.is_some());
        let row = cursor.scroll(10).unwrap();
        assert!(row.is_some());
        let row = cursor.scroll(11).unwrap();
        assert!(row.is_none());
    }

    #[test]
    fn check_connection_next_row() {
        let mut cursor;
        let stmt;
        {
            let mut conn = establish_connection();

            drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
            stmt = conn
                .prepare("SELECT * FROM test_table", CursorMode::Forward)
                .unwrap();
            cursor = stmt.open_cursor().unwrap();
        }
        match cursor.next_row() {
            Ok(_) => panic!("Cursor went to next row when it shouldn't have!"),
            Err(ec) => {
                assert_eq!(ec, -26003); // connection has been dropped
            }
        }
    }

    #[test]
    fn check_statement_next_row() {
        let mut cursor;
        {
            let mut conn = establish_connection();

            drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
            let stmt = conn
                .prepare("SELECT * FROM test_table", CursorMode::Forward)
                .unwrap();
            cursor = stmt.open_cursor().unwrap();
        }
        match cursor.next_row() {
            Ok(_) => panic!("Cursor went to next row when it shouldn't have!"),
            Err(ec) => {
                assert_eq!(ec, -26004); // connection has been dropped
            }
        }
    }
    #[test]
    fn test_current_row() {
        let mut conn = establish_connection();

        drop_create_table(&conn, RESULT_TABLE, RESULT_TABLE_COLUMNS);

        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {RESULT_TABLE} VALUES (:c1,:c2,:c3)"),
                CursorMode::Forward,
            )
            .unwrap();

        for i in 1..4 {
            stmnt.execute_bind(&[&i, &(i + 1), &(i + 2)]).unwrap();
        }

        let stmnt = conn
            .prepare("SELECT * FROM result_table", CursorMode::Forward)
            .unwrap();
        let mut cursor = stmnt.open_cursor().unwrap();

        for i in 0..3 {
            let row = match cursor.current_row() {
                Ok(row) => row,
                _ => panic!("Failed to get current row"),
            };
            assert_eq!(row, i);
            if let Err(rc) = cursor.next_row() {
                panic!("Failed to get next row: {rc}");
            }
        }
    }

    #[test]
    fn test_row_size() {
        let mut conn = establish_connection();
        drop_create_table(&conn, BIGINT_TABLE, BIGINT_TABLE_COLUMNS);

        //let bytes: Vec<u8> = vec![b't', b'e', b's', b't'];
        let bytes: i64 = 9223372036854775807;

        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {BIGINT_TABLE}  VALUES(:b)"),
                CursorMode::Forward,
            )
            .unwrap();

        stmnt.bind(&bytes, 1).unwrap();
        stmnt.execute_bind(&[]).unwrap();
        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {BIGINT_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        let cursor = stmt.open_cursor().unwrap();
        let row_size = cursor.get_row_size().unwrap();
        assert_eq!(row_size, 16);
    }

    #[test]
    fn test_scrolloption() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let values_to_insert = [
            (String::from("one"), 1),
            (String::from("two"), 2),
            (String::from("three"), 3),
            (String::from("four"), 4),
            (String::from("five"), 5),
        ];

        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {EXAMPLE_TABLE}  VALUES(:str,:int)"),
                CursorMode::Forward,
            )
            .unwrap();

        values_to_insert.into_iter().for_each(|(s, i)| {
            stmnt.execute_bind(&[&s, &i]).unwrap();
        });

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Scrollable,
            )
            .unwrap();
        let mut cursor = stmt.open_cursor().unwrap();

        let mut row: &Row;

        // we start before the first row

        // None -> 5
        cursor.set_scroll_option(ScrollOption::LAST);
        row = cursor.scroll(1).unwrap().unwrap();

        assert_eq!(row.get::<String>(1).unwrap().unwrap(), "five");
        assert_eq!(row.get::<i32>(2).unwrap().unwrap(), 5);

        // 5 -> 4
        cursor.set_scroll_option(ScrollOption::PREVIOUS);
        row = cursor.scroll(1).unwrap().unwrap();

        assert_eq!(row.get::<String>(1).unwrap().unwrap(), "four");
        assert_eq!(row.get::<i32>(2).unwrap().unwrap(), 4);

        // 4 -> 1
        cursor.set_scroll_option(ScrollOption::FIRST);
        row = cursor.scroll(231).unwrap().unwrap();

        assert_eq!(row.get::<String>(1).unwrap().unwrap(), "one");
        assert_eq!(row.get::<i32>(2).unwrap().unwrap(), 1);

        // 1 -> 2
        cursor.set_scroll_option(ScrollOption::NEXT);
        row = cursor.scroll(13).unwrap().unwrap();

        assert_eq!(row.get::<String>(1).unwrap().unwrap(), "two");
        assert_eq!(row.get::<i32>(2).unwrap().unwrap(), 2);

        // 2 -> 4 (2+2)
        cursor.set_scroll_option(ScrollOption::RELATIVE);
        row = cursor.scroll(2).unwrap().unwrap();

        assert_eq!(row.get::<String>(1).unwrap().unwrap(), "four");
        assert_eq!(row.get::<i32>(2).unwrap().unwrap(), 4);

        row = cursor.scroll(-3).unwrap().unwrap();
        assert_eq!(row.get::<String>(1).unwrap().unwrap(), "one");
        assert_eq!(row.get::<i32>(2).unwrap().unwrap(), 1);

        cursor.set_scroll_option(ScrollOption::ABSOLUTE);

        row = cursor.scroll(5).unwrap().unwrap();
        assert_eq!(row.get::<String>(1).unwrap().unwrap(), "five");
        assert_eq!(row.get::<i32>(2).unwrap().unwrap(), 5);
    }

    #[test]
    fn test_scroll_option_fail() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let values_to_insert = [
            (String::from("one"), 1),
            (String::from("two"), 2),
            (String::from("three"), 3),
            (String::from("four"), 4),
            (String::from("five"), 5),
        ];

        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {EXAMPLE_TABLE}  VALUES(:str,:int)"),
                CursorMode::Forward,
            )
            .unwrap();

        values_to_insert.into_iter().for_each(|(s, i)| {
            stmnt.execute_bind(&[&s, &i]).unwrap();
        });

        let stmt = conn
            .prepare(
                &format!("SELECT * FROM {EXAMPLE_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();
        let mut cursor = stmt.open_cursor().unwrap();

        cursor.set_scroll_option(ScrollOption::ABSOLUTE);

        // this should fail
        match cursor.scroll(1) {
            Ok(_) => panic!("Cursor should not be able to scroll in forward mode"),
            Err(ec) => {
                assert_eq!(ec, -24101);
            }
        }

        // the following should work
        let mut row = cursor.next().unwrap().unwrap();
        assert_eq!(row.get::<String>(1).unwrap().unwrap(), "one");
        assert_eq!(row.get::<i32>(2).unwrap().unwrap(), 1);

        row = cursor.next().unwrap().unwrap();

        assert_eq!(row.get::<String>(1).unwrap().unwrap(), "two");
        assert_eq!(row.get::<i32>(2).unwrap().unwrap(), 2);
    }
}
