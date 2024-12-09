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

/// Defines enums of options for methods that need them.
pub mod mimer_options {
    use mimerrust_sys as ffi;

    /// Cursor mode options.
    #[derive(PartialEq, Clone, Copy)]
    pub enum CursorMode {
        Forward = ffi::MIMER_FORWARD_ONLY as isize,
        Scrollable = ffi::MIMER_SCROLLABLE as isize,
    }

    /// Scroll options used in [scroll](crate::cursor::Cursor::scroll).
    #[derive(PartialEq, Clone, Copy)]
    pub enum ScrollOption {
        /// Move to the next row.
        NEXT,
        /// Move to the previous row.
        PREVIOUS,
        /// Move to a row number relative to the current position.
        RELATIVE,
        /// Move to an absolute row number.
        ABSOLUTE,
        /// Move to the first row of the result set.
        FIRST,
        /// Move to the last row of the result set.
        LAST,
    }

    impl ScrollOption {
        pub(crate) fn to_c_int(&self) -> i32 {
            match self {
                ScrollOption::NEXT => ffi::MIMER_NEXT as i32,
                ScrollOption::PREVIOUS => ffi::MIMER_PREVIOUS as i32,
                ScrollOption::RELATIVE => ffi::MIMER_RELATIVE as i32,
                ScrollOption::ABSOLUTE => ffi::MIMER_ABSOLUTE as i32,
                ScrollOption::FIRST => ffi::MIMER_FIRST as i32,
                ScrollOption::LAST => ffi::MIMER_LAST as i32,
            }
        }
    }

    /// Transaction mode options.
    #[derive(PartialEq, Clone, Copy)]
    pub enum TransactionMode {
        ReadOnly = ffi::MIMER_TRANS_READONLY as isize,
        ReadWrite = ffi::MIMER_TRANS_READWRITE as isize,
    }

    /// End transaction mode options.
    #[derive(PartialEq, Clone, Copy)]
    pub enum EndTransactionMode {
        Rollback = ffi::MIMER_ROLLBACK as isize,
        Commit = ffi::MIMER_COMMIT as isize,
    }

    /// Parametermodes used in routines
    #[derive(PartialEq, Clone, Copy, Debug)]
    pub enum ParameterMode {
        IN = 1,
        OUT = 2,
        INOUT = 3,
    }

    /// Option for [get_statistics](crate::Connection::get_statistics()).
    pub const BSI_4K: i32 = ffi::BSI_4K_PAGES as i32;
    /// Option for [get_statistics](crate::Connection::get_statistics()).
    pub const BSI_32K: i32 = ffi::BSI_32K_PAGES as i32;
    /// Option for [get_statistics](crate::Connection::get_statistics()).
    pub const BSI_128K: i32 = ffi::BSI_128K_PAGES as i32;
    /// Option for [get_statistics](crate::Connection::get_statistics()).
    pub const BSI_4K_USED: i32 = ffi::BSI_4K_PAGES_USED as i32;
    /// Option for [get_statistics](crate::Connection::get_statistics()).
    pub const BSI_32K_USED: i32 = ffi::BSI_32K_PAGES_USED as i32;
    /// Option for [get_statistics](crate::Connection::get_statistics()).
    pub const BSI_128K_USED: i32 = ffi::BSI_128K_PAGES_USED as i32;
    /// Option for [get_statistics](crate::Connection::get_statistics()).
    pub const BSI_PAGES_USED: i32 = ffi::BSI_PAGES_USED as i32;
}

pub mod traits {
    use mimerrust_sys as ffi;
    use parking_lot::MappedMutexGuard;
    pub(crate) enum MimerHandle<'a> {
        Session(MappedMutexGuard<'a, ffi::MimerSession>),
        Statement(MappedMutexGuard<'a, ffi::MimerStatement>),
    }

    pub(crate) trait GetHandle {
        fn get_handle(&self) -> Result<MimerHandle, i32> {
            Err(-26100)
        }

        fn get_session_handle(&self) -> Result<Option<MappedMutexGuard<ffi::MimerSession>>, i32> {
            Ok(None)
        }

        fn get_statement_handle(
            &self,
        ) -> Result<Option<MappedMutexGuard<ffi::MimerStatement>>, i32> {
            Ok(None)
        }
    }
    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::common::mimer_options::CursorMode;
        use crate::testing::*;

        #[test]
        fn get_handle() {
            let mut conn = establish_connection();

            drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
            let stmnt = conn
                .prepare("select * from test_table", CursorMode::Forward)
                .unwrap();
            assert!(conn.get_session_handle().unwrap().is_some());
            assert!(stmnt.get_statement_handle().unwrap().is_some());
        }
    }
}

pub mod return_codes {
    use mimerrust_sys as ffi;

    pub const MIMER_SUCCESS: &i32 = &(ffi::MIMER_SUCCESS as i32);
    // Perhaps the following codes are not needed, as generally its only relevant to check for MIMER_SUCCESS
    // pub const MIMER_OUTOFMEMORY: &i32 = &(ffi::MIMER_OUTOFMEMORY as i32);
    // pub const MIMER_SEQUENCE_ERROR: &i32 = &(ffi::MIMER_SEQUENCE_ERROR as i32);
    // pub const MIMER_NONEXISTENT_COLUMN_PARAMETER: &i32 = &(ffi::MIMER_NONEXISTENT_COLUMN_PARAMETER as i32);
    // pub const MIMER_HANDLE_INVALID:&i32 = &(ffi::MIMER_HANDLE_INVALID as i32);
    // TODO: continue this mapping
}
