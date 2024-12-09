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
use crate::common::traits::MimerHandle;
use crate::inner_connection::*;
use mimerrust_sys as ffi;

#[doc(hidden)]
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
#[doc(hidden)]
use std::{
    cmp::Ordering,
    ffi::CString,
    result::Result::{Err, Ok},
    sync::Weak,
};
/// Represents the internal parts of a Statement and handles the C API statement struct.
pub struct InnerStatement {
    statement: Mutex<ffi::MimerStatement>,
    pub(crate) inner_connection: Weak<InnerConnection>,
    statement_list_in_connection_id: u64,
}

unsafe impl Send for InnerStatement {} //TODO: Is this safe to be left empty?
unsafe impl Sync for InnerStatement {} //TODO: Is this safe to be left empty?

impl Drop for InnerStatement {
    fn drop(&mut self) {
        let mut handle = self.get_statement_handle().unwrap().unwrap(); //Ok unwraps since if an error occurs in drop it is unrecoverable
        match self.check_connection() {
            Ok(_) => {
                self.inner_connection
                    .upgrade()
                    .unwrap()
                    .remove_statement(self.statement_list_in_connection_id); //Ok unwrap since if an error occurs in drop it is unrecoverable
                unsafe {
                    ffi::MimerEndStatement(&mut *handle);
                }
            }
            Err(-26003) => (),
            // is this is a reasonable panic?
            Err(ec) => panic!("Failed to check connection while dropping statement: {ec}"),
        }
    }
}

impl InnerStatement {
    /// Checks if a connection to the database remains.
    pub(crate) fn check_connection(&self) -> Result<(), i32> {
        if Weak::strong_count(&self.inner_connection) == 0 {
            return Err(-26003); // connection has been dropped
        }
        Ok(())
    }

    /// Creates a new InnerStatement.
    pub(crate) fn new(
        inner_connection: Weak<InnerConnection>,
        sqlstatement: &str,
        cursor_mode: CursorMode,
    ) -> Result<(InnerStatement, usize), i32> {
        let stmnt_char_ptr = CString::new(sqlstatement)
            .or_else(|_| Err(-26999))?
            .into_raw();
        let mut statement = std::ptr::null_mut();
        let rc: i32;

        unsafe {
            rc = ffi::MimerBeginStatement8(
                *inner_connection
                    .upgrade()
                    .ok_or(-26003)?
                    .get_session_handle()?
                    .unwrap(), //Ok unwrap since we know that the connection is a connection
                stmnt_char_ptr,
                cursor_mode as i32,
                &mut statement,
            );

            // retake pointers to free memory
            let _ = CString::from_raw(stmnt_char_ptr);

            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Less => Err(rc),
                Ordering::Equal => {
                    let rc = ffi::MimerParameterCount(statement);
                    match rc.cmp(MIMER_SUCCESS) {
                        Ordering::Equal | Ordering::Greater => {
                            let num_param = rc as usize;
                            Ok((
                                InnerStatement {
                                    statement: Mutex::new(statement),
                                    inner_connection,
                                    statement_list_in_connection_id: statement as u64,
                                },
                                num_param,
                            ))
                        }
                        Ordering::Less => Err(rc),
                    }
                }
                Ordering::Greater => {
                    // i suppose this is a reasonable panic?
                    panic!("Return code is positive from C API function which doesn't return a positive value");
                }
            }
        }
    }

    /// Ends a statement.
    pub(crate) fn end_statement(&self) -> Result<(), i32> {
        let mut handle = self.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        unsafe {
            let rc = ffi::MimerEndStatement(&mut *handle);
            if rc < 0 {
                return Err(rc);
            }
        }
        Ok(())
    }
}

impl GetHandle for InnerStatement {
    fn get_handle(&self) -> Result<MimerHandle, i32> {
        Ok(MimerHandle::Statement(MutexGuard::map(
            self.statement.lock(),
            |inner| &mut *inner,
        )))
    }

    fn get_statement_handle(&self) -> Result<Option<MappedMutexGuard<ffi::MimerStatement>>, i32> {
        Ok(Some(MutexGuard::map(self.statement.lock(), |inner| {
            &mut *inner
        })))
    }
}
