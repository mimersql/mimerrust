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

use crate::common::return_codes::MIMER_SUCCESS;
use crate::common::traits::*;
use crate::inner_statement::*;
use crate::mimer_error::*;
use mimerrust_sys as ffi;

#[doc(hidden)]
use lazy_static::lazy_static;

#[doc(hidden)]
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};

lazy_static! {
    static ref connect_disconnect_mtx: Mutex<i32> = Mutex::new(0);
}

#[doc(hidden)]
use std::{
    cmp::Ordering,
    collections::HashMap,
    ffi::CString,
    result::Result::{Err, Ok},
    sync::Weak,
};

/// Represents the internal parts of a Connection and handles the C API session struct.
pub struct InnerConnection {
    pub(crate) session: Mutex<ffi::MimerSession>,
    pub(crate) statements: Mutex<HashMap<u64, Weak<InnerStatement>>>,
}

unsafe impl Send for InnerConnection {} //TODO: Is this safe to be left empty?
unsafe impl Sync for InnerConnection {} //TODO: Is this safe to be left empty?

impl InnerConnection {
    /// Opens a connection to a MimerSQL database.
    pub fn open(
        database: &str,
        ident: &str,
        password: &str,
    ) -> Result<InnerConnection, MimerError> {
        let mut sess: ffi::MimerSession = std::ptr::null_mut();

        // Convert strings to c compatible char *
        let db_char_ptr = CString::new(database)
            .or_else(|_| Err(MimerError::mimer_error_from_code(-26999)))?
            .into_raw();
        let ident_char_ptr = CString::new(ident)
            .or_else(|_| Err(MimerError::mimer_error_from_code(-26999)))?
            .into_raw();
        let pw_char_ptr = CString::new(password)
            .or_else(|_| Err(MimerError::mimer_error_from_code(-26999)))?
            .into_raw();

        unsafe {
            let _lck = connect_disconnect_mtx.lock();
            let rc = ffi::MimerBeginSession8(db_char_ptr, ident_char_ptr, pw_char_ptr, &mut sess);

            // retake pointers to free memory
            let _ = CString::from_raw(db_char_ptr);
            let _ = CString::from_raw(ident_char_ptr);
            let _ = CString::from_raw(pw_char_ptr);

            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Greater => {
                    // i suppose this is a reasonable panic?
                    panic!("Return code is positive from C API function which doesn't return a positive value")
                }
                Ordering::Equal => (),
                Ordering::Less => return Err(MimerError::mimer_error_from_code(rc)),
            }

            match sess.as_mut() {
                Some(session) => Ok(InnerConnection {
                    session: Mutex::new(session),
                    statements: Mutex::new(HashMap::new()),
                }),

                None => Err(MimerError::mimer_error_from_code(-26002)), // Session pointer returned from C API was NULL
            }
        }
    }

    /// Pushes a statement pointer to the [HashMap] of statements.
    pub(crate) fn push_statement(&self, stmt: Weak<InnerStatement>) {
        let strong_stmt = stmt.upgrade().unwrap(); //Ok unwrap since we know the statement is still alive
        let id = strong_stmt.get_statement_handle().unwrap().unwrap(); //Ok unwraps since we know the statement is still alive and is a statement
        self.statements.lock().insert(*id as u64, stmt);
    }

    /// Removes a statement pointer from the [HashMap] of statements.
    pub(crate) fn remove_statement(&self, id: u64) {
        self.statements.lock().remove(&id);
    }
}

impl Drop for InnerConnection {
    fn drop(&mut self) {
        for stmt in self.statements.lock().values_mut() {
            if let Some(stmt) = stmt.upgrade() {
                stmt.end_statement().unwrap(); //Ok unwrap since if error occurs in drop it is unrecoverable
            }
        }
        unsafe {
            let _lck = connect_disconnect_mtx.lock();
            ffi::MimerEndSessionHard(&mut *self.get_session_handle().unwrap().unwrap());
            //Ok unwraps since if error occurs in drop it is unrecoverable
        }
    }
}

impl GetHandle for InnerConnection {
    fn get_handle(&self) -> Result<MimerHandle, i32> {
        Ok(MimerHandle::Session(MutexGuard::map(
            self.session.lock(),
            |inner| &mut *inner,
        )))
    }

    fn get_session_handle(&self) -> Result<Option<MappedMutexGuard<ffi::MimerSession>>, i32> {
        Ok(Some(MutexGuard::map(self.session.lock(), |inner| {
            &mut *inner
        })))
    }
}
