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
    cursor::*,
    inner_connection::*,
    inner_statement::*,
    match_mimer_BINARY,
    mimer_error::*,
    types::*,
};
use crate::{match_mimer_BLOB, match_mimer_CLOB};
use mimerrust_sys::{self as ffi, MimerStatement_struct};

#[doc(hidden)]
use parking_lot::MappedMutexGuard;
#[doc(hidden)]
use std::{
    cmp::Ordering,
    ffi::CString,
    sync::{Arc, Weak},
};

/// A prepared statement.
///
/// Each prepared statement is created through [prepare](crate::Connection::prepare()), and can only be executed on the connection that created it.
pub struct Statement {
    inner_statement: Arc<InnerStatement>,
    num_parameters: usize,
    cursor_mode: CursorMode,
    batch_bool: bool,
}

impl GetHandle for Statement {
    fn get_handle(&self) -> Result<MimerHandle, i32> {
        let handle = self.inner_statement.get_handle();
        self.inner_statement.check_connection()?;
        handle
    }

    fn get_statement_handle(&self) -> Result<Option<MappedMutexGuard<ffi::MimerStatement>>, i32> {
        let handle = self.inner_statement.get_statement_handle();
        self.inner_statement.check_connection()?;
        handle
    }
}
impl Statement {
    pub(crate) fn new(
        connection: Weak<InnerConnection>,
        sqlstatement: &str,
        cursor_mode: CursorMode,
    ) -> Result<(Weak<InnerStatement>, Statement), i32> {
        let (inner_statement, num_parameters) =
            InnerStatement::new(connection, sqlstatement, cursor_mode)?;
        let inner_arc = Arc::new(inner_statement);

        Ok((
            Arc::downgrade(&inner_arc),
            Statement {
                inner_statement: inner_arc,
                num_parameters,
                cursor_mode,
                batch_bool: false, // controls when we run MimerAddBatch. We dont want to run it "the last time" before we run execute.
            },
        ))
    }

    /// Executes this statement.
    /// Equivalent to calling [execute_bind](crate::Statement::execute_bind()) with an empty set of parameters, i.e "stmnt.execute_bind(&[]);".
    ///
    /// # Errors
    /// Returns [Err] when the statement couldn't be executed, e.g. if the statment has unset named parameters.
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
    /// let stmnt = conn.prepare("INSERT INTO test_table VALUES(:string,:int)", CursorMode::Forward).unwrap();
    ///
    /// let s = String::from("the number one");
    /// let i = 1;
    ///
    /// stmnt.bind(&s,1).unwrap();
    /// stmnt.bind(&i,2).unwrap();
    /// stmnt.execute().unwrap();
    /// ```
    pub fn execute(&self) -> Result<i32, i32> {
        let handle = self.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        if (*handle).is_null() {
            return Err(-26005); // Handle is NULL
        }
        unsafe {
            let rc = ffi::MimerExecute(*handle);
            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Equal => Ok(rc),
                Ordering::Greater => Ok(rc),
                Ordering::Less => Err(rc),
            }
        }
    }

    /// Executes a statement.
    /// If the statement query contains named parameters, the parameter values are expected to be given in order in the "params" argument to this method.
    /// If the statement query does not contain named parameters or if the parameters have already been set, the "params" argument is expected to be empty.
    /// Parameter values can also be set using the [bind](crate::Statement::bind()) method.
    /// Alternatively see [execute](crate::Statement::execute()), which does not set parameters prior to executing.
    ///
    /// # Errors
    /// Returns [Err] when the statement couldn't be executed, e.g. if the database server is stopped or if a parameter could not be set.
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
    /// let stmnt = conn.prepare("INSERT INTO test_table VALUES(:string,:int)", CursorMode::Forward).unwrap();
    ///
    /// let s = String::from("the number one");
    /// let i = 1;
    ///
    /// stmnt.execute_bind(&[&s,&i]).unwrap();
    /// ```
    pub fn execute_bind(&self, params: &[&dyn ToSql]) -> Result<i32, i32> {
        let handle = self.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        if (*handle).is_null() {
            return Err(-26005); // Handle is NULL
        }

        if !params.is_empty() {
            self.set_params(params, *handle)?;
        }
        unsafe {
            let rc = ffi::MimerExecute(*handle);
            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Equal => Ok(rc),
                Ordering::Greater => Ok(rc),
                Ordering::Less => Err(rc),
            }
        }
    }

    /// Sets parameters in a Statement, needed before executing it.
    /// Converts each Rust datatype (that implements the ToSQL trait) into a variant of the MimerDatatype enum.
    /// The MimerDatatype variant is then used with its appropriate setter, e.g. MimerSetInt64 for BigInt(i64).
    pub(crate) fn set_params(
        &self,
        params: &[&dyn ToSql],
        handle: ffi::MimerStatement,
    ) -> Result<i32, i32> {
        let mut i: i16 = 1;

        for param in params {
            if let Err(err) = self.bind_param_auxillary(*param, handle, i) {
                dbg!(format!("set params failed at param index: {}", i));
                return Err(err);
            }

            i += 1;
        }
        Ok(0)
    }

    /// Binds the value of a parameter in a query.
    /// The parameter is identified by its index, starting at 1.
    ///
    /// # Examples
    /// See example for [execute](crate::Statement::execute()).
    pub fn bind(&self, value: &dyn ToSql, idx: i16) -> Result<i32, i32> {
        let handle = self.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        if (*handle).is_null() {
            return Err(-26005); // Handle is NULL
        }

        self.bind_param_auxillary(value, *handle, idx)
    }

    /// binds a single parameter
    fn bind_param_auxillary(
        &self,
        value: &dyn ToSql,
        handle: ffi::MimerStatement,
        idx: i16,
    ) -> Result<i32, i32> {
        let mut rc: i32;

        match value.to_sql() {
            MimerDatatype::Null => unsafe {
                rc = ffi::MimerSetNull(handle, idx);
            },
            MimerDatatype::BigInt(value) => unsafe {
                rc = ffi::MimerSetInt64(handle, idx, value);
            },
            MimerDatatype::Int(value) => unsafe {
                rc = ffi::MimerSetInt32(handle, idx, value);
            },
            MimerDatatype::Bool(value) => unsafe {
                rc = ffi::MimerSetBoolean(handle, idx, value.into());
            },
            MimerDatatype::Double(value) => unsafe {
                rc = ffi::MimerSetDouble(handle, idx, value);
            },
            MimerDatatype::Real(value) => unsafe {
                let t = ffi::MimerParameterType(handle, idx);

                if t < 0 {
                    return Err(t);
                }
                match t as u32 {
                    ffi::MIMER_GIS_LATITUDE | ffi::MIMER_GIS_LONGITUDE => {
                        let arr = value.to_le_bytes();
                        let ptr = arr.as_ptr() as *const std::ffi::c_void;
                        rc = ffi::MimerSetBinary(handle, idx, ptr, 4);
                    }
                    _ => rc = ffi::MimerSetFloat(handle, idx, value),
                }
            },
            MimerDatatype::StringRef(value) => unsafe {
                let t = ffi::MimerParameterType(handle, idx);

                if t < 0 {
                    return Err(t);
                }
                match t as u32 {
                    match_mimer_CLOB!() => {
                        let size = value.bytes().len();
                        let length = value.chars().count();
                        let ptr = value.as_ptr() as *const i8;

                        let mut lob_handle: ffi::MimerLob = std::ptr::null_mut();
                        rc = ffi::MimerSetLob(handle, idx, length, &mut lob_handle);
                        if rc < 0 {
                            return Err(rc);
                        }
                        if size > LOB_CHUNK_MAXSIZE_SET {
                            let mut lefttosend: usize = size;
                            let mut pos: usize = 0;
                            let mut stepback: usize = 0;

                            while lefttosend > 0 && stepback < LOB_CHUNK_MAXSIZE_SET {
                                stepback = 0;
                                if lefttosend <= LOB_CHUNK_MAXSIZE_SET {
                                    rc = ffi::MimerSetNclobData8(
                                        &mut lob_handle,
                                        ptr.add(pos),
                                        lefttosend,
                                    );
                                    if rc < 0 {
                                        return Err(rc);
                                    }
                                    lefttosend = 0;
                                } else {
                                    let bytes = value.bytes().collect::<Vec<u8>>();
                                    // Dont split utf-8 characters
                                    while (pos + LOB_CHUNK_MAXSIZE_SET - stepback) > 0 // check that index is valid (not negative)
                                                // check if the first two bits indicate that we are in a continuation byte (0b10xxxxxx), if so, step back one byte
                                                && (bytes[pos + LOB_CHUNK_MAXSIZE_SET - stepback]
                                                    & 0b1100_0000) // BITWISE AND, fetch first two bits of byte 
                                                    == 0b1000_0000
                                    // Check if we are in the middle of a continuation byte (0b10xxxxxx)
                                    {
                                        stepback += 1; // jump back one byte if we are in the middle of a utf-8 character
                                    }
                                    // Stepback should now point to the beginning of a character
                                    rc = ffi::MimerSetNclobData8(
                                        &mut lob_handle,
                                        ptr.add(pos),
                                        LOB_CHUNK_MAXSIZE_SET - stepback,
                                    );
                                    if rc < 0 {
                                        return Err(rc);
                                    }
                                    lefttosend = lefttosend - LOB_CHUNK_MAXSIZE_SET + stepback;
                                    pos += LOB_CHUNK_MAXSIZE_SET - stepback;
                                }
                            }
                        } else {
                            rc = ffi::MimerSetNclobData8(&mut lob_handle, ptr, size);
                            if rc < 0 {
                                return Err(rc);
                            }
                        }
                    }
                    _ => {
                        let value_cstr = CString::new(value);
                        match value_cstr {
                            Ok(v) => {
                                let v_ptr = v.into_raw();
                                rc = ffi::MimerSetString8(handle, idx, v_ptr);

                                // retake pointer to free memory
                                let _ = CString::from_raw(v_ptr);
                            }
                            Err(_) => return Err(-26007), // RUST API ERROR: "Could not convert UTF-8 string to CString"
                        }
                    }
                }
            },

            MimerDatatype::String(value) => unsafe {
                let value_cstr = CString::new(value);
                match value_cstr {
                    Ok(v) => {
                        let v_ptr = v.into_raw();
                        rc = ffi::MimerSetString8(handle, idx, v_ptr);

                        // retake pointer to free memory
                        let _ = CString::from_raw(v_ptr);
                    }
                    Err(_) => return Err(-26007), // RUST API ERROR: "Could not convert UTF-8 string to CString"
                }
            },
            MimerDatatype::BinaryArrayRef(value) => unsafe {
                let t = ffi::MimerParameterType(handle, idx);

                if t < 0 {
                    return Err(t);
                }

                match t as u32 {
                    ffi::MIMER_UUID if value.len() == 16 => {
                        let ptr = value.as_ptr() as *const std::ffi::c_uchar;
                        rc = ffi::MimerSetUUID(handle, idx, ptr);
                    }
                    match_mimer_BINARY!() => {
                        let ptr = value.as_ptr() as *const std::ffi::c_void;
                        rc = ffi::MimerSetBinary(handle, idx, ptr, value.len());
                    }
                    match_mimer_BLOB!() => {
                        let ptr = value.as_ptr() as *const std::ffi::c_void;

                        let mut lob_handle: ffi::MimerLob = std::ptr::null_mut();
                        let size = value.len();
                        rc = ffi::MimerSetLob(handle, idx, size, &mut lob_handle);
                        match rc.cmp(MIMER_SUCCESS) {
                            Ordering::Equal => {
                                if size > LOB_CHUNK_MAXSIZE_SET {
                                    let mut lefttosend = size;

                                    let mut _k = 0;
                                    while lefttosend > 0 {
                                        if lefttosend <= LOB_CHUNK_MAXSIZE_SET {
                                            let rc = ffi::MimerSetBlobData(
                                                &mut lob_handle,
                                                ptr.add(_k * LOB_CHUNK_MAXSIZE_SET),
                                                lefttosend,
                                            );
                                            match rc.cmp(MIMER_SUCCESS) {
                                                Ordering::Equal => (),
                                                _ => return Err(rc),
                                            }
                                            lefttosend = 0;
                                        } else {
                                            let rc = ffi::MimerSetBlobData(
                                                &mut lob_handle,
                                                ptr.add(_k * LOB_CHUNK_MAXSIZE_SET),
                                                LOB_CHUNK_MAXSIZE_SET,
                                            );
                                            match rc.cmp(MIMER_SUCCESS) {
                                                Ordering::Equal => (),
                                                _ => return Err(rc),
                                            }
                                            lefttosend -= LOB_CHUNK_MAXSIZE_SET;
                                            _k += 1;
                                        }
                                    }
                                } else {
                                    rc = ffi::MimerSetBlobData(&mut lob_handle, ptr, size);
                                    match rc.cmp(MIMER_SUCCESS) {
                                        Ordering::Equal => (),
                                        _ => return Err(rc),
                                    }
                                }
                            }
                            _ => return Err(rc),
                        }
                    }
                    _ => rc = -26203, // RUST API ERROR: "Invalid parameter type for MimerDatatype-variant"
                }
            },

            MimerDatatype::BinaryArray(value) => unsafe {
                let ptr = value.as_ptr() as *const std::ffi::c_void;
                rc = ffi::MimerSetBinary(handle, idx, ptr, value.len());
                match rc.cmp(MIMER_SUCCESS) {
                    Ordering::Equal => (),
                    _ => return Err(rc),
                }
            },
        }

        match rc.cmp(MIMER_SUCCESS) {
            Ordering::Less => Err(rc),
            _ => Ok(rc),
        }
    }
    /// Opens a [Cursor](crate::cursor::Cursor) for a statement.
    ///
    /// The [CursorMode] of the cursor (Forward or Scrollable) is specified when the statement is created in [Prepare](crate::Connection::prepare()).
    ///
    ///
    /// # Errors
    /// Returns [Err] when a cursor couldn't be opened.
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
    ///
    /// let stmnt = conn.prepare("SELECT * FROM test_table", CursorMode::Forward).unwrap();
    ///
    /// let mut cursor = stmnt.open_cursor().unwrap();
    /// ```
    pub fn open_cursor(&self) -> Result<Cursor, i32> {
        Cursor::open(self.inner_statement.clone(), self.cursor_mode)
    }

    /// Returns a MimerError given a [Statement] and a return code.
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
    /// let mut conn = Connection::open(db, ident, pass).unwrap();
    ///
    /// # conn.execute_statement("drop table test_table").ok();
    /// # conn.execute_statement("create table test_table (column_1 VARCHAR(30), column_2 INT)").unwrap();
    /// let stmnt = conn.prepare("INSERT INTO test_table VALUES('the number one',1)", CursorMode::Forward).unwrap();
    ///
    /// let err = match stmnt.get_column_name(1) {
    ///     Ok(_) => panic!("Function worked when it shouldn't have!"),
    ///     Err(ec) => stmnt.get_error(ec),
    /// };
    /// println!("{}",err);
    /// ```
    pub fn get_error(&self, error_code: i32) -> MimerError {
        MimerError::new(self, error_code)
    }

    /// Returns the number of parameters in a statement.
    pub fn num_params(&self) -> Result<usize, i32> {
        let _handle = self.get_statement_handle()?;
        Ok(self.num_parameters)
    }

    /// Detects the input/output mode of a parameter.
    pub fn get_parameter_mode(&self, idx: i16) -> Result<ParameterMode, i32> {
        let handle = self.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        self.get_parameter_mode_auxillary(*handle, idx)
    }

    // it is necessary to have this auxillary function, as deadlocks can occur if we try to invoke get_parameter_mode from a different function that also locks inner statement.
    // we need to know the parameter mode in set_params, in order not to set a parameter that is OUT.
    // this function does not lock the inner statement, and is thus safe to use in set_params.
    fn get_parameter_mode_auxillary(
        &self,
        handle: *mut MimerStatement_struct,
        idx: i16,
    ) -> Result<ParameterMode, i32> {
        unsafe {
            let rc = ffi::MimerParameterMode(handle, idx);
            match rc {
                1 => Ok(ParameterMode::IN),
                2 => Ok(ParameterMode::OUT),
                3 => Ok(ParameterMode::INOUT),
                rc => Err(rc),
            }
        }
    }

    /// Should this be public? You would need too look in mimerapi.h or similar to make sense of the return codes.
    fn _get_parameter_type(&self, idx: i16) -> Result<i32, i32> {
        let handle = self.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        if (*handle).is_null() {
            return Err(-26005); // Handle is NULL
        }

        unsafe {
            let rc = ffi::MimerParameterType(*handle, idx);
            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Less => Err(rc),
                _ => Ok(rc),
            }
        }
    }
    /// Returns the name of a parameter in a statement.
    pub fn get_parameter_name(&self, idx: i16) -> Result<String, i32> {
        self.get_name_auxillary(idx, true)
    }

    /// Returns the name of a column in a statement.
    pub fn get_column_name(&self, idx: i16) -> Result<String, i32> {
        self.get_name_auxillary(idx, false)
    }

    /// Helper function for getting parameter and column names.
    fn get_name_auxillary(&self, idx: i16, is_parameter_name: bool) -> Result<String, i32> {
        let null_ptr: *mut i8 = std::ptr::null_mut();
        let handle = self.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement

        unsafe {
            let buffer_size: i32 = match is_parameter_name {
                true => ffi::MimerParameterName8(*handle, idx, null_ptr, 0),
                false => ffi::MimerColumnName8(*handle, idx, null_ptr, 0),
            };
            match buffer_size.cmp(MIMER_SUCCESS) {
                Ordering::Less => return Err(buffer_size),
                _ => (),
            }

            let c_buffer_size: usize = (buffer_size + 1) as usize;

            let c_str = CString::new(vec![1; c_buffer_size]).unwrap();
            let c_str_ptr = c_str.into_raw();

            let rc: i32 = match is_parameter_name {
                true => ffi::MimerParameterName8(*handle, idx, c_str_ptr, c_buffer_size),
                false => ffi::MimerColumnName8(*handle, idx, c_str_ptr, c_buffer_size),
            };

            // retake pointer to free memory
            let retake_cstr = CString::from_raw(c_str_ptr);
            let maybe_string = retake_cstr.into_string();

            if rc < 0 {
                return Err(rc);
            }

            match maybe_string {
                Ok(s) => Ok(s),
                Err(_) => return Err(-26001),
            }
        }
    }

    /// Returns the number of columns in a statement.
    pub fn column_count(&self) -> Result<i32, i32> {
        let handle = self.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        unsafe {
            let rc = ffi::MimerColumnCount(*handle);
            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Less => Err(rc),
                Ordering::Equal => Ok(rc),
                Ordering::Greater => Ok(rc),
            }
        }
    }

    /// Sets the array size when fetching data from a statement.
    /// By default the Mimer API routines MimerFetch and MimerFetchSkip uses an internal fetch buffer equal to the maximum size of one row.
    /// Depending on the actual size of the data, this buffer may hold more than one row. By increasing the array size, more data is retrieved in each server request.
    ///
    /// # Parameters
    ///
    /// - `size`: The number of rows to retrieve in each request.
    ///
    pub fn set_array_size(&self, size: i32) -> Result<i32, i32> {
        let handle = self.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        unsafe {
            let rc = ffi::MimerSetArraySize(*handle, size);
            match rc.cmp(MIMER_SUCCESS) {
                Ordering::Less => Err(rc),
                Ordering::Equal => Ok(rc),
                Ordering::Greater => Ok(rc),
            }
        }
    }

    /// Set parameters to a prepared statement, and add it to the batch of statments to be executed on the next call to [execute](crate::Statement::execute()).
    /// Note that the statement needs to be declared as mut.
    ///
    /// If the statement query contains named parameters, the parameter values are expected to be given in order in the "params" argument to this method.
    /// If the statement query does not contain named parameters, the "params" argument is expected to be empty.
    /// Can not be used with statements which return result sets, e.g. "SELECT" statements.
    ///
    /// # Errors
    /// Returns [Err] when the parameters could not be set or if the statement handle was invalid.
    ///
    /// # Examples
    /// ```
    /// # use mimerrust::*;
    /// # let db = &std::env::var("MIMER_DATABASE").unwrap();
    /// # let ident = "RUSTUSER";
    /// # let pass = "RUSTPASSWORD";
    /// let mut conn = Connection::open(db, ident, pass).unwrap();
    ///
    /// let mut stmnt = conn.prepare("INSERT INTO test_table VALUES(:string,:int)", CursorMode::Forward).unwrap();
    ///
    /// let s1 = String::from("hello");
    /// let i1 = 1;
    /// let s2 = String::from("world");
    /// let i2 = 2;
    ///
    /// stmnt.add_batch(&[&s1,&i1]).unwrap();
    /// stmnt.add_batch(&[&s2,&i2]).unwrap();
    /// stmnt.execute().unwrap();
    /// ```
    pub fn add_batch(&mut self, params: &[&dyn ToSql]) -> Result<i32, i32> {
        let handle = self.get_statement_handle()?.unwrap(); //Ok unwrap since we know the statement is a statement
        if (*handle).is_null() {
            return Err(-26005); // Handle is NULL
        }
        if self.num_parameters != params.len() {
            return Err(-26006); // Number of parameters given is not equal to unset parameters of the prepared statement
        }
        let mut rc = 0;
        if self.batch_bool {
            unsafe {
                rc = ffi::MimerAddBatch(*handle);
            }
        }
        if !params.is_empty() {
            self.set_params(params, *handle)?;
        }

        drop(handle); // drop is necessary to allow for assignment of self.batchBool
        self.batch_bool = true;
        return match rc.cmp(MIMER_SUCCESS) {
            Ordering::Equal => Ok(rc),
            Ordering::Greater => Ok(rc),
            Ordering::Less => Err(rc),
        };
    }
}

#[cfg(test)]
mod statement_tests {
    use core::panic;

    use chrono::NaiveDate;
    use chrono::NaiveDateTime;
    use chrono::NaiveTime;

    use super::*;
    use crate::testing::*;

    #[test]
    fn statement_column_count() {
        let mut conn = establish_connection();

        drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
        let stmt = conn
            .prepare("SELECT * FROM test_table", CursorMode::Forward)
            .unwrap();
        assert_eq!(stmt.column_count().unwrap(), 2);
    }

    #[test]
    fn statement_get_handle() {
        let mut conn = establish_connection();

        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);
        let option = CursorMode::Forward;
        let stmnt = conn.prepare("select * from test_table", option).unwrap();
        assert!(stmnt.get_statement_handle().unwrap().is_some());
        assert!(stmnt.get_session_handle().unwrap().is_none());
        assert!(conn.get_statement_handle().unwrap().is_none());
        assert!(conn.get_session_handle().unwrap().is_some());
    }

    #[test]
    fn check_connection_execute() {
        let stmt;
        {
            let mut conn = establish_connection();

            drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
            stmt = conn
                .prepare(
                    "INSERT INTO test_table (column_1, column_2) VALUES(:string,:int)",
                    CursorMode::Forward,
                )
                .unwrap();
        }
        let s = String::from("Hello");
        let i = 3;
        let params: &[&dyn ToSql] = &[&s, &i];
        match stmt.execute_bind(params) {
            Ok(_) => panic!("Statement executed when it shouldn't have!"),
            Err(ec) => {
                assert_eq!(ec, -26003); // connection has been dropped
                println!("{}", stmt.get_error(ec))
            }
        }
    }

    #[test]
    fn check_connection_open_cursor() {
        let stmt;
        {
            let mut conn = establish_connection();

            drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
            stmt = conn
                .prepare("SELECT * FROM test_table", CursorMode::Forward)
                .unwrap();
        }
        match stmt.open_cursor() {
            Ok(_) => panic!("Cursor opened when it shouldn't have!"),
            Err(ec) => {
                assert_eq!(ec, -26003); // connection has been dropped
                println!("{}", stmt.get_error(ec))
            }
        }
    }

    #[test]
    fn check_connection_end_statement() {
        let _stmt;
        {
            let mut conn = establish_connection();

            drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
            _stmt = conn
                .prepare("SELECT * FROM test_table", CursorMode::Forward)
                .unwrap();
        }
    }

    #[test]
    fn check_connection_column_count() {
        let stmt;
        {
            let mut conn = establish_connection();

            drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
            stmt = conn
                .prepare("SELECT * FROM test_table", CursorMode::Forward)
                .unwrap();
        }
        match stmt.column_count() {
            Ok(_) => panic!("Column count returned when it shouldn't have!"),
            Err(ec) => {
                assert_eq!(ec, -26003); // connection has been dropped
                println!("{}", stmt.get_error(ec))
            }
        }
    }

    #[test]
    fn check_connection_get_column_name() {
        let stmt;
        {
            let mut conn = establish_connection();

            drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
            stmt = conn
                .prepare("SELECT * FROM test_table", CursorMode::Forward)
                .unwrap();
        }
        match stmt.get_column_name(1) {
            Ok(_) => panic!("Column name returned when it shouldn't have!"),
            Err(ec) => {
                assert_eq!(ec, -26003); // connection has been dropped
                println!("{}", stmt.get_error(ec))
            }
        }
    }

    #[test]
    fn check_connection_get_parameter_name() {
        let stmt;
        {
            let mut conn = establish_connection();

            drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
            stmt = conn
                .prepare(
                    "INSERT INTO test_table (column_1, column_2) VALUES(:string,:int)",
                    CursorMode::Forward,
                )
                .unwrap();
        }
        match stmt.get_parameter_name(1) {
            Ok(_) => panic!("Parameter name returned when it shouldn't have!"),
            Err(ec) => {
                assert_eq!(ec, -26003); // connection has been dropped
                println!("{}", stmt.get_error(ec))
            }
        }
    }

    #[test]
    fn check_connection_num_params() {
        let stmt;
        {
            let mut conn = establish_connection();

            drop_create_table(&conn, &EXAMPLE_TABLE, &EXAMPLE_TABLE_COLUMNS);
            stmt = conn
                .prepare(
                    "INSERT INTO test_table (column_1, column_2) VALUES(:string,:int)",
                    CursorMode::Forward,
                )
                .unwrap();
        }
        match stmt.num_params() {
            Ok(_) => panic!("Number of parameters returned when it shouldn't have!"),
            Err(ec) => {
                assert_eq!(ec, -26003); // connection has been dropped
                println!("{}", stmt.get_error(ec))
            }
        }
    }

    #[test]
    fn statement_set_params_not_null() {
        let mut conn = establish_connection();
        drop_create_table(&conn, &BIG_TABLE, &BIG_TABLE_COLUMNS);
        let option = CursorMode::Forward;

        let stmnt = conn.prepare(&format!("INSERT INTO {BIG_TABLE} {BIG_TABLE_COLUMN_NAMES} VALUES(:STR1,:STR2,:INT,:BIGINT,:BOOL,:DOUBLE,:REAL,:BINARY,:UUID)"), option).unwrap();

        let v1 = String::from("Hello");
        let v2 = String::from("World ÖÖÖÄÅÖÄÖÅÄÖ");
        let v3: i32 = 1;
        let v4: i64 = 2147483647 + 1; // i32 range plus one
        let v5 = true;
        let v6: f64 = 0.12345678901234567; // maximum precision for f64 (17 decimal digits)
        let v7: f32 = 0.9;
        let v8: Vec<u8> = vec![b't', b'e', b's', b't']; // column_8 is BINARY(4) in BIG_TABLE
        let v9: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

        let params: &[&dyn ToSql] = &[&v1, &v2, &v3, &v4, &v5, &v6, &v7, &v8, &v9];
        let handle = stmnt.get_statement_handle().unwrap().unwrap();
        match stmnt.set_params(params, *handle) {
            Ok(_) => (),
            Err(err) => panic!("Failed to set parameters: {err}"),
        }
    }

    #[test]
    fn test_binary() {
        let mut conn = establish_connection();
        drop_create_table(&conn, &BIG_TABLE, &BIG_TABLE_COLUMNS);
        let option = CursorMode::Forward;

        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {BIG_TABLE} (column_8) VALUES(?)"),
                option,
            )
            .unwrap();
        let v1: Vec<u8> = vec![b'a', 1, b'c', b'd'];
        let v2: [u8; 4] = [b'a', 2, 3, 4];
        let v3: [u8; 4] = v2;

        // Check that Vec<u8>m [u8] and &[u8] are all accepted.
        stmnt.execute_bind(&[&v1]).unwrap();
        stmnt.execute_bind(&[&v2]).unwrap();
        stmnt.execute_bind(&[&v3]).unwrap();

        let option_none: Option<i32> = None;
        // Check that we can set the parameter to NULL
        stmnt.execute_bind(&[&option_none]).unwrap();

        // check that we can set shorter binary data
        let short_vec: Vec<u8> = vec![1, 2];
        stmnt.execute_bind(&[&short_vec]).unwrap();

        // check for truncation errors if the binary data is too long
        let too_long_vec: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        match stmnt.execute_bind(&[&too_long_vec]) {
            Ok(_) => panic!("Should have failed to insert too long binary data"),
            Err(err) => assert_eq!(err, -24003), // MIMER TRUNCATION ERROR
        }

        // Test that we can insert an UUID
        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {BIG_TABLE} (column_UUID) VALUES(:UUID)"),
                option,
            )
            .unwrap();
        let uuid = [
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
        ]
        .map(|x| x as u8);
        stmnt.execute_bind(&[&uuid]).unwrap();
    }

    #[test]
    fn statement_get_error() {
        let mut conn = establish_connection();
        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let stmnt = conn
            .prepare(
                &format!(
                    "INSERT INTO {} {} {}",
                    EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMN_NAMES, EXAMPLE_TABLE_EXAMPLE_VALUES
                ),
                CursorMode::Forward,
            )
            .unwrap();
        let err = match stmnt.get_column_name(1) {
            Ok(_) => panic!("Method returned Ok when it should have failed!"),
            Err(ec) => stmnt.get_error(ec),
        };

        assert_eq!(err.get_error_code(), -24102); // Mimer SQL Error: Object does not exist.
    }

    #[test]
    fn execute_option_statement() {
        let mut conn = establish_connection();

        drop_create_table(&conn, NULLABLE_TABLE, NULLABLE_TABLE_COLUMNS);

        let int = Some(1);
        let string1: Option<String> = None;
        let string2 = String::from("test");
        let params: &[&dyn ToSql] = &[&int, &string1, &string2];

        let option = CursorMode::Forward;
        let stmnt = conn.prepare(&format!("INSERT INTO {NULLABLE_TABLE} {NULLABLE_TABLE_COLUMN_NAMES} VALUES(:INT,?,:STRING2)"), option).unwrap();

        match stmnt.execute_bind(params) {
            Ok(_) => (),
            Err(err) => panic!("Failed to set execute statement: {err}"),
        }
    }

    #[test]
    fn test_parameter_names() {
        let mut conn = establish_connection();

        drop_create_table(&conn, NULLABLE_TABLE, NULLABLE_TABLE_COLUMNS);

        let int = Some(1);
        let hello = String::from("Hello");
        let string1 = Some(hello);
        let string2 = String::from("world");

        let params: &[&dyn ToSql] = &[&int, &string1, &string2];

        let option = CursorMode::Forward;
        let stmnt = conn.prepare(&format!("INSERT INTO {NULLABLE_TABLE} {NULLABLE_TABLE_COLUMN_NAMES} VALUES(:NAME1,:NAME2,:NAME3)"), option).unwrap();
        assert_eq!("NAME1", stmnt.get_parameter_name(1).unwrap());
        assert_eq!("NAME2", stmnt.get_parameter_name(2).unwrap());
        assert_eq!("NAME3", stmnt.get_parameter_name(3).unwrap());

        match stmnt.execute_bind(params) {
            Ok(_) => (),
            Err(err) => panic!("Failed to execute statement: {err}"),
        }
    }

    #[test]
    fn test_column_names() {
        let mut conn = establish_connection();

        drop_create_table(&conn, BIG_TABLE, BIG_TABLE_COLUMNS);

        let option = CursorMode::Forward;
        let stmnt = conn
            .prepare(&format!("SELECT * FROM {BIG_TABLE}"), option)
            .unwrap();

        let mut i = 1;
        for name in BIG_TABLE_COLUMN_NAMES[1..BIG_TABLE_COLUMN_NAMES.len() - 1].split(",") {
            assert_eq!(name.trim(), stmnt.get_column_name(i).unwrap());
            i += 1;
        }
    }

    #[test]
    fn test_execute_blob() {
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
    }

    #[test]
    fn test_too_big_blob() {
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

        let blob = vec![1; 1025];
        let params: &[&dyn ToSql] = &[&blob];
        match stmnt.execute_bind(params) {
            Ok(_) => panic!("Should have failed to insert too big blob"),
            Err(err) => assert_eq!(err, -24003), // MIMER TRUNCATION ERROR
        }
    }

    #[test]
    fn test_gigablob() {
        let mut conn = establish_connection();

        drop_create_table(&conn, BLOB_TABLE_GIGA, BLOB_TABLE_GIGA_COLUMNS);

        let option = CursorMode::Forward;
        let stmnt = conn
            .prepare(
                &format!(
                    "INSERT INTO {BLOB_TABLE_GIGA} {BLOB_TABLE_GIGA_COLUMN_NAMES} VALUES(:BLOB)"
                ),
                option,
            )
            .unwrap();

        const GIGABYTE: usize = 1_000_000_000;
        let blob = vec![1; GIGABYTE];
        let params: &[&dyn ToSql] = &[&blob];
        match stmnt.execute_bind(params) {
            Ok(_) => (),
            Err(_) => panic!("Failed to insert gigablob"),
        }
    }

    #[test]
    fn test_small_clob() {
        let mut conn = establish_connection();

        drop_create_table(&conn, CLOB_TABLE, CLOB_TABLE_COLUMNS);

        let option = CursorMode::Forward;
        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {CLOB_TABLE} {CLOB_TABLE_COLUMN_NAMES} VALUES(:CLOB)"),
                option,
            )
            .unwrap();

        let clob = String::from("Hello, this is a clob Ö");
        let params: &[&dyn ToSql] = &[&clob];
        match stmnt.execute_bind(params) {
            Ok(_) => (),
            Err(code) => panic!("Failed to insert clob: {code}"),
        }
    }
    #[test]
    fn test_bigger_clob() {
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
        match stmnt.execute_bind(params) {
            Ok(_) => (),
            Err(rc) => panic!("Failed to insert clob: rc {}", rc),
        }
    }

    #[ignore = "Takes too long"]
    #[test]
    fn test_giga_clob() {
        let mut conn = establish_connection();

        drop_create_table(&conn, CLOB_TABLE_GIGA, CLOB_TABLE_GIGA_COLUMNS);

        let option = CursorMode::Forward;
        let stmnt = conn
            .prepare(
                &format!(
                    "INSERT INTO {CLOB_TABLE_GIGA} {CLOB_TABLE_GIGA_COLUMN_NAMES} VALUES(:CLOB)"
                ),
                option,
            )
            .unwrap();
        
        
        let size_in_gb: usize = 1 * 1024 * 1024 * 1024;
        let s = "a".repeat(size_in_gb);

        let params: &[&dyn ToSql] = &[&s];
        match stmnt.execute_bind(params) {
            Ok(_) => (),
            Err(rc) => panic!("Failed to insert clob: rc {}", rc),
        }
    }

    #[test]
    fn test_cutting_clob() {
        let mut conn = establish_connection();

        drop_create_table(&conn, CLOB_TABLE, CLOB_TABLE_COLUMNS);

        let option = CursorMode::Forward;
        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {CLOB_TABLE} {CLOB_TABLE_COLUMN_NAMES} VALUES(:CLOB)"),
                option,
            )
            .unwrap();

        let mut clob = String::new();

        for _ in 1..(LOB_CHUNK_MAXSIZE_SET) {
            clob.push_str("H");
        }
        clob.push_str("Ö");
        let params: &[&dyn ToSql] = &[&clob];
        match stmnt.execute_bind(params) {
            Ok(_) => (),
            Err(rc) => panic!("Failed to insert clob: rc {}", rc),
        }
    }

    #[test]
    fn test_temporal() {
        let mut conn = establish_connection();

        drop_create_table(&conn, TEMPORAL_TABLE, TEMPORAL_TABLE_COLUMNS);

        let option = CursorMode::Forward;
        let stmnt = conn
            .prepare(
                &format!("INSERT INTO {TEMPORAL_TABLE} {TEMPORAL_TABLE_COLUMN_NAMES} VALUES(:DATE,:TIME,:DATETIME)"),
                option,
            )
            .unwrap();

        let date: NaiveDate = NaiveDate::from_ymd_opt(2024, 6, 17).unwrap();
        let time = NaiveTime::from_hms_opt(12, 34, 56).unwrap();
        let date_time = NaiveDateTime::new(date, time);

        match stmnt.execute_bind(&[&date, &time, &date_time]) {
            Ok(_) => (),
            Err(rc) => panic!("Failed to insert row: rc {}", rc),
        }

        let stmnt = conn
            .prepare(&format!("SELECT * FROM {TEMPORAL_TABLE}",), option)
            .unwrap();
        let mut cursor = stmnt.open_cursor().unwrap();
        let row = cursor.next_row().unwrap().unwrap();

        let fetched_date = row.get::<NaiveDate>(1).unwrap().unwrap();
        assert_eq!(fetched_date, date);
        let fetched_time = row.get::<NaiveTime>(2).unwrap().unwrap();
        assert_eq!(fetched_time, time);
        let fetched_datetime = row.get::<NaiveDateTime>(3).unwrap().unwrap();
        assert_eq!(fetched_datetime, date_time);
    }

    #[test]
    fn test_interval() {
        let mut conn = establish_connection();

        drop_create_table(&conn, INTERVAL_TABLE, INTERVAL_TABLE_COLUMNS);

        let stmnt = conn
            .prepare(
                &format!(
                    "INSERT INTO {INTERVAL_TABLE} {INTERVAL_TABLE_COLUMN_NAMES} 
                VALUES(
                :iYear,:iMonth,:iYearToMonth,
                :iDay,:iHour,:iMinute,:iSecond,
                :iDayToHour,:iDayToMinute,:iDayToSecond,
                :iHourToMinute,:iHourToSecond,
                :iMinuteToSecond
                )"
                ),
                CursorMode::Forward,
            )
            .unwrap();

        let one_year = String::from("1");
        let one_month = String::from("1");
        let one_year_and_two_months = String::from("1-02");
        let two_days = String::from("2");
        let three_hours = String::from("3");
        let four_minutes = String::from("4");
        let five_seconds = String::from("5");
        let two_days_and_three_hours = String::from("02 03");
        let two_days_and_three_hours_and_four_minutes = String::from("02 03:04");
        let two_days_and_three_hours_four_minutes_and_five_seconds = String::from("02 03:04:05");
        let three_hours_and_four_minutes = String::from("03:04");
        let three_hours_four_minutes_and_five_seconds = String::from("03:04:05");
        let four_minutes_and_five_seconds = String::from("04:05");

        let params = [
            &one_year,
            &one_month,
            &one_year_and_two_months,
            &two_days,
            &three_hours,
            &four_minutes,
            &five_seconds,
            &two_days_and_three_hours,
            &two_days_and_three_hours_and_four_minutes,
            &two_days_and_three_hours_four_minutes_and_five_seconds,
            &three_hours_and_four_minutes,
            &three_hours_four_minutes_and_five_seconds,
            &four_minutes_and_five_seconds,
        ];

        match stmnt.execute_bind(
            params
                .into_iter()
                .map(|p| p as &dyn ToSql)
                .collect::<Vec<&dyn ToSql>>()
                .as_slice(),
        ) {
            Err(rc) => {
                dbg!(stmnt.get_error(rc));
                panic!("Failed to execute statement");
            }
            _ => (),
        }

        let stmnt = conn
            .prepare(
                &format!("SELECT * FROM {INTERVAL_TABLE}",),
                CursorMode::Forward,
            )
            .unwrap();

        let mut cursor = stmnt.open_cursor().unwrap();
        let row = cursor.next_row().unwrap().unwrap();

        assert_eq!(row.get::<String>(1).unwrap().unwrap(), one_year);
        assert_eq!(row.get::<String>(2).unwrap().unwrap(), one_month);
        assert_eq!(
            row.get::<String>(3).unwrap().unwrap(),
            one_year_and_two_months
        );
    }

    #[test]
    fn test_batch() {
        let mut conn = establish_connection();

        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let mut stmnt = conn
            .prepare(
                &format!(
                    "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_COLUMN_NAMES} VALUES(:str,:int)"
                ),
                CursorMode::Forward,
            )
            .unwrap();

        let values_to_enter = [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)];
        for v in values_to_enter {
            let params: &[&dyn ToSql] = &[&String::from(v.0), &v.1];
            stmnt.add_batch(params).unwrap();
        }
        stmnt.execute().unwrap();
    }
    #[test]
    fn test_batch_concurrency() {
        // tests executing a batch, and asserts that another statements execute does not interfere.
        let mut conn = establish_connection();

        drop_create_table(&conn, EXAMPLE_TABLE, EXAMPLE_TABLE_COLUMNS);

        let option = CursorMode::Forward;
        let mut stmnt_batch = conn
            .prepare(
                &format!(
                    "INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_COLUMN_NAMES} VALUES(:str,:int)"
                ),
                option,
            )
            .unwrap();

        let stmnt_other = conn.prepare(&format!("INSERT INTO {EXAMPLE_TABLE} {EXAMPLE_TABLE_COLUMN_NAMES} {EXAMPLE_TABLE_EXAMPLE_VALUES}"),option,).unwrap();

        let values_to_enter_in_batch = [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)];
        for v in values_to_enter_in_batch {
            let params: &[&dyn ToSql] = &[&String::from(v.0), &v.1];
            stmnt_batch.add_batch(params).unwrap();
        }
        stmnt_other.execute_bind(&[]).unwrap();
        stmnt_batch.execute().unwrap();
    }

    #[test]
    fn test_get_parameter_mode() {
        let mut conn = establish_connection();

        // Drop existing procedure if it exists
        if let Err(rc) = conn.execute_statement(&format!("DROP PROCEDURE MATHMAGIC")) {
            assert_eq!(rc, -12517);
        } // Object does not exist error

        // Create procedure MATHMAGIC
        conn.execute_statement(PROCEDURE_MATHMAGIC_DEF).unwrap();

        let stmnt: Statement;

        match conn.prepare("CALL MATHMAGIC(:x, :y, :z);", CursorMode::Forward) {
            Ok(s) => stmnt = s,
            Err(rc) => panic!("{}", conn.get_error(rc)),
        }

        assert_eq!(stmnt.get_parameter_mode(1).unwrap(), ParameterMode::IN);
        assert_ne!(stmnt.get_parameter_mode(1).unwrap(), ParameterMode::OUT);
        assert_ne!(stmnt.get_parameter_mode(1).unwrap(), ParameterMode::INOUT);
        assert_eq!(stmnt.get_parameter_mode(2).unwrap(), ParameterMode::OUT);
        assert_ne!(stmnt.get_parameter_mode(2).unwrap(), ParameterMode::IN);
        assert_ne!(stmnt.get_parameter_mode(2).unwrap(), ParameterMode::INOUT);
        assert_eq!(stmnt.get_parameter_mode(3).unwrap(), ParameterMode::INOUT);
        assert_ne!(stmnt.get_parameter_mode(3).unwrap(), ParameterMode::IN);
        assert_ne!(stmnt.get_parameter_mode(3).unwrap(), ParameterMode::OUT);
    }

    // This test covers quite a lot of functionality of this API.
    // This test will run the procedure MATHMAGIC, which takes three parameters, x, y and z.
    // The values of x and z are set to 1 and 3 respectively.
    // The procedure will then do some nonsense math and set x to 1, y to -1 and z to 4.
    // After the procedure has been run, the values of x, y and z are inserted into a table.
    // Lastly, the values inserted into the table are verified to be the correct ones.
    #[test]
    fn test_procedure() {
        let mut conn = establish_connection();

        drop_create_table(&conn, RESULT_TABLE, RESULT_TABLE_COLUMNS);

        // Drop existing procedure if it exists
        if let Err(rc) = conn.execute_statement(&format!("DROP PROCEDURE MATHMAGIC")) {
            assert_eq!(rc, -12517);
        } // Object does not exist error

        // create procedure MATHMAGIC
        conn.execute_statement(PROCEDURE_MATHMAGIC_DEF).unwrap();
        let mut stmnt: Statement;

        // prepare a compound statement
        match conn.prepare(&format!("BEGIN CALL MATHMAGIC(:x, :y, :z); INSERT INTO {RESULT_TABLE} {RESULT_TABLE_COLUMN_NAMES} VALUES (:x, :y, :z); END"), CursorMode::Forward) {
            Ok(s) => stmnt = s,
            Err(rc) => panic!("{}", conn.get_error(rc)),
        }

        // bind the parameters. Note that the out parameter (:y) is not bound.
        stmnt.bind(&1, 1).unwrap();
        stmnt.bind(&3, 3).unwrap();

        // execute the compound statement
        if let Err(rc) = stmnt.execute_bind(&[]) {
            panic!("{}", stmnt.get_error(rc));
        }

        stmnt = conn
            .prepare(
                &format!("SELECT * FROM {RESULT_TABLE}"),
                CursorMode::Forward,
            )
            .unwrap();

        let mut c = stmnt.open_cursor().unwrap();
        let row = c.next_row().unwrap().unwrap();
        let x: i32 = row.get(1).unwrap().unwrap();
        let y: i32 = row.get(2).unwrap().unwrap();
        let z: i32 = row.get(3).unwrap().unwrap();

        assert_eq!(x, 1);
        assert_eq!(y, -1);
        assert_eq!(z, 4);
    }

    // this test is quite poor, as none of the functionality it tests is desireable for this API (apart from the geo:Point).
    #[test]
    fn test_geo() {
        let mut conn = establish_connection();

        drop_create_table(&conn, SPATIAL_TABLE, SPATIAL_TABLE_COLUMNS);

        // test inserting spatial data
        let mut stmnt = conn
            .prepare(
                &format!("INSERT INTO {SPATIAL_TABLE} {SPATIAL_TABLE_COLUMN_NAMES} VALUES(:coord, :lat, :lon, :location)"),
                CursorMode::Forward,
            )
            .unwrap();

        let p = geo::Point::new(1, 2);
        stmnt.bind(&p, 1).unwrap();

        let location: (f32, f32) = (43.2, 32.1);
        stmnt.bind(&location.0, 2).unwrap();
        stmnt.bind(&location.1, 3).unwrap();
        stmnt.bind(&location, 4).unwrap();

        stmnt.execute_bind(&[]).unwrap();

        // test fetching spatial data and assert that data is correct
        stmnt = conn
            .prepare(
                &format!("SELECT * FROM {SPATIAL_TABLE}",),
                CursorMode::Forward,
            )
            .unwrap();

        let mut cursor = stmnt.open_cursor().unwrap();

        let row = cursor.next_row().unwrap().unwrap();
        assert_eq!(p, row.get::<geo::Point<i32>>(1).unwrap().unwrap());

        assert_eq!(row.get::<f32>(2).unwrap().unwrap(), location.0); // check fetched longitude
        assert_eq!(row.get::<f32>(3).unwrap().unwrap(), location.1); // check fetched latitude
        assert_eq!(row.get::<(f32, f32)>(4).unwrap().unwrap(), location); // check fetched location
    }
}
