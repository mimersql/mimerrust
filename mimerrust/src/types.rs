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

#[doc(hidden)]
use std::str::FromStr;

pub(crate) const LOB_CHUNK_MAXSIZE_SET: usize = 1048500;

/// Represents Mimer SQL data types.
/// Can be seen as an "intermediary"-datatype between Rust and Mimer SQL.
#[derive(Debug, PartialEq)]
pub enum MimerDatatype<'a> {
    Null,
    BigInt(i64),
    Int(i32),
    Double(f64),
    Real(f32),
    String(String),
    StringRef(&'a str),
    Bool(bool),
    BinaryArray(Vec<u8>),
    BinaryArrayRef(&'a [u8]),
}

/// Defines translation of datatypes from Rust to Mimer SQL.
///
/// The following table shows the datatype mappings from Rust to Mimer SQL implemented in this crate.
/// Note that it is possible for a Rust type to be mapped to multiple Mimer SQL types. What type is used then depends on the column type.
///
/// | Rust type | Mimer SQL type |
/// |---------|---------|
/// | [`Option<T>`] where T: [ToSql]    | *NULL* if [None], otherwise the appropriate conversion for the type T and column|
/// | [i32]     | *INTEGER*, *BIGINT* or *SMALLINT*     |
/// | [i64]     | *INTEGER*, *BIGINT* or *SMALLINT*     |
/// | [String]     | String datatypes[^string_datatypes], *CHARACTER LARGE OBJECT* and *NATIONAL CHARACTER LARGE OBJECT*|
/// | [f32]     | *REAL*, *DOUBLE PRECISION*, BINARY(4)[^f32binary4]|
/// | ([f32],[f32])     | *BINARY(8)*[^f32f32]  |
/// | [f64]     | *REAL* and *DOUBLE PRECISION*|
/// | [bool]     | *BOOLEAN* |
/// | [`Vec<u8>`]/\[u8; N\]     | *BINARY*, *BINARY VARYING*, *BINARY LARGE OBJECT* |
///
/// The ToSql trait is also implemented for a number of types from external crates, among which are [uuid::Uuid] and various types from the [chrono] crate.
///
///
/// | Rust type | Mimer SQL type |
/// |---------|---------|
/// | [uuid::Uuid][^uuid]     |  *BINARY*, *BINARY VARYING*, *BINARY LARGE OBJECT*|
/// | [chrono::NaiveDate]     | *DATE*|
/// | [chrono::NaiveTime]     | *TIME*|
/// | [chrono::NaiveDateTime]     | *TIMESTAMP*|
/// | [`geo::Point<i32>`]      | *BINARY*|
///
/// [^string_datatypes]: String datatypes include *CHARACTER*, *CHARACTER VARYING*, *NATIONAL CHARACTER*, *NATIONAL CHARACTER VARYING*, *DATE*, *TIME*, *TIMESTAMP*, *DECIMAL* and *NUMERIC*.
///
/// [^f32binary4]: Converts into an 4 byte binary sequence if column type is *BUILTIN.GIS_LATITUDE* or *BUILTIN.GIS_LONGITUDE*.
/// Note that values of type *BUILTIN.GIS_LATITUDE* must be within the interval [-90,90], and values of type *BUILTIN.GIS_LONGITUDE* within [-180,180].
///
/// [^f32f32]: Converts into an 8 byte binary sequence, where each f32 makes up 4 bytes. Mainly intended for *BUILTIN.GIS_LOCATION*.
/// The location latitude and longitude must be within the interval [-90,90] and [-180,180] respectively.
///
/// [^uuid]: Converts into a 16 byte binary sequence. Mainly intended for *BUILTIN.UUID*.
///
pub trait ToSql {
    fn to_sql(&self) -> MimerDatatype;
}

/// Defines translation of datatypes from Mimer SQL to Rust.
///
/// Multiple translations are possible for a single Mimer SQL type, depending on the column type.
/// For instance, a *DATE* column can be translated to a [chrono::NaiveDate] or a [String]. An example of this follows below:
/// ```
///  # use mimerrust::*;
/// # let db = &std::env::var("MIMER_DATABASE").unwrap();
/// # let ident = "RUSTUSER";
/// # let pass = "RUSTPASSWORD";
/// use chrono::NaiveDate;
/// let mut conn = Connection::open(db, ident, pass).unwrap();
/// # _ = conn.execute_statement("DROP TABLE date_table");
/// conn.execute_statement("CREATE TABLE date_table (column1 DATE)").unwrap();
///
/// let stmnt = conn.prepare("INSERT INTO date_table (column1) VALUES(:param)", CursorMode::Forward).unwrap();
///
/// let date: NaiveDate = NaiveDate::from_ymd_opt(2024, 7, 09).unwrap();
/// stmnt.execute_bind(&[&date]).unwrap();
///
/// let stmnt = conn.prepare("SELECT * FROM date_table", CursorMode::Forward).unwrap();
/// let mut cursor = stmnt.open_cursor().unwrap();
/// let row = cursor.next_row().unwrap().unwrap();
/// let fetched_date = row.get::<chrono::NaiveDate>(1).unwrap().unwrap();
/// let fetched_string = row.get::<String>(1).unwrap().unwrap();
/// assert_eq!(fetched_string, fetched_date.to_string());
/// ```
///
pub trait FromSql: Sized {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32>;
}

impl<T> ToSql for Option<T>
where
    T: ToSql,
{
    fn to_sql(&self) -> MimerDatatype {
        match self {
            Some(v) => v.to_sql(),
            None => MimerDatatype::Null,
        }
    }
}

impl ToSql for i32 {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::Int(*self)
    }
}
impl FromSql for i32 {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::Int(val) => Ok(val),
            _ => Err(-26200),
        }
    }
}

impl ToSql for i64 {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::BigInt(*self)
    }
}
impl FromSql for i64 {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::BigInt(val) => Ok(val),
            _ => Err(-26200),
        }
    }
}

impl ToSql for &str {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::StringRef(self)
    }
}
impl ToSql for String {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::StringRef(self)
    }
}
impl FromSql for String {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::String(val) => Ok(val.to_string()),
            _ => Err(-26200),
        }
    }
}

impl ToSql for f32 {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::Real(*self)
    }
}
impl FromSql for f32 {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::Real(val) => Ok(val),
            MimerDatatype::BinaryArray(val) => {
                if val.len() != 4 {
                    Err(-26200)
                } else {
                    Ok(f32::from_le_bytes(val[0..4].try_into().unwrap()))
                }
            }
            _ => Err(-26200),
        }
    }
}

impl ToSql for f64 {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::Double(*self)
    }
}
impl FromSql for f64 {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::Double(val) => Ok(val),
            _ => Err(-26200),
        }
    }
}

impl ToSql for bool {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::Bool(*self)
    }
}
impl FromSql for bool {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::Bool(val) => Ok(val),
            _ => Err(-26200),
        }
    }
}

impl ToSql for Vec<u8> {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::BinaryArrayRef(self)
    }
}
impl<const N: usize> ToSql for [u8; N] {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::BinaryArrayRef(self)
    }
}
impl FromSql for Vec<u8> {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::BinaryArray(val) => Ok(val),
            _ => Err(-26200),
        }
    }
}

impl ToSql for uuid::Uuid {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::BinaryArrayRef(self.as_bytes())
    }
}
impl FromSql for uuid::Uuid {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::BinaryArray(val) => {
                let mut bytes: [u8; 16] = [0; 16];
                bytes.copy_from_slice(&val[..16]);
                Ok(uuid::Uuid::from_bytes(bytes))
            }
            _ => Err(-26200),
        }
    }
}

impl ToSql for chrono::NaiveDate {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::String(self.to_string())
    }
}
impl FromSql for chrono::NaiveDate {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::String(str) => match chrono::NaiveDate::from_str(str.as_ref()) {
                Ok(date) => Ok(date),
                Err(_) => Err(-26200),
            },
            _ => Err(-26200),
        }
    }
}

impl ToSql for chrono::NaiveTime {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::String(self.to_string())
    }
}
impl FromSql for chrono::NaiveTime {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::String(str) => match chrono::NaiveTime::from_str(str.as_ref()) {
                Ok(time) => Ok(time),
                Err(_) => Err(-26200),
            },
            _ => Err(-26200),
        }
    }
}

impl ToSql for chrono::NaiveDateTime {
    fn to_sql(&self) -> MimerDatatype {
        MimerDatatype::String(self.to_string())
    }
}
impl FromSql for chrono::NaiveDateTime {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::String(str) => {
                match chrono::NaiveDateTime::parse_from_str(&str, "%Y-%m-%d %H:%M:%S") {
                    Ok(date_time) => Ok(date_time),
                    Err(_) => Err(-26200),
                }
            }
            _ => Err(-26200),
        }
    }
}

impl ToSql for (f32, f32) {
    fn to_sql(&self) -> MimerDatatype {
        let mut bytes: [u8; 8] = [0; 8];
        bytes[..4].copy_from_slice(&self.0.to_le_bytes());
        bytes[4..].copy_from_slice(&self.1.to_le_bytes());
        MimerDatatype::BinaryArray(bytes.to_vec())
    }
}

impl FromSql for (f32, f32) {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::BinaryArray(v) => {
                if v.len() != 8 {
                    return Err(-26200);
                }
                Ok((
                    f32::from_le_bytes(v[0..4].try_into().unwrap()), // first element
                    f32::from_le_bytes(v[4..8].try_into().unwrap()), // second element
                ))
            }
            _ => Err(-26200),
        }
    }
}

impl ToSql for geo::Point<i32> {
    fn to_sql(&self) -> MimerDatatype {
        let mut bytes: [u8; 8] = [0; 8];
        bytes[..4].copy_from_slice(&self.x().to_le_bytes());
        bytes[4..].copy_from_slice(&self.y().to_le_bytes());
        MimerDatatype::BinaryArray(bytes.to_vec())
    }
}
impl FromSql for geo::Point<i32> {
    fn from_sql(value: MimerDatatype) -> Result<Self, i32> {
        match value {
            MimerDatatype::BinaryArray(v) => {
                if v.len() != 8 {
                    return Err(-26200);
                }
                Ok(geo::Point::new(
                    i32::from_le_bytes(v[0..4].try_into().unwrap()),
                    i32::from_le_bytes(v[4..8].try_into().unwrap()),
                ))
            }
            _ => Err(-26200),
        }
    }
}

#[macro_export]
#[doc(hidden)]
macro_rules! match_mimer_temporal {
    () => {
        ffi::MIMER_DATE
            | ffi::MIMER_TIME
            | ffi::MIMER_TIMESTAMP
            | ffi::MIMER_INTERVAL_YEAR
            | ffi::MIMER_INTERVAL_MONTH
            | ffi::MIMER_INTERVAL_DAY
            | ffi::MIMER_INTERVAL_HOUR
            | ffi::MIMER_INTERVAL_MINUTE
            | ffi::MIMER_INTERVAL_SECOND
            | ffi::MIMER_INTERVAL_YEAR_TO_MONTH
            | ffi::MIMER_INTERVAL_DAY_TO_HOUR
            | ffi::MIMER_INTERVAL_DAY_TO_MINUTE
            | ffi::MIMER_INTERVAL_DAY_TO_SECOND
            | ffi::MIMER_INTERVAL_HOUR_TO_MINUTE
            | ffi::MIMER_INTERVAL_HOUR_TO_SECOND
            | ffi::MIMER_INTERVAL_MINUTE_TO_SECOND
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! match_mimer_strings {
    () => {
        ffi::MIMER_CHARACTER_VARYING
            | ffi::MIMER_CHARACTER
            | ffi::MIMER_NCHAR
            | ffi::MIMER_NCHAR_VARYING
            | ffi::MIMER_UTF8
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! match_mimer_small_ints {
    () => {
        ffi::MIMER_T_INTEGER
            | ffi::MIMER_NATIVE_SMALLINT_NULLABLE
            | ffi::MIMER_NATIVE_INTEGER_NULLABLE
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! match_mimer_big_ints {
    () => {
        ffi::MIMER_NATIVE_BIGINT_NULLABLE
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! match_mimer_real {
    () => {
        ffi::MIMER_NATIVE_REAL_NULLABLE
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! match_mimer_doubles {
    () => {
        ffi::MIMER_T_DOUBLE | ffi::MIMER_NATIVE_DOUBLE_NULLABLE
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! match_mimer_booleans {
    () => {
        ffi::MIMER_BOOLEAN
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! match_mimer_BINARY {
    () => {
        // "normal" binary datatypes
        ffi::MIMER_BINARY | ffi::MIMER_BINARY_VARYING
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! match_mimer_spatial {
    () => {
        // spatial data represented as BINARY
        ffi::MIMER_GIS_LOCATION
            | ffi::MIMER_GIS_LATITUDE
            | ffi::MIMER_GIS_LONGITUDE
            | ffi::MIMER_GIS_COORDINATE
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! match_mimer_BLOB {
    () => {
        ffi::MIMER_BLOB | ffi::MIMER_NATIVE_BLOB
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! match_mimer_CLOB {
    () => {
        ffi::MIMER_CLOB | ffi::MIMER_NCLOB | ffi::MIMER_NATIVE_CLOB | ffi::MIMER_NATIVE_NCLOB
    };
}
