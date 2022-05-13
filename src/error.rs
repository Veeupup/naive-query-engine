/*
 * @Author: Veeupup
 * @Date: 2022-05-12 16:45:51
 * @Email: code@tanweime.com
*/
use arrow::error::ArrowError;
use std::io;

pub type Result<T> = std::result::Result<T, ErrorCode>;

#[derive(Debug)]
pub enum ErrorCode {
    /// Error return by arrow
    ArrowError(ArrowError),

    IoError(io::Error),

    NoSuchField,

    LogicalError,

    #[allow(unused)]
    Others,
}

impl From<ArrowError> for ErrorCode {
    fn from(e: ArrowError) -> Self {
        ErrorCode::ArrowError(e)
    }
}

impl From<io::Error> for ErrorCode {
    fn from(e: io::Error) -> Self {
        ErrorCode::IoError(e)
    }
}
