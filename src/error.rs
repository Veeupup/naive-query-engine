/*
 * @Author: Veeupup
 * @Date: 2022-05-12 16:45:51
 * @Email: code@tanweime.com
*/

use arrow::error::ArrowError;

pub type Result<T> = std::result::Result<T, VeeError>;

#[derive(Debug)]
pub enum VeeError {
    /// Error return by arrow
    ArrowError(ArrowError),

    OtherError,
}

impl From<ArrowError> for VeeError {
    fn from(e: ArrowError) -> Self {
        VeeError::ArrowError(e)
    }
}
