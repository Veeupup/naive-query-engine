/*
 * @Author: Veeupup
 * @Date: 2022-05-14 17:33:26
 * @Email: code@tanweime.com
*/

use crate::error::ErrorCode;
use crate::error::Result;
use arrow::{record_batch::RecordBatch, util::pretty};

pub fn print_result(result: &[RecordBatch]) -> Result<()> {
    pretty::print_batches(result).map_err(ErrorCode::ArrowError)
}
