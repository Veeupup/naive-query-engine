/*
 * @Author: Veeupup
 * @Date: 2022-05-13 15:10:51
 * @Email: code@tanweime.com
*/

use arrow::{
    array::{Array, ArrayRef},
    datatypes::DataType,
};

use crate::logical_plan::expression::ScalarValue;

#[derive(Debug, Clone)]
pub enum ColumnValue {
    /// Array of values
    Array(ArrayRef),
    /// A single value,
    Const(ScalarValue, usize),
}

impl ColumnValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnValue::Array(array) => array.data_type().clone(),
            ColumnValue::Const(scalar, _) => scalar.data_field().data_type().clone(),
        }
    }

    pub fn into_array(self) -> ArrayRef {
        match self {
            ColumnValue::Array(array) => array,
            ColumnValue::Const(scalar, num_rows) => scalar.into_array(num_rows),
        }
    }
}
