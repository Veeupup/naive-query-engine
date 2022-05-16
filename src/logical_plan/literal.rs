/*
 * @Author: Veeupup 
 * @Date: 2022-05-16 23:36:56 
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-16 23:45:02
 */

use super::expression::{LogicalExpr, ScalarValue};

pub fn lit<T: Literal>(n: T) -> LogicalExpr {
    n.lit()
}

pub trait Literal {
    fn lit(&self) -> LogicalExpr;
}

impl Literal for String {
    fn lit(&self) -> LogicalExpr {
        LogicalExpr::Literal(ScalarValue::Utf8(Some(self.clone())))
    }
}

impl Literal for &str {
    fn lit(&self) -> LogicalExpr {
        LogicalExpr::Literal(ScalarValue::Utf8(Some((*self).to_owned())))
    }
}

macro_rules! impl_literal {
    ($TYPE: ty, $SCALAR: ident) => {
        impl Literal for $TYPE {
            fn lit(&self) -> LogicalExpr {
                LogicalExpr::Literal(ScalarValue::$SCALAR(Some(self.clone())))
            }
        }
    };
}

impl_literal!(bool, Boolean);
impl_literal!(i64, Int64);
impl_literal!(u64, UInt64);
impl_literal!(f64, Float64);

