/*
 * @Author: Veeupup
 * @Date: 2022-05-12 20:28:35
 * @Email: code@tanweime.com
*/

use std::iter::repeat;

use arrow::array::Float32Array;
use arrow::array::StringArray;
use arrow::array::{
    new_null_array, ArrayRef, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::kernels::aggregate;
use arrow::datatypes::{DataType, Field, Int32Type};
use std::sync::Arc;

use crate::error::ErrorCode;
use crate::error::Result;

use crate::logical_plan::plan::LogicalPlan;

#[derive(Clone, Debug)]
pub enum LogicalExpression {
    /// An expression with a specific name.
    Alias(Box<LogicalExpression>, String),
    /// A named reference to a qualified filed in a schema.
    Column(Column),
    /// A constant value.
    Literal(ScalarValue),
    /// A binary expression such as "age > 21"
    BinaryExpr(BinaryExpr),
    /// Negation of an expression. The expression's type must be a boolean to make sense.
    Not(Box<LogicalExpression>),
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast {
        /// The expression being cast
        expr: Box<LogicalExpression>,
        /// The `DataType` the expression will yield
        data_type: DataType,
    },
    /// Represents the call of a built-in scalar function with a set of arguments.
    ScalarFunction(ScalarFunction),
    /// Represents the call of an aggregate built-in function with arguments.
    AggregateFunction(AggregateFunction),
    // Represents a reference to all fields in a schema.
    // Wildcard,
    // TODO(veeupup): add more expresssions
}

impl LogicalExpression {
    pub fn column(name: String) -> LogicalExpression {
        LogicalExpression::Column(Column(name))
    }

    /// TODO(veeupup): consider return Vec<Field>
    pub fn data_field(&self, input: &LogicalPlan) -> Result<Field> {
        match self {
            LogicalExpression::Alias(expr, alias) => {
                let field = expr.data_field(input)?;
                Ok(Field::new(
                    alias,
                    field.data_type().clone(),
                    field.is_nullable(),
                ))
            }
            LogicalExpression::Column(col) => {
                for field in input.schema().fields() {
                    if field.name() == col.0.as_str() {
                        return Ok(field.clone());
                    }
                }
                Err(ErrorCode::NoSuchField)
            }
            LogicalExpression::Literal(scalar_val) => Ok(scalar_val.data_field()),
            LogicalExpression::BinaryExpr(expr) => expr.data_field(input),
            LogicalExpression::Not(expr) => Ok(Field::new(
                format!("Not {}", expr.data_field(input)?.name()).as_str(),
                DataType::Boolean,
                true,
            )),
            LogicalExpression::Cast { expr, data_type } => Ok(Field::new(
                expr.data_field(input)?.name(),
                data_type.clone(),
                true,
            )),
            LogicalExpression::ScalarFunction(scalar_func) => scalar_func.data_field(input),
            LogicalExpression::AggregateFunction(aggr_func) => aggr_func.data_field(input),
            // LogicalExpression::Wildcard => ,
        }
    }
}

/// A named reference to a qualified field in a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Column(pub String);

#[derive(Debug, Clone)]

pub enum ScalarValue {
    /// represents `DataType::Null` (castable to/from any other type)
    Null,
    /// true or false value
    Boolean(Option<bool>),
    /// 32bit float
    Float32(Option<f32>),
    /// 64bit float
    Float64(Option<f64>),
    /// signed 8bit int
    Int8(Option<i8>),
    /// signed 16bit int
    Int16(Option<i16>),
    /// signed 32bit int
    Int32(Option<i32>),
    /// signed 64bit int
    Int64(Option<i64>),
    /// unsigned 8bit int
    UInt8(Option<u8>),
    /// unsigned 16bit int
    UInt16(Option<u16>),
    /// unsigned 32bit int
    UInt32(Option<u32>),
    /// unsigned 64bit int
    UInt64(Option<u64>),
    /// utf-8 encoded string.
    Utf8(Option<String>),
}

macro_rules! build_array_from_option {
    ($DATA_TYPE:ident, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => Arc::new($ARRAY_TYPE::from_value(value, $SIZE)),
            None => new_null_array(&DataType::$DATA_TYPE, $SIZE),
        }
    }};
}

impl ScalarValue {
    pub fn data_field(&self) -> Field {
        match self {
            ScalarValue::Null => Field::new("Null", DataType::Null, true),
            ScalarValue::Boolean(_) => Field::new("bool", DataType::Boolean, true),
            ScalarValue::Float32(_) => Field::new("f32", DataType::Float32, true),
            ScalarValue::Float64(_) => Field::new("f64", DataType::Float64, true),
            ScalarValue::Int8(_) => Field::new("i8", DataType::Int8, true),
            ScalarValue::Int16(_) => Field::new("i16", DataType::Int16, true),
            ScalarValue::Int32(_) => Field::new("i32", DataType::Int32, true),
            ScalarValue::Int64(_) => Field::new("i64", DataType::Int64, true),
            ScalarValue::UInt8(_) => Field::new("u8", DataType::UInt8, true),
            ScalarValue::UInt16(_) => Field::new("u16", DataType::UInt16, true),
            ScalarValue::UInt32(_) => Field::new("u32", DataType::UInt32, true),
            ScalarValue::UInt64(_) => Field::new("u64", DataType::UInt64, true),
            ScalarValue::Utf8(_) => Field::new("string", DataType::Utf8, true),
        }
    }

    pub fn into_array(self, size: usize) -> ArrayRef {
        match self {
            ScalarValue::Null => new_null_array(&DataType::Null, size),
            ScalarValue::Boolean(e) => Arc::new(BooleanArray::from(vec![e; size])) as ArrayRef,
            ScalarValue::Float32(e) => build_array_from_option!(Float32, Float32Array, e, size),
            ScalarValue::Float64(e) => build_array_from_option!(Float64, Float64Array, e, size),
            ScalarValue::Int8(e) => build_array_from_option!(Int8, Int8Array, e, size),
            ScalarValue::Int16(e) => build_array_from_option!(Int16, Int16Array, e, size),
            ScalarValue::Int32(e) => build_array_from_option!(Int32, Int32Array, e, size),
            ScalarValue::Int64(e) => build_array_from_option!(Int64, Int64Array, e, size),
            ScalarValue::UInt8(e) => build_array_from_option!(UInt8, UInt8Array, e, size),
            ScalarValue::UInt16(e) => build_array_from_option!(UInt16, UInt16Array, e, size),
            ScalarValue::UInt32(e) => build_array_from_option!(UInt32, UInt32Array, e, size),
            ScalarValue::UInt64(e) => build_array_from_option!(UInt64, UInt64Array, e, size),
            ScalarValue::Utf8(e) => match e {
                Some(value) => Arc::new(StringArray::from_iter_values(repeat(value).take(size))),
                None => new_null_array(&DataType::Utf8, size),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    /// Left-hand side of the expression
    pub left: Box<LogicalExpression>,
    /// The comparison operator
    pub op: Operator,
    /// Right-hand side of the expression
    pub right: Box<LogicalExpression>,
}

impl BinaryExpr {
    pub fn data_field(&self, input: &LogicalPlan) -> Result<Field> {
        let left = self.left.data_field(input)?;
        let left = left.name();
        let right = self.right.data_field(input)?;
        let right = right.name();
        let field = match self.op {
            Operator::Eq => Field::new(
                format!("{} = {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::NotEq => Field::new(
                format!("{} != {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::Lt => Field::new(
                format!("{} < {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::LtEq => Field::new(
                format!("{} <= {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::Gt => Field::new(
                format!("{} > {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::GtEq => Field::new(
                format!("{} >= {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::Plus => Field::new(
                format!("{} + {}", left, right).as_str(),
                self.left.data_field(input)?.data_type().clone(),
                true,
            ),
            Operator::Minus => Field::new(
                format!("{} - {}", left, right).as_str(),
                self.left.data_field(input)?.data_type().clone(),
                true,
            ),
            Operator::Multiply => Field::new(
                format!("{} * {}", left, right).as_str(),
                self.left.data_field(input)?.data_type().clone(),
                true,
            ),
            Operator::Divide => Field::new(
                format!("{} / {}", left, right).as_str(),
                self.left.data_field(input)?.data_type().clone(),
                true,
            ),
            Operator::Modulo => Field::new(
                format!("{} % {}", left, right).as_str(),
                self.left.data_field(input)?.data_type().clone(),
                true,
            ),
            Operator::And => Field::new(
                format!("{} and {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
            Operator::Or => Field::new(
                format!("{} or {}", left, right).as_str(),
                DataType::Boolean,
                true,
            ),
        };
        Ok(field)
    }
}

#[derive(Debug, Clone)]
pub enum Operator {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is smaller than right side
    Lt,
    /// Left side is smaller or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater or equal to right side
    GtEq,
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication operator, like `*`
    Multiply,
    /// Division operator, like `/`
    Divide,
    /// Remainder operator, like `%`
    Modulo,
    /// Logical AND, like `&&`
    And,
    /// Logical OR, like `||`
    Or,
}

#[derive(Debug, Clone)]
pub struct ScalarFunction {
    /// The function
    pub fun: ScalarFunc,
    /// List of expressions to feed to the functions as arguments
    /// TODO(veeupup): we should check the args' type and nums
    pub args: Vec<LogicalExpression>,
}

impl ScalarFunction {
    pub fn data_field(&self, input: &LogicalPlan) -> Result<Field> {
        // TODO(veeupup): we should make scalar func more specific and should check if valid before creating them
        let field = self.args[0].data_field(input)?;
        let field = match self.fun {
            ScalarFunc::Abs => Field::new(
                format!("abs({})", field.name()).as_str(),
                DataType::Int64,
                true,
            ),
            ScalarFunc::Add => Field::new(
                format!("add({})", field.name()).as_str(),
                DataType::Int64,
                true,
            ),
            ScalarFunc::Sub => Field::new(
                format!("sub({})", field.name()).as_str(),
                DataType::Int64,
                true,
            ),
        };
        Ok(field)
    }
}

#[derive(Debug, Clone)]
pub enum ScalarFunc {
    // math functions
    Abs,
    Add,
    Sub,
}

#[derive(Debug, Clone)]
pub struct AggregateFunction {
    /// Name of the function
    pub fun: AggregateFunc,
    /// List of expressions to feed to the functions as arguments
    pub args: Arc<LogicalExpression>,
}

impl AggregateFunction {
    pub fn data_field(&self, input: &LogicalPlan) -> Result<Field> {
        let dt = self.args.data_field(input)?;
        let field = match self.fun {
            AggregateFunc::Count => Field::new(
                format!("count({})", dt.name()).as_str(),
                dt.data_type().clone(),
                true,
            ),
            AggregateFunc::Sum => Field::new(
                format!("sum({})", dt.name()).as_str(),
                dt.data_type().clone(),
                true,
            ),
            AggregateFunc::Min => Field::new(
                format!("min({})", dt.name()).as_str(),
                dt.data_type().clone(),
                true,
            ),
            AggregateFunc::Max => Field::new(
                format!("max({})", dt.name()).as_str(),
                dt.data_type().clone(),
                true,
            ),
            AggregateFunc::Avg => Field::new(
                format!("avg({})", dt.name()).as_str(),
                dt.data_type().clone(),
                true,
            ),
        };
        Ok(field)
    }
}

#[derive(Debug, Clone)]
pub enum AggregateFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}
