/*
 * @Author: Veeupup
 * @Date: 2022-05-12 20:28:35
 * @Email: code@tanweime.com
*/

use arrow::datatypes::{DataType, Field, Int32Type};

#[derive(Clone, Debug)]
pub enum LogicalExpression {
    /// An expression with a specific name.
    Alias(Box<LogicalExpression>, String),
    /// A named reference to a qualified filed in a schema.
    Column(Column),
    /// A constant value.
    Literal(ScalarValue),
    /// A binary expression such as "age > 21"
    BinaryExpr {
        /// Left-hand side of the expression
        left: Box<LogicalExpression>,
        /// The comparison operator
        op: Operator,
        /// Right-hand side of the expression
        right: Box<LogicalExpression>,
    },
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
    ScalarFunction {
        /// The function
        fun: ScalarFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<LogicalExpression>,
    },
    /// Represents the call of an aggregate built-in function with arguments.
    AggregateFunction {
        /// Name of the function
        fun: AggregateFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<LogicalExpression>,
    },
    /// Represents a reference to all fields in a schema.
    Wildcard,
    // TODO(veeupup): add more expresssions
}

impl LogicalExpression {
    pub fn data_field(&self) -> Field {
        // todo!()
        Field::new("a", DataType::Int32, false)
    }
}

/// A named reference to a qualified field in a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Column {
    /// field/column name.
    pub name: String,
}

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
    /// 128bit decimal, using the i128 to represent the decimal
    Decimal128(Option<i128>, usize, usize),
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
pub enum ScalarFunction {
    // math functions
    Abs,
    Add,
    Sub,
}

#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}
