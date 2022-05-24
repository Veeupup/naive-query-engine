/*
 * @Author: Veeupup
 * @Date: 2022-05-13 19:36:47
 * @Email: code@tanweime.com
 *
 * SqlPlanner creates a logical plan from a parsed SQL statement.
 *
*/

use std::collections::HashSet;

use arrow::datatypes::DataType;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, Join, JoinConstraint, JoinOperator, OrderByExpr, SetExpr,
    Statement, TableWithJoins, UnaryOperator,
};
use sqlparser::ast::{DataType as SQLDataType, Offset};
use sqlparser::ast::{Ident, ObjectName, SelectItem, TableFactor, Value};

use crate::error::ErrorCode;
use crate::logical_plan;
use crate::logical_plan::expression::{
    BinaryExpr, CastExpr, Column, LogicalExpr, Operator, ScalarValue, UnaryExpr,
};
use crate::logical_plan::literal::lit;
use crate::logical_plan::plan::{JoinType, TableScan};

use crate::logical_plan::schema::NaiveSchema;
use crate::{
    catalog::Catalog,
    error::Result,
    logical_plan::{plan::LogicalPlan, DataFrame},
};

/// SQLPlanner: convert statement to logical plan
pub struct SQLPlanner<'a> {
    catalog: &'a Catalog,
}

impl<'a> SQLPlanner<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    pub fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::Query(query) => {
                let plan = self.set_expr_to_plan(query.body)?;
                let plan = self.order_by(plan, query.order_by)?;
                // We should take offset before limit.
                let plan = self.offset(plan, query.offset)?;
                self.limit(plan, query.limit)
            }
            _ => unimplemented!(),
        }
    }

    fn set_expr_to_plan(&self, set_expr: SetExpr) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(select) => {
                let plans = self.plan_from_tables(select.from)?;

                let plan = self.plan_selection(select.selection, plans)?;

                // TODO(veeupup): aggregate expr
                let select_exprs = self.prepare_select_exprs(&plan, &select.projection)?;
                // filter aggregate expr, these exps should not pass to projection
                let aggr_exprs_haystack = select_exprs;
                let (aggr_exprs, project_exprs) = self.find_agrr_exprs(&aggr_exprs_haystack);
                let plan = if aggr_exprs.is_empty() {
                    plan
                } else {
                    self.plan_from_aggregate(plan, aggr_exprs, select.group_by)?
                };
                // TODO(veeupup): group by

                // process the SELECT expressions, with wildcards expanded
                let plan = self.plan_from_projection(plan, project_exprs)?;

                Ok(plan)
            }
            _ => todo!(),
        }
    }

    fn plan_from_aggregate(
        &self,
        plan: LogicalPlan,
        aggr_exprs: Vec<LogicalExpr>,
        group_by: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let mut group_by_exprs = vec![];
        for expr in &group_by {
            group_by_exprs.push(self.sql_to_expr(expr)?);
        }

        let mut aggr_func = vec![];
        for aggr_expr in &aggr_exprs {
            if let LogicalExpr::AggregateFunction(aggr) = aggr_expr {
                aggr_func.push(aggr.clone());
            }
        }

        let df = DataFrame::new(plan);
        Ok(df.aggregate(group_by_exprs, aggr_func).logical_plan())
    }

    // return tuple
    // tuple[0] means aggr exprs
    // tuple[1] means normal projection
    fn find_agrr_exprs(&self, exprs: &[LogicalExpr]) -> (Vec<LogicalExpr>, Vec<LogicalExpr>) {
        let mut aggr_exprs = vec![];
        let mut project_exprs = vec![];
        for expr in exprs {
            match expr {
                LogicalExpr::AggregateFunction(_) => aggr_exprs.push(expr.clone()),
                _ => project_exprs.push(expr.clone()),
            }
        }
        (aggr_exprs, project_exprs)
    }

    fn prepare_select_exprs(
        &self,
        plan: &LogicalPlan,
        projection: &[SelectItem],
    ) -> Result<Vec<LogicalExpr>> {
        let input_schema = plan.schema();

        Ok(projection
            .iter()
            .map(|expr| self.select_item_to_expr(expr))
            .collect::<Result<Vec<LogicalExpr>>>()?
            .iter()
            .flat_map(|expr| Self::expand_wildcard(expr, input_schema))
            .collect::<Vec<LogicalExpr>>())
    }

    /// Generate a relational expression from a select SQL expression
    fn select_item_to_expr(&self, sql: &SelectItem) -> Result<LogicalExpr> {
        match sql {
            SelectItem::UnnamedExpr(expr) => self.sql_to_expr(expr),
            SelectItem::Wildcard => Ok(LogicalExpr::Wildcard),
            _ => unimplemented!(),
        }
    }

    fn expand_wildcard(expr: &LogicalExpr, schema: &NaiveSchema) -> Vec<LogicalExpr> {
        match expr {
            LogicalExpr::Wildcard => schema
                .fields()
                .iter()
                .map(|f| LogicalExpr::column(None, f.name().to_string()))
                .collect::<Vec<LogicalExpr>>(),
            _ => vec![expr.clone()],
        }
    }

    fn order_by(&self, plan: LogicalPlan, _order_by: Vec<OrderByExpr>) -> Result<LogicalPlan> {
        // TODO(veeupup): order by
        Ok(plan)
    }

    fn limit(&self, plan: LogicalPlan, limit: Option<Expr>) -> Result<LogicalPlan> {
        match limit {
            Some(limit_expr) => {
                let n = match self.sql_to_expr(&limit_expr)? {
                    LogicalExpr::Literal(ScalarValue::Int64(Some(n))) => Ok(n as usize),
                    _ => Err(ErrorCode::PlanError(
                        "Unexpected expression for LIMIT clause".to_string(),
                    )),
                }?;
                Ok(DataFrame { plan }.limit(n).logical_plan())
            }
            None => Ok(plan),
        }
    }

    fn offset(&self, plan: LogicalPlan, offset: Option<Offset>) -> Result<LogicalPlan> {
        match offset {
            Some(offset) => {
                let n = match self.sql_to_expr(&offset.value)? {
                    LogicalExpr::Literal(ScalarValue::Int64(Some(n))) => Ok(n as usize),
                    _ => Err(ErrorCode::PlanError(
                        "Unexpected expression for Offset clause".to_string(),
                    )),
                }?;
                Ok(DataFrame { plan }.offset(n).logical_plan())
            }
            None => Ok(plan),
        }
    }

    fn plan_from_tables(&self, from: Vec<TableWithJoins>) -> Result<Vec<LogicalPlan>> {
        match from.len() {
            0 => todo!("support select with no from"),
            _ => from
                .iter()
                .map(|t| self.plan_table_with_joins(t))
                .collect::<Result<Vec<_>>>(),
        }
    }

    fn plan_table_with_joins(&self, t: &TableWithJoins) -> Result<LogicalPlan> {
        let left = self.parse_table(&t.relation)?;
        match t.joins.len() {
            0 => Ok(left),
            n => {
                let mut left = self.parse_table_join(left, &t.joins[0])?;
                for i in 1..n {
                    left = self.parse_table_join(left, &t.joins[i])?;
                }
                Ok(left)
            }
        }
    }

    fn parse_table_join(&self, left: LogicalPlan, join: &Join) -> Result<LogicalPlan> {
        let right = self.parse_table(&join.relation)?;
        match &join.join_operator {
            JoinOperator::LeftOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Left)
            }
            JoinOperator::RightOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Right)
            }
            JoinOperator::Inner(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Inner)
            }
            JoinOperator::CrossJoin => {
                self.parse_join(left, right, &JoinConstraint::None, JoinType::Cross)
            }
            // TODO(veeupup): cross join
            _other => Err(ErrorCode::NotImplemented),
        }
    }

    fn parse_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        constraint: &JoinConstraint,
        join_type: JoinType,
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(sql_expr) => {
                let mut keys: Vec<(Column, Column)> = vec![];
                let expr = self.sql_to_expr(sql_expr)?;

                let mut filters = vec![];
                extract_join_keys(&expr, &mut keys, &mut filters);

                let left_keys = keys.iter().map(|pair| pair.0.clone()).collect();
                let right_keys = keys.iter().map(|pair| pair.1.clone()).collect();

                if filters.is_empty() {
                    let join =
                        DataFrame::new(left).join(&right, join_type, (left_keys, right_keys))?;
                    Ok(join.logical_plan())
                } else if join_type == JoinType::Inner {
                    let join =
                        DataFrame::new(left).join(&right, join_type, (left_keys, right_keys))?;
                    let join = join.filter(
                        filters
                            .iter()
                            .skip(1)
                            .fold(filters[0].clone(), |acc, e| acc.and(e.clone())),
                    );
                    Ok(join.logical_plan())
                } else {
                    Err(ErrorCode::NotImplemented)
                }
            }
            JoinConstraint::None => {
                let join = DataFrame::new(left).join(&right, join_type, (vec![], vec![]))?;
                Ok(join.logical_plan())
            }
            _ => Err(ErrorCode::NotImplemented),
        }
    }

    fn parse_table(&self, relation: &TableFactor) -> Result<LogicalPlan> {
        match &relation {
            TableFactor::Table { name, .. } => {
                let table_name = Self::normalize_sql_object_name(name);
                let source = self.catalog.get_table(&table_name)?;
                Ok(LogicalPlan::TableScan(TableScan {
                    source,
                    projection: None,
                }))
            }
            _ => unimplemented!(),
        }
    }

    fn plan_from_projection(
        &self,
        plan: LogicalPlan,
        projection: Vec<LogicalExpr>,
    ) -> Result<LogicalPlan> {
        let df = DataFrame::new(plan);
        Ok(df.project(projection)?.logical_plan())
    }

    fn plan_selection(
        &self,
        selection: Option<Expr>,
        plans: Vec<LogicalPlan>,
    ) -> Result<LogicalPlan> {
        // TODO(veeupup): handle joins
        match selection {
            Some(expr) => {
                let mut fields = vec![];
                for plan in &plans {
                    fields.extend_from_slice(plan.schema().fields());
                }
                let filter_expr = self.sql_to_expr(&expr)?;

                // look for expressions of the form `<column> = <column>`
                let mut possible_join_keys = vec![];
                extract_possible_join_keys(&filter_expr, &mut possible_join_keys)?;

                let mut all_join_keys = HashSet::new();
                let mut left = plans[0].clone();
                for right in plans.iter().skip(1) {
                    let left_schema = left.schema();
                    let right_schema = right.schema();
                    let mut join_keys = vec![];
                    for (l, r) in &possible_join_keys {
                        if left_schema
                            .field_with_unqualified_name(l.name.as_str())
                            .is_ok()
                            && right_schema
                                .field_with_unqualified_name(r.name.as_str())
                                .is_ok()
                        {
                            join_keys.push((l.clone(), r.clone()));
                        } else if left_schema
                            .field_with_unqualified_name(r.name.as_str())
                            .is_ok()
                            && right_schema
                                .field_with_unqualified_name(l.name.as_str())
                                .is_ok()
                        {
                            join_keys.push((r.clone(), l.clone()));
                        }
                    }
                    if !join_keys.is_empty() {
                        let left_keys: Vec<Column> =
                            join_keys.iter().map(|(l, _)| l.clone()).collect();
                        let right_keys: Vec<Column> =
                            join_keys.iter().map(|(_, r)| r.clone()).collect();
                        let df = DataFrame::new(left);
                        left = df
                            .join(right, JoinType::Inner, (left_keys, right_keys))?
                            .logical_plan();
                    } else {
                        return Err(ErrorCode::NotImplemented);
                    }

                    all_join_keys.extend(join_keys);
                }
                // remove join expressions from filter
                match remove_join_expressions(&filter_expr, &all_join_keys)? {
                    Some(filter_expr) => {
                        Ok(DataFrame::new(left).filter(filter_expr).logical_plan())
                    }
                    _ => Ok(left),
                }
            }
            None => {
                if plans.len() == 1 {
                    Ok(plans[0].clone())
                } else {
                    // CROSS JOIN NOT SUPPORTED YET
                    Err(ErrorCode::NotImplemented)
                }
            }
        }
    }

    /// Normalize a SQL object name
    fn normalize_sql_object_name(sql_object_name: &ObjectName) -> String {
        sql_object_name
            .0
            .iter()
            .map(normalize_ident)
            .collect::<Vec<String>>()
            .join(".")
    }

    fn sql_to_expr(&self, sql: &Expr) -> Result<LogicalExpr> {
        match sql {
            Expr::Value(Value::Boolean(n)) => Ok(lit(*n)),
            Expr::Value(Value::Number(n, _)) => match n.parse::<i64>() {
                Ok(n) => Ok(lit(n)),
                Err(_) => Ok(lit(n.parse::<f64>().unwrap())),
            },
            Expr::Value(Value::SingleQuotedString(ref s)) => Ok(lit(s.clone())),
            Expr::Value(Value::Null) => Ok(LogicalExpr::Literal(ScalarValue::Null)),
            Expr::Identifier(id) => Ok(LogicalExpr::column(None, normalize_ident(id))),
            // TODO(veeupup): cast func
            Expr::Cast { expr, data_type } => self.parse_sql_cast(expr, data_type),
            Expr::BinaryOp { left, op, right } => self.parse_sql_binary_op(left, op, right),
            Expr::UnaryOp { op, expr } => self.parse_sql_unary_op(op, expr),
            Expr::CompoundIdentifier(ids) => {
                let mut var_names = ids.iter().map(|id| id.value.clone()).collect::<Vec<_>>();

                match (var_names.pop(), var_names.pop()) {
                    (Some(name), Some(table)) if var_names.is_empty() => {
                        // table.column identifier
                        Ok(LogicalExpr::Column(Column {
                            table: Some(table),
                            name,
                        }))
                    }
                    _ => Err(ErrorCode::NotImplemented),
                }
            }
            Expr::Function(function) => {
                let name = if !function.name.0.is_empty() {
                    function.name.to_string()
                } else {
                    return Err(ErrorCode::PlanError(
                        "Function not support with quote".to_string(),
                    ));
                };

                // calculate parameters
                let mut args = vec![];
                for arg in &function.args {
                    let arg = match arg {
                        FunctionArg::Named { name: _, arg } => self.sql_to_expr(arg),
                        FunctionArg::Unnamed(arg) => self.sql_to_expr(arg),
                    }?;
                    args.push(arg);
                }

                // check if scalar function
                if let Ok(func) = LogicalExpr::try_create_scalar_func(&name, &args) {
                    return Ok(func);
                };

                // check aggregate function
                if let Ok(func) = LogicalExpr::try_create_aggregate_func(&name, &args) {
                    return Ok(func);
                };

                Err(ErrorCode::NoMatchFunction(format!(
                    "Not find match func: {}",
                    name
                )))
            }
            _ => todo!(),
        }
    }

    fn parse_sql_binary_op(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<LogicalExpr> {
        let op = match op {
            BinaryOperator::Eq => Operator::Eq,
            BinaryOperator::NotEq => Operator::NotEq,
            BinaryOperator::Lt => Operator::Lt,
            BinaryOperator::LtEq => Operator::LtEq,
            BinaryOperator::Gt => Operator::Gt,
            BinaryOperator::GtEq => Operator::GtEq,
            BinaryOperator::Plus => Operator::Plus,
            BinaryOperator::Minus => Operator::Minus,
            BinaryOperator::Multiply => Operator::Multiply,
            BinaryOperator::Divide => Operator::Divide,
            BinaryOperator::Modulus => Operator::Modulos,
            BinaryOperator::And => Operator::And,
            BinaryOperator::Or => Operator::Or,
            _ => unimplemented!(),
        };
        Ok(LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(self.sql_to_expr(left)?),
            op,
            right: Box::new(self.sql_to_expr(right)?),
        }))
    }

    fn parse_sql_unary_op(&self, op: &UnaryOperator, expr: &Expr) -> Result<LogicalExpr> {
        let func = match op {
            UnaryOperator::PGAbs => logical_plan::expression::UnaryOperator::Abs,
            _ => unimplemented!(),
        };
        Ok(LogicalExpr::UnaryExpr(UnaryExpr {
            func,
            arg: Box::new(self.sql_to_expr(expr)?),
        }))
    }

    fn parse_sql_cast(&self, expr: &Expr, data_type: &SQLDataType) -> Result<LogicalExpr> {
        Ok(LogicalExpr::Cast(CastExpr {
            expr: Box::new(self.sql_to_expr(expr)?),
            data_type: convert_data_type(data_type)?,
        }))
    }
}

pub fn convert_data_type(sql_type: &SQLDataType) -> Result<DataType> {
    match sql_type {
        SQLDataType::Boolean => Ok(DataType::Boolean),
        SQLDataType::SmallInt => Ok(DataType::Int16),
        SQLDataType::Int => Ok(DataType::Int32),
        SQLDataType::BigInt => Ok(DataType::Int64),
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real => Ok(DataType::Float32),
        SQLDataType::Double => Ok(DataType::Float64),
        SQLDataType::Char(_) | SQLDataType::Varchar(_) => Ok(DataType::Utf8),
        SQLDataType::Timestamp => todo!(),
        SQLDataType::Date => Ok(DataType::Date32),
        SQLDataType::Decimal(_, _) => todo!(),
        other => Err(ErrorCode::NoMatchFunction(format!(
            "Unsupported SQL type {:?}",
            other
        ))),
    }
}

// Normalize an identifer to a lowercase string unless the identifier is quoted.
fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}

/// Come from apache/arrow-datafusion
/// Extracts equijoin ON condition be a single Eq or multiple conjunctive Eqs
/// Filters matching this pattern are added to `accum`
/// Filters that don't match this pattern are added to `accum_filter`
/// Examples:
///
/// foo = bar => accum=[(foo, bar)] accum_filter=[]
/// foo = bar AND bar = baz => accum=[(foo, bar), (bar, baz)] accum_filter=[]
/// foo = bar AND baz > 1 => accum=[(foo, bar)] accum_filter=[baz > 1]
///
fn extract_join_keys(
    expr: &LogicalExpr,
    accum: &mut Vec<(Column, Column)>,
    accum_filter: &mut Vec<LogicalExpr>,
) {
    match expr {
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (LogicalExpr::Column(l), LogicalExpr::Column(r)) => {
                    accum.push((l.clone(), r.clone()));
                }
                _other => {
                    accum_filter.push(expr.clone());
                }
            },
            Operator::And => {
                extract_join_keys(left, accum, accum_filter);
                extract_join_keys(right, accum, accum_filter);
            }
            _other
                if matches!(**left, LogicalExpr::Column(_))
                    || matches!(**right, LogicalExpr::Column(_)) =>
            {
                accum_filter.push(expr.clone());
            }
            _other => {
                extract_join_keys(left, accum, accum_filter);
                extract_join_keys(right, accum, accum_filter);
            }
        },
        _other => {
            accum_filter.push(expr.clone());
        }
    }
}

/// Extract join keys from a WHERE clause
fn extract_possible_join_keys(expr: &LogicalExpr, accum: &mut Vec<(Column, Column)>) -> Result<()> {
    match expr {
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (LogicalExpr::Column(l), LogicalExpr::Column(r)) => {
                    accum.push((l.clone(), r.clone()));
                    Ok(())
                }
                _ => Ok(()),
            },
            Operator::And => {
                extract_possible_join_keys(left, accum)?;
                extract_possible_join_keys(right, accum)
            }
            _ => Ok(()),
        },
        _ => Ok(()),
    }
}

/// Remove join expressions from a filter expression
fn remove_join_expressions(
    expr: &LogicalExpr,
    join_columns: &HashSet<(Column, Column)>,
) -> Result<Option<LogicalExpr>> {
    match expr {
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (LogicalExpr::Column(l), LogicalExpr::Column(r)) => {
                    if join_columns.contains(&(l.clone(), r.clone()))
                        || join_columns.contains(&(r.clone(), l.clone()))
                    {
                        Ok(None)
                    } else {
                        Ok(Some(expr.clone()))
                    }
                }
                _ => Ok(Some(expr.clone())),
            },
            Operator::And => {
                let l = remove_join_expressions(left, join_columns)?;
                let r = remove_join_expressions(right, join_columns)?;
                match (l, r) {
                    (Some(ll), Some(rr)) => Ok(Some(LogicalExpr::and(ll, rr))),
                    (Some(ll), _) => Ok(Some(ll)),
                    (_, Some(rr)) => Ok(Some(rr)),
                    _ => Ok(None),
                }
            }
            _ => Ok(Some(expr.clone())),
        },
        _ => Ok(Some(expr.clone())),
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::CsvConfig;
    use crate::{db::NaiveDB, print_result};
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn select_with_projection_filter() -> Result<()> {
        let mut db = NaiveDB::default();
        db.create_csv_table("t1", "data/test_data.csv", CsvConfig::default())?;

        {
            let ret = db.run_sql("select id, name from t1")?;

            assert_eq!(ret.len(), 1);

            let batch = &ret[0];
            let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 4, 5, 6, 7, 8, 9]));
            let name_excepted: ArrayRef = Arc::new(StringArray::from(vec![
                "veeupup", "alex", "lynne", "alice", "bob", "jack", "cock", "primer",
            ]));

            assert_eq!(batch.column(0), &id_excepted);
            assert_eq!(batch.column(1), &name_excepted);
        }

        {
            let ret = db.run_sql("select id, name, age from t1 where id > 1")?;

            assert_eq!(ret.len(), 1);

            let batch = &ret[0];
            let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![2, 4, 5, 6, 7, 8, 9]));
            let name_excepted: ArrayRef = Arc::new(StringArray::from(vec![
                "alex", "lynne", "alice", "bob", "jack", "cock", "primer",
            ]));
            let age_excepted: ArrayRef =
                Arc::new(Int64Array::from(vec![20, 18, 19, 20, 21, 22, 23]));

            assert_eq!(batch.column(0), &id_excepted);
            assert_eq!(batch.column(1), &name_excepted);
            assert_eq!(batch.column(2), &age_excepted);
        }

        // TODO: we should add some assert result here
        {
            db.create_csv_table("employee", "data/employee.csv", CsvConfig::default())?;
            db.create_csv_table("rank", "data/rank.csv", CsvConfig::default())?;

            let ret = db
                .run_sql("select id, name, rank_name from employee innner join rank on employee.id = rank.id");

            print_result(&ret?)?;
        }

        {
            let ret =
                db.run_sql("select * from employee innner join rank on employee.id = rank.id");

            print_result(&ret?)?;
        }

        {
            let ret = db.run_sql("select count(id), sum(id) from t1");

            print_result(&ret?)?;
        }
        {
            let ret =
                db.run_sql("select count(id), sum(age), sum(score) from t1 group by id % 3")?;

            print_result(&ret)?;
        }

        Ok(())
    }
}
