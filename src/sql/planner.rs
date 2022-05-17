/*
 * @Author: Veeupup
 * @Date: 2022-05-13 19:36:47
 * @Email: code@tanweime.com
 *
 * SqlPlanner creates a logical plan from a parsed SQL statement.
 *
*/

use log::debug;
use sqlparser::ast::{BinaryOperator, Expr, OrderByExpr, SetExpr, Statement, TableWithJoins};
use sqlparser::ast::{Ident, ObjectName, SelectItem, TableFactor, Value};

use crate::logical_plan::expression::{BinaryExpr, LogicalExpr, Operator, ScalarValue};
use crate::logical_plan::literal::lit;
use crate::logical_plan::plan::TableScan;
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
                self.limit(plan, query.limit)
            }
            _ => unimplemented!(),
        }
    }

    fn set_expr_to_plan(&self, set_expr: SetExpr) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(select) => {
                // get table source
                let df = self.plan_from_tables(select.from)?;

                // TODO(veeupup): process where filter here
                let df = self.plan_selection(select.selection, df)?;

                // process the SELECT expressions, with wildcards expanded
                let df = self.plan_from_projection(df, select.projection)?;

                // TODO(veeupup): aggregate expr

                // TODO(veeupup): group by

                Ok(df.logical_plan())
            }
            _ => todo!(),
        }
    }

    fn order_by(&self, plan: LogicalPlan, _order_by: Vec<OrderByExpr>) -> Result<LogicalPlan> {
        // TODO(veeupup): order by
        Ok(plan)
    }

    fn limit(&self, plan: LogicalPlan, _limit: Option<Expr>) -> Result<LogicalPlan> {
        // TODO(veeupup): limit
        Ok(plan)
    }

    fn plan_from_tables(&self, from: Vec<TableWithJoins>) -> Result<DataFrame> {
        // TODO(veeupup): support select with no from
        // TODO(veeupup): support select with join, multi table
        debug_assert!(!from.is_empty());
        match &from[0].relation {
            TableFactor::Table { name, alias: _, .. } => {
                let table_name = Self::normalize_sql_object_name(name);
                let source = self.catalog.get_table(&table_name)?;
                let plan = LogicalPlan::TableScan(TableScan {
                    source,
                    projection: None,
                });
                Ok(DataFrame { plan })
            }
            _ => todo!(),
        }
    }

    fn plan_from_projection(
        &self,
        df: DataFrame,
        projection: Vec<SelectItem>,
    ) -> Result<DataFrame> {
        let proj = projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(expr) => self.sql_to_expr(expr),
                SelectItem::Wildcard => {
                    todo!()
                }
                _ => todo!(),
            })
            .flat_map(|result| match result {
                Ok(expr) => Ok(expr),
                Err(err) => Err(err),
            })
            .collect::<Vec<_>>();
        debug!("projection: {:?}", proj);
        Ok(df.project(proj))
    }

    fn plan_selection(&self, selection: Option<Expr>, df: DataFrame) -> Result<DataFrame> {
        match selection {
            Some(predicate_expr) => {
                let filter_expr = self.sql_to_expr(&predicate_expr)?;
                let df = df.filter(filter_expr);
                Ok(df)
            }
            None => Ok(df),
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
            Expr::Value(Value::Number(n, _)) => {
                let num = match n.parse::<i64>() {
                    Ok(n) => Ok(lit(n)),
                    Err(_) => Ok(lit(n.parse::<f64>().unwrap())),
                };
                num
            }
            Expr::Value(Value::SingleQuotedString(ref s)) => Ok(lit(s.clone())),
            Expr::Value(Value::Null) => Ok(LogicalExpr::Literal(ScalarValue::Null)),
            Expr::Identifier(id) => Ok(LogicalExpr::column(normalize_ident(id))),
            // TODO(veeupup): cast func
            Expr::BinaryOp { left, op, right } => self.parse_sql_binary_op(left, op, right),
            _ => todo!(),
        }
    }

    fn parse_sql_binary_op(
        &self,
        left: &Box<Expr>,
        op: &BinaryOperator,
        right: &Box<Expr>,
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
            BinaryOperator::Modulo => Operator::Modulo,
            BinaryOperator::And => Operator::And,
            BinaryOperator::Or => Operator::Or,
            _ => unimplemented!(),
        };
        Ok(LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(self.sql_to_expr(&left)?),
            op,
            right: Box::new(self.sql_to_expr(&right)?),
        }))
    }
}

// Normalize an identifer to a lowercase string unless the identifier is quoted.
fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use crate::db::NaiveDB;
    use crate::error::Result;
    use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn select_with_projection_filter() -> Result<()> {
        let mut db = NaiveDB::default();
        db.create_csv_table("t1", "test_data.csv")?;

        {
            let ret = db.run_sql("select id, name from t1")?;

            assert_eq!(ret.len(), 1);

            let batch = &ret[0];
            let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 4]));
            let name_excepted: ArrayRef =
                Arc::new(StringArray::from(vec!["veeupup", "alex", "lynne"]));

            assert_eq!(batch.column(0), &id_excepted);
            assert_eq!(batch.column(1), &name_excepted);
        }

        {
            let ret = db.run_sql("select id, name, age from t1 where id > 1")?;

            assert_eq!(ret.len(), 1);

            let batch = &ret[0];
            let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![2, 4]));
            let name_excepted: ArrayRef = Arc::new(StringArray::from(vec!["alex", "lynne"]));
            let age_excepted: ArrayRef = Arc::new(Int64Array::from(vec![20, 18]));

            assert_eq!(batch.column(0), &id_excepted);
            assert_eq!(batch.column(1), &name_excepted);
            assert_eq!(batch.column(2), &age_excepted);
        }

        Ok(())
    }
}
