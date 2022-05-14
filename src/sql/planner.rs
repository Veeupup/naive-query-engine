/*
 * @Author: Veeupup
 * @Date: 2022-05-13 19:36:47
 * @Email: code@tanweime.com
 *
 * SqlPlanner creates a logical plan from a parsed SQL statement.
 *
*/

use log::debug;
use sqlparser::ast::{Expr, OrderByExpr, SetExpr, Statement, TableWithJoins};
use sqlparser::ast::{Ident, ObjectName, SelectItem, TableFactor, Value};
use std::sync::Arc;

use crate::logical_plan::expression::{LogicalExpression, ScalarValue};
use crate::logical_plan::plan::TableScan;
use crate::{
    catalog::Catalog,
    datasource::TableRef,
    error::{ErrorCode, Result},
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

                // process the SELECT expressions, with wildcards expanded
                let df = self.plan_from_projection(df, select.projection)?;

                // TODO(veeupup): aggregate expr

                // TODO(veeupup): group by

                Ok(df.logical_plan().to_owned())
            }
            _ => todo!(),
        }
    }

    fn order_by(&self, plan: LogicalPlan, order_by: Vec<OrderByExpr>) -> Result<LogicalPlan> {
        // TODO(veeupup): order by
        Ok(plan)
    }

    fn limit(&self, plan: LogicalPlan, limit: Option<Expr>) -> Result<LogicalPlan> {
        // TODO(veeupup): limit
        Ok(plan)
    }

    fn plan_from_tables(&self, from: Vec<TableWithJoins>) -> Result<DataFrame> {
        // TODO(veeupup): support select with no from
        // TODO(veeupup): support select with join, multi table
        debug_assert!(from.len() > 0);
        match &from[0].relation {
            TableFactor::Table { name, alias, .. } => {
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

    /// Normalize a SQL object name
    fn normalize_sql_object_name(sql_object_name: &ObjectName) -> String {
        sql_object_name
            .0
            .iter()
            .map(normalize_ident)
            .collect::<Vec<String>>()
            .join(".")
    }

    fn sql_to_expr(&self, sql: &Expr) -> Result<LogicalExpression> {
        match sql {
            Expr::Value(Value::Number(n, _)) => Ok(LogicalExpression::Literal(ScalarValue::Utf8(
                Some(n.clone()),
            ))),
            Expr::Identifier(id) => Ok(LogicalExpression::column(normalize_ident(id))),
            _ => todo!(),
        }
    }
}

// Normalize an identifer to a lowercase string unless the identifier is quoted.
fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}
