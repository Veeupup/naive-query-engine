/*
 * @Author: Veeupup
 * @Date: 2022-05-13 19:36:47
 * @Email: code@tanweime.com
 *
 * SqlPlanner creates a logical plan from a parsed SQL statement.
 *
*/

use sqlparser::ast::{Expr, OrderByExpr, SetExpr, Statement};

use crate::{
    catalog::Catalog,
    datasource::TableRef,
    error::{ErrorCode, Result},
    logical_plan::plan::LogicalPlan,
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
        todo!()
    }

    fn order_by(&self, plan: LogicalPlan, order_by: Vec<OrderByExpr>) -> Result<LogicalPlan> {
        // TODO(veeupup): order by
        Ok(plan)
    }

    fn limit(&self, plan: LogicalPlan, limit: Option<Expr>) -> Result<LogicalPlan> {
        // TODO(veeupup): limit
        Ok(plan)
    }
}
