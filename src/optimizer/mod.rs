/*
 * @Author: Veeupup
 * @Date: 2022-05-13 17:59:40
 * @Email: code@tanweime.com
*/

mod projection_push_down;

use crate::logical_plan::plan::LogicalPlan;
use std::sync::Arc;

#[derive(Default)]
pub struct Optimizer {
    rules: Vec<Arc<dyn OptimizerRule>>,
}

pub trait OptimizerRule {
    fn optimize(&self, plan: &LogicalPlan) -> LogicalPlan;
}

impl Optimizer {
    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut plan = plan;
        for rule in &self.rules {
            plan = rule.optimize(&plan);
        }
        plan
    }
}
