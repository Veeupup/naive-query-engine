/*
 * @Author: Veeupup
 * @Date: 2022-05-13 18:54:33
 * @Email: code@tanweime.com
*/

use super::OptimizerRule;
use crate::logical_plan::plan::LogicalPlan;

pub struct ProjectionPushDown;

impl OptimizerRule for ProjectionPushDown {
    fn optimize(&self, plan: &LogicalPlan) -> LogicalPlan {
        plan.clone()
    }
}
