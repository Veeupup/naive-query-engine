/*
 * @Author: Veeupup
 * @Date: 2022-05-13 14:23:58
 * @Email: code@tanweime.com
*/

use std::fmt::Debug;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::{error::Result, logical_plan::schema::NaiveSchema};

pub trait PhysicalPlan: Debug {
    fn schema(&self) -> &NaiveSchema;

    // TODO(veeupup): return by using streaming mode
    fn execute(&self) -> Result<Vec<RecordBatch>>;

    fn children(&self) -> Result<Vec<PhysicalPlanRef>>;
}

pub type PhysicalPlanRef = Arc<dyn PhysicalPlan>;
