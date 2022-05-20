/*
 * @Author: Veeupup
 * @Date: 2022-05-20 14:12:40
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-20 19:09:41
 */

pub mod count;
pub mod sum;

use std::sync::Arc;
use std::fmt::Debug;

use crate::logical_plan::{plan::LogicalPlan, schema::NaiveSchema, expression::ScalarValue};

use super::{PhysicalPlanRef, PhysicalPlan};
use arrow::record_batch::RecordBatch;
use crate::physical_plan::PhysicalExprRef;
use crate::Result;

#[derive(Debug)]
pub struct PhysicalAggregatePlan {
    pub group_expr: Vec<PhysicalExprRef>,
    pub aggr_expr: Vec<Box<dyn AggregateOperator>>,
    pub input: PhysicalPlanRef,
    // pub input_schema: NaiveSchema,
    // output schema
    pub schema: NaiveSchema,
}

impl PhysicalAggregatePlan {
    pub fn create(
        group_expr: Vec<PhysicalExprRef>,
        aggr_expr: Vec<Box<dyn AggregateOperator>>,
        input: PhysicalPlanRef,
        schema: NaiveSchema,
    ) -> PhysicalPlanRef {
        Arc::new(Self {
            group_expr,
            aggr_expr,
            input,
            schema,
        })
    }
}

impl PhysicalPlan for PhysicalAggregatePlan {
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }

    fn children(&self) -> crate::Result<Vec<PhysicalPlanRef>> {
        todo!()
    }

    fn execute(&self) -> crate::Result<Vec<RecordBatch>> {
        todo!()
    }
}

pub trait AggregateOperator: Debug {
    fn update(&mut self, val: ScalarValue) -> Result<()>;

    fn evaluate(&self) -> Result<ScalarValue>;
}
