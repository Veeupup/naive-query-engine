/*
 * @Author: Veeupup 
 * @Date: 2022-05-19 14:17:29 
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-19 15:25:32
*/

use arrow::compute::concat;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use twox_hash::XxHash64;

use super::PhysicalPlan;
use super::PhysicalPlanRef;
use crate::error::ErrorCode;
use crate::logical_plan::expression::Column;
use crate::logical_plan::plan::JoinType;
use crate::logical_plan::schema::NaiveSchema;
use crate::physical_plan::ColumnExpr;
use crate::physical_plan::PhysicalExpr;

use crate::Result;
use crate::print_result;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;

/// HashJoin has two phase for join
/// 1. build phase will build HashMap about outer table using on column as hashval
///     hashmap: col hash val -> vec<row id>
/// 2. probe phase will probe all inner table by using on col to check
#[derive(Debug)]
pub struct HashJoin {
    left: PhysicalPlanRef,
    right: PhysicalPlanRef,
    on: Vec<(Column, Column)>,
    join_type: JoinType,
    schema: NaiveSchema,
    /// on col hash val and row id
    /// chain hash table
    hashtable: Mutex<HashMap<u64, Vec<usize>>>,
    /// data, combine all data in one record batch
    data: Mutex<Option<RecordBatch>>,
}

impl HashJoin {
    pub fn new(
        left: PhysicalPlanRef,
        right: PhysicalPlanRef,
        on: Vec<(Column, Column)>,
        join_type: JoinType,
        schema: NaiveSchema,
    ) -> PhysicalPlanRef {
        Arc::new(Self {
            left,
            right,
            on,
            join_type,
            schema,
            hashtable: Mutex::new(HashMap::new()),
            data: Mutex::new(None),
        })
    }

    pub fn build(&self) -> Result<()> {
        let left = self.left.execute()?;

        let single_batch = concat_batches(&self.left.schema().clone().into(), &left)?;

        print_result(&vec![single_batch]);
        Ok(())
    }

    pub fn probe(&self) -> Result<Vec<RecordBatch>> {
        todo!()
    }
}

/// Concatenates an array of `RecordBatch` into one batch
pub fn concat_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let mut arrays = Vec::with_capacity(schema.fields().len());
    for i in 0..schema.fields().len() {
        let array = concat(
            &batches
                .iter()
                .map(|batch| batch.column(i).as_ref())
                .collect::<Vec<_>>(),
        )?;
        arrays.push(array);
    }
    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

impl PhysicalPlan for HashJoin {
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.build()?;

        self.probe()
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.left.clone(), self.right.clone()])
    }
}



