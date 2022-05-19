/*
 * @Author: Veeupup
 * @Date: 2022-05-19 14:17:29
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-19 17:28:50
*/

use arrow::array::ArrayRef;
use arrow::array::Int64Builder;
use arrow::array::PrimitiveArray;
use arrow::array::StringArray;
use arrow::compute;
use arrow::compute::concat;
use arrow::datatypes::DataType;

use arrow::datatypes::Int64Type;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::UInt64Type;
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
use std::collections::HashMap;


use std::hash::Hasher;

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

macro_rules! build_match {
    ($LEFT_COL: expr, $TYPE: ty, $SINGLE_BATCH: expr, $HASHTABLE: expr, $WRITE_DT: ident) => {{
        let left_col = $LEFT_COL
            .as_any()
            .downcast_ref::<PrimitiveArray<$TYPE>>()
            .unwrap();

        // build hashmap
        for i in 0..$SINGLE_BATCH.num_rows() {
            let left_val = left_col.value(i);
            let mut hasher = XxHash64::default();
            hasher.$WRITE_DT(left_val);
            let hash_val = hasher.finish();
            if let Some(vec) = $HASHTABLE.get_mut(&hash_val) {
                vec.push(i);
            } else {
                $HASHTABLE.insert(hash_val, vec![i]);
            }
        }
    }};
}

macro_rules! probe_match {
    ($RIGHT_COL: expr, $LEFT_COL: expr, $TYPE: ty, $RIGHT_BATCH: expr, $HASHTABLE: expr, $OUTER_POS: expr, $INNER_POS: expr, $WRITE_DT: ident) => {{
        let right_col = $RIGHT_COL.as_any().downcast_ref::<$TYPE>().unwrap();
        let left_col = $LEFT_COL.as_any().downcast_ref::<$TYPE>().unwrap();

        // probe
        for i in 0..$RIGHT_BATCH.num_rows() {
            let right_val = right_col.value(i);
            let mut hasher = XxHash64::default();
            hasher.$WRITE_DT(right_val);
            let hash_val = hasher.finish();

            if let Some(left_pos) = $HASHTABLE.get(&hash_val) {
                for idx in left_pos {
                    // hash val same, but we need to check whether real value equal or not
                    if left_col.value(*idx) == right_col.value(i) {
                        $OUTER_POS.append_value(*idx as i64);
                        $INNER_POS.append_value(i as i64);
                    }
                }
            }
        }
    }};
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

    pub fn build(&self) -> Result<Vec<ArrayRef>> {
        if self.on.len() == 0 {
            return Err(ErrorCode::PlanError(
                "Inner Join on Conditions can't not be empty".to_string(),
            ));
        }

        let left = self.left.execute()?;
        let single_batch = concat_batches(&self.left.schema().clone().into(), &left)?;

        let (left_col, _) = &self.on[0];
        let left_col = ColumnExpr::try_create(Some(left_col.name.clone()), None)?;
        let left_col = left_col.evaluate(&single_batch)?.into_array();

        let mut hashtable = self.hashtable.lock().unwrap();
        match left_col.data_type() {
            DataType::Int64 => {
                build_match!(left_col, Int64Type, single_batch, hashtable, write_i64)
            }
            DataType::UInt64 => {
                build_match!(left_col, UInt64Type, single_batch, hashtable, write_u64)
            }
            DataType::Utf8 => {
                let left_col = left_col.as_any().downcast_ref::<StringArray>().unwrap();

                // build hashmap
                for i in 0..single_batch.num_rows() {
                    let mut hasher = XxHash64::default();
                    hasher.write(left_col.value(i).as_bytes());
                    let hash_val = hasher.finish();
                    if let Some(vec) = hashtable.get_mut(&hash_val) {
                        vec.push(i);
                    } else {
                        hashtable.insert(hash_val, vec![i]);
                    }
                }
            }
            _ => return Err(ErrorCode::NotImplemented),
        }

        *self.data.lock().unwrap() = Some(single_batch);
        Ok(vec![left_col])
    }

    pub fn probe(&self, left_cols: Vec<ArrayRef>) -> Result<Vec<RecordBatch>> {
        let right_batches = self.right.execute()?;

        let (_, right_col) = &self.on[0];
        let right_col = ColumnExpr::try_create(Some(right_col.name.clone()), None)?;
        let left_col = &left_cols[0];

        let mut batches = vec![];

        for right_batch in &right_batches {
            let right_col = right_col.evaluate(&right_batch)?.into_array();

            let hashtable = self.hashtable.lock().unwrap();

            let mut outer_pos = Int64Builder::new(left_col.len());
            let mut inner_pos = Int64Builder::new(right_col.len());
            match right_col.data_type() {
                DataType::Int64 => probe_match!(
                    right_col,
                    left_col,
                    PrimitiveArray<Int64Type>,
                    right_batch,
                    hashtable,
                    outer_pos,
                    inner_pos,
                    write_i64
                ),
                DataType::UInt64 => probe_match!(
                    right_col,
                    left_col,
                    PrimitiveArray<UInt64Type>,
                    right_batch,
                    hashtable,
                    outer_pos,
                    inner_pos,
                    write_u64
                ),
                DataType::Utf8 => {
                    let right_col = right_col.as_any().downcast_ref::<StringArray>().unwrap();
                    let left_col = left_col.as_any().downcast_ref::<StringArray>().unwrap();

                    // probe
                    for i in 0..right_batch.num_rows() {
                        let mut hasher = XxHash64::default();
                        hasher.write(right_col.value(i).as_bytes());
                        let hash_val = hasher.finish();

                        if let Some(left_pos) = hashtable.get(&hash_val) {
                            for idx in left_pos {
                                // hash val same, but we need to check whether real value equal or not
                                if left_col.value(*idx) == right_col.value(i) {
                                    outer_pos.append_value(*idx as i64);
                                    inner_pos.append_value(i as i64);
                                }
                            }
                        }
                    }
                }
                _ => return Err(ErrorCode::NotImplemented),
            }

            let mut columns = vec![];

            let outer_pos = outer_pos.finish();
            let inner_pos = inner_pos.finish();

            // add left columns
            let data = self.data.lock().unwrap();
            if let Some(outer_table) = &*data {
                for i in 0..self.left.schema().fields().len() {
                    let array = outer_table.column(i);
                    columns.push(compute::take(array.as_ref(), &outer_pos, None)?);
                }

                // add right columns
                for i in 0..self.right.schema().fields().len() {
                    let array = right_batch.column(i);
                    columns.push(compute::take(array.as_ref(), &inner_pos, None)?);
                }

                let batch = RecordBatch::try_new(self.schema.clone().into(), columns)?;
                batches.push(batch);
            }
        }

        Ok(batches)
    }
}

/// Concatenates an array of `RecordBatch` into one batch
pub fn concat_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<RecordBatch> {
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
        let left_cols = self.build()?;

        self.probe(left_cols)
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.left.clone(), self.right.clone()])
    }
}
