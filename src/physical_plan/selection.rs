/*
 * @Author: Veeupup
 * @Date: 2022-05-14 18:20:27
 * @Email: code@tanweime.com
*/

use std::sync::Arc;

use super::{PhysicalExpr, PhysicalExprRef, PhysicalPlan, PhysicalPlanRef};
use crate::logical_plan::schema::NaiveSchema;
use crate::Result;
use arrow::array::{
    Float64Array, Float64Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
    UInt64Array, UInt64Builder,
};
use arrow::record_batch::RecordBatch;
use arrow::{
    array::{Array, BooleanArray, BooleanBuilder},
    datatypes::{DataType},
};

#[derive(Debug)]
pub struct SelectionPlan {
    input: PhysicalPlanRef,
    expr: PhysicalExprRef,
}

impl SelectionPlan {
    pub fn create(input: PhysicalPlanRef, expr: PhysicalExprRef) -> PhysicalPlanRef {
        Arc::new(Self { input, expr })
    }
}

macro_rules! build_array_by_predicate {
    ($COLUMN: ident, $PREDICATE: expr, $ARRAY_TYPE: ty, $ARRAY_BUILDER: ty) => {{
        let array = $COLUMN.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let mut builder = <$ARRAY_BUILDER>::new(array.len());
        let iter = $PREDICATE.iter().zip(array.iter());
        for (valid, val) in iter {
            match valid {
                Some(valid) => {
                    if valid {
                        builder.append_option(val)?;
                    }
                }
                None => builder.append_option(None)?,
            }
        }
        Arc::new(builder.finish())
    }};
}

impl PhysicalPlan for SelectionPlan {
    fn schema(&self) -> &NaiveSchema {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let input = self.input.execute()?;
        let predicate = self.expr.evaluate(&input[0])?.into_array();
        let predicate = predicate.as_any().downcast_ref::<BooleanArray>().unwrap();

        let mut batches = vec![];

        for batch in &input {
            let mut columns = vec![];
            for col in batch.columns() {
                let dt = col.data_type();
                let column: Arc<dyn Array> = match dt {
                    DataType::Boolean => {
                        build_array_by_predicate!(col, predicate, BooleanArray, BooleanBuilder)
                    }
                    DataType::UInt64 => {
                        build_array_by_predicate!(col, predicate, UInt64Array, UInt64Builder)
                    }
                    DataType::Int64 => {
                        build_array_by_predicate!(col, predicate, Int64Array, Int64Builder)
                    }
                    DataType::Float64 => {
                        build_array_by_predicate!(col, predicate, Float64Array, Float64Builder)
                    }
                    DataType::Utf8 => {
                        let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                        let mut builder = StringBuilder::new(array.len());
                        let iter = predicate.iter().zip(array.iter());
                        for (valid, val) in iter {
                            match valid {
                                Some(valid) => {
                                    if valid {
                                        builder.append_option(val)?;
                                    }
                                }
                                None => builder.append_option(None::<&str>)?,
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    _ => unimplemented!(),
                };
                columns.push(column);
            }
            let record_batch =
                RecordBatch::try_new(Arc::new(self.schema().clone().into()), columns)?;
            batches.push(record_batch);
        }
        Ok(batches)
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{CsvConfig, CsvTable, TableSource};
    use crate::logical_plan::expression::{Operator, ScalarValue};
    use crate::physical_plan::expression::ColumnExpr;
    use crate::physical_plan::scan::ScanPlan;
    use crate::physical_plan::{PhysicalBinaryExpr, PhysicalLiteralExpr, ProjectionPlan};
    use crate::print_result;
    use arrow::{
        array::{Array, ArrayRef, Int64Array, StringArray},
        datatypes::Schema,
    };

    #[test]
    fn test_selection() -> Result<()> {
        let source = CsvTable::try_create("data/test_data.csv", CsvConfig::default())?;
        let schema = Arc::new(Schema::new(vec![
            source.schema().field(0).clone(),
            source.schema().field(1).clone(),
            source.schema().field(2).clone(),
        ]));
        let scan_plan = ScanPlan::create(source, None);

        let expr = vec![
            ColumnExpr::try_create(None, Some(0))?,
            ColumnExpr::try_create(Some("name".to_string()), None)?,
            ColumnExpr::try_create(None, Some(2))?,
        ];
        let proj_plan = ProjectionPlan::create(scan_plan, schema, expr);

        // TODO(veeupup): selection expression

        {
            let add_expr = PhysicalBinaryExpr::new(
                ColumnExpr::try_create(Some("id".to_string()), None)?,
                Operator::Plus,
                PhysicalLiteralExpr::new(ScalarValue::Int64(Some(1))),
            );

            let expr = PhysicalBinaryExpr::new(
                add_expr,
                Operator::Gt,
                PhysicalLiteralExpr::new(ScalarValue::Int64(Some(5))),
            );

            let selection_plan = SelectionPlan::create(proj_plan, expr);

            let res = selection_plan.execute()?;

            assert_eq!(res.len(), 1);
            let batch = &res[0];

            print_result(&res)?;

            let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![5, 6, 7, 8, 9]));
            let name_excepted: ArrayRef = Arc::new(StringArray::from(vec![
                "alice", "bob", "jack", "cock", "primer",
            ]));

            assert_eq!(batch.column(0), &id_excepted);
            assert_eq!(batch.column(1), &name_excepted);
        }

        // TODO(veeupup): add more test about binary expression

        Ok(())
    }
}
