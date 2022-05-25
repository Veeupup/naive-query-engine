use super::PhysicalPlan;
use crate::error::Result;

pub trait PhysicalPlanVistor {
    // Invoke before visit PhysicalPlan
    fn pre_visit(&mut self, plan: &dyn PhysicalPlan) -> Result<()>;

    // Invoke before after PhysicalPlan
    fn post_visit(&mut self, plan: &dyn PhysicalPlan) -> Result<()>;
}

pub fn _visit_physical_plan<V: PhysicalPlanVistor>(
    plan: &dyn PhysicalPlan,
    visitor: &mut V,
) -> Result<()> {
    let children = plan.children()?;
    visitor.pre_visit(plan)?;

    for child in children {
        _visit_physical_plan(child.as_ref(), visitor)?;
    }
    visitor.post_visit(plan)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        datasource::CsvTable,
        physical_plan::{PhysicalPlan, ScanPlan},
        CsvConfig,
    };

    use super::{PhysicalPlanVistor, _visit_physical_plan};
    use crate::error::Result;

    struct TestVisitor {
        v: usize,
    }

    impl PhysicalPlanVistor for TestVisitor {
        fn pre_visit(&mut self, _: &dyn PhysicalPlan) -> Result<()> {
            println!("pre_v: {}", self.v);
            Ok(())
        }

        fn post_visit(&mut self, _: &dyn PhysicalPlan) -> Result<()> {
            println!("post_v: {}", self.v);
            Ok(())
        }
    }

    #[test]
    fn test_visitor() -> Result<()> {
        let source = CsvTable::try_create("data/test_data.csv", CsvConfig::default())?;

        let scan_plan = ScanPlan::create(source, None);
        _visit_physical_plan(scan_plan.as_ref(), &mut TestVisitor { v: 1 })?;
        Ok(())
    }
}
