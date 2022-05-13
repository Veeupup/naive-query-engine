/*
 * @Author: Veeupup
 * @Date: 2022-05-12 20:15:59
 * @Email: code@tanweime.com
 *
 * A logical plan represents a relation (a set of tuples) with a known schema. Each logical plan can
 * have zero or more logical plans as inputs. It is convenient for a logical plan to expose its child plans
 * so that a visitor pattern can be used to walk through the plan.
 *
*/

mod dataframe;
pub mod expression;
pub mod plan;

pub use dataframe::DataFrame;
