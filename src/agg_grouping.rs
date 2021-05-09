use sqlparser::ast::*;

use crate::ops::*;
use crate::planner::VirtualSchema;
use crate::projection::*;

#[derive(Debug)]
pub struct AggGrouping {
    groups: Vec<ColRef>,
    agg_field: Option<ColRef>,
}

impl AggGrouping {
    pub fn from_query(query: &Select, projection: &Projection) -> AggGrouping {
        let mut grouping_items = Vec::with_capacity(0);

        for expr in &query.group_by {
            let idents = match expr {
                Expr::CompoundIdentifier(idents) => idents,
                _ => panic!("Groupby must contain a raw identifier."),
            };

            grouping_items.push(idents.as_slice().into());
        }

        let mut agg_field = Option::None;
        for proj_obj in &projection.items {
            if matches!(&proj_obj, ColRef::AggregateRef { func: _, source: _ }) {
                if agg_field.is_none() {
                    agg_field = Option::Some(proj_obj.clone());
                } else if agg_field.as_ref().unwrap() != proj_obj {
                    panic!("Multiple different aggregations present!")
                }
            }
        }

        assert!(grouping_items.is_empty() == agg_field.is_none());

        AggGrouping {
            groups: grouping_items,
            agg_field,
        }
    }
}

impl AggGrouping {
    pub fn apply_agg_grouping_ops(&self, op: Op) -> Op {
        if self.groups.is_empty() {
            return op;
        }

        let mut vs = self.groups.clone();
        vs.push(self.agg_field.as_ref().unwrap().clone());

        let vs = VirtualSchema::from_custom(vs);

        Op::AggGroupOp(Box::new(OpAggGroup {
            input: op,
            grouping: self.groups.clone(),
            agg_field: self.agg_field.clone().unwrap(),
            ls: Option::None,
            vs,
            cfg_name: Option::None,
        }))
    }
}
