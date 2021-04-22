use sqlparser::ast::*;

use crate::ops::*;

#[derive(Debug)]
pub struct Projection {
    items: Vec<ColRef>,
}

fn _convert_item(item: &SelectItem) -> ColRef {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            if let Expr::CompoundIdentifier(idents) = expr {
                idents.as_slice().into()
            } else {
                panic!("Projection must be an identifier.")
            }
        }
        SelectItem::ExprWithAlias { expr, alias: _ } => {
            if let Expr::CompoundIdentifier(idents) = expr {
                idents.as_slice().into()
            } else {
                panic!("Projection must be an identifier.")
            }
        }
        SelectItem::QualifiedWildcard(_) => panic!("Wildcards are not supported"),
        SelectItem::Wildcard => panic!("Wildcards are not supported"),
    }
}

impl From<&Vec<sqlparser::ast::SelectItem>> for Projection {
    fn from(items: &Vec<SelectItem>) -> Projection {
        let mut projection_items = Vec::with_capacity(items.len());

        for item in items {
            projection_items.push(_convert_item(item));
        }

        Projection {
            items: projection_items,
        }
    }
}

impl Projection {
    pub fn apply_project_ops(&self, op: Op) -> Op {
        let vs = op.virtual_schema();
        Op::ProjectionOp(Box::new(OpProjection {
            input: op,
            projection: self.items.clone(),
            vs,
            cfg_name: Option::None,
        }))
    }
}
