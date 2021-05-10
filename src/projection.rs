use sqlparser::ast::*;

use crate::ops::*;

#[derive(Debug)]
pub struct Projection {
    pub items: Vec<ColRef>,
}

pub fn convert_expr(expr: &Expr) -> ColRef {
    match expr {
        Expr::CompoundIdentifier(idents) => idents.as_slice().into(),
        Expr::Identifier(_) => panic!("Identifiers must be fully qualified!"),
        Expr::Function(function) => {
            assert!(function.name.0.len() == 1);
            let name = &function.name.0[0].value;

            assert!(function.args.len() == 1);
            let arg = &function.args[0];
            let arg = match arg {
                FunctionArg::Named { name: _, arg: _ } => panic!("Named arguments not supported!"),
                FunctionArg::Unnamed(expr) => convert_expr(&expr),
            };

            ColRef::AggregateRef {
                func: AggFunc::as_agg_func(name).unwrap(),
                source: Box::new(arg),
            }
        }
        _ => panic!("Projection must be an identifier."),
    }
}

fn _convert_item(item: &SelectItem) -> ColRef {
    match item {
        SelectItem::UnnamedExpr(expr) => convert_expr(expr),
        SelectItem::ExprWithAlias { expr, alias: _ } => convert_expr(expr),
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
    pub fn needed_projection(&self) -> Vec<ColRef> {
        self.items.clone()
    }
}
