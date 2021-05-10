use sqlparser::ast::*;

use crate::ops::*;

#[derive(Debug)]
pub struct Projection {
    pub items: Vec<ColRef>,
    pub aliases: Vec<String>,
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
                func: AggFunc::parse_agg_func(name).expect("Invalid agg function name"),
                source: Box::new(arg),
            }
        }
        _ => panic!("Projection must be an identifier."),
    }
}

fn _convert_item(item: &SelectItem) -> (ColRef, Option<String>) {
    match item {
        SelectItem::UnnamedExpr(expr) => (convert_expr(expr), Option::None),
        SelectItem::ExprWithAlias { expr, alias } => {
            (convert_expr(expr), Option::Some(alias.value.clone()))
        }
        SelectItem::QualifiedWildcard(_) => panic!("Wildcards are not supported"),
        SelectItem::Wildcard => panic!("Wildcards are not supported"),
    }
}

impl From<&Vec<sqlparser::ast::SelectItem>> for Projection {
    fn from(items: &Vec<SelectItem>) -> Projection {
        let mut projection_items = Vec::with_capacity(items.len());
        let mut projection_aliases = Vec::with_capacity(items.len());

        for (i, item) in items.iter().enumerate() {
            let (internal_item, alias_optional) = _convert_item(item);
            let alias = alias_optional.unwrap_or(format!("_{}", i));
            projection_items.push(internal_item);
            projection_aliases.push(alias);
        }

        Projection {
            items: projection_items,
            aliases: projection_aliases,
        }
    }
}

impl Projection {
    pub fn needed_projection(&self) -> Vec<ColRef> {
        self.items.clone()
    }

    pub fn needed_projection_aliases(&self) -> Vec<String> {
        self.aliases.clone()
    }
}
