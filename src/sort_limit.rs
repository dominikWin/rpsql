use sqlparser::ast::*;

use crate::ops::{ColRef, Op, OpSortLimit, Order};
use crate::projection::convert_expr;

#[derive(Debug)]
pub struct SortLimit {
    order_columns: Vec<ColRef>,
    order: Option<Order>,
    limit: u64,
}

impl SortLimit {
    pub fn apply_sort_limit_ops(&self, op: Op) -> Op {
        if self.limit == i32::MAX as u64 && self.order_columns.is_empty() {
            return op;
        }

        let vs = op.virtual_schema();

        Op::SortLimitOp(Box::new(OpSortLimit {
            input: op,
            order_columns: self.order_columns.clone(),
            order: self.order.clone(),
            limit: self.limit,
            ls: Option::None,
            vs,
            cfg_name: None,
        }))
    }
}

fn _get_limit(expr: &Expr) -> u64 {
    match expr {
        Expr::Value(value) => match value {
            Value::Number(string, _) => string.parse::<u64>().unwrap(),
            _ => panic!("Limit value must be a number"),
        },
        _ => panic!("Limit expr must be a value"),
    }
}

impl From<&Query> for SortLimit {
    fn from(query: &Query) -> SortLimit {
        let mut order = Option::None;
        let mut order_columns = Vec::new();
        let mut limit = i32::MAX as u64;

        for order_expr in &query.order_by {
            order_columns.push(convert_expr(&order_expr.expr));

            // We can only order either ASC or DESC, so pick the first one used
            if order.is_none() {
                if let Some(asc) = order_expr.asc {
                    order = match asc {
                        true => Option::Some(Order::ASC),
                        false => Option::Some(Order::DESC),
                    }
                }
            }
        }

        if let Some(limit_expr) = &query.limit {
            limit = _get_limit(limit_expr);
        }

        SortLimit {
            order,
            order_columns,
            limit,
        }
    }
}
