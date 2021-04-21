use std::collections::HashSet;

use sqlparser::ast::*;

use crate::ops::{ColRef, Op, OpFilter};
use crate::planner::EqualityFactSet;

#[derive(Debug)]
pub enum Selection {
    Identity(ColRef),
    And(Vec<Selection>),
    Eq(Box<Selection>, Box<Selection>),
    NotEq(Box<Selection>, Box<Selection>),
    Lt(Box<Selection>, Box<Selection>),
    Gt(Box<Selection>, Box<Selection>),
    LtEq(Box<Selection>, Box<Selection>),
    GtEq(Box<Selection>, Box<Selection>),
    Const(String),
}

impl Selection {
    pub fn normalized(self) -> Selection {
        match self {
            Selection::And(subselections) => {
                let mut top_level_subselections = vec![];
                for subsecection in subselections {
                    let normalized_subselection = subsecection.normalized();
                    if let Selection::And(mut nested_subselections) = normalized_subselection {
                        top_level_subselections.append(&mut nested_subselections);
                    } else {
                        top_level_subselections.push(normalized_subselection);
                    }
                }
                Selection::And(top_level_subselections)
            }
            _ => Selection::And(vec![self]),
        }
    }

    pub fn potential_equijoins(&self) -> HashSet<(ColRef, ColRef)> {
        let mut out = HashSet::new();

        if let Selection::And(selections) = self {
            for selection in selections {
                if let Selection::Eq(l, r) = selection {
                    if let Selection::Identity(lref) = l.as_ref() {
                        if let Selection::Identity(rref) = r.as_ref() {
                            out.insert((lref.clone(), rref.clone()));
                        }
                    }
                }
            }
        } else {
            // This should be normalized already
            unreachable!();
        }

        out
    }

    pub fn apply_filter_ops(&self, mut op: Op, existing_equalities: &EqualityFactSet) -> Op {
        fn _compose_binary_filter(bin_op: &str, op: Op, colref: &ColRef, rep: &str) -> Op {
            let vs = op.virtual_schema();
            Op::FilterOp(Box::new(OpFilter {
                field: op.virtual_schema().get_field_idx(colref),
                input: op,
                op: bin_op.to_string(),
                value: rep.to_string(),
                vs,
                cfg_name: Option::None,
            }))
        }

        match self {
            Selection::Identity(_) => panic!("Identity filter not supported"),
            Selection::Const(_) => panic!("Const filter not supported"),
            Selection::And(subfilters) => {
                for subfilter in subfilters {
                    op = subfilter.apply_filter_ops(op, existing_equalities);
                }
                op
            }
            Selection::Eq(l, r) => {
                match (l.as_ref(), r.as_ref()) {
                    (Selection::Const(_), Selection::Const(_)) => unimplemented!(),
                    (Selection::Identity(colref), Selection::Const(rep))
                    | (Selection::Const(rep), Selection::Identity(colref)) => {
                        _compose_binary_filter("==", op, colref, rep)
                    }
                    (Selection::Identity(colref1), Selection::Identity(colref2)) => {
                        if existing_equalities.are_equal(colref1, colref2) {
                            // Nothing to do!
                            op
                        } else {
                            panic!("Can't apply new identity-identity equality")
                        }
                    }
                    _ => panic!("Unknown equality configuration."),
                }
            }
            Selection::NotEq(l, r) => match (l.as_ref(), r.as_ref()) {
                (Selection::Identity(colref), Selection::Const(rep))
                | (Selection::Const(rep), Selection::Identity(colref)) => {
                    _compose_binary_filter("!=", op, colref, rep)
                }
                _ => panic!("Unknown inequality configuration."),
            },
            Selection::Lt(l, r) => match (l.as_ref(), r.as_ref()) {
                (Selection::Identity(colref), Selection::Const(rep)) => {
                    _compose_binary_filter("<", op, colref, rep)
                }
                (Selection::Const(rep), Selection::Identity(colref)) => {
                    _compose_binary_filter(">", op, colref, rep)
                }
                _ => panic!("Unknown less-than configuration."),
            },
            Selection::Gt(l, r) => match (l.as_ref(), r.as_ref()) {
                (Selection::Identity(colref), Selection::Const(rep)) => {
                    _compose_binary_filter(">", op, colref, rep)
                }
                (Selection::Const(rep), Selection::Identity(colref)) => {
                    _compose_binary_filter("<", op, colref, rep)
                }
                _ => panic!("Unknown greater-than configuration."),
            },
            Selection::LtEq(l, r) => match (l.as_ref(), r.as_ref()) {
                (Selection::Identity(colref), Selection::Const(rep)) => {
                    _compose_binary_filter("<=", op, colref, rep)
                }
                (Selection::Const(rep), Selection::Identity(colref)) => {
                    _compose_binary_filter(">=", op, colref, rep)
                }
                _ => panic!("Unknown less-than-equal configuration."),
            },
            Selection::GtEq(l, r) => match (l.as_ref(), r.as_ref()) {
                (Selection::Identity(colref), Selection::Const(rep)) => {
                    _compose_binary_filter(">=", op, colref, rep)
                }
                (Selection::Const(rep), Selection::Identity(colref)) => {
                    _compose_binary_filter("<=", op, colref, rep)
                }
                _ => panic!("Unknown greater-than-equal configuration."),
            },
        }
    }
}

impl From<&sqlparser::ast::Expr> for Selection {
    fn from(expr: &Expr) -> Selection {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    Selection::And(vec![left.as_ref().into(), right.as_ref().into()])
                }
                BinaryOperator::Eq => Selection::Eq(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                BinaryOperator::NotEq => Selection::NotEq(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                BinaryOperator::Lt => Selection::Lt(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                BinaryOperator::Gt => Selection::Gt(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                BinaryOperator::LtEq => Selection::LtEq(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                BinaryOperator::GtEq => Selection::GtEq(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                _ => panic!("Unsupported binary op"),
            },
            Expr::CompoundIdentifier(idents) => Selection::Identity(idents.as_slice().into()),
            Expr::Nested(subexpr) => subexpr.as_ref().into(),
            Expr::Value(v) => match v {
                Value::Number(val_str, _) => Selection::Const(val_str.to_string()),
                _ => panic!("Unsupported value type"),
            },
            _ => panic!("Unsupported selection type: {:?}", expr),
        }
    }
}
