use std::collections::HashMap;

use sqlparser::ast::*;

use crate::metadata::*;
use crate::ops::*;

#[derive(Debug, Clone, PartialEq)]
pub struct ColRef {
    pub table: String,
    pub column: String,
}

impl From<&[Ident]> for ColRef {
    fn from(idents: &[Ident]) -> ColRef {
        if idents.len() != 2 {
            panic!("You need to have both a table and col name")
        }

        ColRef {
            table: idents[0].to_string(),
            column: idents[1].to_string(),
        }
    }
}

impl ColRef {
    fn resolve_aliases(&self, namespace: &HashMap<String, String>) -> ColRef {
        let true_name = &namespace[&self.table];
        ColRef {
            table: true_name.to_string(),
            column: self.column.clone(),
        }
    }
}

impl Metadata {
    fn attribute_index(&self, colref: &ColRef) -> u32 {
        let table_meta = self
            .tables
            .iter()
            .filter(|t| t.name == colref.table)
            .last()
            .unwrap();

        let table_schema = &table_meta.schema;

        for i in 0..table_schema.columns.len() {
            let (name, _type) = &table_schema.columns[i];
            if name == &colref.column {
                return i as u32;
            }
        }

        panic!();
    }
}

#[derive(Debug, Clone)]
pub struct LocalSchema {
    pub columns: Vec<ColRef>,
}

impl LocalSchema {
    fn get_field_idx(&self, colref: &ColRef) -> u32 {
        for i in 0..self.columns.len() {
            if colref == &self.columns[i] {
                return i as u32;
            }
        }

        panic!(
            "Failed to find column reference {:?} in local schema.",
            colref
        )
    }

    fn from_meta_table(meta_schema: &MetaTableDef, table_alias: &str) -> LocalSchema {
        let mut columns = Vec::new();

        for column in &meta_schema.schema.columns {
            columns.push(ColRef {
                table: table_alias.to_string(),
                column: column.0.clone(),
            })
        }

        LocalSchema { columns }
    }

    fn cat(schema1: &LocalSchema, schema2: &LocalSchema) -> LocalSchema {
        let mut columns = schema1.columns.clone();
        columns.append(&mut schema2.columns.clone());

        LocalSchema { columns }
    }
}

#[derive(Debug)]
enum Selection {
    Identity(ColRef),
    And(Vec<Selection>),
    Eq(Box<Selection>, Box<Selection>),
    Const(String),
}

impl Selection {
    fn as_equijoin(&self) -> Option<(ColRef, ColRef)> {
        match self {
            Selection::Eq(l, r) => {
                if let Selection::Identity(lref) = l.as_ref() {
                    if let Selection::Identity(rref) = r.as_ref() {
                        Option::Some((lref.clone(), rref.clone()))
                    } else {
                        Option::None
                    }
                } else {
                    Option::None
                }
            }
            _ => Option::None,
        }
    }

    fn apply_filter_ops(&self, mut op: Op) -> Op {
        match self {
            Selection::Identity(_) => panic!("Identity filter not supported"),
            Selection::Const(_) => panic!("Const filter not supported"),
            Selection::And(subfilters) => {
                for subfilter in subfilters {
                    op = subfilter.apply_filter_ops(op);
                }
                op
            }
            Selection::Eq(l, r) => {
                if let Selection::Const(_rep) = l.as_ref() {
                    unimplemented!()
                } else if let Selection::Const(rep) = r.as_ref() {
                    if let Selection::Identity(colref) = l.as_ref() {
                        let ls = op.local_schema();
                        Op::FilterOp(Box::new(OpFilter {
                            r#type: "filter".to_string(),
                            field: op.local_schema().get_field_idx(colref),
                            input: op,
                            op: "==".to_string(),
                            value: rep.to_string(),
                            ls,
                        }))
                    } else {
                        panic!("Can't match dual-value filter")
                    }
                } else {
                    panic!("Can't match dual-ref filter")
                }
            }
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

fn unwrap_table_name(parts: &[Ident]) -> String {
    if parts.len() != 1 {
        panic!("Invalid table name {:?}", parts);
    }

    parts[0].value.to_string()
}

pub fn plan(query: &Query, meta: &Metadata) -> Option<Op> {
    let setexpr = &query.body;
    let select = match setexpr {
        SetExpr::Select(select) => select,
        _ => panic!("Not a select"),
    };

    let from = &select.from;

    let mut table_namespace = HashMap::<String, String>::new();

    for table in from {
        let relation = &table.relation;
        if let TableFactor::Table {
            name,
            alias,
            args: _,
            with_hints: _,
        } = relation
        {
            let name = unwrap_table_name(&name.0);
            let alias = if let Some(alias) = alias {
                alias.name.value.to_string()
            } else {
                name.clone()
            };

            table_namespace.insert(alias, name);
        } else {
            panic!("Not a table");
        }
    }

    let mut root_op = Option::<Op>::None;

    let mut scans = Vec::<OpScan>::new();
    for (alias, table) in table_namespace.iter() {
        let table_meta = meta
            .tables
            .iter()
            .filter(|t| &t.name == table)
            .last()
            .unwrap();

        let scan = OpScan {
            r#type: "parallelscan".to_string(),
            tab_name: table.clone(),
            file: table_meta.file.to_string(),
            filetype: table_meta.filetype.to_string(),
            schema: table_meta.schema.columns.iter().map(|c| c.1).collect(),
            ls: LocalSchema::from_meta_table(table_meta, alias),
        };

        scans.push(scan);
    }

    let selection: Selection = if let Some(filter) = &select.selection {
        filter.into()
    } else {
        Selection::And(vec![])
    };

    fn _find_scan(scans: &mut Vec<OpScan>, table: &str) -> Option<OpScan> {
        for i in 0..scans.len() {
            if &scans[i].tab_name == table {
                return Option::Some(scans.remove(i));
            }
        }
        Option::None
    }

    if scans.len() == 1 {
        // No join
        root_op = Some(Op::ScanOp(Box::new(scans.pop().unwrap())));
    } else {
        // With join; a shitty dual-table optimizer lives here
        if let Some((ltable_ref, rtable_ref)) = selection.as_equijoin() {
            let ltable_ref = ltable_ref.resolve_aliases(&table_namespace);
            let rtable_ref = rtable_ref.resolve_aliases(&table_namespace);

            let ltable = _find_scan(&mut scans, &ltable_ref.table).unwrap();
            let rtable = _find_scan(&mut scans, &rtable_ref.table).unwrap();

            let ls = LocalSchema::cat(&ltable.ls, &rtable.ls);
            let join = Op::JoinOp(Box::new(OpJoin {
                r#type: "hashjoin".to_string(),
                probe: Op::ScanOp(Box::new(ltable)),
                probe_join_attribute: meta.attribute_index(&ltable_ref),
                build: Op::ScanOp(Box::new(rtable)),
                build_join_attribute: meta.attribute_index(&rtable_ref),
                ls,
            }));

            root_op = Some(join);
        }
    }

    if let Some(op) = root_op {
        root_op = Some(selection.apply_filter_ops(op));
    }

    root_op
}
