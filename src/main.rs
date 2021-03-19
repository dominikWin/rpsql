use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

struct Metadata {
    tables: Vec<MetaTableDef>,
}

struct MetaTableDef {
    name: String,
    path: String,
}

#[derive(Debug)]
enum Op {
    ScanOp(Box<OpScan>),
    JoinOp(Box<OpJoin>),
}

#[derive(Debug)]
struct OpScan {
    r#type: String,
    path: String,
}

#[derive(Debug)]
struct OpJoin {
    r#type: String,
    build: Op,
    probe: Op,
}

fn plan(query: &Query, meta: &Metadata) {
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
    for (_alias, table) in table_namespace.into_iter() {
        let scan = OpScan {
            r#type: "parallelscan".to_string(),
            path: meta
                .tables
                .iter()
                .filter(|t| t.name == table)
                .last()
                .unwrap()
                .path
                .to_string(),
        };

        if let Some(other_table) = root_op {
            root_op = Option::Some(Op::JoinOp(Box::new(OpJoin {
                r#type: "hashjoin".to_string(),
                probe: Op::ScanOp(Box::new(scan)),
                build: other_table,
            })));
        } else {
            root_op = Option::Some(Op::ScanOp(Box::new(scan)));
        }
    }

    println!("{:#?}", root_op.unwrap());
}

fn unwrap_table_name(parts: &[Ident]) -> String {
    if parts.len() != 1 {
        panic!("Invalid table name {:?}", parts);
    }

    parts[0].value.to_string()
}

fn main() {
    let sql = "SELECT ZIP, SUM(PRICE-COST)
    FROM LINEITEM L, PART P, ORDERS O
    WHERE L.PKEY=P.PKEY AND L.OKEY=O.OKEY
    GROUP BY ZIP
    ORDER BY ZIP ASC;";

    let meta = Metadata {
        tables: vec![
            MetaTableDef {
                name: "LINEITEM".to_string(),
                path: "lineitem.tbl.bz2".to_string(),
            },
            MetaTableDef {
                name: "ORDERS".to_string(),
                path: "order.tbl.bz2".to_string(),
            },
            MetaTableDef {
                name: "PART".to_string(),
                path: "part.tbl.bz2".to_string(),
            },
        ],
    };

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    // println!("{:#?}", ast);

    let query: Box<Query> = match ast {
        Statement::Query(q) => q,
        _ => panic!("Not a query"),
    };
    plan(&query, &meta);
}
