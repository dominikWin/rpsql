use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

mod agg_grouping;
mod conf_writer;
mod metadata;
mod ops;
mod optimizer;
mod planner;
mod projection;
mod selection;
mod sort_limit;

use conf_writer::plan_to_json;
use metadata::*;
use planner::*;

fn main() {
    // let sql =
    //     "SELECT P.COST, P.PKEY FROM PART P, ORDERS O1 WHERE (O1.OKEY = P.PKEY) AND (P.PKEY <= 8) AND O1.OKEY >= 2 AND O1.ZIP <> 3800";
    let sql = "SELECT SUM(P.COST), O.ZIP
    FROM LINEITEM L, PART P, ORDERS O
    WHERE L.PKEY=P.PKEY AND L.OKEY=O.OKEY
    GROUP BY O.ZIP
    ORDER BY O.ZIP ASC";

    let meta = Metadata::new();

    let dialect = GenericDialect {};

    let ast = Parser::parse_sql(&dialect, sql).unwrap().remove(0);
    // println!("{:#?}", ast);

    let query: Box<Query> = match ast {
        Statement::Query(q) => q,
        _ => panic!("Not a query"),
    };
    let exec_plan = plan(&query, &meta);

    // println!("{:#?}", exec_plan);
    println!("{}", plan_to_json(exec_plan, &meta));
}
