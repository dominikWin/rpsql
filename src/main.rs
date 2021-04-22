use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

mod conf_writer;
mod metadata;
mod ops;
mod optimizer;
mod planner;
mod projection;
mod selection;

use conf_writer::plan_to_json;
use metadata::*;
use planner::*;

fn main() {
    let sql = "SELECT COST FROM PART P, ORDERS O1, ORDERS O2 WHERE ((O1.OKEY = O2.OKEY) AND O1.OKEY = P.PKEY) AND (P.PKEY = 5);";

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
