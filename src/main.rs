use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use clap::{App, Arg};

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
    let matches = App::new("RPSQL Query Planner")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::with_name("QUERY")
                .help("Defines the SQL query to use")
                .required(true),
        )
        .arg(
            Arg::with_name("explain")
                .help("Shows a human-readable version of the plan")
                .long("explain")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("meta_file")
                .help("Reads the metadata from a json file")
                .long("meta")
                .takes_value(true),
        )
        .get_matches();

    let sql = matches.value_of("QUERY").unwrap();

    let meta = if let Some(meta_path) = matches.value_of("meta_file") {
        Metadata::from_path(meta_path)
    } else {
        Metadata::from_default()
    };

    let dialect = GenericDialect {};

    let ast = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    let query: Box<Query> = match ast {
        Statement::Query(q) => q,
        _ => panic!("Not a query"),
    };

    let (exec_plan, _col_names) = plan(&query, &meta);

    if matches.is_present("explain") {
        println!("{}", exec_plan);
        return;
    }

    println!("{}", plan_to_json(exec_plan, &meta));
}
