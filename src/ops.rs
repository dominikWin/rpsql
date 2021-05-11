use std::fmt;

use crate::metadata::MetaType;
use crate::planner::{LocalSchema, VirtualSchema};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColRef {
    TableRef { table: String, column: String },
    AggregateRef { func: AggFunc, source: Box<ColRef> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggFunc {
    Sum,
    Count,
}

impl AggFunc {
    pub fn parse_agg_func(name: &str) -> Option<AggFunc> {
        match name.to_lowercase().as_ref() {
            "sum" => Option::Some(AggFunc::Sum),
            "count" => Option::Some(AggFunc::Count),
            _ => Option::None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Op {
    ScanOp(Box<OpScan>),
    JoinOp(Box<OpJoin>),
    FilterOp(Box<OpFilter>),
    ProjectionOp(Box<OpProjection>),
    AggGroupOp(Box<OpAggGroup>),
    SortLimitOp(Box<OpSortLimit>),
    SubqueryProjOp(Box<OpSubqueryProj>),
}

impl Op {
    pub fn virtual_schema(&self) -> VirtualSchema {
        match self {
            Op::ScanOp(op) => op.vs.clone(),
            Op::FilterOp(op) => op.vs.clone(),
            Op::JoinOp(op) => op.vs.clone(),
            Op::ProjectionOp(op) => op.vs.clone(),
            Op::AggGroupOp(op) => op.vs.clone(),
            Op::SortLimitOp(op) => op.vs.clone(),
            Op::SubqueryProjOp(op) => op.vs.clone(),
        }
    }

    pub fn local_schema(&self) -> Option<LocalSchema> {
        match self {
            Op::ScanOp(op) => op.ls.clone(),
            Op::FilterOp(op) => op.ls.clone(),
            Op::JoinOp(op) => op.ls.clone(),
            Op::ProjectionOp(op) => op.ls.clone(),
            Op::AggGroupOp(op) => op.ls.clone(),
            Op::SortLimitOp(op) => op.ls.clone(),
            Op::SubqueryProjOp(op) => op.ls.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpScan {
    pub file: String,
    pub filetype: String,
    pub tab_name: String,
    pub schema: Vec<MetaType>,
    pub ls: Option<LocalSchema>,
    pub vs: VirtualSchema,
    pub cfg_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OpJoin {
    pub build: Op,
    pub build_join_attribute: ColRef,
    pub probe: Op,
    pub probe_join_attribute: ColRef,
    pub ls: Option<LocalSchema>,
    pub vs: VirtualSchema,
    pub cfg_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OpFilter {
    pub input: Op,
    pub op: String,
    pub field: ColRef,
    pub value: String,
    pub ls: Option<LocalSchema>,
    pub vs: VirtualSchema,
    pub cfg_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OpProjection {
    pub input: Op,
    pub projection: Vec<ColRef>,
    pub ls: Option<LocalSchema>,
    pub vs: VirtualSchema,
    pub cfg_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OpAggGroup {
    pub input: Op,
    pub grouping: Vec<ColRef>,
    pub agg_field: ColRef,
    pub ls: Option<LocalSchema>,
    pub vs: VirtualSchema,
    pub cfg_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Order {
    ASC,
    DESC,
}

#[derive(Debug, Clone)]
pub struct OpSortLimit {
    pub input: Op,
    pub order_columns: Vec<ColRef>,
    pub order: Option<Order>,
    pub limit: u64,
    pub ls: Option<LocalSchema>,
    pub vs: VirtualSchema,
    pub cfg_name: Option<String>,
}

/**
 * Null op, the optimizer knows that the schemas are provided to it.
 */
#[derive(Debug, Clone)]
pub struct OpSubqueryProj {
    pub input: Op,
    pub ls: Option<LocalSchema>,
    pub vs: VirtualSchema,
}

fn _indent_str(s: &str) -> String {
    s.replace("\n", &format!("\n{:ident$}", "", ident = 8))
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Op::ScanOp(op) => write!(f, "{}", op),
            Op::JoinOp(op) => write!(f, "{}", op),
            Op::FilterOp(op) => write!(f, "{}", op),
            Op::ProjectionOp(op) => write!(f, "{}", op),
            Op::AggGroupOp(op) => write!(f, "{}", op),
            Op::SortLimitOp(op) => write!(f, "{}", op),
            Op::SubqueryProjOp(op) => write!(f, "{}", op),
        }
    }
}

impl fmt::Display for ColRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ColRef::TableRef { table, column } => write!(f, "{}.{}", table, column),
            ColRef::AggregateRef { func, source } => write!(f, "{:?}({})", func, source),
        }
    }
}

fn _fmt_colref_slice(slice: &[ColRef]) -> String {
    format!(
        "[{}]",
        slice
            .iter()
            .map(|x| format!("{}", x))
            .collect::<Vec<String>>()
            .join(", ")
    )
}

impl fmt::Display for OpJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HashJoin ({} == {})\n(build)>{}\n(probe)>{}",
            self.build_join_attribute,
            self.probe_join_attribute,
            _indent_str(&format!("{}", self.build)),
            _indent_str(&format!("{}", self.probe))
        )
    }
}

impl fmt::Display for OpSubqueryProj {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.input)
    }
}

impl fmt::Display for OpFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Filter ({} {} {})\n{}",
            self.field, self.op, self.value, self.input
        )
    }
}

impl fmt::Display for OpAggGroup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AggGroup (grouping={}, agg_field={})\n{}",
            _fmt_colref_slice(&self.grouping),
            self.agg_field,
            self.input
        )
    }
}

impl fmt::Display for OpSortLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SortLimit (order_by={}, limit={})\n{}",
            _fmt_colref_slice(&self.order_columns),
            self.limit,
            self.input
        )
    }
}

impl fmt::Display for OpProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Projection ({})\n{}",
            _fmt_colref_slice(&self.projection),
            self.input
        )
    }
}

impl fmt::Display for OpScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Scan (table={}, file={})", self.tab_name, self.file)
    }
}
