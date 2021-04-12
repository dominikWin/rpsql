use crate::metadata::MetaType;
use crate::planner::LocalSchema;

#[derive(Debug)]
pub enum Op {
    ScanOp(Box<OpScan>),
    JoinOp(Box<OpJoin>),
    FilterOp(Box<OpFilter>),
}

impl Op {
    pub fn local_schema<'a>(&self) -> LocalSchema {
        match self {
            Op::ScanOp(op) => op.ls.clone(),
            Op::FilterOp(op) => op.ls.clone(),
            Op::JoinOp(op) => op.ls.clone(),
        }
    }
}

#[derive(Debug)]
pub struct OpScan {
    pub r#type: String,
    pub file: String,
    pub filetype: String,
    pub tab_name: String,
    pub schema: Vec<MetaType>,
    pub ls: LocalSchema,
    pub cfg_name: Option<String>,
}

#[derive(Debug)]
pub struct OpJoin {
    pub r#type: String,
    pub build: Op,
    pub build_join_attribute: u32,
    pub probe: Op,
    pub probe_join_attribute: u32,
    pub ls: LocalSchema,
    pub cfg_name: Option<String>,
}

#[derive(Debug)]
pub struct OpFilter {
    pub r#type: String,
    pub input: Op,
    pub op: String,
    pub field: u32,
    pub value: String,
    pub ls: LocalSchema,
    pub cfg_name: Option<String>,
}
