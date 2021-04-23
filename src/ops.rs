use crate::metadata::MetaType;
use crate::planner::{LocalSchema, VirtualSchema};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColRef {
    pub table: String,
    pub column: String,
}

#[derive(Debug, Clone)]
pub enum Op {
    ScanOp(Box<OpScan>),
    JoinOp(Box<OpJoin>),
    FilterOp(Box<OpFilter>),
    ProjectionOp(Box<OpProjection>),
}

impl Op {
    pub fn virtual_schema(&self) -> VirtualSchema {
        match self {
            Op::ScanOp(op) => op.vs.clone(),
            Op::FilterOp(op) => op.vs.clone(),
            Op::JoinOp(op) => op.vs.clone(),
            Op::ProjectionOp(op) => op.vs.clone(),
        }
    }

    pub fn local_schema(&self) -> Option<LocalSchema> {
        match self {
            Op::ScanOp(op) => op.ls.clone(),
            Op::FilterOp(op) => op.ls.clone(),
            Op::JoinOp(op) => op.ls.clone(),
            Op::ProjectionOp(op) => op.ls.clone(),
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
