use std::collections::HashMap;

use crate::metadata::{MetaType, Metadata};
use crate::ops::*;

use json::{object, JsonValue};

struct ConfigNamespace {
    operators: HashMap<String, u32>,
}

impl ConfigNamespace {
    fn new() -> ConfigNamespace {
        ConfigNamespace {
            operators: HashMap::new(),
        }
    }

    fn name_operator(&mut self, requested_name: &str) -> String {
        if let Some(n) = self.operators.get(requested_name) {
            let n = n + 1;
            self.operators.insert(requested_name.to_string(), n);
            format!("{}{}", requested_name, n)
        } else {
            self.operators.insert(requested_name.to_string(), 1);
            requested_name.to_string()
        }
    }
}

pub fn plan_to_json(mut plan: Op, meta: &Metadata) -> String {
    let mut namespace = ConfigNamespace::new();
    plan.name_op(&mut namespace);

    let mut data = object! {
        path: meta.path.clone(),
        buffsize: meta.buffsize,
        treeroot: plan.node(),
    };

    if let JsonValue::Object(obj) = &mut data {
        plan.preflight(obj);
    }

    data.dump()
}

impl OpScan {
    fn name_op(&mut self, namespace: &mut ConfigNamespace) {
        if self.cfg_name.is_none() {
            self.cfg_name =
                Option::Some(namespace.name_operator(&format!("scan{}", self.tab_name)));
        }
    }

    fn preflight(&self, global: &mut object::Object) {
        let schema = JsonValue::Array(
            self.schema
                .iter()
                .map(|c| JsonValue::String(c.preflight_str()))
                .collect(),
        );

        global[self.cfg_name.as_ref().unwrap()] = object! {
            type: "scan",
            filetype: self.filetype.clone(),
            file: self.file.clone(),
            schema: schema
        };
    }

    fn node(&self) -> JsonValue {
        object! {
            name: self.cfg_name.as_ref().unwrap().to_string(),
        }
    }
}

impl OpJoin {
    fn name_op(&mut self, namespace: &mut ConfigNamespace) {
        self.build.name_op(namespace);
        self.probe.name_op(namespace);

        if self.cfg_name.is_none() {
            self.cfg_name = Option::Some(namespace.name_operator("join"));
        }
    }

    fn preflight(&self, global: &mut object::Object) {
        self.build.preflight(global);
        self.probe.preflight(global);

        let mut projection = Vec::<String>::new();
        for i in 0..self.build.virtual_schema().columns.len() {
            projection.push(format!("B${}", i));
        }
        for i in 0..self.probe.virtual_schema().columns.len() {
            projection.push(format!("P${}", i));
        }

        global[self.cfg_name.as_ref().unwrap()] = object! {
            type: "hashjoin",
            buildjattr: self.build_join_attribute,
            probejattr: self.probe_join_attribute,
            hash: {
                fn: "modulo",
                buckets: 10000,
            },
            tuplesperbucket: 4,
            projection_tuple: projection,
            threadgroups_tuple: [[0]],
            allocpolicy: "striped",
        };
    }

    fn node(&self) -> JsonValue {
        object! {
            name: self.cfg_name.as_ref().unwrap().to_string(),
            build: self.build.node(),
            probe: self.probe.node(),
        }
    }
}

impl OpFilter {
    fn name_op(&mut self, namespace: &mut ConfigNamespace) {
        self.input.name_op(namespace);

        if self.cfg_name.is_none() {
            self.cfg_name = Option::Some(namespace.name_operator("filter"));
        }
    }

    fn preflight(&self, global: &mut object::Object) {
        self.input.preflight(global);

        global[self.cfg_name.as_ref().unwrap()] = object! {
            type: "filter",
            op: self.op.clone(),
            field: self.field,
            value: self.value.clone(),
        };
    }

    fn node(&self) -> JsonValue {
        object! {
            name: self.cfg_name.as_ref().unwrap().to_string(),
            input: self.input.node(),
        }
    }
}

impl Op {
    fn name_op(&mut self, namespace: &mut ConfigNamespace) {
        match self {
            Op::ScanOp(op) => op.name_op(namespace),
            Op::JoinOp(op) => op.name_op(namespace),
            Op::FilterOp(op) => op.name_op(namespace),
        }
    }

    fn preflight(&self, global: &mut object::Object) {
        match self {
            Op::ScanOp(op) => op.preflight(global),
            Op::JoinOp(op) => op.preflight(global),
            Op::FilterOp(op) => op.preflight(global),
        }
    }

    fn node(&self) -> JsonValue {
        match self {
            Op::ScanOp(op) => op.node(),
            Op::JoinOp(op) => op.node(),
            Op::FilterOp(op) => op.node(),
        }
    }
}

impl MetaType {
    fn preflight_str(&self) -> String {
        match self {
            MetaType::LONG => "long",
            MetaType::DEC => "dec",
        }
        .to_string()
    }
}
