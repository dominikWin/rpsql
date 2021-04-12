use crate::metadata::{MetaType, Metadata};
use crate::ops::*;

use json::{object, JsonValue};

pub fn plan_to_json(plan: &Op, meta: &Metadata) -> String {
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
    fn preflight(&self, global: &mut object::Object) {
        let name = format!("scan{}", self.tab_name);

        let schema = JsonValue::Array(
            self.schema
                .iter()
                .map(|c| JsonValue::String(c.preflight_str()))
                .collect(),
        );

        global[&name] = object! {
            type: "scan",
            filetype: self.filetype.clone(),
            file: self.file.clone(),
            schema: schema
        };
    }

    fn node(&self) -> JsonValue {
        let name = format!("scan{}", self.tab_name);

        object! {
            name: name
        }
    }
}

impl OpJoin {
    fn preflight(&self, global: &mut object::Object) {
        self.build.preflight(global);
        self.probe.preflight(global);

        global["joinX"] = object! {
            type: "hashjoin",
            buildjattr: self.build_join_attribute,
            probejattr: self.probe_join_attribute,
        };
    }

    fn node(&self) -> JsonValue {
        object! {
            name: "joinX",
            build: self.build.node(),
            probe: self.probe.node(),
        }
    }
}

impl OpFilter {
    fn preflight(&self, global: &mut object::Object) {
        self.input.preflight(global);

        global["filterX"] = object! {
            type: "filter",
            op: self.op.clone(),
            field: self.field,
            value: self.value.clone(),
        };
    }

    fn node(&self) -> JsonValue {
        object! {
            name: "filterX",
            input: self.input.node(),
        }
    }
}

impl Op {
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
