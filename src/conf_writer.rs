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

        let buildcols = self.build.local_schema().unwrap().columns;
        let probecols = self.probe.local_schema().unwrap().columns;

        let mut projection = Vec::<String>::new();
        for colref in &self.ls.as_ref().unwrap().columns {
            for i in 0..buildcols.len() {
                if &buildcols[i] == colref {
                    projection.push(format!("B${}", i));
                }
            }
            for i in 0..probecols.len() {
                if &probecols[i] == colref {
                    projection.push(format!("P${}", i));
                }
            }
        }

        let buildjattr = self
            .build
            .local_schema()
            .unwrap()
            .get_field_idx(&self.build_join_attribute);
        let probejattr = self
            .probe
            .local_schema()
            .unwrap()
            .get_field_idx(&self.probe_join_attribute);

        global[self.cfg_name.as_ref().unwrap()] = object! {
            type: "hashjoin",
            buildjattr: buildjattr,
            probejattr: probejattr,
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

        let field = self.ls.as_ref().unwrap().get_field_idx(&self.field);

        global[self.cfg_name.as_ref().unwrap()] = object! {
            type: "filter",
            op: self.op.clone(),
            field: field,
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

impl OpProjection {
    fn name_op(&mut self, namespace: &mut ConfigNamespace) {
        self.input.name_op(namespace);

        if self.cfg_name.is_none() {
            self.cfg_name = Option::Some(namespace.name_operator("project"));
        }
    }

    fn preflight(&self, global: &mut object::Object) {
        self.input.preflight(global);

        let mut projection = Vec::<String>::new();
        for col in &self.projection {
            for (idx, vcol) in self.vs.columns.iter().enumerate() {
                if col == vcol {
                    projection.push(format!("${}", idx));
                    break;
                }
            }
        }

        global[self.cfg_name.as_ref().unwrap()] = object! {
            type: "projection",
            projection: projection,
        };
    }

    fn node(&self) -> JsonValue {
        object! {
            name: self.cfg_name.as_ref().unwrap().to_string(),
            input: self.input.node(),
        }
    }
}

impl OpAggGroup {
    fn name_op(&mut self, namespace: &mut ConfigNamespace) {
        self.input.name_op(namespace);

        if self.cfg_name.is_none() {
            self.cfg_name = Option::Some(namespace.name_operator("agg"));
        }
    }

    fn preflight(&self, global: &mut object::Object) {
        self.input.preflight(global);

        let mut fields = Vec::new();
        for group_field in &self.grouping {
            fields.push(self.ls.as_ref().unwrap().get_field_idx(group_field));
        }

        let sumfield_idx = self.ls.as_ref().unwrap().get_field_idx(&self.agg_field);
        let hashfield_idx = fields[0];

        global[self.cfg_name.as_ref().unwrap()] = object! {
            type: "aggregate_sum",
            fields_tuple: fields,
            sumfield: sumfield_idx,
            hash: {
                fn: "modulo",
                buckets: 10000,
                field: hashfield_idx,
            },
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
            Op::ProjectionOp(op) => op.name_op(namespace),
            Op::AggGroupOp(op) => op.name_op(namespace),
        }
    }

    fn preflight(&self, global: &mut object::Object) {
        match self {
            Op::ScanOp(op) => op.preflight(global),
            Op::JoinOp(op) => op.preflight(global),
            Op::FilterOp(op) => op.preflight(global),
            Op::ProjectionOp(op) => op.preflight(global),
            Op::AggGroupOp(op) => op.preflight(global),
        }
    }

    fn node(&self) -> JsonValue {
        match self {
            Op::ScanOp(op) => op.node(),
            Op::JoinOp(op) => op.node(),
            Op::FilterOp(op) => op.node(),
            Op::ProjectionOp(op) => op.node(),
            Op::AggGroupOp(op) => op.node(),
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
