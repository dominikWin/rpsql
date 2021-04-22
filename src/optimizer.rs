use crate::ops::*;

pub fn pushdown_filters(op: Op) -> Op {
    match op {
        Op::ScanOp(op_scan) => Op::ScanOp(op_scan),
        Op::JoinOp(mut op_join) => {
            op_join.build = pushdown_filters(op_join.build);
            op_join.probe = pushdown_filters(op_join.probe);
            Op::JoinOp(op_join)
        }
        Op::FilterOp(mut op_filter) => {
            op_filter.input = pushdown_filters(op_filter.input);

            match &op_filter.input {
                Op::ScanOp(_) => Op::FilterOp(op_filter),
                Op::FilterOp(_) => Op::FilterOp(op_filter),
                Op::JoinOp(sub_op) => {
                    if sub_op.build.virtual_schema().contains(&op_filter.field) {
                        Op::JoinOp(Box::new(OpJoin {
                            build: Op::FilterOp(Box::new(OpFilter {
                                input: sub_op.build.clone(),
                                op: op_filter.op,
                                field: op_filter.field,
                                value: op_filter.value,
                                vs: sub_op.build.virtual_schema(),
                                cfg_name: op_filter.cfg_name,
                            })),
                            build_join_attribute: sub_op.build_join_attribute.clone(),
                            probe: sub_op.probe.clone(),
                            probe_join_attribute: sub_op.probe_join_attribute.clone(),
                            vs: sub_op.vs.clone(),
                            cfg_name: sub_op.cfg_name.clone(),
                        }))
                    } else if sub_op.probe.virtual_schema().contains(&op_filter.field) {
                        Op::JoinOp(Box::new(OpJoin {
                            build: sub_op.build.clone(),
                            build_join_attribute: sub_op.build_join_attribute.clone(),
                            probe: Op::FilterOp(Box::new(OpFilter {
                                input: sub_op.probe.clone(),
                                op: op_filter.op,
                                field: op_filter.field,
                                value: op_filter.value,
                                vs: sub_op.probe.virtual_schema(),
                                cfg_name: op_filter.cfg_name,
                            })),
                            probe_join_attribute: sub_op.probe_join_attribute.clone(),
                            vs: sub_op.vs.clone(),
                            cfg_name: sub_op.cfg_name.clone(),
                        }))
                    } else {
                        Op::FilterOp(op_filter)
                    }
                }
                Op::ProjectionOp(sub_op) => Op::ProjectionOp(Box::new(OpProjection {
                    input: Op::FilterOp(Box::new(OpFilter {
                        input: sub_op.input.clone(),
                        op: op_filter.op,
                        field: op_filter.field,
                        value: op_filter.value,
                        vs: sub_op.input.virtual_schema(),
                        cfg_name: op_filter.cfg_name,
                    })),
                    projection: sub_op.projection.clone(),
                    vs: sub_op.vs.clone(),
                    cfg_name: sub_op.cfg_name.clone(),
                })),
            }
        }
        Op::ProjectionOp(mut op_project) => {
            op_project.input = pushdown_filters(op_project.input);
            Op::ProjectionOp(op_project)
        }
    }
}
