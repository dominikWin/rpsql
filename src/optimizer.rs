use std::collections::HashSet;
use std::iter::FromIterator;

use crate::ops::*;
use crate::planner::LocalSchema;

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
                Op::AggGroupOp(_) => {
                    panic!("Can't pushdown filters below agg group (depending on HAVING clause)")
                }
                Op::JoinOp(sub_op) => {
                    if sub_op.build.virtual_schema().contains(&op_filter.field) {
                        Op::JoinOp(Box::new(OpJoin {
                            build: Op::FilterOp(Box::new(OpFilter {
                                input: sub_op.build.clone(),
                                op: op_filter.op,
                                field: op_filter.field,
                                value: op_filter.value,
                                ls: sub_op.build.local_schema(),
                                vs: sub_op.build.virtual_schema(),
                                cfg_name: op_filter.cfg_name,
                            })),
                            build_join_attribute: sub_op.build_join_attribute.clone(),
                            probe: sub_op.probe.clone(),
                            probe_join_attribute: sub_op.probe_join_attribute.clone(),
                            ls: sub_op.ls.clone(),
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
                                ls: sub_op.probe.local_schema(),
                                vs: sub_op.probe.virtual_schema(),
                                cfg_name: op_filter.cfg_name,
                            })),
                            probe_join_attribute: sub_op.probe_join_attribute.clone(),
                            ls: sub_op.ls.clone(),
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
                        ls: sub_op.input.local_schema(),
                        vs: sub_op.input.virtual_schema(),
                        cfg_name: op_filter.cfg_name,
                    })),
                    projection: sub_op.projection.clone(),
                    ls: sub_op.ls.clone(),
                    vs: sub_op.vs.clone(),
                    cfg_name: sub_op.cfg_name.clone(),
                })),
                Op::SortLimitOp(sub_op) => Op::SortLimitOp(Box::new(OpSortLimit {
                    input: Op::FilterOp(Box::new(OpFilter {
                        input: sub_op.input.clone(),
                        op: op_filter.op,
                        field: op_filter.field,
                        value: op_filter.value,
                        ls: sub_op.input.local_schema(),
                        vs: sub_op.input.virtual_schema(),
                        cfg_name: op_filter.cfg_name,
                    })),
                    order_columns: sub_op.order_columns.clone(),
                    order: sub_op.order.clone(),
                    limit: sub_op.limit,
                    ls: sub_op.ls.clone(),
                    vs: sub_op.vs.clone(),
                    cfg_name: sub_op.cfg_name.clone(),
                })),
            }
        }
        Op::ProjectionOp(mut op_project) => {
            op_project.input = pushdown_filters(op_project.input);
            Op::ProjectionOp(op_project)
        }
        Op::AggGroupOp(mut op_agg_group) => {
            op_agg_group.input = pushdown_filters(op_agg_group.input);
            Op::AggGroupOp(op_agg_group)
        }
        Op::SortLimitOp(mut op_sort_limit) => {
            op_sort_limit.input = pushdown_filters(op_sort_limit.input);
            Op::SortLimitOp(op_sort_limit)
        }
    }
}

fn _coerce_projection(op: Op, target_projection: &[ColRef], force_order: bool) -> Op {
    let op_projection = op.local_schema().unwrap().columns;

    if !force_order {
        // Only enforce ordering if we need to
        if HashSet::<&ColRef>::from_iter(target_projection.iter())
            == HashSet::<&ColRef>::from_iter(op_projection.iter())
        {
            return op;
        }
    } else if target_projection.to_vec() == op_projection {
        return op;
    }

    let vs = op.virtual_schema();
    Op::ProjectionOp(Box::new(OpProjection {
        input: op,
        projection: target_projection.to_vec(),
        ls: Option::Some(LocalSchema {
            columns: target_projection.to_vec(),
        }),
        vs,
        cfg_name: Option::None,
    }))
}

pub fn local_project(op: Op, target_projection: &[ColRef], force_order: bool) -> Op {
    /*
     * On the head identify the requirements needed by the sub-ops.
     * On the tail construct a local schema and apply a projection if nessesary.
     */

    for col in target_projection {
        debug_assert!(op.virtual_schema().columns.contains(col));
    }

    let op = match op {
        Op::ScanOp(mut op) => {
            // Scans always return all their data
            op.ls = Option::Some(LocalSchema {
                columns: op.vs.columns.clone(),
            });

            _coerce_projection(Op::ScanOp(op), target_projection, force_order)
        }
        Op::JoinOp(mut op) => {
            let mut requirements = target_projection.to_vec();

            // Add the build and probe requirements and makre sure they're at the beginning
            requirements.retain(|c| c != &op.build_join_attribute);
            requirements.retain(|c| c != &op.probe_join_attribute);
            requirements.insert(0, op.probe_join_attribute.clone());
            requirements.insert(0, op.build_join_attribute.clone());

            let mut buildreqs = Vec::new();
            let mut probereqs = Vec::new();

            for req in requirements {
                if op.build.virtual_schema().columns.contains(&req) {
                    buildreqs.push(req);
                } else {
                    debug_assert!(op.probe.virtual_schema().columns.contains(&req));
                    probereqs.push(req);
                }
            }

            // Order matters only in build case
            op.build = local_project(op.build, &buildreqs, true);
            op.probe = local_project(op.probe, &probereqs, false);

            // Joins have a built-in project operator
            // They target the local schema

            op.ls = Option::Some(LocalSchema {
                columns: target_projection.to_vec(),
            });

            Op::JoinOp(op)
        }
        Op::FilterOp(mut op) => {
            let mut requirements = target_projection.to_vec();
            if !requirements.contains(&op.field) {
                requirements.push(op.field.clone());
            }

            op.input = local_project(op.input, &requirements, false);

            op.ls = op.input.local_schema();

            _coerce_projection(Op::FilterOp(op), target_projection, force_order)
        }
        Op::ProjectionOp(mut op) => {
            op.input = local_project(op.input, target_projection, false);

            _coerce_projection(op.input, target_projection, force_order)
        }
        Op::AggGroupOp(mut op) => {
            let mut requirements = op.grouping.clone();

            match &op.agg_field {
                ColRef::AggregateRef { func: _, source } => {
                    requirements.push(source.as_ref().clone());
                }
                _ => panic!("Agg field must be aggregate type"),
            }

            let mut ls = op.grouping.clone();
            ls.push(op.agg_field.clone());
            op.ls = Option::Some(LocalSchema { columns: ls });

            op.input = local_project(op.input, &requirements, true);

            _coerce_projection(Op::AggGroupOp(op), target_projection, force_order)
        }
        Op::SortLimitOp(mut op) => {
            let mut requirements = op.order_columns.clone();
            requirements.extend_from_slice(target_projection);

            op.input = local_project(op.input, &requirements, false);
            op.ls = op.input.local_schema();

            _coerce_projection(Op::SortLimitOp(op), target_projection, force_order)
        }
    };

    op
}
