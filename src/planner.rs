use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

use sqlparser::ast::*;

use crate::metadata::*;
use crate::ops::*;

impl From<&[Ident]> for ColRef {
    fn from(idents: &[Ident]) -> ColRef {
        if idents.len() != 2 {
            panic!("You need to have both a table and col name")
        }

        ColRef {
            table: idents[0].to_string(),
            column: idents[1].to_string(),
        }
    }
}

impl ColRef {
    fn resolve_aliases(&self, namespace: &HashMap<String, String>) -> ColRef {
        let true_name = &namespace[&self.table];
        ColRef {
            table: true_name.to_string(),
            column: self.column.clone(),
        }
    }
}

impl Metadata {
    fn attribute_index(&self, colref: &ColRef) -> u32 {
        let table_meta = self
            .tables
            .iter()
            .filter(|t| t.name == colref.table)
            .last()
            .unwrap();

        let table_schema = &table_meta.schema;

        for i in 0..table_schema.columns.len() {
            let (name, _type) = &table_schema.columns[i];
            if name == &colref.column {
                return i as u32;
            }
        }

        panic!("Attempting to reference non-existant colref {:?}", colref);
    }
}

#[derive(Debug, Clone)]
pub struct VirtualSchema {
    pub columns: Vec<ColRef>,
}

impl VirtualSchema {
    fn get_field_idx(&self, colref: &ColRef) -> u32 {
        for i in 0..self.columns.len() {
            if colref == &self.columns[i] {
                return i as u32;
            }
        }

        panic!(
            "Failed to find column reference {:?} in local schema.",
            colref
        )
    }

    fn from_meta_table(meta_schema: &MetaTableDef, table_alias: &str) -> VirtualSchema {
        let mut columns = Vec::new();

        for column in &meta_schema.schema.columns {
            columns.push(ColRef {
                table: table_alias.to_string(),
                column: column.0.clone(),
            })
        }

        VirtualSchema { columns }
    }

    fn cat(schema1: &VirtualSchema, schema2: &VirtualSchema) -> VirtualSchema {
        let mut columns = schema1.columns.clone();
        columns.append(&mut schema2.columns.clone());

        VirtualSchema { columns }
    }
}

#[derive(Debug)]
struct EqualityFactSet {
    sets: Vec<HashSet<ColRef>>,
}

impl EqualityFactSet {
    fn new() -> EqualityFactSet {
        EqualityFactSet { sets: Vec::new() }
    }

    fn insert_rule(&mut self, c1: &ColRef, c2: &ColRef) {
        let mut c1_idx = Option::None;
        let mut c2_idx = Option::None;
        for (i, set) in self.sets.iter().enumerate() {
            if set.contains(c1) {
                c1_idx = Option::Some(i);
            }
            if set.contains(c2) {
                c2_idx = Option::Some(i);
            }
        }

        match (c1_idx, c2_idx) {
            (Option::Some(c1_idx), Option::Some(c2_idx)) => {
                // If they are equal we already know their equality
                if c1_idx != c2_idx {
                    let c2_set = self.sets.remove(c2_idx);
                    self.sets[c1_idx].extend(c2_set);
                }
            }
            (Option::Some(c1_idx), None) => {
                self.sets[c1_idx].insert(c2.clone());
            }
            (Option::None, Option::Some(c2_idx)) => {
                self.sets[c2_idx].insert(c1.clone());
            }
            (Option::None, Option::None) => {
                let mut newset = HashSet::new();
                newset.insert(c1.clone());
                newset.insert(c2.clone());
                self.sets.push(newset);
            }
        }
    }

    fn are_equal(&self, c1: &ColRef, c2: &ColRef) -> bool {
        for set in &self.sets {
            if set.contains(c1) && set.contains(c2) {
                return true;
            }
        }

        false
    }
}

#[derive(Debug)]
enum Selection {
    Identity(ColRef),
    And(Vec<Selection>),
    Eq(Box<Selection>, Box<Selection>),
    NotEq(Box<Selection>, Box<Selection>),
    Lt(Box<Selection>, Box<Selection>),
    Gt(Box<Selection>, Box<Selection>),
    LtEq(Box<Selection>, Box<Selection>),
    GtEq(Box<Selection>, Box<Selection>),
    Const(String),
}

impl Selection {
    fn normalized(self) -> Selection {
        match self {
            Selection::And(subselections) => {
                let mut top_level_subselections = vec![];
                for subsecection in subselections {
                    let normalized_subselection = subsecection.normalized();
                    if let Selection::And(mut nested_subselections) = normalized_subselection {
                        top_level_subselections.append(&mut nested_subselections);
                    } else {
                        top_level_subselections.push(normalized_subselection);
                    }
                }
                Selection::And(top_level_subselections)
            }
            _ => Selection::And(vec![self]),
        }
    }

    fn potential_equijoins(&self) -> HashSet<(ColRef, ColRef)> {
        let mut out = HashSet::new();

        if let Selection::And(selections) = self {
            for selection in selections {
                match selection {
                    Selection::Eq(l, r) => {
                        if let Selection::Identity(lref) = l.as_ref() {
                            if let Selection::Identity(rref) = r.as_ref() {
                                out.insert((lref.clone(), rref.clone()));
                            }
                        }
                    }
                    _ => {}
                }
            }
        } else {
            // This should be normalized already
            unreachable!();
        }

        out
    }

    fn apply_filter_ops(&self, mut op: Op, existing_equalities: &EqualityFactSet) -> Op {
        fn _compose_binary_filter(bin_op: &str, op: Op, colref: &ColRef, rep: &str) -> Op {
            let vs = op.virtual_schema();
            Op::FilterOp(Box::new(OpFilter {
                field: op.virtual_schema().get_field_idx(colref),
                input: op,
                op: bin_op.to_string(),
                value: rep.to_string(),
                vs,
                cfg_name: Option::None,
            }))
        }

        match self {
            Selection::Identity(_) => panic!("Identity filter not supported"),
            Selection::Const(_) => panic!("Const filter not supported"),
            Selection::And(subfilters) => {
                for subfilter in subfilters {
                    op = subfilter.apply_filter_ops(op, existing_equalities);
                }
                op
            }
            Selection::Eq(l, r) => {
                match (l.as_ref(), r.as_ref()) {
                    (Selection::Const(_), Selection::Const(_)) => unimplemented!(),
                    (Selection::Identity(colref), Selection::Const(rep))
                    | (Selection::Const(rep), Selection::Identity(colref)) => {
                        _compose_binary_filter("==", op, colref, rep)
                    }
                    (Selection::Identity(colref1), Selection::Identity(colref2)) => {
                        if existing_equalities.are_equal(colref1, colref2) {
                            // Nothing to do!
                            op
                        } else {
                            panic!("Can't apply new identity-identity equality")
                        }
                    }
                    _ => panic!("Unknown equality configuration."),
                }
            }
            Selection::NotEq(l, r) => match (l.as_ref(), r.as_ref()) {
                (Selection::Identity(colref), Selection::Const(rep))
                | (Selection::Const(rep), Selection::Identity(colref)) => {
                    _compose_binary_filter("!=", op, colref, rep)
                }
                _ => panic!("Unknown inequality configuration."),
            },
            Selection::Lt(l, r) => match (l.as_ref(), r.as_ref()) {
                (Selection::Identity(colref), Selection::Const(rep)) => {
                    _compose_binary_filter("<", op, colref, rep)
                }
                (Selection::Const(rep), Selection::Identity(colref)) => {
                    _compose_binary_filter(">", op, colref, rep)
                }
                _ => panic!("Unknown less-than configuration."),
            },
            Selection::Gt(l, r) => match (l.as_ref(), r.as_ref()) {
                (Selection::Identity(colref), Selection::Const(rep)) => {
                    _compose_binary_filter(">", op, colref, rep)
                }
                (Selection::Const(rep), Selection::Identity(colref)) => {
                    _compose_binary_filter("<", op, colref, rep)
                }
                _ => panic!("Unknown greater-than configuration."),
            },
            Selection::LtEq(l, r) => match (l.as_ref(), r.as_ref()) {
                (Selection::Identity(colref), Selection::Const(rep)) => {
                    _compose_binary_filter("<=", op, colref, rep)
                }
                (Selection::Const(rep), Selection::Identity(colref)) => {
                    _compose_binary_filter(">=", op, colref, rep)
                }
                _ => panic!("Unknown less-than-equal configuration."),
            },
            Selection::GtEq(l, r) => match (l.as_ref(), r.as_ref()) {
                (Selection::Identity(colref), Selection::Const(rep)) => {
                    _compose_binary_filter(">=", op, colref, rep)
                }
                (Selection::Const(rep), Selection::Identity(colref)) => {
                    _compose_binary_filter("<=", op, colref, rep)
                }
                _ => panic!("Unknown greater-than-equal configuration."),
            },
        }
    }
}

impl From<&sqlparser::ast::Expr> for Selection {
    fn from(expr: &Expr) -> Selection {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    Selection::And(vec![left.as_ref().into(), right.as_ref().into()])
                }
                BinaryOperator::Eq => Selection::Eq(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                BinaryOperator::NotEq => Selection::NotEq(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                BinaryOperator::Lt => Selection::Lt(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                BinaryOperator::Gt => Selection::Gt(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                BinaryOperator::LtEq => Selection::LtEq(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                BinaryOperator::GtEq => Selection::GtEq(
                    Box::new(left.as_ref().into()),
                    Box::new(right.as_ref().into()),
                ),
                _ => panic!("Unsupported binary op"),
            },
            Expr::CompoundIdentifier(idents) => Selection::Identity(idents.as_slice().into()),
            Expr::Nested(subexpr) => subexpr.as_ref().into(),
            Expr::Value(v) => match v {
                Value::Number(val_str, _) => Selection::Const(val_str.to_string()),
                _ => panic!("Unsupported value type"),
            },
            _ => panic!("Unsupported selection type: {:?}", expr),
        }
    }
}

fn unwrap_table_name(parts: &[Ident]) -> String {
    if parts.len() != 1 {
        panic!("Invalid table name {:?}", parts);
    }

    parts[0].value.to_string()
}

fn make_scan(table: &str, alias: &str, meta: &Metadata) -> OpScan {
    let table_meta = meta
        .tables
        .iter()
        .filter(|t| &t.name == table)
        .last()
        .unwrap();

    OpScan {
        tab_name: table.to_string(),
        file: table_meta.file.to_string(),
        filetype: table_meta.filetype.to_string(),
        schema: table_meta.schema.columns.iter().map(|c| c.1).collect(),
        vs: VirtualSchema::from_meta_table(table_meta, alias),
        cfg_name: Option::None,
    }
}

fn plan_joins(
    from_namespace: &HashMap<String, String>,
    potential_joins: &HashSet<(ColRef, ColRef)>,
    meta: &Metadata,
) -> (Op, EqualityFactSet) {
    #[derive(Hash, PartialEq, Eq, Debug)]
    struct Vertex {
        table: String,
        alias: String,
    }

    #[derive(Hash, PartialEq, Eq, Debug)]
    struct Edge {
        left_colref: ColRef,
        right_colref: ColRef,
        left: Rc<Vertex>,
        right: Rc<Vertex>,
    }

    let mut v = HashSet::<Rc<Vertex>>::new();
    for (alias, table) in from_namespace.iter() {
        v.insert(Rc::new(Vertex {
            table: table.to_string(),
            alias: alias.to_string(),
        }));
    }

    // Handle simple case first
    if v.len() == 1 {
        let single_tab = v.iter().next().unwrap();
        return (
            Op::ScanOp(Box::new(make_scan(
                &single_tab.table,
                &single_tab.alias,
                meta,
            ))),
            EqualityFactSet::new(),
        );
    }

    fn _find_vertex(v: &HashSet<Rc<Vertex>>, colref: &ColRef) -> Rc<Vertex> {
        for test_vertex in v {
            if test_vertex.alias == colref.table {
                return Rc::clone(test_vertex);
            }
        }
        panic!("Failed to resolve table {}", colref.table);
    }

    let mut e = HashSet::<Rc<Edge>>::new();
    for potential_join in potential_joins {
        let left = _find_vertex(&v, &potential_join.0);
        let right = _find_vertex(&v, &potential_join.1);
        e.insert(Rc::new(Edge {
            left_colref: potential_join.0.clone(),
            right_colref: potential_join.1.clone(),
            left,
            right,
        }));
    }

    fn _partial_hamiltonian_path(
        v: &HashSet<Rc<Vertex>>,
        e: &HashSet<Rc<Edge>>,
        visited: &HashSet<Rc<Vertex>>,
        current: &Rc<Vertex>,
    ) -> Option<Vec<Rc<Edge>>> {
        if v.len() == visited.len() {
            return Option::Some(Vec::new());
        }

        for edge in e {
            let next = if &edge.left == current && !visited.contains(&edge.right) {
                &edge.right
            } else if &edge.right == current && !visited.contains(&edge.left) {
                &edge.left
            } else {
                continue;
            };

            let mut next_visited = visited.clone();
            next_visited.insert(Rc::clone(next));

            let future = _partial_hamiltonian_path(v, e, &next_visited, &next);
            if let Some(mut path) = future {
                path.push(Rc::clone(edge));
                return Option::Some(path);
            }
        }

        Option::None
    }

    fn _hamiltonian_path(
        v: &HashSet<Rc<Vertex>>,
        e: &HashSet<Rc<Edge>>,
        visited: &HashSet<Rc<Vertex>>,
    ) -> Option<(Vec<Rc<Edge>>, Rc<Vertex>)> {
        if v.len() == visited.len() {
            return Option::None;
        }

        for starting_vertex in v {
            let mut visited = HashSet::<Rc<Vertex>>::new();
            visited.insert(Rc::clone(starting_vertex));

            if let Some(mut path) = _partial_hamiltonian_path(v, e, &visited, starting_vertex) {
                path.reverse(); // We construct on the tail
                return Option::Some((path, Rc::clone(starting_vertex)));
            }
        }

        return Option::None;
    }

    let (path, start) = _hamiltonian_path(&v, &e, &HashSet::new())
        .expect("No way to represent query with equijoins!");

    fn associate_join(
        start_vertex: &Rc<Vertex>,
        current_edge: &Rc<Edge>,
        future: &[Rc<Edge>],
        table_namespace: &HashMap<String, String>,
        meta: &Metadata,
    ) -> Op {
        let (start_cref, next_cref, next_vertex) = if start_vertex == &current_edge.left {
            (
                &current_edge.left_colref,
                &current_edge.right_colref,
                &current_edge.right,
            )
        } else {
            (
                &current_edge.right_colref,
                &current_edge.left_colref,
                &current_edge.left,
            )
        };

        let probe_op = if future.is_empty() {
            Op::ScanOp(Box::new(make_scan(
                &next_vertex.table,
                &next_vertex.alias,
                meta,
            )))
        } else {
            associate_join(
                &next_vertex,
                &future[0],
                &future[1..],
                table_namespace,
                meta,
            )
        };
        let build_op = Op::ScanOp(Box::new(make_scan(
            &start_vertex.table,
            &start_vertex.alias,
            meta,
        )));

        let vs = VirtualSchema::cat(&build_op.virtual_schema(), &probe_op.virtual_schema());
        Op::JoinOp(Box::new(OpJoin {
            probe: probe_op,
            probe_join_attribute: meta
                .attribute_index(&next_cref.resolve_aliases(&table_namespace)),
            build: build_op,
            build_join_attribute: meta
                .attribute_index(&start_cref.resolve_aliases(&table_namespace)),
            vs,
            cfg_name: Option::None,
        }))
    }

    let mut equality_set = EqualityFactSet::new();
    for edge in &path {
        equality_set.insert_rule(&edge.left_colref, &edge.right_colref);
    }

    (
        associate_join(&start, &path[0], &path[1..], from_namespace, meta),
        equality_set,
    )
}

pub fn plan(query: &Query, meta: &Metadata) -> Op {
    let setexpr = &query.body;
    let select = match setexpr {
        SetExpr::Select(select) => select,
        _ => panic!("Not a select"),
    };

    let from = &select.from;

    let mut table_namespace = HashMap::<String, String>::new();

    for table in from {
        let relation = &table.relation;
        if let TableFactor::Table {
            name,
            alias,
            args: _,
            with_hints: _,
        } = relation
        {
            let name = unwrap_table_name(&name.0);
            let alias = if let Some(alias) = alias {
                alias.name.value.to_string()
            } else {
                name.clone()
            };

            table_namespace.insert(alias, name);
        } else {
            panic!("Not a table");
        }
    }

    let selection: Selection = if let Some(filter) = &select.selection {
        filter.into()
    } else {
        Selection::And(vec![])
    };
    let selection = selection.normalized();

    let potential_equijoins = selection.potential_equijoins();

    let (op, equality_set) = plan_joins(&table_namespace, &potential_equijoins, meta);

    let mut root_op = op;

    root_op = selection.apply_filter_ops(root_op, &equality_set);

    root_op
}
