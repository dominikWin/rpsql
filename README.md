# rpsql

A very simple heuristic SQL query planner.
Can output to a format readable by [pythia](https://code.osu.edu/pythia) if processed with convert.py.

### Usage

An example query and plan is in `sample_query/`.

`rpsql <QUERY>`

See `--help` for more.

### SQL Support

- Joins (must be representable as equijoins)
- Predicates (inequalities against constants)
- Grouping and Aggregation (must be done together, at most one function)
- Sorting and Limit (must be done together, limit defaults to `2^31-1`)
- Subqueries
  - Not CTEs
- Projection

Predicate and projection pushdown optimizations are supported.
