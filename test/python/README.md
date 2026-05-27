# Python Integration Tests

These tests verify substrait extension behavior that is difficult to express
in the C or SQL test harnesses — particularly scenarios requiring dynamic plan
manipulation (e.g. rewriting `namedTable.names` before feeding plans back via
`from_substrait`).

## Prerequisites

Python 3.10+ with the following packages:

```bash
pip install duckdb protobuf substrait
```

## Running

From the repository root, after building the extension:

```bash
make release
python test/python/test_named_table_resolution.py
```

Or with an explicit extension path:

```bash
DUCKDB_SUBSTRAIT_EXTENSION_PATH=/path/to/substrait.duckdb_extension \
  python test/python/test_named_table_resolution.py
```

## Tests

### `test_named_table_resolution.py`

Verifies that `from_substrait` correctly resolves multi-component
`namedTable.names` when tables reside in non-default schemas.

The test creates a schema (`myschema`) with a table, generates a substrait plan
template from a same-shaped table in the default schema, rewrites
`namedTable.names` to 1-, 2-, and 3-component formats, and verifies that
`from_substrait` resolves them correctly.

No external databases or ATTACH are required — the test runs entirely in-memory.
