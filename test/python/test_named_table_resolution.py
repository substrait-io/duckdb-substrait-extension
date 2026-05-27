"""Reproduce: namedTable.names with multiple components fails on unpatched extension.

This demonstrates that from_substrait does not resolve tables correctly
when namedTable.names has more than one component (e.g. ["myschema", "suppliers"]).

Background:
  TransformReadOp only reads names(0) and passes DEFAULT_SCHEMA to
  TableInfo, so multi-component names like ["myschema", "suppliers"]
  are misinterpreted as a table named "myschema" in schema "main".

Expected behavior (after patch):
  - 2-component ["schema", "table"] resolves correctly
  - 3-component ["catalog", "schema", "table"] also resolves

Actual behavior (before patch):
  - names(0) is used as the table name, ignoring subsequent components:
    "Table with name myschema does not exist!"

Usage:
  make release
  pip install duckdb protobuf substrait
  python test/python/test_named_table_resolution.py
"""

import json
import os
import sys
from pathlib import Path

import duckdb
from google.protobuf.json_format import Parse
from substrait import plan_pb2

# --- Configuration -----------------------------------------------------------

SUBSTRAIT_EXT = os.environ.get("DUCKDB_SUBSTRAIT_EXTENSION_PATH")
if not SUBSTRAIT_EXT:
    repo_root = Path(__file__).resolve().parent.parent.parent
    SUBSTRAIT_EXT = str(
        repo_root / "build" / "release" / "extension" / "substrait" / "substrait.duckdb_extension"
    )

if not Path(SUBSTRAIT_EXT).exists():
    sys.exit(f"ERROR: Extension not found at {SUBSTRAIT_EXT}\n       Run 'make release' first.")

print(f"DuckDB version : {duckdb.__version__}")
print(f"Extension      : {SUBSTRAIT_EXT}")
print()

# --- Helper ------------------------------------------------------------------

def set_named_table_names(plan_json: str, new_names: list[str]) -> str:
    """Replace all namedTable.names in a plan JSON with new_names."""
    obj = json.loads(plan_json)

    def _replace(node):
        if isinstance(node, dict):
            for key in list(node.keys()):
                if key == "namedTable":
                    node[key]["names"] = new_names
                else:
                    _replace(node[key])
        elif isinstance(node, list):
            for item in node:
                _replace(item)

    _replace(obj)
    return json.dumps(obj)


# --- Setup -------------------------------------------------------------------

con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
con.load_extension(SUBSTRAIT_EXT)

# Create a table in a non-default schema
con.sql("CREATE SCHEMA myschema")
con.sql("CREATE TABLE myschema.suppliers (supplier_id BIGINT, supplier_name VARCHAR)")
con.sql("INSERT INTO myschema.suppliers VALUES (1, 'Acme Corp'), (2, 'Widgets Ltd'), (3, 'Gizmo Inc')")

# Create same-shaped table in default schema to generate a template substrait plan
con.sql("CREATE TABLE suppliers (supplier_id BIGINT, supplier_name VARCHAR)")
con.sql("INSERT INTO suppliers VALUES (0, 'template')")
TEMPLATE_PLAN_JSON = con.sql(
    "CALL get_substrait_json('SELECT supplier_id, supplier_name FROM suppliers LIMIT 5')"
).fetchone()[0]

# --- Tests -------------------------------------------------------------------

tests = [
    {
        "description": '2-component: ["myschema", "suppliers"]',
        "names": ["myschema", "suppliers"],
        "should_pass": True,
    },
    {
        "description": '3-component: ["memory", "myschema", "suppliers"]',
        "names": ["memory", "myschema", "suppliers"],
        "should_pass": True,
    },
    {
        "description": '1-component: ["suppliers"] (default schema, baseline)',
        "names": ["suppliers"],
        "should_pass": True,
    },
]

print("=" * 70)
print("TEST RESULTS")
print("=" * 70)

all_passed = True
for t in tests:
    plan_json = set_named_table_names(TEMPLATE_PLAN_JSON, t["names"])
    plan = Parse(plan_json, plan_pb2.Plan())
    try:
        result = con.sql("CALL from_substrait(?)", params=[plan.SerializePartialToString()])
        rows = result.fetchall()
        status = "PASS" if t["should_pass"] else "XPASS (unexpected)"
        print(f"\n  [{status}] {t['description']}")
        print(f"         {len(rows)} rows returned, first: {rows[0]}")
    except Exception as e:
        if t["should_pass"]:
            status = "FAIL"
            all_passed = False
        else:
            status = "XFAIL (expected)"
        print(f"\n  [{status}] {t['description']}")
        print(f"         Error: {e}")

con.close()

print()
print("=" * 70)
if all_passed:
    print("ALL TESTS PASSED - multi-component namedTable.names resolution works!")
else:
    print("SOME TESTS FAILED - the extension does not resolve multi-component names.")
    print("Apply the fix to src/from_substrait.cpp, rebuild, and re-run.")
print("=" * 70)

sys.exit(0 if all_passed else 1)
