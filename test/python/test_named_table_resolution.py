"""Reproduce: namedTable.names with multiple components fails on unpatched extension.

This script demonstrates that from_substrait does not resolve tables in
attached databases when namedTable.names has more than one component
(e.g. ["ecommerce", "suppliers"]).

Background:
  Tables in attached databases (via ATTACH) live in a separate catalog
  namespace. Single-component names (["suppliers"]) cannot resolve to them
  because Catalog::GetEntry with INVALID_CATALOG and DEFAULT_SCHEMA only
  searches the default catalog. Multi-component names are required.

Expected behavior (after patch):
  - 2-component ["db_name", "table_name"] resolves to the attached DB
  - 3-component ["catalog", "schema", "table"] also resolves

Actual behavior (before patch):
  - The extension only reads names(0), treating "ecommerce" as a table
    name and ignoring "suppliers" entirely, producing:
    "Table with name ecommerce does not exist!"

Usage:
  # Build the extension first:
  make release

  # Install Python dependencies (requires Python 3.10+):
  pip install duckdb protobuf substrait

  # Run (from repo root):
  python test/python/test_named_table_resolution.py

  # Or with explicit extension path:
  DUCKDB_SUBSTRAIT_EXTENSION_PATH=build/release/extension/substrait/substrait.duckdb_extension \
    python test/python/test_named_table_resolution.py
"""

import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

import duckdb
from google.protobuf.json_format import Parse
from substrait import plan_pb2

# --- Configuration -----------------------------------------------------------

SUBSTRAIT_EXT = os.environ.get("DUCKDB_SUBSTRAIT_EXTENSION_PATH")
if not SUBSTRAIT_EXT:
    # Default to the build output relative to repo root
    repo_root = Path(__file__).resolve().parent.parent.parent
    SUBSTRAIT_EXT = str(
        repo_root / "build" / "release" / "extension" / "substrait" / "substrait.duckdb_extension"
    )

# --- Preflight checks --------------------------------------------------------

if not Path(SUBSTRAIT_EXT).exists():
    sys.exit(f"ERROR: Extension not found at {SUBSTRAIT_EXT}\n       Run 'make release' first.")

print(f"DuckDB version : {duckdb.__version__}")
print(f"Extension      : {SUBSTRAIT_EXT}")
print()

# --- Create temporary SQLite test databases ----------------------------------

tmpdir = tempfile.mkdtemp(prefix="substrait_test_")
ECOMMERCE_DB = Path(tmpdir) / "ecommerce.sqlite"
HR_DB = Path(tmpdir) / "hr.sqlite"

# ecommerce database with a suppliers table
conn = sqlite3.connect(str(ECOMMERCE_DB))
conn.execute("CREATE TABLE suppliers (supplier_id INTEGER PRIMARY KEY, supplier_name TEXT)")
conn.execute("INSERT INTO suppliers VALUES (1, 'TechSource Inc'), (2, 'Digital Dynamics'), (3, 'ElectroWorld')")
conn.commit()
conn.close()

# hr database with an employees table
conn = sqlite3.connect(str(HR_DB))
conn.execute("CREATE TABLE employees (employee_id INTEGER PRIMARY KEY, first_name TEXT)")
conn.execute("INSERT INTO employees VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
conn.commit()
conn.close()

print(f"Test databases : {tmpdir}/")
print()

# --- Step 1: Generate a template substrait plan from a native DuckDB table ---
#
# We create a simple native table with the same column layout as
# ecommerce.suppliers (supplier_id BIGINT, supplier_name VARCHAR), generate
# a substrait plan via get_substrait_json, then swap namedTable.names to
# point at the attached SQLite table.

con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
con.load_extension(SUBSTRAIT_EXT)
con.sql("CREATE TABLE suppliers (supplier_id BIGINT, supplier_name VARCHAR)")
con.sql("INSERT INTO suppliers VALUES (1, 'placeholder'), (2, 'placeholder2')")
TEMPLATE_PLAN_JSON = con.sql(
    "CALL get_substrait_json('SELECT supplier_id, supplier_name FROM suppliers LIMIT 5')"
).fetchone()[0]
con.close()


# --- Helpers -----------------------------------------------------------------

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


def run_substrait(plan_json: str, databases: list[Path]) -> tuple[bool, str]:
    """Execute a substrait plan (via binary protobuf) against attached SQLite DBs.

    Returns (success, message).
    """
    plan = Parse(plan_json, plan_pb2.Plan())
    con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    con.load_extension(SUBSTRAIT_EXT)
    con.install_extension("sqlite")
    con.load_extension("sqlite")
    for db_path in databases:
        con.sql(f"ATTACH '{db_path}' (TYPE sqlite)")
    try:
        result = con.sql("CALL from_substrait(?)", params=[plan.SerializePartialToString()])
        rows = result.fetchall()
        con.close()
        return True, f"{len(rows)} rows returned, first: {rows[0] if rows else '(empty)'}"
    except Exception as e:
        con.close()
        return False, str(e)


# --- Step 2: Run the tests ---------------------------------------------------

tests = [
    {
        "description": '2-component: ["ecommerce", "suppliers"]',
        "names": ["ecommerce", "suppliers"],
        "databases": [ECOMMERCE_DB],
        "should_pass": True,
    },
    {
        "description": '3-component: ["ecommerce", "main", "suppliers"]',
        "names": ["ecommerce", "main", "suppliers"],
        "databases": [ECOMMERCE_DB],
        "should_pass": True,
    },
    {
        "description": '2-component cross-DB: ["hr", "employees"] with both DBs attached',
        "names": ["hr", "employees"],
        "databases": [ECOMMERCE_DB, HR_DB],
        "should_pass": True,
    },
    {
        "description": '1-component: ["suppliers"] (no DB prefix, expected to fail)',
        "names": ["suppliers"],
        "databases": [ECOMMERCE_DB],
        "should_pass": False,
    },
]

print("=" * 70)
print("TEST RESULTS")
print("=" * 70)

all_passed = True
for t in tests:
    plan_json = set_named_table_names(TEMPLATE_PLAN_JSON, t["names"])
    ok, msg = run_substrait(plan_json, t["databases"])

    if t["should_pass"]:
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_passed = False
    else:
        # This test is expected to fail; we just document the behavior
        status = "XFAIL (expected)" if not ok else "XPASS (unexpected success)"

    print(f"\n  [{status}] {t['description']}")
    print(f"         namedTable.names = {json.dumps(t['names'])}")
    print(f"         {'Result' if ok else 'Error'}: {msg}")

print()
print("=" * 70)
if all_passed:
    print("ALL TESTS PASSED - multi-component namedTable.names resolution works!")
else:
    print("SOME TESTS FAILED - the extension does not resolve multi-component names.")
    print("Apply the fix to src/from_substrait.cpp, rebuild (make release),")
    print("and re-run this script.")
print("=" * 70)

# --- Cleanup -----------------------------------------------------------------
import shutil
shutil.rmtree(tmpdir, ignore_errors=True)

sys.exit(0 if all_passed else 1)
