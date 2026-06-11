"""Reproduce: from_substrait crashes when baseSchema column count exceeds actual table columns.

Bug: TransformProjectOp uses the runtime column count (input_rel->Columns().size())
to compute expression indices, but the emit outputMapping was generated based on
the plan's baseSchema column count.  When the actual table has fewer columns than
the plan declares, the expression index overflows, accessing out-of-bounds memory
in the protobuf repeated field and causing a segfault.

Root cause (src/from_substrait.cpp, TransformProjectOp):
    expressions[i] = TransformExpr(
        sop.project().expressions(mapping[i] - num_input_columns), &iterator);

    Here num_input_columns comes from input_rel->Columns().size() (the actual table),
    but mapping[i] was computed against the plan's baseSchema column count.  If the
    actual table has fewer columns, (mapping[i] - num_input_columns) can exceed the
    number of project expressions, reading garbage memory.

Scenario:
    A plan is generated against a table with 10 columns and uses emit mapping [10]
    (i.e., project expression at index 0). At execution time the actual table has
    only 4 columns, so num_input_columns=4, and the code tries to access
    expressions[10-4] = expressions[6] which does not exist -> SIGSEGV.

Expected behavior (after fix):
    The extension should raise an error ("expression index out of range" or
    "schema mismatch") instead of crashing.

Usage:
    make release
    pip install duckdb protobuf substrait
    python test/python/test_schema_mismatch_crash.py
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
    repo_root = Path(__file__).resolve().parent.parent.parent
    SUBSTRAIT_EXT = str(
        repo_root / "build" / "release" / "extension" / "substrait" / "substrait.duckdb_extension"
    )

if not Path(SUBSTRAIT_EXT).exists():
    sys.exit(f"ERROR: Extension not found at {SUBSTRAIT_EXT}\n       Run 'make release' first.")

print(f"DuckDB version : {duckdb.__version__}")
print(f"Extension      : {SUBSTRAIT_EXT}")
print()

# --- Create a test SQLite database with a SMALL table (4 columns) -----------

tmpdir = tempfile.mkdtemp(prefix="substrait_crash_test_")
DB_PATH = Path(tmpdir) / "testdb.sqlite"

conn = sqlite3.connect(str(DB_PATH))
conn.execute("""
    CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        order_date TEXT,
        total REAL
    )
""")
conn.execute("INSERT INTO orders VALUES (1, 100, '2024-01-01', 99.99)")
conn.execute("INSERT INTO orders VALUES (2, 101, '2024-01-02', 149.50)")
conn.commit()
conn.close()

print(f"Test database: {DB_PATH} (orders table with 4 columns)")
print()

# --- Build a substrait plan with a baseSchema declaring MORE columns --------
#
# The plan declares 10 columns in baseSchema for the "orders" table but the
# actual SQLite table only has 4.  The emit mapping [10] points to the first
# project expression (index 0 relative to 10 input columns), but at runtime
# num_input_columns=4, so the code computes expressions[10-4]=expressions[6]
# which is out of bounds (only 1 expression exists).

PLAN_JSON = json.dumps({
    "relations": [{
        "root": {
            "input": {
                "project": {
                    "common": {
                        "emit": {
                            "outputMapping": [10]
                        }
                    },
                    "input": {
                        "read": {
                            "common": {"direct": {}},
                            "baseSchema": {
                                "names": [
                                    "order_id", "customer_id", "order_date", "total",
                                    "col5", "col6", "col7", "col8", "col9", "col10"
                                ],
                                "struct": {
                                    "types": [
                                        {"i32": {"nullability": "NULLABILITY_REQUIRED"}},
                                        {"i32": {"nullability": "NULLABILITY_NULLABLE"}},
                                        {"string": {"nullability": "NULLABILITY_NULLABLE"}},
                                        {"fp64": {"nullability": "NULLABILITY_NULLABLE"}},
                                        {"string": {"nullability": "NULLABILITY_NULLABLE"}},
                                        {"string": {"nullability": "NULLABILITY_NULLABLE"}},
                                        {"string": {"nullability": "NULLABILITY_NULLABLE"}},
                                        {"string": {"nullability": "NULLABILITY_NULLABLE"}},
                                        {"string": {"nullability": "NULLABILITY_NULLABLE"}},
                                        {"string": {"nullability": "NULLABILITY_NULLABLE"}},
                                    ],
                                    "nullability": "NULLABILITY_REQUIRED"
                                }
                            },
                            "namedTable": {"names": ["testdb", "orders"]}
                        }
                    },
                    "expressions": [{
                        "selection": {
                            "directReference": {"structField": {"field": 3}},
                            "rootReference": {}
                        }
                    }]
                }
            },
            "names": ["total"]
        }
    }],
    "version": {"minorNumber": 85}
})

# --- Execute the plan --------------------------------------------------------

print("Executing plan with schema mismatch (10 declared columns, 4 actual)...", flush=True)
print("If unpatched, this will SIGSEGV (exit code 139).", flush=True)
print(flush=True)

plan = Parse(PLAN_JSON, plan_pb2.Plan())

con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
con.load_extension(SUBSTRAIT_EXT)
con.install_extension("sqlite")
con.load_extension("sqlite")
con.sql(f"ATTACH '{DB_PATH}' (TYPE sqlite)")

try:
    result = con.sql("CALL from_substrait(?)", params=[plan.SerializePartialToString()])
    rows = result.fetchall()
    # If we get here with correct results, the fix properly handled the mapping
    print(f"OK — query returned {len(rows)} rows: {rows}")
    print("Extension handled the schema mismatch gracefully.")
    exit_code = 0
except Exception as e:
    # An exception is acceptable (better than a crash!)
    print(f"Exception (acceptable — no crash): {e}")
    exit_code = 0
finally:
    con.close()

# --- Cleanup -----------------------------------------------------------------
import shutil
shutil.rmtree(tmpdir, ignore_errors=True)

print()
print("=" * 70)
print("PASSED — extension did not crash on schema mismatch.")
print("=" * 70)
sys.exit(exit_code)
