#include "catch.hpp"
#include "test_helpers.hpp"
#include "test_substrait_c_utils.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test baseSchema narrower than physical table", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Physical table has 6 columns
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE wide_table (id INTEGER, name VARCHAR, category VARCHAR, price DOUBLE, extra1 INTEGER, extra2 VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO wide_table VALUES "
	    "(1, 'apple', 'fruit', 1.50, 99, 'x'), "
	    "(2, 'banana', 'fruit', 0.75, 88, 'y'), "
	    "(3, 'hammer', 'tool', 15.00, 77, 'z')"));

	// Plan declares baseSchema with only 3 columns (id, name, price)
	// and projects [name, price] via emit mapping [3, 4] (skipping 3 baseSchema cols, taking expressions 0 and 1)
	// Without the fix, emit mapping indices resolve against 6 physical columns, causing wrong results
	auto plan_json = R"({
		"relations": [{
			"root": {
				"input": {
					"project": {
						"common": {
							"emit": {
								"outputMapping": [3, 4]
							}
						},
						"input": {
							"read": {
								"common": {"direct": {}},
								"baseSchema": {
									"names": ["id", "name", "price"],
									"struct": {
										"types": [
											{"i32": {"nullability": "NULLABILITY_REQUIRED"}},
											{"string": {"nullability": "NULLABILITY_REQUIRED"}},
											{"fp64": {"nullability": "NULLABILITY_REQUIRED"}}
										],
										"nullability": "NULLABILITY_REQUIRED"
									}
								},
								"namedTable": {"names": ["wide_table"]}
							}
						},
						"expressions": [
							{"selection": {"directReference": {"structField": {"field": 1}}, "rootReference": {}}},
							{"selection": {"directReference": {"structField": {"field": 2}}, "rootReference": {}}}
						]
					}
				},
				"names": ["name", "price"]
			}
		}],
		"version": {"minorNumber": 85}
	})";

	auto result = FromSubstraitJSON(con, plan_json);
	// name column must contain strings, price column must contain doubles
	REQUIRE(CHECK_COLUMN(result, 0, {"apple", "banana", "hammer"}));
	REQUIRE(CHECK_COLUMN(result, 1, {1.50, 0.75, 15.00}));
}

TEST_CASE("Test baseSchema narrower than physical table with aggregate", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Physical table has 6 columns, plan only uses 3
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE wide_items (id INTEGER, name VARCHAR, category VARCHAR, price DOUBLE, stock INTEGER, warehouse VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO wide_items VALUES "
	    "(1, 'apple', 'fruit', 1.50, 100, 'A'), "
	    "(2, 'apple', 'fruit', 2.00, 50, 'B'), "
	    "(3, 'banana', 'fruit', 0.75, 200, 'A'), "
	    "(4, 'hammer', 'tool', 15.00, 30, 'C')"));

	// Plan: SELECT name, count(id) FROM wide_items GROUP BY name
	// baseSchema declares only [id, name, category] — 3 cols, not 6
	// Grouping by field 1 (name), counting field 0 (id)
	auto plan_json = R"({
		"extensions": [
			{"extensionFunction": {"functionAnchor": 1, "name": "count:any", "extensionUrnReference": 1}}
		],
		"relations": [{
			"root": {
				"input": {
					"aggregate": {
						"input": {
							"read": {
								"common": {"direct": {}},
								"baseSchema": {
									"names": ["id", "name", "category"],
									"struct": {
										"types": [
											{"i32": {"nullability": "NULLABILITY_REQUIRED"}},
											{"string": {"nullability": "NULLABILITY_REQUIRED"}},
											{"string": {"nullability": "NULLABILITY_REQUIRED"}}
										],
										"nullability": "NULLABILITY_REQUIRED"
									}
								},
								"namedTable": {"names": ["wide_items"]}
							}
						},
						"groupings": [{"expressionReferences": [0]}],
						"measures": [{
							"measure": {
								"functionReference": 1,
								"outputType": {"i64": {"nullability": "NULLABILITY_REQUIRED"}},
								"arguments": [{"value": {"selection": {"directReference": {"structField": {}}, "rootReference": {}}}}]
							}
						}],
						"groupingExpressions": [
							{"selection": {"directReference": {"structField": {"field": 1}}, "rootReference": {}}}
						]
					}
				},
				"names": ["name", "cnt"]
			}
		}],
		"version": {"minorNumber": 85},
		"extensionUrns": [{"extensionUrnAnchor": 1, "urn": "extension:io.substrait:functions_aggregate_generic"}]
	})";

	auto result = FromSubstraitJSON(con, plan_json);
	// Verify correct types (order may vary since no ORDER BY)
	REQUIRE(result->ColumnCount() == 2);
	REQUIRE(result->types[0] == LogicalType::VARCHAR);
	REQUIRE(result->types[1] == LogicalType::BIGINT);
}

TEST_CASE("Test baseSchema narrower than physical table with non-matching column order", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Physical table: (alpha, beta, gamma, delta, epsilon)
	// Plan's baseSchema: (beta, delta, alpha) — different ORDER from physical
	// This is the critical case: baseSchema columns are matched by NAME, not position
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE greek (alpha INTEGER, beta VARCHAR, gamma DOUBLE, delta INTEGER, epsilon VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO greek VALUES "
	    "(10, 'ten', 1.1, 100, 'X'), "
	    "(20, 'twenty', 2.2, 200, 'Y'), "
	    "(30, 'thirty', 3.3, 300, 'Z')"));

	// Plan declares baseSchema [beta, delta, alpha] (3 cols in different order than physical)
	// and does a simple passthrough project (emit all 3)
	auto plan_json = R"({
		"relations": [{
			"root": {
				"input": {
					"project": {
						"common": {
							"emit": {"outputMapping": [3, 4, 5]}
						},
						"input": {
							"read": {
								"common": {"direct": {}},
								"baseSchema": {
									"names": ["beta", "delta", "alpha"],
									"struct": {
										"types": [
											{"string": {"nullability": "NULLABILITY_REQUIRED"}},
											{"i32": {"nullability": "NULLABILITY_REQUIRED"}},
											{"i32": {"nullability": "NULLABILITY_REQUIRED"}}
										],
										"nullability": "NULLABILITY_REQUIRED"
									}
								},
								"namedTable": {"names": ["greek"]}
							}
						},
						"expressions": [
							{"selection": {"directReference": {"structField": {}}, "rootReference": {}}},
							{"selection": {"directReference": {"structField": {"field": 1}}, "rootReference": {}}},
							{"selection": {"directReference": {"structField": {"field": 2}}, "rootReference": {}}}
						]
					}
				},
				"names": ["beta", "delta", "alpha"]
			}
		}],
		"version": {"minorNumber": 85}
	})";

	auto result = FromSubstraitJSON(con, plan_json);
	// beta is VARCHAR, delta is INTEGER, alpha is INTEGER
	REQUIRE(CHECK_COLUMN(result, 0, {"ten", "twenty", "thirty"}));
	REQUIRE(CHECK_COLUMN(result, 1, {100, 200, 300}));
	REQUIRE(CHECK_COLUMN(result, 2, {10, 20, 30}));
}
