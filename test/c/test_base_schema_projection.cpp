#include "catch.hpp"
#include "test_helpers.hpp"
#include "test_substrait_c_utils.hpp"

#include <nlohmann/json.hpp>
#include <vector>

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

TEST_CASE("Test named table baseSchema reorders equal-width columns", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE reordered_table (a INTEGER, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO reordered_table VALUES (11, 22)"));

	auto plan_json = R"({"relations":[{"root":{"input":{"read":{"baseSchema":{"names":["b","a"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"namedTable":{"names":["reordered_table"]}}},"names":["b","a"]}}]})";
	auto result = FromSubstraitJSON(con, plan_json);
	REQUIRE(CHECK_COLUMN(result, 0, {22}));
	REQUIRE(CHECK_COLUMN(result, 1, {11}));
}

static string LocalParquetPlan(const string &parquet_path, const string &base_schema_json,
                               const std::vector<std::string> &root_names) {
	auto plan = nlohmann::json::parse(R"({"relations":[{"root":{"input":{"read":{"localFiles":{"items":[{"parquet":{}}]}}}}}]})");
	auto &root = plan["relations"][0]["root"];
	auto &read = root["input"]["read"];
	read["baseSchema"] = nlohmann::json::parse(base_schema_json);
	read["localFiles"]["items"][0]["uriFile"] = parquet_path;
	root["names"] = root_names;
	return plan.dump();
}

TEST_CASE("Test localFiles baseSchema binds Parquet columns by name", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto parquet_path = TestCreatePath("local_files_base_schema.parquet");
	TestDeleteFile(parquet_path);
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT 11::INTEGER AS a, 22::INTEGER AS b, 33::INTEGER AS extra) TO '" +
	                          parquet_path + "' (FORMAT PARQUET)"));

	auto plan_json = LocalParquetPlan(
	    parquet_path,
	    R"({"names":["b","a"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}})",
	    {"b", "a"});

	auto result = FromSubstraitJSON(con, plan_json);
	REQUIRE(CHECK_COLUMN(result, 0, {22}));
	REQUIRE(CHECK_COLUMN(result, 1, {11}));
}

TEST_CASE("Test localFiles projection uses baseSchema column order", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto parquet_path = TestCreatePath("local_files_projected_base_schema.parquet");
	TestDeleteFile(parquet_path);
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT 11::INTEGER AS a, 22::INTEGER AS b, 33::INTEGER AS extra) TO '" +
	                          parquet_path + "' (FORMAT PARQUET)"));

	auto plan = nlohmann::json::parse(LocalParquetPlan(
	    parquet_path,
	    R"({"names":["b","a"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}})",
	    {"b"}));
	plan["relations"][0]["root"]["input"]["read"]["projection"] =
	    nlohmann::json::parse(R"({"select":{"structItems":[{}]}})");
	auto result = FromSubstraitJSON(con, plan.dump());
	REQUIRE(result->ColumnCount() == 1);
	REQUIRE(CHECK_COLUMN(result, 0, {22}));
}

TEST_CASE("Test localFiles baseSchema preserves declared Hive columns", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto partition_dir = TestCreatePath("part=blue");
	TestCreateDirectory(partition_dir);
	auto parquet_path = TestJoinPath(partition_dir, "local_files_partition.parquet");
	TestDeleteFile(parquet_path);
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT 11::INTEGER AS a, 22::INTEGER AS b) TO '" + parquet_path +
	                          "' (FORMAT PARQUET)"));

	auto declared_plan = LocalParquetPlan(
	    parquet_path,
	    R"({"names":["part","a"],"struct":{"types":[{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}})",
	    {"part", "a"});
	auto declared_result = FromSubstraitJSON(con, declared_plan);
	REQUIRE(CHECK_COLUMN(declared_result, 0, {"blue"}));
	REQUIRE(CHECK_COLUMN(declared_result, 1, {11}));

	auto pruned_plan = LocalParquetPlan(
	    parquet_path,
	    R"({"names":["b","a"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}})",
	    {"b", "a"});
	auto pruned_result = FromSubstraitJSON(con, pruned_plan);
	REQUIRE(CHECK_COLUMN(pruned_result, 0, {22}));
	REQUIRE(CHECK_COLUMN(pruned_result, 1, {11}));
}

TEST_CASE("Test localFiles baseSchema skips nested field names", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto parquet_path = TestCreatePath("local_files_nested_base_schema.parquet");
	TestDeleteFile(parquet_path);
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT {'child': 11}::STRUCT(child INTEGER) AS payload, "
	                          "22::INTEGER AS a, 33::INTEGER AS extra) TO '" +
	                          parquet_path + "' (FORMAT PARQUET)"));

	auto plan_json = LocalParquetPlan(
	    parquet_path,
	    R"({"names":["a","payload","child"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}})",
	    {"a", "payload"});
	auto result = FromSubstraitJSON(con, plan_json);
	REQUIRE(result->ColumnCount() == 2);
	REQUIRE(CHECK_COLUMN(result, 0, {22}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::STRUCT({{"child", Value::INTEGER(11)}})}));
}

TEST_CASE("Test localFiles baseSchema handles collection struct names", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto parquet_path = TestCreatePath("local_files_collection_structs.parquet");
	TestDeleteFile(parquet_path);
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT [{'x': 1}]::STRUCT(x INTEGER)[] AS list_col, "
	                          "map(['k'], [{'y': 2}])::MAP(VARCHAR, STRUCT(y INTEGER)) AS map_col, "
	                          "{'z': 3}::STRUCT(z INTEGER) AS payload) TO '" +
	                          parquet_path + "' (FORMAT PARQUET)"));

	auto plan = nlohmann::json::parse(GetSubstraitJSON(con, "SELECT * FROM read_parquet('" + parquet_path + "')"));
	auto &names = plan["relations"][0]["root"]["input"]["read"]["baseSchema"]["names"];
	REQUIRE(names == nlohmann::json::array({"list_col", "map_col", "payload", "z"}));
	auto list_value = Value::LIST({Value::STRUCT({{"x", Value::INTEGER(1)}})});
	auto map_value = Value::MAP(LogicalType::VARCHAR, LogicalType::STRUCT({{"y", LogicalType::INTEGER}}),
	                            {Value("k")}, {Value::STRUCT({{"y", Value::INTEGER(2)}})});
	auto legacy_result = FromSubstraitJSON(con, plan.dump());
	REQUIRE(legacy_result->ColumnCount() == 3);
	REQUIRE(CHECK_COLUMN(legacy_result, 0, {list_value}));
	REQUIRE(CHECK_COLUMN(legacy_result, 1, {map_value}));
	REQUIRE(CHECK_COLUMN(legacy_result, 2, {Value::STRUCT({{"z", Value::INTEGER(3)}})}));

	names = nlohmann::json::array({"list_col", "map_col", "payload"});
	auto top_level_result = FromSubstraitJSON(con, plan.dump());
	REQUIRE(top_level_result->ColumnCount() == 3);
	REQUIRE(CHECK_COLUMN(top_level_result, 0, {list_value}));
	REQUIRE(CHECK_COLUMN(top_level_result, 1, {map_value}));
	REQUIRE(CHECK_COLUMN(top_level_result, 2, {Value::STRUCT({{"z", Value::INTEGER(3)}})}));

	names = nlohmann::json::array({"list_col", "x", "map_col", "y", "payload", "z"});
	auto complete_result = FromSubstraitJSON(con, plan.dump());
	REQUIRE(complete_result->ColumnCount() == 3);
	REQUIRE(CHECK_COLUMN(complete_result, 0, {list_value}));
	REQUIRE(CHECK_COLUMN(complete_result, 1, {map_value}));
	REQUIRE(CHECK_COLUMN(complete_result, 2, {Value::STRUCT({{"z", Value::INTEGER(3)}})}));

	names[1] = "wrong";
	REQUIRE_THROWS(FromSubstraitJSON(con, plan.dump()));
	names[1] = "x";
	names[3] = "wrong";
	REQUIRE_THROWS(FromSubstraitJSON(con, plan.dump()));
	names = nlohmann::json::array({"list_col", "map_col", "payload", "wrong"});
	REQUIRE_THROWS(FromSubstraitJSON(con, plan.dump()));
	names = nlohmann::json::array({"list_col", "x", "map_col", "payload", "z"});
	REQUIRE_THROWS(FromSubstraitJSON(con, plan.dump()));
}

TEST_CASE("Test localFiles baseSchema resolves nested type aliases", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto parquet_path = TestCreatePath("local_files_alias_base_schema.parquet");
	TestDeleteFile(parquet_path);
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT {'child': 11}::STRUCT(child INTEGER) AS payload, "
	                          "22::INTEGER AS a) TO '" + parquet_path + "' (FORMAT PARQUET)"));

	auto plan = nlohmann::json::parse(LocalParquetPlan(
	    parquet_path,
	    R"({"names":["payload","child","a"],"struct":{"types":[{"alias":{"typeAliasReference":1}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}})",
	    {"payload", "child", "a"}));
	plan["typeAliases"] = nlohmann::json::parse(
	    R"([{"typeAliasAnchor":1,"type":{"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_NULLABLE"}}}])");
	auto result = FromSubstraitJSON(con, plan.dump());
	REQUIRE(CHECK_COLUMN(result, 0, {Value::STRUCT({{"child", Value::INTEGER(11)}})}));
	REQUIRE(CHECK_COLUMN(result, 1, {22}));
}

TEST_CASE("Test localFiles rejects reordered nested baseSchema fields", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto parquet_path = TestCreatePath("local_files_reordered_nested_base_schema.parquet");
	TestDeleteFile(parquet_path);
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT {'x': 11, 'y': 22}::STRUCT(x INTEGER, y INTEGER) AS payload) TO '" +
	                          parquet_path + "' (FORMAT PARQUET)"));

	auto plan_json = LocalParquetPlan(
	    parquet_path,
	    R"({"names":["payload","y","x"],"struct":{"types":[{"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}})",
	    {"payload", "y", "x"});
	REQUIRE_THROWS(FromSubstraitJSON(con, plan_json));
}
