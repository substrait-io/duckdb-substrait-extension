#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "test_substrait_c_utils.hpp"

#include <chrono>
#include <thread>
#include <iostream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test C Project input columns with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (10), (20), (30)"));
	CreateEmployeeTable(con);

	auto expected_json_str = R"({"relations":[{"root":{"input":{"read":{"baseSchema":{"names":["i"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{}]},"maintainSingularStruct":true},"namedTable":{"names":["integers"]}}},"names":["i"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	auto json_str = con.GetSubstraitJSON("SELECT i FROM integers");
	REQUIRE(json_str == expected_json_str);
	auto result = con.FromSubstraitJSON(json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {10, 20, 30}));
}

TEST_CASE("Test C Project 1 input column 1 transformation with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (10), (20), (30)"));
	CreateEmployeeTable(con);

	auto expected_json_str = R"({"extensionUris":[{"extensionUriAnchor":1,"uri":"https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"}],"extensions":[{"extensionFunction":{"extensionUriReference":1,"functionAnchor":1,"name":"multiply:i32_i32"}}],"relations":[{"root":{"input":{"project":{"input":{"read":{"baseSchema":{"names":["i"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{}]},"maintainSingularStruct":true},"namedTable":{"names":["integers"]}}},"expressions":[{"scalarFunction":{"functionReference":1,"outputType":{"i32":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{}},"rootReference":{}}}},{"value":{"selection":{"directReference":{"structField":{}},"rootReference":{}}}}]}}]}},"names":["i","isquare"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	auto json_str = con.GetSubstraitJSON("SELECT i, i *i as isquare FROM integers");
	REQUIRE(json_str == expected_json_str);
	auto result = con.FromSubstraitJSON(json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {10, 20, 30}));
	REQUIRE(CHECK_COLUMN(result, 1, {100, 400, 900}));
}

TEST_CASE("Test C Project two passthrough columns with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	// This should not have a ProjectRel node
	auto json_str = con.GetSubstraitJSON("SELECT name, salary FROM employees");
	auto expected_json_str = R"({"relations":[{"root":{"input":{"read":{"baseSchema":{"names":["employee_id","name","department_id","salary"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":10,"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{"field":1},{"field":3}]},"maintainSingularStruct":true},"namedTable":{"names":["employees"]}}},"names":["name","salary"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	REQUIRE(json_str == expected_json_str);
	auto result = con.FromSubstraitJSON(json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 1, {120000, 80000, 50000, 95000, 60000}));
}

TEST_CASE("Test C Project one passthrough column one transformation with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	// TODO make this work
	auto json_str = con.GetSubstraitJSON("SELECT name, salary * 1.2 as new_salary FROM employees");
	// auto expected_json_str = R"()";
	// REQUIRE(json_str == expected_json_str);
	auto result = con.FromSubstraitJSON(json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 1, {144000, 96000, 60000, 114000, 72000}));
}
