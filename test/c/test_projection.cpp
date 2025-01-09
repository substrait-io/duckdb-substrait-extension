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

TEST_CASE("Test C Project all columns with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	// This should not have a ProjectRel node
	auto json_str = con.GetSubstraitJSON("SELECT * FROM employees");
	auto expected_json_str = R"({"relations":[{"root":{"input":{"read":{"baseSchema":{"names":["employee_id","name","department_id","salary"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":10,"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{},{"field":1},{"field":2},{"field":3}]},"maintainSingularStruct":true},"namedTable":{"names":["employees"]}}},"names":["employee_id","name","department_id","salary"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	REQUIRE(json_str == expected_json_str);
	auto result = con.FromSubstraitJSON(json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 1, 3, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {120000, 80000, 50000, 95000, 60000}));
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

TEST_CASE("Test C Project two passthrough columns with filter", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	// This should not have a ProjectRel node
	auto json_str = con.GetSubstraitJSON("SELECT name, salary FROM employees where department_id = 1");
	auto expected_json_str = R"({"extensionUris":[{"extensionUriAnchor":1,"uri":"https://github.com/substrait-io/substrait/blob/main/extensions/"}],"extensions":[{"extensionFunction":{"extensionUriReference":1,"functionAnchor":1,"name":"equal:i32_i32"}}],"relations":[{"root":{"input":{"read":{"baseSchema":{"names":["employee_id","name","department_id","salary"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":10,"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"filter":{"scalarFunction":{"functionReference":1,"outputType":{"bool":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":2}},"rootReference":{}}}},{"value":{"literal":{"i32":1}}}]}},"projection":{"select":{"structItems":[{"field":1},{"field":3}]},"maintainSingularStruct":true},"namedTable":{"names":["employees"]}}},"names":["name","salary"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	REQUIRE(json_str == expected_json_str);
	auto result = con.FromSubstraitJSON(json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {"John Doe", "Alice Johnson" }));
	REQUIRE(CHECK_COLUMN(result, 1, {120000, 50000 }));
}

TEST_CASE("Test C Project 1 passthrough column, 1 transformation with column elimination", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	auto json_str = con.GetSubstraitJSON("SELECT name, salary * 1.2 as new_salary FROM employees");
	auto expected_json_str = R"({"extensionUris":[{"extensionUriAnchor":1,"uri":"https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic_decimal.yaml"}],"extensions":[{"extensionFunction":{"extensionUriReference":1,"functionAnchor":1,"name":"multiply:decimal_decimal"}}],"relations":[{"root":{"input":{"project":{"common":{"emit":{"outputMapping":[0,2]}},"input":{"read":{"baseSchema":{"names":["employee_id","name","department_id","salary"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":10,"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{"field":1},{"field":3}]},"maintainSingularStruct":true},"namedTable":{"names":["employees"]}}},"expressions":[{"scalarFunction":{"functionReference":1,"outputType":{"decimal":{"scale":3,"precision":12,"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}}},{"value":{"literal":{"decimal":{"value":"DAAAAAAAAAAAAAAAAAAAAA==","precision":12,"scale":1}}}}]}}]}},"names":["name","new_salary"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	REQUIRE(json_str == expected_json_str);
	auto result = con.FromSubstraitJSON(json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 1, {144000, 96000, 60000, 114000, 72000}));
}

TEST_CASE("Test C Project 1 passthrough column and 1 aggregate transformation", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	auto json_str = con.GetSubstraitJSON("SELECT department_id, AVG(salary) AS avg_salary FROM employees GROUP BY department_id");
	auto expected_json_str = R"({"extensionUris":[{"extensionUriAnchor":1,"uri":"https://github.com/substrait-io/substrait/blob/main/extensions/"}],"extensions":[{"extensionFunction":{"extensionUriReference":1,"functionAnchor":1,"name":"avg:decimal"}}],"relations":[{"root":{"input":{"aggregate":{"input":{"read":{"baseSchema":{"names":["employee_id","name","department_id","salary"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":10,"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{"field":2},{"field":3}]},"maintainSingularStruct":true},"namedTable":{"names":["employees"]}}},"groupings":[{"groupingExpressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}],"measures":[{"measure":{"functionReference":1,"outputType":{"fp64":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}}}]}}]}},"names":["department_id","avg_salary"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	REQUIRE(json_str == expected_json_str);
	auto result = con.FromSubstraitJSON(json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {85000, 70000, 95000}));
}

TEST_CASE("Test C Project on Join with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);
	CreateDepartmentsTable(con);

	auto result = ExecuteViaSubstraitJSON(con,
		"SELECT e.employee_id, e.name, d.department_name "
		"FROM employees e "
		"JOIN departments d "
		"ON e.department_id = d.department_id"
	);

	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"HR", "Engineering", "HR", "Finance", "Engineering"}));
}

TEST_CASE("Test Project with bad plan", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	auto query_json =  R"({"relations":[{"root":{"input":{"project":{"input":{"fetch":{"input":{"read":{"baseSchema":{"names":["i"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{}]},"maintainSingularStruct":true},"namedTable":{"names":["integers"]}}},"count":"5"}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}},"names":["i"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	REQUIRE_THROWS(con.FromSubstraitJSON(query_json));
}

TEST_CASE("Test Project with duplicate columns", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	auto query_json =  R"({"relations":[{"root":{"input":{"project":{"input":{"fetch":{"input":{"read":{"baseSchema":{"names":["i"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{}]},"maintainSingularStruct":true},"namedTable":{"names":["integers"]}}},"count":"5"}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}},"names":["i", "integers"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	auto res1 = con.FromSubstraitJSON(query_json);
	REQUIRE(CHECK_COLUMN(res1, 0, {1, 2, 3, Value()}));
}