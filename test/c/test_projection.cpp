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

TEST_CASE("Test Project simple join on tables with multiple columns", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CALL dbgen(sf=0.000001)"));

	auto query_text_2 = "SELECT extract(year FROM o_orderdate), l_extendedprice * (1 - l_discount) AS amount FROM lineitem, orders WHERE o_orderkey = l_orderkey";
	auto json2 = con.GetSubstraitJSON(query_text_2);
	auto expected_json = R"cust_raw({"extensionUris":[{"extensionUriAnchor":1,"uri":"https://github.com/substrait-io/substrait/blob/main/extensions/"},{"extensionUriAnchor":2,"uri":"https://github.com/substrait-io/substrait/blob/main/extensions/functions_datetime.yaml"},{"extensionUriAnchor":3,"uri":"https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic_decimal.yaml"}],"extensions":[{"extensionFunction":{"extensionUriReference":1,"functionAnchor":1,"name":"equal:i64_i64"}},{"extensionFunction":{"extensionUriReference":2,"functionAnchor":2,"name":"extract:date"}},{"extensionFunction":{"extensionUriReference":3,"functionAnchor":3,"name":"subtract:decimal_decimal"}},{"extensionFunction":{"extensionUriReference":3,"functionAnchor":4,"name":"multiply:decimal_decimal"}}],"relations":[{"root":{"input":{"project":{"common":{"emit":{"outputMapping":[3,4]}},"input":{"project":{"common":{"emit":{"outputMapping":[26,27,28]}},"input":{"join":{"left":{"project":{"common":{"emit":{"outputMapping":[7,6,8,5,9,4,10,3,11,2,12,1,13,0,14]}},"input":{"project":{"common":{"emit":{"outputMapping":[3,2,4,1,5,0,6]}},"input":{"read":{"baseSchema":{"names":["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice","l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct","l_shipmode","l_comment"],"struct":{"types":[{"i64":{"nullability":"NULLABILITY_REQUIRED"}},{"i64":{"nullability":"NULLABILITY_REQUIRED"}},{"i64":{"nullability":"NULLABILITY_REQUIRED"}},{"i64":{"nullability":"NULLABILITY_REQUIRED"}},{"decimal":{"scale":2,"precision":15,"nullability":"NULLABILITY_REQUIRED"}},{"decimal":{"scale":2,"precision":15,"nullability":"NULLABILITY_REQUIRED"}},{"decimal":{"scale":2,"precision":15,"nullability":"NULLABILITY_REQUIRED"}},{"decimal":{"scale":2,"precision":15,"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_REQUIRED"}},{"date":{"nullability":"NULLABILITY_REQUIRED"}},{"date":{"nullability":"NULLABILITY_REQUIRED"}},{"date":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_REQUIRED"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{},{"field":5},{"field":6}]},"maintainSingularStruct":true},"namedTable":{"names":["lineitem"]}}},"expressions":[{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}}]}},"expressions":[{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}}]}},"right":{"project":{"common":{"emit":{"outputMapping":[5,4,6,3,7,2,8,1,9,0,10]}},"input":{"project":{"common":{"emit":{"outputMapping":[2,1,3,0,4]}},"input":{"read":{"baseSchema":{"names":["o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk","o_shippriority","o_comment"],"struct":{"types":[{"i64":{"nullability":"NULLABILITY_REQUIRED"}},{"i64":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_REQUIRED"}},{"decimal":{"scale":2,"precision":15,"nullability":"NULLABILITY_REQUIRED"}},{"date":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_REQUIRED"}},{"i32":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_REQUIRED"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{},{"field":4}]},"maintainSingularStruct":true},"namedTable":{"names":["orders"]}}},"expressions":[{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}}]}},"expressions":[{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}},{"literal":{"null":{}}}]}},"expression":{"scalarFunction":{"functionReference":1,"outputType":{"bool":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":3}},"rootReference":{}}}},{"value":{"selection":{"directReference":{"structField":{"field":18}},"rootReference":{}}}}]}},"type":"JOIN_TYPE_INNER"}},"expressions":[{"selection":{"directReference":{"structField":{"field":7}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":11}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":22}},"rootReference":{}}}]}},"expressions":[{"scalarFunction":{"functionReference":2,"outputType":{"i64":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"enum":"year"},{"value":{"selection":{"directReference":{"structField":{"field":2}},"rootReference":{}}}}]}},{"scalarFunction":{"functionReference":4,"outputType":{"decimal":{"scale":4,"precision":18,"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{}},"rootReference":{}}}},{"value":{"scalarFunction":{"functionReference":3,"outputType":{"decimal":{"scale":2,"precision":16,"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"literal":{"decimal":{"value":"ZAAAAAAAAAAAAAAAAAAAAA==","precision":16,"scale":2}}}},{"value":{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}}}]}}}]}}]}},"names":["\"year\"(o_orderdate)","amount"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})cust_raw";
	REQUIRE(json2 == expected_json);
}
