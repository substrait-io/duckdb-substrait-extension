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
	auto json_str = GetSubstraitJSON(con, "SELECT i FROM integers");
	REQUIRE(json_str == expected_json_str);
	auto result = FromSubstraitJSON(con, json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {10, 20, 30}));
}

TEST_CASE("Test C Project input columns with limit Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (10), (20), (30)"));
	CreateEmployeeTable(con);

	auto json_str = GetSubstraitJSON(con, "SELECT * FROM integers limit 2");
	auto result = FromSubstraitJSON(con, json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {10, 20}));
}

TEST_CASE("Test C Project 1 input column 1 transformation with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (10), (20), (30)"));
	CreateEmployeeTable(con);

	auto expected_json_str = R"({"extensionUris":[{"extensionUriAnchor":1,"uri":"https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"}],"extensions":[{"extensionFunction":{"extensionUriReference":1,"functionAnchor":1,"name":"multiply:i32_i32"}}],"relations":[{"root":{"input":{"project":{"input":{"read":{"baseSchema":{"names":["i"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{}]},"maintainSingularStruct":true},"namedTable":{"names":["integers"]}}},"expressions":[{"scalarFunction":{"functionReference":1,"outputType":{"i32":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{}},"rootReference":{}}}},{"value":{"selection":{"directReference":{"structField":{}},"rootReference":{}}}}]}}]}},"names":["i","isquare"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	auto json_str = GetSubstraitJSON(con, "SELECT i, i *i as isquare FROM integers");
	REQUIRE(json_str == expected_json_str);
	auto result = FromSubstraitJSON(con, json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {10, 20, 30}));
	REQUIRE(CHECK_COLUMN(result, 1, {100, 400, 900}));
}

TEST_CASE("Test C Project all columns with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	// This should not have a ProjectRel node
	auto json_str = GetSubstraitJSON(con, "SELECT * FROM employees");
	auto expected_json_str = R"({"relations":[{"root":{"input":{"read":{"baseSchema":{"names":["employee_id","name","department_id","salary"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":10,"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{},{"field":1},{"field":2},{"field":3}]},"maintainSingularStruct":true},"namedTable":{"names":["employees"]}}},"names":["employee_id","name","department_id","salary"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	REQUIRE(json_str == expected_json_str);
	auto result = FromSubstraitJSON(con, json_str);
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
	auto json_str = GetSubstraitJSON(con, "SELECT name, salary FROM employees");
	auto expected_json_str = R"({"relations":[{"root":{"input":{"read":{"baseSchema":{"names":["employee_id","name","department_id","salary"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":10,"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{"field":1},{"field":3}]},"maintainSingularStruct":true},"namedTable":{"names":["employees"]}}},"names":["name","salary"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	REQUIRE(json_str == expected_json_str);
	auto result = FromSubstraitJSON(con, json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 1, {120000, 80000, 50000, 95000, 60000}));
}

TEST_CASE("Test C Project two passthrough columns with filter", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	// This should not have a ProjectRel node
	auto json_str = GetSubstraitJSON(con, "SELECT name, salary FROM employees where department_id = 1");
	auto expected_json_str = R"({"extensionUris":[{"extensionUriAnchor":1,"uri":"https://github.com/substrait-io/substrait/blob/main/extensions/"}],"extensions":[{"extensionFunction":{"extensionUriReference":1,"functionAnchor":1,"name":"equal:i32_i32"}}],"relations":[{"root":{"input":{"read":{"baseSchema":{"names":["employee_id","name","department_id","salary"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":10,"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"filter":{"scalarFunction":{"functionReference":1,"outputType":{"bool":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":2}},"rootReference":{}}}},{"value":{"literal":{"i32":1}}}]}},"projection":{"select":{"structItems":[{"field":1},{"field":3}]},"maintainSingularStruct":true},"namedTable":{"names":["employees"]}}},"names":["name","salary"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	REQUIRE(json_str == expected_json_str);
	auto result = FromSubstraitJSON(con, json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {"John Doe", "Alice Johnson" }));
	REQUIRE(CHECK_COLUMN(result, 1, {120000, 50000 }));
}

TEST_CASE("Test C Project 1 passthrough column, 1 transformation with column elimination", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	auto json_str = GetSubstraitJSON(con, "SELECT name, salary * 1.2 as new_salary FROM employees");
	auto expected_json_str = R"({"extensionUris":[{"extensionUriAnchor":1,"uri":"https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic_decimal.yaml"}],"extensions":[{"extensionFunction":{"extensionUriReference":1,"functionAnchor":1,"name":"multiply:decimal_decimal"}}],"relations":[{"root":{"input":{"project":{"common":{"emit":{"outputMapping":[0,2]}},"input":{"read":{"baseSchema":{"names":["employee_id","name","department_id","salary"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":10,"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{"field":1},{"field":3}]},"maintainSingularStruct":true},"namedTable":{"names":["employees"]}}},"expressions":[{"scalarFunction":{"functionReference":1,"outputType":{"decimal":{"scale":3,"precision":12,"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}}},{"value":{"literal":{"decimal":{"value":"DAAAAAAAAAAAAAAAAAAAAA==","precision":12,"scale":1}}}}]}}]}},"names":["name","new_salary"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	REQUIRE(json_str == expected_json_str);
	auto result = FromSubstraitJSON(con, json_str);
	REQUIRE(CHECK_COLUMN(result, 0, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 1, {144000, 96000, 60000, 114000, 72000}));
}

TEST_CASE("Test C Project 1 passthrough column and 1 aggregate transformation", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	auto json_str = GetSubstraitJSON(con, "SELECT department_id, AVG(salary) AS avg_salary FROM employees GROUP BY department_id");
	auto expected_json_str = R"({"extensionUris":[{"extensionUriAnchor":1,"uri":"https://github.com/substrait-io/substrait/blob/main/extensions/"}],"extensions":[{"extensionFunction":{"extensionUriReference":1,"functionAnchor":1,"name":"avg:decimal"}}],"relations":[{"root":{"input":{"aggregate":{"input":{"read":{"baseSchema":{"names":["employee_id","name","department_id","salary"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_REQUIRED"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":10,"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{"field":2},{"field":3}]},"maintainSingularStruct":true},"namedTable":{"names":["employees"]}}},"groupings":[{"groupingExpressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}],"measures":[{"measure":{"functionReference":1,"outputType":{"fp64":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}}}]}}]}},"names":["department_id","avg_salary"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	REQUIRE(json_str == expected_json_str);
	auto result = FromSubstraitJSON(con, json_str);
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
	REQUIRE_THROWS(FromSubstraitJSON(con, query_json));
}

TEST_CASE("Test Project with duplicate columns", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	auto query_json =  R"({"relations":[{"root":{"input":{"project":{"input":{"fetch":{"input":{"read":{"baseSchema":{"names":["i"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{}]},"maintainSingularStruct":true},"namedTable":{"names":["integers"]}}},"count":"5"}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}},"names":["i", "integers"]}}],"version":{"minorNumber":53,"producer":"DuckDB"}})";
	auto res1 = FromSubstraitJSON(con, query_json);
	REQUIRE(CHECK_COLUMN(res1, 0, {1, 2, 3, Value()}));
}

TEST_CASE("Test Project simple join on tables with multiple columns", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE orders ( o_orderkey INT PRIMARY KEY, o_orderdate DATE );"));
	REQUIRE_NO_FAIL(con.Query(" CREATE TABLE lineitem ( l_orderkey INT, l_extendedprice DECIMAL(10,2),"
		" l_discount DECIMAL(4,2), FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey)); "));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO orders (o_orderkey, o_orderdate) VALUES "
		" (1, '2022-03-15'), (2, '2023-07-20'), (3, '2024-01-10');"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO lineitem (l_orderkey, l_extendedprice, l_discount) VALUES "
		" (1, 100.00, 0.10), (1, 200.00, 0.05), (2, 150.00, 0.20), (3, 300.00, 0.15);"));

	auto query_text_2 = "SELECT extract(year FROM o_orderdate), l_extendedprice * (1 - l_discount) AS amount FROM lineitem, orders WHERE o_orderkey = l_orderkey";
	auto json_plan = GetSubstraitJSON(con, query_text_2);
	auto res1 = FromSubstraitJSON(con, json_plan);
	REQUIRE(CHECK_COLUMN(res1, 0, {2022, 2022, 2023, 2024}));
}

TEST_CASE("Test tpch Q12", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query(" CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_orderpriority VARCHAR(15))"));
	REQUIRE_NO_FAIL(con.Query(" INSERT INTO orders (o_orderkey, o_orderpriority) VALUES (1, '1-URGENT'), (2, '2-HIGH'), (3, '3-MEDIUM'), (4, '4-LOW'), ;"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE lineitem ( l_orderkey INT, l_shipmode VARCHAR(10), l_commitdate DATE, l_receiptdate DATE, l_shipdate DATE );"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO lineitem (l_orderkey, l_shipmode, l_commitdate, l_receiptdate, l_shipdate) VALUES"
	              "(1, 'MAIL', '1994-03-15', '1994-03-20', '1994-03-10'), "
	              "(2, 'MAIL', '1994-06-12', '1994-06-15', '1994-06-10'), "
	              "(3, 'SHIP', '1994-08-05', '1994-08-10', '1994-08-01'), "
	              "(4, 'SHIP', '1994-11-02', '1994-11-05', '1994-11-01');"));

	auto query_text1 = "SELECT l_shipmode, o_orderpriority FROM orders, lineitem WHERE o_orderkey = l_orderkey  AND l_shipmode IN ('MAIL', 'SHIP') ";
	auto jsonPlan1 = GetSubstraitJSON(con, query_text1);
	auto res1 = FromSubstraitJSON(con, jsonPlan1);
	REQUIRE(CHECK_COLUMN(res1, 0, {"MAIL", "MAIL", "SHIP", "SHIP"}));

	auto query_text2 = "SELECT l_shipmode, o_orderpriority FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= CAST('1994-01-01' AS date) AND l_receiptdate < CAST('1995-01-01' AS date)";
	auto jsonPlan2 = GetSubstraitJSON(con, query_text2);
	auto res2 = FromSubstraitJSON(con, jsonPlan2);
	REQUIRE(CHECK_COLUMN(res2, 0, {"MAIL", "MAIL", "SHIP", "SHIP"}));

	auto query_text3 = "SELECT l_shipmode, sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= CAST('1994-01-01' AS date) AND l_receiptdate < CAST('1995-01-01' AS date) GROUP BY l_shipmode ORDER BY l_shipmode;";
	auto jsonPlan3 = GetSubstraitJSON(con, query_text3);
	auto res3 = FromSubstraitJSON(con, jsonPlan3);
	REQUIRE(CHECK_COLUMN(res3, 0, {"MAIL", "SHIP"}));
}

void CreateTablesForTpcdsQ32(Connection& con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE store_sales ( ss_sold_date_sk INT, ss_item_sk INT, ss_store_sk INT,"
		" ss_cdemo_sk INT, ss_quantity INT, ss_list_price DECIMAL(10,2), ss_coupon_amt DECIMAL(10,2),"
		" ss_sales_price DECIMAL(10,2) );"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE customer_demographics ( cd_demo_sk INT PRIMARY KEY, cd_gender CHAR(1),"
		"cd_marital_status VARCHAR(10), cd_education_status VARCHAR(20) );"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE date_dim ( d_date_sk INT PRIMARY KEY, d_year INT );"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE store ( s_store_sk INT PRIMARY KEY, s_state CHAR(2) );"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE item ( i_item_sk INT PRIMARY KEY, i_item_id VARCHAR(10) );"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO store_sales VALUES "
			"(20000101, 101, 1, 201, 2, 100.00, 10.00, 90.00),  "
			"(20000102, 102, 1, 202, 3, 150.00, 15.00, 135.00), "
			"(20000103, 103, 2, 203, 4, 200.00, 20.00, 180.00), "
			"(20000104, 104, 2, 204, 5, 250.00, 25.00, 225.00), "
			"(20000105, 105, 3, 205, 6, 300.00, 30.00, 270.00);"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO customer_demographics VALUES "
						   "(201, 'M', 'D', 'College'),"
						   "(202, 'M', 'D', 'College'),"
						   "(203, 'M', 'D', 'College'),"
						   "(204, 'F', 'D', 'College'),"
						   "(205, 'M', 'D', 'College')," ));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO date_dim VALUES "
	              "(20000101, 2000), (20000102, 2000), (20000103, 2000), (20000104, 2000), (20000105, 2000);"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO store VALUES  (1, 'TN'), (2, 'TX'), (3, 'NY');"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO item VALUES "
		"(101, 'I001'), (102, 'I002'), (103, 'I003'), (104, 'I004'), (105, 'I005');"));

}

TEST_CASE("Test multiple joins in tpcds Q32", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	CreateTablesForTpcdsQ32(con);
	auto query_text1 = "SELECT i_item_id, s_state, "
		// " GROUPING(s_state) AS g_state, "
		" AVG(ss_quantity) AS agg1, "
		" AVG(ss_list_price) AS agg2, "
		" AVG(ss_coupon_amt) AS agg3, "
		" AVG(ss_sales_price) AS agg4 "
		" FROM store_sales"
		" JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk "
		" JOIN date_dim ON ss_sold_date_sk = d_date_sk "
		" JOIN store ON ss_store_sk = s_store_sk "
		" JOIN item ON ss_item_sk = i_item_sk "
		" WHERE cd_gender = 'M' AND cd_marital_status = 'D' AND cd_education_status = 'College' "
		" AND d_year = 2000 "
		" AND s_state IN ('TN', 'TX', 'NY') "
		" GROUP BY (i_item_id, s_state) "
		" ORDER BY i_item_id "
		// " ORDER BY i_item_id, s_state "
		// " LIMIT 100;"
		;

	auto jsonPlan1 = GetSubstraitJSON(con, query_text1);
	auto res1 = FromSubstraitJSON(con, jsonPlan1);
	// Printer::Print(jsonPlan1);

	// auto res1 = con.Query(query_text1);
	REQUIRE(CHECK_COLUMN(res1, 0, {"I001", "I002", "I003", "I005"}));
	REQUIRE(CHECK_COLUMN(res1, 1, {"TN", "TN", "TX", "NY"}));
	REQUIRE(CHECK_COLUMN(res1, 2, {2.0, 3.0, 4.0,  6.0}));
	REQUIRE(CHECK_COLUMN(res1, 3, {100.0, 150.0, 200.0, 300.0}));
	REQUIRE(CHECK_COLUMN(res1, 4, {10.0, 15.0, 20.0, 30.0}));
	REQUIRE(CHECK_COLUMN(res1, 5, {90.0, 135.0, 180.0, 270.0}));
}

TEST_CASE("Test tpcds Q32", "[substrait-api]") {
	SKIP_TEST("SKIP: Groupings is not handled in GroupBy in to and from substrait");
	return;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	CreateTablesForTpcdsQ32(con);

	auto query_text1 = "SELECT i_item_id, s_state, "
		" GROUPING(s_state) AS g_state, "
		" AVG(ss_quantity) AS agg1, "
		" AVG(ss_list_price) AS agg2, "
		" AVG(ss_coupon_amt) AS agg3, "
		" AVG(ss_sales_price) AS agg4 "
		" FROM store_sales"
		" JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk "
		" JOIN date_dim ON ss_sold_date_sk = d_date_sk "
		" JOIN store ON ss_store_sk = s_store_sk "
		" JOIN item ON ss_item_sk = i_item_sk "
		" WHERE cd_gender = 'M' AND cd_marital_status = 'D' AND cd_education_status = 'College' "
		" AND d_year = 2000 "
		" AND s_state IN ('TN', 'TX', 'NY') "
		" GROUP BY ROLLUP (i_item_id, s_state) "
		" ORDER BY i_item_id, s_state "
		" LIMIT 100;"
		;

	auto jsonPlan1 = GetSubstraitJSON(con, query_text1);
	auto res1 = FromSubstraitJSON(con, jsonPlan1);
	Printer::Print(jsonPlan1);

	// auto res1 = con.Query(query_text1);
	REQUIRE(CHECK_COLUMN(res1, 0, {"I001", "I001", "I002", "I002", "I003", "I003", "I005", "I005", duckdb::Value{}}));
	REQUIRE(CHECK_COLUMN(res1, 1, { "TN", duckdb::Value{}, "TN", duckdb::Value{}, "TX", duckdb::Value{}, "NY", duckdb::Value{}, duckdb::Value{}}));
	REQUIRE(CHECK_COLUMN(res1, 2, {0, 1, 0, 1, 0, 1, 0, 1, 1}));
	REQUIRE(CHECK_COLUMN(res1, 3, {2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 6.0, 6.0, 3.75}));
	REQUIRE(CHECK_COLUMN(res1, 4, {100.0, 100.0, 150.0, 150.0, 200.0, 200.0, 300.0, 300.0, 187.5}));
	REQUIRE(CHECK_COLUMN(res1, 5, {10.0, 10.0, 15.0, 15.0, 20.0, 20.0, 30.0, 30.0, 18.75}));
	REQUIRE(CHECK_COLUMN(res1, 6, {90.0, 90.0, 135.0,  135.0, 180.0, 180.0, 270.0, 270.0, 168.5}));
}