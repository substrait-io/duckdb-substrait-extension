#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/connection_manager.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test C Get and To Substrait API", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);
  con.EnableQueryVerification();
  // create the database
  REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
  REQUIRE_NO_FAIL(
      con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

  auto proto = con.GetSubstrait("select * from integers limit 2");
  auto result = con.FromSubstrait(proto);

  REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

  REQUIRE_THROWS(con.GetSubstrait("select * from p"));

  REQUIRE_THROWS(con.FromSubstrait("this is not valid"));
}

TEST_CASE("Test C Get and To Json-Substrait API", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);
  con.EnableQueryVerification();
  // create the database
  REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
  REQUIRE_NO_FAIL(
      con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

  auto json = con.GetSubstraitJSON("select * from integers limit 2");
  auto result = con.FromSubstraitJSON(json);

  REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

  REQUIRE_THROWS(con.GetSubstraitJSON("select * from p"));

  REQUIRE_THROWS(con.FromSubstraitJSON("this is not valid"));
}

duckdb::unique_ptr<QueryResult> ExecuteViaSubstrait(Connection &con, const string &sql) {
  auto proto = con.GetSubstrait(sql);
  return con.FromSubstrait(proto);
}

duckdb::unique_ptr<QueryResult> ExecuteViaSubstraitJSON(Connection &con, const string &sql) {
  auto json_str = con.GetSubstraitJSON(sql);
  return con.FromSubstraitJSON(json_str);
}

void CreateEmployeeTable(Connection& con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE employees ("
					  "employee_id INTEGER PRIMARY KEY, "
					  "name VARCHAR(100), "
					  "department_id INTEGER, "
					  "salary DECIMAL(10, 2))"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO employees VALUES "
						  "(1, 'John Doe', 1, 120000), "
						  "(2, 'Jane Smith', 2, 80000), "
						  "(3, 'Alice Johnson', 1, 50000), "
						  "(4, 'Bob Brown', 3, 95000), "
						  "(5, 'Charlie Black', 2, 60000)"));
}

void CreatePartTimeEmployeeTable(Connection& con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE part_time_employees ("
					  "id INTEGER PRIMARY KEY, "
					  "name VARCHAR(100), "
					  "department_id INTEGER, "
					  "hourly_rate DECIMAL(10, 2))"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO part_time_employees VALUES "
						  "(6, 'David White', 1, 30000), "
						  "(7, 'Eve Green', 2, 40000)"));
}

void CreateDepartmentsTable(Connection& con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE departments (department_id INTEGER PRIMARY KEY, department_name VARCHAR(100))"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO departments VALUES "
						  "(1, 'HR'), "
						  "(2, 'Engineering'), "
						  "(3, 'Finance')"));
}

TEST_CASE("Test C CTAS Select columns with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE employee_salaries AS "
		"SELECT name, salary FROM employees"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from employee_salaries");
	REQUIRE(CHECK_COLUMN(result, 0, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 1, {120000, 80000, 50000, 95000, 60000}));
}

TEST_CASE("Test C CTAS Filter with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE filtered_employees AS "
		"SELECT * FROM employees "
		"WHERE salary > 80000;"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from filtered_employees");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Bob Brown"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {120000, 95000}));
}

TEST_CASE("Test C CTAS Case_When with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE categorized_employees AS "
		"SELECT name, "
		"CASE "
			"WHEN salary > 100000 THEN 'High' "
			"WHEN salary BETWEEN 60000 AND 100000 THEN 'Medium' "
			"ELSE 'Low' "
		"END AS salary_category "
		"FROM employees"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from categorized_employees");
	REQUIRE(CHECK_COLUMN(result, 0, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"High", "Medium", "Low", "Medium", "Medium"}));
}

TEST_CASE("Test C CTAS OrderBy with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE ordered_employees AS "
		"SELECT * FROM employees "
		"ORDER BY salary DESC"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from ordered_employees");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4, 2, 5, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Bob Brown", "Jane Smith", "Charlie Black", "Alice Johnson"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 3, 2, 2, 1}));
	REQUIRE(CHECK_COLUMN(result, 3, {120000, 95000, 80000, 60000, 50000}));
}

TEST_CASE("Test C CTAS SubQuery with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE high_salary_employees AS "
		"SELECT * "
		"FROM ( "
			"SELECT employee_id, name, salary "
			"FROM employees "
			"WHERE salary > 100000)"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from high_salary_employees");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe"}));
	REQUIRE(CHECK_COLUMN(result, 2, {120000}));
}

TEST_CASE("Test C CTAS Distinct with Substrait API", "[substrait-api]") {
	SKIP_TEST("SKIP: Distinct operator has unsupported child type");  // TODO fix TransformDistinct
	return;

	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);
	ExecuteViaSubstraitJSON(con, "CREATE TABLE unique_departments AS "
		"SELECT DISTINCT department_id FROM employees"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from unique_departments");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
}

TEST_CASE("Test C CTAS Aggregation with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE department_summary AS "
		"SELECT department_id, COUNT(*) AS employee_count "
		"FROM employees "
		"GROUP BY department_id"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from department_summary");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 2, 1}));
}

TEST_CASE("Test C CTAS Join with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);
	CreateDepartmentsTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE employee_departments AS "
		"SELECT e.employee_id, e.name, d.department_name "
		"FROM employees e "
		"JOIN departments d "
		"ON e.department_id = d.department_id"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from employee_departments");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"HR", "Engineering", "HR", "Finance", "Engineering"}));
}

TEST_CASE("Test C CTAS Union with Substrait API", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	CreateEmployeeTable(con);
	CreatePartTimeEmployeeTable(con);

	ExecuteViaSubstraitJSON(con, "CREATE TABLE all_employees AS "
		"SELECT employee_id, name, department_id, salary "
		"FROM employees "
		"UNION "
		"SELECT id, name, department_id, hourly_rate * 2000 AS salary "
		"FROM part_time_employees "
		"ORDER BY employee_id"
	);

	auto result = ExecuteViaSubstrait(con, "SELECT * from all_employees");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, 7}));
	REQUIRE(CHECK_COLUMN(result, 1, {"John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Black", "David White", "Eve Green"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 1, 3, 2, 1, 2}));
}