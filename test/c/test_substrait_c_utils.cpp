#include "test_helpers.hpp"
#include "test_substrait_c_utils.hpp"

using namespace duckdb;
using namespace std;


string GetSubstrait(Connection &con,const string &query) {
	duckdb::vector<Value> params;
	params.emplace_back(query);
	auto result = con.TableFunction("get_substrait", params)->Execute();
	auto protobuf = result->FetchRaw()->GetValue(0, 0);
	return protobuf.GetValueUnsafe<string_t>().GetString();
}

string GetSubstraitJSON(Connection &con,const string &query) {
	duckdb::vector<Value> params;
	params.emplace_back(query);
	auto result = con.TableFunction("get_substrait_json", params)->Execute();
	auto protobuf = result->FetchRaw()->GetValue(0, 0);
	return protobuf.GetValueUnsafe<string_t>().GetString();
}

duckdb::unique_ptr<QueryResult> FromSubstrait(Connection &con, const string &proto) {
	duckdb::vector<Value> params;
	params.emplace_back(Value::BLOB_RAW(proto));
	return con.TableFunction("from_substrait", params)->Execute();
}

duckdb::unique_ptr<QueryResult> FromSubstraitJSON(Connection &con, const string &proto) {
	duckdb::vector<Value> params;
	params.emplace_back(proto);
	return con.TableFunction("from_substrait_json", params)->Execute();
}

duckdb::unique_ptr<QueryResult> ExecuteViaSubstrait(Connection &con, const string &sql) {
	auto proto = GetSubstrait(con, sql);
	return FromSubstrait(con,proto);
}

duckdb::unique_ptr<QueryResult> ExecuteViaSubstraitJSON(Connection &con, const string &sql) {
	auto json_str = GetSubstraitJSON(con,sql);
	return FromSubstraitJSON(con,json_str);
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
