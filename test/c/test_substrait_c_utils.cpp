#include "test_helpers.hpp"
#include "test_substrait_c_utils.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"

using namespace duckdb;
using namespace std;

// The tests expect a bind error to throw and an execution error to come back in the result.
static duckdb::unique_ptr<QueryResult> CallTableFunction(Connection &con, const string &name, const Value &argument) {
	duckdb::vector<duckdb::unique_ptr<ParsedExpression>> children;
	children.push_back(ConstantExpression::FromValue(argument));
	auto table_function = make_uniq<TableFunctionRef>();
	table_function->function = make_uniq<FunctionExpression>(Identifier(name), std::move(children));
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());
	node->from_table = std::move(table_function);
	auto statement = make_uniq<SelectStatement>();
	statement->node = std::move(node);
	auto &context = *con.context;
	context.RunFunctionInTransaction([&]() {
		auto to_bind = statement->Copy();
		Binder::CreateBinder(context)->Bind(*to_bind);
	});
	return con.Query(std::move(statement));
}

string GetSubstrait(Connection &con,const string &query) {
	auto result = CallTableFunction(con, "get_substrait", Value(query));
	auto protobuf = result->FetchRaw()->GetValue(0, 0);
	return protobuf.GetValueUnsafe<string_t>().GetString();
}

string GetSubstraitJSON(Connection &con,const string &query) {
	auto result = CallTableFunction(con, "get_substrait_json", Value(query));
	auto protobuf = result->FetchRaw()->GetValue(0, 0);
	return protobuf.GetValueUnsafe<string_t>().GetString();
}

duckdb::unique_ptr<QueryResult> FromSubstrait(Connection &con, const string &proto) {
	return CallTableFunction(con, "from_substrait", Value::BLOB_RAW(proto));
}

duckdb::unique_ptr<QueryResult> FromSubstraitJSON(Connection &con, const string &proto) {
	return CallTableFunction(con, "from_substrait_json", Value(proto));
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
