#include "test_helpers.hpp"
#include "test_substrait_c_utils.hpp"
// #include <nlohmann/json.hpp>

using namespace duckdb;
using namespace std;
// using json = nlohmann::json;


duckdb::unique_ptr<QueryResult> ExecuteViaSubstrait(Connection &con, const string &sql) {
	auto proto = con.GetSubstrait(sql);
	return con.FromSubstrait(proto);
}

// std::string PrettyPrintJson(const std::string &input) {
// 	try {
// 		json parsed_json = json::parse(input);
// 		return parsed_json.dump(4); // Indentation of 4 spaces
// 	} catch (const json::parse_error& e) {
// 		std::cerr << "Error parsing JSON: " << e.what() << std::endl;
// 		return "";
// 	}
// }

duckdb::unique_ptr<QueryResult> ExecuteViaSubstraitJSON(Connection &con, const string &sql) {
	auto json_str = con.GetSubstraitJSON(sql);
	return con.FromSubstraitJSON(json_str);
}

void CreateEmployeeTable(Connection &con) {
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
