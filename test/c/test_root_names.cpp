#include "catch.hpp"
#include "test_helpers.hpp"
#include "test_substrait_c_utils.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test root names preserved with CAST + ORDER BY", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE products (product_name VARCHAR, price DECIMAL(10,2))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO products VALUES ('Widget', 9.99), ('Gadget', 19.99)"));

	// CAST + ORDER BY causes DuckDB to insert a passthrough projection with
	// BoundReferenceExpression nodes that carry positional names (#0, #1).
	// Root names must come from the inner projection with actual aliases.
	auto json_str = GetSubstraitJSON(con,
		"SELECT CAST(product_name AS VARCHAR) AS product_name, "
		"CAST(price AS DOUBLE) AS price FROM products ORDER BY product_name");

	// Root names must be the actual column aliases, not positional (#0, #1)
	REQUIRE(json_str.find("\"names\":[\"product_name\",\"price\"]") != string::npos);
	REQUIRE(json_str.find("\"names\":[\"#0\",\"#1\"]") == string::npos);
}

TEST_CASE("Test root names preserved with arithmetic + ORDER BY", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE products (product_name VARCHAR, price DECIMAL(10,2))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO products VALUES ('Widget', 9.99), ('Gadget', 19.99)"));

	auto json_str = GetSubstraitJSON(con,
		"SELECT product_name, price * 1.1 AS adjusted_price "
		"FROM products ORDER BY product_name");

	REQUIRE(json_str.find("\"names\":[\"product_name\",\"adjusted_price\"]") != string::npos);
	REQUIRE(json_str.find("\"#0\"") == string::npos);
	REQUIRE(json_str.find("\"#1\"") == string::npos);
}

TEST_CASE("Test no crash when plan has no LogicalProjection", "[substrait-api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// NOT NULL is critical: it allows the optimizer to push column selection
	// into the scan and eliminate the LogicalProjection entirely.
	REQUIRE_NO_FAIL(con.Query(
		"CREATE TABLE fin_customer_risk_profile ("
		"customer_id INTEGER NOT NULL, "
		"risk_rating VARCHAR NOT NULL, "
		"aml_risk_level VARCHAR NOT NULL)"));

	// Without the fix, this throws:
	// "Root node has more than 1, or 0 children (0) up to reaching a projection node. Type 30"
	auto json_str = GetSubstraitJSON(con,
		"SELECT customer_id, aml_risk_level FROM fin_customer_risk_profile "
		"WHERE risk_rating = 'HIGH'");

	// Should succeed and produce valid JSON with root names
	REQUIRE(!json_str.empty());
	REQUIRE(json_str.find("\"names\"") != string::npos);
}
