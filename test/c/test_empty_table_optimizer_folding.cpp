#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "test_substrait_c_utils.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test that plan compiled on empty table works after data is inserted", "[substrait-api]") {
	// Bug: When get_substrait_json compiles a query against an empty table,
	// the optimizer folds the query into a LogicalEmptyResult which serializes
	// as a virtualTable with no namedTable reference. The resulting plan
	// always returns 0 rows, even when the table has matching data.
	//
	// A Substrait plan should be portable — correct for any data conforming
	// to the schema, not just the table state at compile time.

	DuckDB db(nullptr);
	Connection con(db);

	// Create table but do NOT insert data yet.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE alerts (id INTEGER, status VARCHAR, reason VARCHAR)"));

	// Compile the plan while the table is empty.
	// The optimizer sees 0 rows and folds to virtualTable {}.
	auto plan_json = GetSubstraitJSON(con, "SELECT id, reason FROM alerts WHERE status = 'PENDING' ORDER BY id");

	// Now insert data into the same table.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO alerts VALUES (1, 'PENDING', 'Large transfer'), "
	                          "(2, 'RESOLVED', 'False alarm'), "
	                          "(3, 'PENDING', 'Suspicious pattern')"));

	// Execute the previously compiled plan.
	// Expected: returns the 2 rows matching status='PENDING'.
	// Actual (bug): returns 0 rows because the plan was folded to empty.
	auto result = FromSubstraitJSON(con, plan_json);
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Large transfer", "Suspicious pattern"}));
}

TEST_CASE("Test that plan compiled on empty table with joins works after data is inserted", "[substrait-api]") {
	// Same bug but with JOINs — the more common real-world scenario.

	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE customers (id INTEGER, name VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount REAL)"));

	// Compile while both tables are empty.
	auto plan_json = GetSubstraitJSON(con,
	    "SELECT c.name, o.amount FROM customers c "
	    "JOIN orders o ON c.id = o.customer_id "
	    "WHERE o.amount > 100 ORDER BY o.amount DESC");

	// Insert data.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO orders VALUES (1, 1, 50.0), (2, 1, 200.0), (3, 2, 150.0)"));

	// Execute the plan — should return 2 rows where amount > 100.
	auto result = FromSubstraitJSON(con, plan_json);
	REQUIRE(CHECK_COLUMN(result, 0, {"Alice", "Bob"}));
	REQUIRE(CHECK_COLUMN(result, 1, {200.0, 150.0}));
}
