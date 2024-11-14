#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/connection_manager.hpp"

#include <chrono>
#include <thread>
#include <iostream>

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


TEST_CASE("Test C VirtualTable input Literal", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);

  auto json = con.GetSubstraitJSON("select * from (values (1, 2),(3, 4))");
  REQUIRE(!json.empty());
  std::cout << json << std::endl;
}

TEST_CASE("Test C VirtualTable input Expression", "[substrait-api]") {
  DuckDB db(nullptr);
  Connection con(db);

  auto json = con.GetSubstraitJSON("select * from (values (1+1,2+2),(3+3,4+4)) as temp(a,b)");
  REQUIRE(!json.empty());
  std::cout << json << std::endl;

}
