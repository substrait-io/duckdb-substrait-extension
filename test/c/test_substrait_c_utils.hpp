#ifndef TEST_SUBSTRAIT_C_UTILS_HPP
#define TEST_SUBSTRAIT_C_UTILS_HPP

#include "duckdb.hpp"
#include "duckdb/main/connection_manager.hpp"

using namespace duckdb;
void CreateEmployeeTable(Connection& con);
void CreatePartTimeEmployeeTable(Connection& con);
void CreateDepartmentsTable(Connection& con);

duckdb::unique_ptr<QueryResult>  ExecuteViaSubstraitJSON(Connection &con, const std::string &query);
duckdb::unique_ptr<QueryResult>  ExecuteViaSubstrait(Connection &con, const std::string &query);

#endif
