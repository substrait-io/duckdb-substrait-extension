#include "catch.hpp"
#include <fstream>
#include <string>
#include <nlohmann/json-schema.hpp>
#include <tojson.hpp>

using nlohmann::json;
using nlohmann::json_schema::json_validator;
using namespace tojson;

TEST_CASE("Dialect YAML file exists", "[dialect-validation]") {
	const std::string dialectPath = "duckdb_dialect.yaml";
	std::ifstream file(dialectPath);
	REQUIRE(file.good());
}

TEST_CASE("Dialect schema file exists", "[dialect-validation]") {
	const std::string schemaPath = std::string(DIALECT_SCHEMA_DIR) + "/dialect_schema.yaml";
	std::ifstream file(schemaPath);
	REQUIRE(file.good());
}

TEST_CASE("Dialect YAML conforms to schema", "[dialect-validation]") {
	json dialect = loadyaml("duckdb_dialect.yaml");
	json schema = loadyaml(std::string(DIALECT_SCHEMA_DIR) + "/dialect_schema.yaml");

    json_validator validator;
	REQUIRE_NOTHROW(validator.set_root_schema(schema));
	REQUIRE_NOTHROW(validator.validate(dialect));
}

