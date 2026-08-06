#include "catch.hpp"
#include <string>
#include <nlohmann/json-schema.hpp>
#include <tojson.hpp>

using nlohmann::json;
using nlohmann::json_schema::json_validator;

// Both paths are baked in at configure time (see test/c/CMakeLists.txt): the
// dialect lives in the source tree and the schema in the installed
// substrait-extensions data dir, so neither depends on the working directory.
static const std::string DIALECT_PATH = DIALECT_YAML;
static const std::string SCHEMA_PATH = std::string(SUBSTRAIT_DATA_DIR) + "/text/dialect_schema.yaml";

TEST_CASE("Dialect YAML conforms to schema", "[dialect-validation]") {
	json dialect = tojson::loadyaml(DIALECT_PATH);
	json schema = tojson::loadyaml(SCHEMA_PATH);

	json_validator validator;
	validator.set_root_schema(schema);
	try {
		validator.validate(dialect);
	} catch (const std::exception &e) {
		FAIL(DIALECT_PATH << " does not conform to " << SCHEMA_PATH << ": " << e.what());
	}
}
