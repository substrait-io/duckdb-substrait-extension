#include "catch.hpp"
#include "test_helpers.hpp"
#include "test_substrait_c_utils.hpp"
#include <nlohmann/json.hpp>

using namespace duckdb;

namespace {
using TimestampJSON = nlohmann::json;

TimestampJSON TimestampLiteral(int precision, int64_t value) {
	return {{"literal", {{"precisionTimestamp", {{"precision", precision}, {"value", std::to_string(value)}}}}}};
}

TimestampJSON TimestampRead(int precision, const duckdb::vector<int64_t> &values) {
	auto fields = TimestampJSON::array();
	for (auto value : values) {
		fields.push_back({{"fields", {TimestampLiteral(precision, value)}}});
	}
	return {{"read",
	         {{"baseSchema",
	           {{"names", {"value"}},
	            {"struct",
	             {{"nullability", "NULLABILITY_REQUIRED"},
	              {"types",
	               {{{"precisionTimestamp", {{"precision", precision}, {"nullability", "NULLABILITY_REQUIRED"}}}}}}}}}},
	          {"virtualTable", {{"expressions", fields}}}}}};
}

TimestampJSON TimestampProject(const TimestampJSON &expression) {
	auto input = TimestampJSON::parse(R"({"read": {
		"baseSchema": {"struct": {"nullability": "NULLABILITY_REQUIRED"}},
		"virtualTable": {"expressions": [{}]}
	}})");
	return {{"project", {{"input", input}, {"expressions", {expression}}}}};
}

TimestampJSON TimestampPlan(const TimestampJSON &rel) {
	return {{"relations", {{{"root", {{"input", rel}, {"names", {"value"}}}}}}},
	        {"extensions", {{{"extensionFunction", {{"functionAnchor", 1}, {"name", "equal"}}}}}},
	        {"version", {{"minorNumber", 98}}}};
}

void CheckTimestampResult(Connection &con, const TimestampJSON &rel, const LogicalType &type,
                          const duckdb::vector<int64_t> &values) {
	auto result = FromSubstraitJSON(con, TimestampPlan(rel).dump());
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->types == duckdb::vector<LogicalType> {type});
	idx_t row = 0;
	while (auto chunk = result->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			REQUIRE(row < values.size());
			const auto value = chunk->GetValue(0, i);
			REQUIRE_FALSE(value.IsNull());
			// Inspect the stored units without a timestamp cast that could hide a precision loss.
			REQUIRE(value.GetValueUnsafe<int64_t>() == values[row]);
			row++;
		}
	}
	REQUIRE(row == values.size());
}
} // namespace

TEST_CASE("Timestamp literals preserve precision in virtual tables", "[substrait-api][timestamp-literal]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto index = GENERATE(0, 1, 2, 3);
	const duckdb::vector<int> precisions {0, 3, 6, 9};
	const duckdb::vector<LogicalType> types {LogicalType::TIMESTAMP_S, LogicalType::TIMESTAMP_MS,
	                                         LogicalType::TIMESTAMP, LogicalType::TIMESTAMP_NS};
	const duckdb::vector<int64_t> values {-1234567891, -1, 0, 1, 1234567891};
	CAPTURE(precisions[index]);
	CheckTimestampResult(con, TimestampRead(precisions[index], values), types[index], values);
	CheckTimestampResult(con, TimestampRead(precisions[index], {}), types[index], {});
}

TEST_CASE("Timestamp literals preserve precision in projects", "[substrait-api][timestamp-literal]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto value = GENERATE(int64_t(-1234567891), int64_t(-1), int64_t(0), int64_t(1), int64_t(1234567891));
	CheckTimestampResult(con, TimestampProject(TimestampLiteral(9, value)), LogicalType::TIMESTAMP_NS, {value});
	// A project over an existing column must retain the appended literal's type as well.
	TimestampJSON project = {{"project",
	                          {{"input", TimestampRead(9, {0})},
	                           {"expressions", {TimestampLiteral(9, value)}},
	                           {"common", {{"emit", {{"outputMapping", {1}}}}}}}}};
	CheckTimestampResult(con, project, LogicalType::TIMESTAMP_NS, {value});
}

TEST_CASE("Timestamp literals distinguish nanoseconds in filters", "[substrait-api][timestamp-literal]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto value = GENERATE(int64_t(-1234567891), int64_t(-1), int64_t(1), int64_t(1234567891));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE timestamps(value TIMESTAMP_NS)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO timestamps VALUES (make_timestamp_ns(" + std::to_string(value) +
	                          ")), (make_timestamp_ns(" + std::to_string(value + 1) + "))"));
	auto input = TimestampRead(9, {});
	input["read"].erase("virtualTable");
	input["read"]["namedTable"] = {{"names", {"timestamps"}}};
	TimestampJSON selection = {
	    {"selection",
	     {{"directReference", {{"structField", {{"field", 0}}}}}, {"rootReference", TimestampJSON::object()}}}};
	TimestampJSON condition = {{"scalarFunction",
	                            {{"functionReference", 1},
	                             {"outputType", {{"bool", {{"nullability", "NULLABILITY_REQUIRED"}}}}},
	                             {"arguments", {{{"value", selection}}, {{"value", TimestampLiteral(9, value)}}}}}}};
	CheckTimestampResult(con, {{"filter", {{"input", input}, {"condition", condition}}}}, LogicalType::TIMESTAMP_NS,
	                     {value});
}

TEST_CASE("Timestamp precision survives nested literals", "[substrait-api][timestamp-literal]") {
	DuckDB db(nullptr);
	Connection con(db);
	TimestampJSON literal = {
	    {"literal",
	     {{"list", {{"values", {TimestampLiteral(9, -1)["literal"], TimestampLiteral(9, 1234567891)["literal"]}}}}}}};
	auto result = FromSubstraitJSON(con, TimestampPlan(TimestampProject(literal)).dump());
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->types == duckdb::vector<LogicalType> {LogicalType::LIST(LogicalType::TIMESTAMP_NS)});
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	const auto value = chunk->GetValue(0, 0);
	const auto &children = ListValue::GetChildren(value);
	REQUIRE(children.size() == 2);
	REQUIRE_FALSE(children[0].IsNull());
	REQUIRE_FALSE(children[1].IsNull());
	REQUIRE(children[0].GetValueUnsafe<int64_t>() == -1);
	REQUIRE(children[1].GetValueUnsafe<int64_t>() == 1234567891);
}

TEST_CASE("Unsupported timestamp literal precisions match schema errors", "[substrait-api][timestamp-literal]") {
	DuckDB db(nullptr);
	Connection con(db);
	const auto precision = GENERATE(-1, 1, 2, 4, 5, 7, 8, 10, 12);
	const auto message = "Unsupported timestamp precision: " + std::to_string(precision);
	REQUIRE_THROWS_WITH(FromSubstraitJSON(con, TimestampPlan(TimestampRead(precision, {})).dump()),
	                    Catch::Matchers::Contains(message));
	REQUIRE_THROWS_WITH(FromSubstraitJSON(con, TimestampPlan(TimestampRead(precision, {1234567891})).dump()),
	                    Catch::Matchers::Contains(message));
	REQUIRE_THROWS_WITH(
	    FromSubstraitJSON(con, TimestampPlan(TimestampProject(TimestampLiteral(precision, 1234567891))).dump()),
	    Catch::Matchers::Contains(message));
	REQUIRE_NO_FAIL(con.Query("SELECT 42"));
}
