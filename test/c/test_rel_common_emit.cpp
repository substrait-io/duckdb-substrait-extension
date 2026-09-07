#include "catch.hpp"
#include "test_helpers.hpp"
#include "test_substrait_c_utils.hpp"
#include <nlohmann/json.hpp>

using namespace duckdb;

namespace {
using EmitJSON = nlohmann::json;

EmitJSON EmitField(int index) {
	return {{"selection",
	         {{"directReference", {{"structField", {{"field", index}}}}}, {"rootReference", EmitJSON::object()}}}};
}

EmitJSON EmitRead() {
	return EmitJSON::parse(R"({"read": {
		"baseSchema": {"names": ["number", "text", "flag"], "struct": {
			"types": [{"i64": {"nullability": "NULLABILITY_REQUIRED"}},
			          {"string": {"nullability": "NULLABILITY_REQUIRED"}},
			          {"bool": {"nullability": "NULLABILITY_REQUIRED"}}],
			"nullability": "NULLABILITY_REQUIRED"}},
		"virtualTable": {"expressions": [{"fields": [
			{"literal": {"i64": "10"}}, {"literal": {"string": "x"}},
			{"literal": {"boolean": true}}]}]}
	}})");
}

EmitJSON EmitRelation(const string &kind) {
	auto input = EmitRead();
	if (kind == "read") {
		return input;
	}
	EmitJSON op = {{"input", input}};
	if (kind == "filter") {
		op["condition"] = EmitField(2);
	} else if (kind == "sort") {
		op["sorts"] = {{{"expr", EmitField(0)}, {"direction", "SORT_DIRECTION_ASC_NULLS_LAST"}}};
	} else if (kind == "fetch") {
		op["countExpr"] = {{"literal", {{"i64", "1"}}}};
	} else if (kind == "aggregate") {
		op["groupingExpressions"] = {EmitField(0), EmitField(1), EmitField(2)};
		op["groupings"] = {{{"expressionReferences", {0, 1, 2}}}};
	} else if (kind == "project") {
		op["expressions"] = {EmitField(1)};
	} else if (kind == "set") {
		op = {{"inputs", {input, input}}, {"op", "SET_OP_UNION_ALL"}};
	} else if (kind == "window") {
		op["windowFunctions"] = {{{"functionReference", 2},
		                          {"outputType", {{"i64", {{"nullability", "NULLABILITY_REQUIRED"}}}}},
		                          {"boundsType", "BOUNDS_TYPE_ROWS"},
		                          {"lowerBound", {{"unbounded", EmitJSON::object()}}},
		                          {"upperBound", {{"unbounded", EmitJSON::object()}}}}};
	} else {
		op = {{"left", input}, {"right", input}};
		if (kind != "cross") {
			op["expression"] = {{"literal", {{"boolean", true}}}};
			op["type"] = "JOIN_TYPE_INNER";
		}
	}
	return {{kind, op}};
}

EmitJSON EmitPlan(const EmitJSON &rel, const EmitJSON &names) {
	return {{"relations", {{{"root", {{"input", rel}, {"names", names}}}}}},
	        {"extensions",
	         {{{"extensionFunction", {{"functionAnchor", 1}, {"name", "count"}}}},
	          {{"extensionFunction", {{"functionAnchor", 2}, {"name", "row_number"}}}}}},
	        {"version", {{"minorNumber", 98}}}};
}

void CheckEmitColumns(Connection &con, const EmitJSON &rel, const duckdb::vector<LogicalType> &types,
                      const duckdb::vector<duckdb::vector<Value>> &columns) {
	EmitJSON names = EmitJSON::array();
	duckdb::vector<string> expected_names;
	for (idx_t i = 0; i < types.size(); i++) {
		expected_names.push_back("output_" + to_string(i));
		names.push_back(expected_names.back());
	}
	auto result = FromSubstraitJSON(con, EmitPlan(rel, names).dump());
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->names == expected_names);
	REQUIRE(result->types == types);
	for (idx_t i = 0; i < columns.size(); i++) {
		REQUIRE(CHECK_COLUMN(result, i, columns[i]));
	}
}
} // namespace

TEST_CASE("RelCommon emit reorders, omits and duplicates relation outputs", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (const string kind :
	     {"read", "filter", "sort", "fetch", "join", "aggregate", "cross", "set", "window", "lateralJoin", "project"}) {
		DYNAMIC_SECTION(kind) {
			auto rel = EmitRelation(kind);
			rel[kind]["common"]["emit"]["outputMapping"] = {2, 0, 2};
			idx_t rows = kind == "set" ? 2 : 1;
			CheckEmitColumns(con, rel, {LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::BOOLEAN},
			                 {duckdb::vector<Value>(rows, Value::BOOLEAN(true)),
			                  duckdb::vector<Value>(rows, Value::BIGINT(10)),
			                  duckdb::vector<Value>(rows, Value::BOOLEAN(true))});
		}
	}
}

TEST_CASE("RelCommon emit preserves direct and omitted mappings", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (const auto common : {EmitJSON(), EmitJSON::object(), EmitJSON::parse(R"({"direct": {}})")}) {
		DYNAMIC_SECTION(common.dump()) {
			auto rel = EmitRead();
			if (!common.is_null()) {
				rel["read"]["common"] = common;
			}
			CheckEmitColumns(con, rel, {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BOOLEAN},
			                 {{Value::BIGINT(10)}, {Value("x")}, {Value::BOOLEAN(true)}});
		}
	}
}

TEST_CASE("Read emit follows filtering and read projection", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto rel = EmitRead();
	rel["read"]["filter"] = EmitField(2);
	rel["read"]["projection"] = {{"select", {{"structItems", {{{"field", 1}}, {{"field", 2}}}}}},
	                             {"maintainSingularStruct", true}};
	rel["read"]["common"]["emit"]["outputMapping"] = {1, 0, 1};
	CheckEmitColumns(con, rel, {LogicalType::BOOLEAN, LogicalType::VARCHAR, LogicalType::BOOLEAN},
	                 {{Value::BOOLEAN(true)}, {Value("x")}, {Value::BOOLEAN(true)}});
}

TEST_CASE("Project indexes input after emit and applies its own mapping once", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto input = EmitRead();
	input["read"]["common"]["emit"]["outputMapping"] = {2, 0};
	EmitJSON rel = {
	    {"project",
	     {{"input", input}, {"expressions", {EmitField(1)}}, {"common", {{"emit", {{"outputMapping", {2, 0, 2}}}}}}}}};
	CheckEmitColumns(con, rel, {LogicalType::BIGINT, LogicalType::BOOLEAN, LogicalType::BIGINT},
	                 {{Value::BIGINT(10)}, {Value::BOOLEAN(true)}, {Value::BIGINT(10)}});

	// A parent mapping must address the already emitted Project output.
	rel = {
	    {"filter", {{"input", rel}, {"condition", EmitField(1)}, {"common", {{"emit", {{"outputMapping", {1, 2}}}}}}}}};
	CheckEmitColumns(con, rel, {LogicalType::BOOLEAN, LogicalType::BIGINT},
	                 {{Value::BOOLEAN(true)}, {Value::BIGINT(10)}});
}

TEST_CASE("Emit addresses generated aggregate and window columns", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (const string kind : {"aggregate", "window"}) {
		DYNAMIC_SECTION(kind) {
			auto rel = EmitRelation(kind);
			if (kind == "aggregate") {
				rel[kind]["measures"] = {{{"measure",
				                           {{"functionReference", 1},
				                            {"outputType", {{"i64", {{"nullability", "NULLABILITY_REQUIRED"}}}}}}}}};
			}
			rel[kind]["common"]["emit"]["outputMapping"] = {3, 0, 3};
			CheckEmitColumns(con, rel, {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
			                 {{Value::BIGINT(1)}, {Value::BIGINT(10)}, {Value::BIGINT(1)}});
		}
	}
}

TEST_CASE("Join and cross emit can select either side", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (const string kind : {"join", "cross", "lateralJoin"}) {
		DYNAMIC_SECTION(kind) {
			auto rel = EmitRelation(kind);
			rel[kind]["right"]["read"]["virtualTable"]["expressions"][0]["fields"][0]["literal"]["i64"] = "20";
			rel[kind]["common"]["emit"]["outputMapping"] = {3, 0, 3};
			CheckEmitColumns(con, rel, {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
			                 {{Value::BIGINT(20)}, {Value::BIGINT(10)}, {Value::BIGINT(20)}});
		}
	}
}

TEST_CASE("Emit rejects invalid indices and unsupported zero-column outputs", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	for (const string kind : {"read", "project"}) {
		for (const auto mapping :
		     {EmitJSON::array(), EmitJSON({-1}), EmitJSON({kind == "read" ? 3 : 4}), EmitJSON({2147483647})}) {
			DYNAMIC_SECTION(kind << " " << mapping.dump()) {
				auto rel = EmitRelation(kind);
				rel[kind]["common"]["emit"]["outputMapping"] = mapping;
				auto plan = EmitPlan(rel, mapping.empty() ? EmitJSON::array() : EmitJSON({"output"})).dump();
				REQUIRE_THROWS_WITH(FromSubstraitJSON(con, plan), Catch::Matchers::Contains("emit"));
				REQUIRE_NO_FAIL(con.Query("SELECT 42"));
			}
		}
	}
}

TEST_CASE("Filter sort and fetch use columns before their own emit", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto input = EmitRead();
	auto row = input["read"]["virtualTable"]["expressions"][0];
	row["fields"][0]["literal"]["i64"] = "20";
	row["fields"][2]["literal"]["boolean"] = false;
	input["read"]["virtualTable"]["expressions"].push_back(row);
	row["fields"][0]["literal"]["i64"] = "30";
	row["fields"][2]["literal"]["boolean"] = true;
	input["read"]["virtualTable"]["expressions"].push_back(row);

	EmitJSON rel = {
	    {"filter",
	     {{"input", input}, {"condition", EmitField(2)}, {"common", {{"emit", {{"outputMapping", {1, 0}}}}}}}}};
	rel = {{"sort",
	        {{"input", rel},
	         {"sorts", {{{"expr", EmitField(1)}, {"direction", "SORT_DIRECTION_DESC_NULLS_LAST"}}}},
	         {"common", {{"emit", {{"outputMapping", {1, 0}}}}}}}}};
	rel = {{"fetch",
	        {{"input", rel},
	         {"countExpr", {{"literal", {{"i64", "1"}}}}},
	         {"offsetExpr", {{"literal", {{"i64", "1"}}}}},
	         {"common", {{"emit", {{"outputMapping", {1, 0}}}}}}}}};
	CheckEmitColumns(con, rel, {LogicalType::VARCHAR, LogicalType::BIGINT}, {{Value("x")}, {Value::BIGINT(10)}});
}

TEST_CASE("Emit keeps an empty read empty with the mapped types", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto rel = EmitRead();
	rel["read"]["virtualTable"]["expressions"] = EmitJSON::array();
	rel["read"]["common"]["emit"]["outputMapping"] = {2, 0};
	CheckEmitColumns(con, rel, {LogicalType::BOOLEAN, LogicalType::BIGINT}, {{}, {}});
}

TEST_CASE("Emit on a named read follows the declared schema", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE emit_named(extra INTEGER, number BIGINT, text VARCHAR, flag BOOLEAN)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO emit_named VALUES (99, 10, 'x', true)"));
	auto rel = EmitRead();
	rel["read"].erase("virtualTable");
	rel["read"]["namedTable"] = {{"names", {"emit_named"}}};
	rel["read"]["common"]["emit"]["outputMapping"] = {2, 0};
	CheckEmitColumns(con, rel, {LogicalType::BOOLEAN, LogicalType::BIGINT},
	                 {{Value::BOOLEAN(true)}, {Value::BIGINT(10)}});
}

TEST_CASE("Lateral join does not silently discard its extracted filter emit", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto rel = EmitRelation("lateralJoin");
	auto right = EmitRelation("filter");
	right["filter"]["common"]["emit"]["outputMapping"] = {2, 0};
	rel["lateralJoin"]["right"] = right;
	auto plan = EmitPlan(rel, {"a", "b", "c", "d", "e"}).dump();
	REQUIRE_THROWS_WITH(FromSubstraitJSON(con, plan), Catch::Matchers::Contains("right-side FilterRel"));
}

TEST_CASE("Project does not bypass emit on an empty virtual read", "[substrait-api][emit]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto input = EmitRead();
	input["read"]["virtualTable"]["expressions"] = EmitJSON::array();
	input["read"]["common"]["emit"]["outputMapping"] = {2, 0};
	EmitJSON rel = {
	    {"project",
	     {{"input", input}, {"expressions", {EmitField(1)}}, {"common", {{"emit", {{"outputMapping", {2}}}}}}}}};
	CheckEmitColumns(con, rel, {LogicalType::BIGINT}, {{}});
}
