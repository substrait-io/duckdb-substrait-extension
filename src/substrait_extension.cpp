#define DUCKDB_EXTENSION_MAIN

#include "substrait_extension.hpp"
#include "from_substrait_ast.hpp"
#include "to_substrait.hpp"

#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#endif

namespace duckdb {

struct ToSubstraitFunctionData : public TableFunctionData {
	ToSubstraitFunctionData() = default;
	string query;
	bool enable_optimizer = false;
	//! We will fail the conversion on possible warnings
	bool strict = false;
	bool finished = false;
	//! Original options from the connection
	ClientConfig original_config;
	set<OptimizerType> original_disabled_optimizers;

	// Setup configurations
	void PrepareConnection(ClientContext &context) {
		// First collect original options
		original_config = context.config;
		original_disabled_optimizers = DBConfig::GetConfig(context).options.disabled_optimizers;

		// The user might want to disable the optimizer of the new connection
		context.config.enable_optimizer = enable_optimizer;
		context.config.use_replacement_scans = false;
		// We want for sure to disable the internal compression optimizations.
		// These are DuckDB specific, no other system implements these. Also,
		// respect the user's settings if they chose to disable any specific optimizers.
		//
		// The InClauseRewriter optimization converts large `IN` clauses to a
		// "mark join" against a `ColumnDataCollection`, which may not make
		// sense in other systems and would complicate the conversion to Substrait.
		set<OptimizerType> disabled_optimizers = DBConfig::GetConfig(context).options.disabled_optimizers;
		// Disable optimizers that cause issues with Substrait generation
		// Keep FILTER_PUSHDOWN, EXPRESSION_REWRITER enabled - consumer needs filters separated from aggregates
		disabled_optimizers.insert(OptimizerType::IN_CLAUSE);
		disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
		disabled_optimizers.insert(OptimizerType::MATERIALIZED_CTE);
		disabled_optimizers.insert(OptimizerType::TOP_N);
		disabled_optimizers.insert(OptimizerType::COLUMN_LIFETIME);
		disabled_optimizers.insert(OptimizerType::COMMON_AGGREGATE);
		DBConfig::GetConfig(context).options.disabled_optimizers = disabled_optimizers;
	}

	unique_ptr<LogicalOperator> ExtractPlan(ClientContext &context) {
		PrepareConnection(context);
		unique_ptr<LogicalOperator> plan;
		try {
			Parser parser(context.GetParserOptions());
			parser.ParseQuery(query);

			Planner planner(context);
			planner.CreatePlan(std::move(parser.statements[0]));
			D_ASSERT(planner.plan);

			plan = std::move(planner.plan);

			if (context.config.enable_optimizer) {
				Optimizer optimizer(*planner.binder, context);
				plan = optimizer.Optimize(std::move(plan));
			}

			ColumnBindingResolver resolver;
			ColumnBindingResolver::Verify(*plan);
			resolver.VisitOperator(*plan);
			plan->ResolveOperatorTypes();
		} catch (...) {
			CleanupConnection(context);
			throw;
		}

		CleanupConnection(context);
		return plan;
	}

	// Reset configuration
	void CleanupConnection(ClientContext &context) const {
		DBConfig::GetConfig(context).options.disabled_optimizers = original_disabled_optimizers;
		context.config = original_config;
	}
};

static void SetOptions(ToSubstraitFunctionData &function, const ClientConfig &config,
                       const named_parameter_map_t &named_params) {
	bool optimizer_option_set = false;
	for (const auto &param : named_params) {
		auto loption = StringUtil::Lower(param.first);
		// If the user has explicitly requested to enable/disable the optimizer when
		// generating Substrait, then that takes precedence.
		if (loption == "enable_optimizer") {
			function.enable_optimizer = BooleanValue::Get(param.second);
			optimizer_option_set = true;
		}
		if (loption == "strict") {
			function.strict = BooleanValue::Get(param.second);
		}
	}
	if (!optimizer_option_set) {
		// If the user has not specified what they want, fall back to the settings
		// on the connection (e.g. if the optimizer was disabled by the user at
		// the connection level, it would be surprising to enable the optimizer
		// when generating Substrait).
		function.enable_optimizer = config.enable_optimizer;
	}
}

static unique_ptr<ToSubstraitFunctionData> InitToSubstraitFunctionData(const ClientConfig &config,
                                                                       TableFunctionBindInput &input) {
	auto result = make_uniq<ToSubstraitFunctionData>();
	result->query = input.inputs[0].ToString();
	SetOptions(*result, config, input.named_parameters);
	return result;
}

static unique_ptr<FunctionData> ToSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("Plan Blob");
	return InitToSubstraitFunctionData(context.config, input);
}

static unique_ptr<FunctionData> ToJsonBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Json");
	return InitToSubstraitFunctionData(context.config, input);
}

//! This function matches results of substrait plans with direct Duckdb queries
//! Is only executed when pragma enable_verification = true
//! IMPORTANT: Uses the NEW AST-based implementation (SubstraitToAST) instead of
//! the old Relation-based implementation, allowing us to remove legacy code.
static void VerifySubstraitRoundtrip(unique_ptr<LogicalOperator> &query_plan, ClientContext &context,
                                     ToSubstraitFunctionData &data, const string &serialized, bool is_json) {
	// Execute the original SQL query
	auto actual_result = context.Query(data.query, false);

	// Materialize the actual result
	unique_ptr<MaterializedQueryResult> actual_materialized;
	if (actual_result->type == QueryResultType::STREAM_RESULT) {
		auto &stream_query = actual_result->Cast<StreamQueryResult>();
		actual_materialized = stream_query.Materialize();
	} else if (actual_result->type == QueryResultType::MATERIALIZED_RESULT) {
		actual_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(actual_result));
	}

	// NEW: Execute Substrait plan using AST-based approach via prepared statement
	// This is more efficient than string concatenation and avoids nested query parsing issues
	unique_ptr<MaterializedQueryResult> substrait_materialized;

	// IMPORTANT: Temporarily disable verification to prevent infinite recursion
	bool original_verification_state = context.config.query_verification_enabled;
	context.config.query_verification_enabled = false;

	try {
		if (is_json) {
			// Use prepared statement for JSON input
			auto stmt = context.Prepare("SELECT * FROM from_substrait_json(?)");
			auto pending = stmt->PendingQuery(Value(serialized));
			auto substrait_result = pending->Execute();

			if (substrait_result->type == QueryResultType::STREAM_RESULT) {
				auto &stream_query = substrait_result->Cast<StreamQueryResult>();
				substrait_materialized = stream_query.Materialize();
			} else {
				substrait_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(substrait_result));
			}
		} else {
			// Use prepared statement for binary blob input (more efficient than hex encoding!)
			auto stmt = context.Prepare("SELECT * FROM from_substrait(?)");
			auto pending = stmt->PendingQuery(Value::BLOB(serialized));
			auto substrait_result = pending->Execute();

			if (substrait_result->type == QueryResultType::STREAM_RESULT) {
				auto &stream_query = substrait_result->Cast<StreamQueryResult>();
				substrait_materialized = stream_query.Materialize();
			} else {
				substrait_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(substrait_result));
			}
		}

		// Restore verification state
		context.config.query_verification_enabled = original_verification_state;

	} catch (...) {
		// Restore verification state on exception
		context.config.query_verification_enabled = original_verification_state;
		throw;
	}

	// Compare results
	substrait_materialized->names = actual_materialized->names;
	auto &actual_col_coll = actual_materialized->Collection();
	auto &subs_col_coll = substrait_materialized->Collection();
	string error_message;
	if (!ColumnDataCollection::ResultEquals(actual_col_coll, subs_col_coll, error_message)) {
		query_plan->Print();
		Printer::Print("Substrait plan verification failed");
		actual_col_coll.Print();
		subs_col_coll.Print();
		throw InternalException("The query result of DuckDB's query plan does not match Substrait : " + error_message);
	}
}

static void VerifyBlobRoundtrip(unique_ptr<LogicalOperator> &query_plan, ClientContext &context,
                                ToSubstraitFunctionData &data, const string &serialized) {
	VerifySubstraitRoundtrip(query_plan, context, data, serialized, false);
}

static void VerifyJSONRoundtrip(unique_ptr<LogicalOperator> &query_plan, ClientContext &context,
                                ToSubstraitFunctionData &data, const string &serialized) {
	VerifySubstraitRoundtrip(query_plan, context, data, serialized, true);
}

static void ToSubFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                  unique_ptr<LogicalOperator> &query_plan, string &serialized) {
	output.SetCardinality(1);
	query_plan = data.ExtractPlan(context);
	auto transformer_d2s = DuckDBToSubstrait(context, *query_plan, data.strict);
	serialized = transformer_d2s.SerializeToString();
	output.SetValue(0, 0, Value::BLOB_RAW(serialized));
}

static void ToJsonFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                   unique_ptr<LogicalOperator> &query_plan, string &serialized) {
	output.SetCardinality(1);
	query_plan = data.ExtractPlan(context);
	auto transformer_d2s = DuckDBToSubstrait(context, *query_plan, data.strict);
	;
	serialized = transformer_d2s.SerializeToJson();
	output.SetValue(0, 0, serialized);
}

static void ToSubFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ToSubstraitFunctionData>();
	if (data.finished) {
		return;
	}
	unique_ptr<LogicalOperator> query_plan;
	string serialized;
	ToSubFunctionInternal(context, data, output, query_plan, serialized);

	data.finished = true;

	if (!context.config.query_verification_enabled) {
		return;
	}
	VerifyBlobRoundtrip(query_plan, context, data, serialized);
	// Also run the ToJson path and verify round-trip for that
	DataChunk other_output;
	other_output.Initialize(context, {LogicalType::VARCHAR});
	ToJsonFunctionInternal(context, data, other_output, query_plan, serialized);
	VerifyJSONRoundtrip(query_plan, context, data, serialized);
}

static void ToJsonFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ToSubstraitFunctionData>();
	if (data.finished) {
		return;
	}
	unique_ptr<LogicalOperator> query_plan;
	string serialized;
	ToJsonFunctionInternal(context, data, output, query_plan, serialized);

	data.finished = true;

	if (!context.config.query_verification_enabled) {
		return;
	}
	VerifyJSONRoundtrip(query_plan, context, data, serialized);
	// Also run the ToJson path and verify round-trip for that
	DataChunk other_output;
	other_output.Initialize(context, {LogicalType::BLOB});
	ToSubFunctionInternal(context, data, other_output, query_plan, serialized);
	VerifyBlobRoundtrip(query_plan, context, data, serialized);
}

static unique_ptr<TableRef> SubstraitBindReplace(ClientContext &context, TableFunctionBindInput &input, bool is_json) {
	// NEW: Use SubstraitToAST for pure AST transformation (no Relations, no binding)
	//
	// This avoids lock re-entrancy issues by building AST nodes directly
	// instead of creating Relations that try to bind themselves.

	if (input.inputs[0].IsNull()) {
		throw BinderException("from_substrait cannot be called with a NULL parameter");
	}
	string serialized = input.inputs[0].GetValueUnsafe<string>();

	// Transform Substrait → DuckDB TableRef (pure AST, no binding!)
	SubstraitToAST transformer(context, serialized, is_json);
	auto table_ref = transformer.TransformPlanToTableRef();

	return table_ref;  // ✅ Returns AST directly - DuckDB will bind it in the outer query context
}

static unique_ptr<TableRef> FromSubstraitBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	return SubstraitBindReplace(context, input, false);
}

static unique_ptr<TableRef> FromSubstraitBindReplaceJSON(ClientContext &context, TableFunctionBindInput &input) {
	return SubstraitBindReplace(context, input, true);
}

// NOTE: The from_substrait functions use the bind_replace pattern (like DuckDB's query() function)
// This avoids nested query execution and lock conflicts by incorporating the Substrait plan
// directly into the outer query's AST, rather than executing it separately.

void InitializeGetSubstrait(const Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	// create the get_substrait table function that allows us to get a substrait
	// binary from a valid SQL Query
	TableFunction to_sub_func("get_substrait", {LogicalType::VARCHAR}, ToSubFunction, ToSubstraitBind);
	to_sub_func.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
	to_sub_func.named_parameters["strict"] = LogicalType::BOOLEAN;
	CreateTableFunctionInfo to_sub_info(to_sub_func);
	catalog.CreateTableFunction(*con.context, to_sub_info);
}

void InitializeGetSubstraitJSON(const Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	// create the get_substrait table function that allows us to get a substrait
	// JSON from a valid SQL Query
	TableFunction get_substrait_json("get_substrait_json", {LogicalType::VARCHAR}, ToJsonFunction, ToJsonBind);

	get_substrait_json.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
	CreateTableFunctionInfo get_substrait_json_info(get_substrait_json);
	catalog.CreateTableFunction(*con.context, get_substrait_json_info);
}

void InitializeFromSubstrait(const Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// Create the from_substrait table function that allows executing a Substrait plan
	//
	// IMPORTANT: This follows the same pattern as DuckDB's built-in query() function:
	// - Uses ONLY bind_replace (no bind or scan functions)
	// - Transforms Substrait plan into a TableRef (AST node) during bind phase
	// - Returns the TableRef which is incorporated into the outer query plan
	// - DuckDB executes everything in a single unified execution context
	// - This avoids nested query execution and lock conflicts
	//
	// NO Execute() calls are made - the Substrait plan becomes part of the query AST.
	TableFunction from_sub_func("from_substrait", {LogicalType::BLOB}, nullptr, nullptr);
	from_sub_func.bind_replace = FromSubstraitBindReplace;
	CreateTableFunctionInfo from_sub_info(from_sub_func);
	catalog.CreateTableFunction(*con.context, from_sub_info);
}

void InitializeFromSubstraitJSON(const Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// Same pattern as from_substrait, but accepts JSON-formatted Substrait plans
	TableFunction from_sub_func_json("from_substrait_json", {LogicalType::VARCHAR}, nullptr, nullptr);
	from_sub_func_json.bind_replace = FromSubstraitBindReplaceJSON;
	CreateTableFunctionInfo from_sub_info_json(from_sub_func_json);
	catalog.CreateTableFunction(*con.context, from_sub_info_json);
}

void SubstraitExtension::Load(ExtensionLoader &loader) {
	// Get the DatabaseInstance from the loader
	auto &db_instance = loader.GetDatabaseInstance();
	DuckDB db_wrapper(db_instance);
	Connection con(db_wrapper);
	con.BeginTransaction();

	InitializeGetSubstrait(con);
	InitializeGetSubstraitJSON(con);

	InitializeFromSubstrait(con);
	InitializeFromSubstraitJSON(con);

	con.Commit();
}

std::string SubstraitExtension::Name() {
	return "substrait";
}

} // namespace duckdb

namespace {
static void LoadSubstraitExtension(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadStaticExtension<duckdb::SubstraitExtension>();
}
} // namespace

extern "C" {

DUCKDB_EXTENSION_API void substrait_init(duckdb::DatabaseInstance &db) {
	LoadSubstraitExtension(db);
}

DUCKDB_EXTENSION_API void substrait_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
	duckdb::SubstraitExtension extension;
	extension.Load(loader);
}

DUCKDB_EXTENSION_API const char *substrait_version() {
	return duckdb::DuckDB::LibraryVersion();
}

DUCKDB_EXTENSION_API const char *substrait_duckdb_cpp_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
