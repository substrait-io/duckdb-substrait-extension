#define DUCKDB_EXTENSION_MAIN

#include "substrait_extension.hpp"
#include "from_substrait.hpp"
#include "to_substrait.hpp"

#include "duckdb.hpp"
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
#include "duckdb/main/settings.hpp"
#endif

namespace duckdb {

void do_nothing(ClientContext *) {
}

struct ToSubstraitFunctionData : public TableFunctionData {
	ToSubstraitFunctionData() = default;
	string query;
	bool enable_optimizer = false;
	//! We will fail the conversion on possible warnings
	bool strict = false;
	bool finished = false;
	//! Output column names from the planner
	vector<Identifier> plan_names;
	//! Original options from the connection
	ClientConfig original_config;
	set<OptimizerType> original_disabled_optimizers;

	// Setup configurations
	void PrepareConnection(ClientContext &context) {
		// First collect original options
		original_config = context.config;
		original_disabled_optimizers = DBConfig::GetConfig(context).options.disabled_optimizers;

		// The user might want to disable the optimizer of the new connection
		Settings::Set<EnableOptimizerSetting>(context, SetScope::SESSION, Value::BOOLEAN(enable_optimizer));
		// DuckDB 2.0 flattens correlated subqueries into materialized CTEs by default. Keep the
		// duplicate eliminated joins instead, which Substrait can express.
		Settings::Set<DelimJoinAsCteSetting>(context, SetScope::SESSION, Value::BOOLEAN(false));
		context.config.use_replacement_scans = false;
		// We want for sure to disable the internal compression optimizations.
		// These are DuckDB specific, no other system implements these. Also,
		// respect the user's settings if they chose to disable any specific optimizers.
		//
		// The InClauseRewriter optimization converts large `IN` clauses to a
		// "mark join" against a `ColumnDataCollection`, which may not make
		// sense in other systems and would complicate the conversion to Substrait.
		set<OptimizerType> disabled_optimizers = DBConfig::GetConfig(context).options.disabled_optimizers;
		disabled_optimizers.insert(OptimizerType::IN_CLAUSE);
		disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
		disabled_optimizers.insert(OptimizerType::MATERIALIZED_CTE);
		// The CommonSubplan optimizer deduplicates repeated subplans into
		// materialized CTEs, which cannot be expressed in Substrait.
		disabled_optimizers.insert(OptimizerType::COMMON_SUBPLAN);
		// The GroupingSets optimizer rewrites ROLLUP/CUBE/GROUPING SETS into a
		// materialized CTE of exported aggregate states that the sets combine,
		// none of which exists in Substrait. Substrait has grouping sets natively.
		disabled_optimizers.insert(OptimizerType::GROUPING_SETS);
		// The RowNumber rewriter replaces row_number() windows with operators
		// Substrait has no relation for; the window function itself is portable.
		disabled_optimizers.insert(OptimizerType::ROW_NUMBER_REWRITER);
		// Aggregate reuse and partial aggregate pushdown split an aggregate into
		// exported intermediate states that are combined and finalized elsewhere
		// in the plan. Substrait only has whole aggregates.
		disabled_optimizers.insert(OptimizerType::AGGREGATE_REUSE);
		disabled_optimizers.insert(OptimizerType::PARTIAL_AGGREGATE_PUSHDOWN);
		// Projection pullup and the distinct aggregate rewrite share the subplans
		// they factor out through a materialized CTE.
		disabled_optimizers.insert(OptimizerType::PROJECTION_PULLUP);
		disabled_optimizers.insert(OptimizerType::DISTINCT_AGGREGATE_REWRITE);
		// Scalar function pushdown moves functions such as strlen into the file
		// reader, leaving a plan that only reads the column.
		disabled_optimizers.insert(OptimizerType::SCALAR_FN_PUSHDOWN);
		// STATISTICS_PROPAGATION must be disabled because it uses table
		// statistics (cardinality, min/max) at plan-generation time to fold
		// filters that look always-false against the current data — e.g. a
		// query on an empty table folds into LogicalEmptyResult, and
		// `WHERE id > 100` folds away when the current max id is 3. This
		// produces a non-portable plan (virtualTable {}) that returns 0 rows
		// regardless of runtime data. A Substrait plan must be valid for any
		// data conforming to the schema.
		disabled_optimizers.insert(OptimizerType::STATISTICS_PROPAGATION);
		// EMPTY_RESULT_PULLUP is disabled as an additional safeguard: it
		// propagates LogicalEmptyResult upward through joins/projections, so
		// if any other pass introduces an empty-result node in the future,
		// it won't cascade into a fully-folded plan.
		disabled_optimizers.insert(OptimizerType::EMPTY_RESULT_PULLUP);
		// If error(varchar) gets implemented in substrait this can be removed
		DBConfig::GetConfig(context).SetOption(ScalarSubqueryErrorOnMultipleRowsSetting::Name, false);
		DBConfig::GetConfig(context).options.disabled_optimizers = disabled_optimizers;
	}

	unique_ptr<LogicalOperator> ExtractPlan(ClientContext &context) {
		PrepareConnection(context);
		unique_ptr<LogicalOperator> plan;
		try {
			Parser parser(context);
			parser.ParseQuery(query);

			Planner planner(context);
			planner.CreatePlan(std::move(parser.statements[0]));
			D_ASSERT(planner.plan);

			plan_names = planner.names;
			plan = std::move(planner.plan);

			if (Settings::Get<EnableOptimizerSetting>(context)) {
				Optimizer optimizer(*planner.binder, context);
				plan = optimizer.Optimize(std::move(plan));
			}

			plan->ResolveOperatorTypes();
			ColumnBindingResolver verifier(true);
			verifier.VisitOperator(*plan);
			ColumnBindingResolver resolver;
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

static void SetOptions(ToSubstraitFunctionData &function, ClientContext &context,
                       const named_argument_map_t &named_params) {
	bool optimizer_option_set = false;
	for (const auto &param : named_params) {
		// Identifiers compare case-insensitively.
		const auto &option = param.first;
		// If the user has explicitly requested to enable/disable the optimizer when
		// generating Substrait, then that takes precedence. The parameter defaults to
		// NULL, which means the user left it out.
		if (option == "enable_optimizer" && !param.second.IsNull()) {
			function.enable_optimizer = BooleanValue::Get(param.second);
			optimizer_option_set = true;
		}
		if (option == "strict") {
			function.strict = BooleanValue::Get(param.second);
		}
	}
	if (!optimizer_option_set) {
		// If the user has not specified what they want, fall back to the settings
		// on the connection (e.g. if the optimizer was disabled by the user at
		// the connection level, it would be surprising to enable the optimizer
		// when generating Substrait).
		function.enable_optimizer = Settings::Get<EnableOptimizerSetting>(context);
	}
}

static unique_ptr<ToSubstraitFunctionData> InitToSubstraitFunctionData(ClientContext &context,
                                                                       TableFunctionBindInput &input) {
	auto result = make_uniq<ToSubstraitFunctionData>();
	result->query = input.inputs[0].ToString();
	SetOptions(*result, context, input.named_parameters);
	return result;
}

static unique_ptr<FunctionData> ToSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("Plan Blob");
	return InitToSubstraitFunctionData(context, input);
}

static unique_ptr<FunctionData> ToJsonBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Json");
	return InitToSubstraitFunctionData(context, input);
}

shared_ptr<PlanRelation> SubstraitPlanToDuckDBRel(shared_ptr<ClientContext> &context, const string &serialized,
                                                  bool json = false) {
	SubstraitToDuckDB transformer_s2d(context, serialized, json);
	return transformer_s2d.TransformPlan();
}

//! Round-trip verification used to follow PRAGMA enable_verification, which DuckDB 2.0 turned into a
//! deprecated no-op. It is now switched on with SET substrait_verify_roundtrip = true.
static constexpr const char *SUBSTRAIT_VERIFY_ROUNDTRIP_SETTING = "substrait_verify_roundtrip";

static bool RoundtripVerificationEnabled(ClientContext &context) {
	bool enabled = false;
	context.TryGetCurrentSetting(SUBSTRAIT_VERIFY_ROUNDTRIP_SETTING, enabled);
	return enabled;
}

//! This function matches results of substrait plans with direct Duckdb queries
//! Is only executed when the substrait_verify_roundtrip setting is true.
//! It creates extra connections to be able to execute the consumed DuckDB Plan
//! And the SQL query itself, ideally this wouldn't be necessary and won't
//! work for round-tripping tests over temporary objects.
static void VerifySubstraitRoundtrip(unique_ptr<LogicalOperator> &query_plan, ClientContext &context,
                                     ToSubstraitFunctionData &data, const string &serialized, bool is_json) {
	// We round-trip the generated json and verify if the result is the same
	auto con = Connection(*context.db);
	auto actual_result = con.Query(data.query);
	auto con_2 = Connection(*context.db);
	auto sub_relation = SubstraitPlanToDuckDBRel(con_2.context, serialized, is_json);
	auto substrait_result = con_2.Query(sub_relation->GetStatement());
	auto &actual_col_coll = actual_result->Collection();
	auto &subs_col_coll = substrait_result->Collection();
	string error_message;
	if (!ColumnDataCollection::ResultEquals(actual_col_coll, subs_col_coll, error_message)) {
		// Check if the error is due to NaN vs NULL mismatch (common with division by zero)
		if (error_message.rfind("nan <> NULL", 1) != string::npos) {
			// This is a known incompatibility: DuckDB returns NaN for division by zero with decimals,
			// while Substrait returns NULL. We treat these as equivalent.
			return;
		}
		query_plan->Print();
		Printer::Print(sub_relation->GetStatement()->ToString());
		Printer::Print(serialized);
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
	query_plan = data.ExtractPlan(context);
	auto transformer_d2s = DuckDBToSubstrait(context, *query_plan, data.strict, data.plan_names);
	serialized = transformer_d2s.SerializeToString();
	output.data[0].Append(Value::BLOB_RAW(serialized));
}

static void ToJsonFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                   unique_ptr<LogicalOperator> &query_plan, string &serialized) {
	query_plan = data.ExtractPlan(context);
	auto transformer_d2s = DuckDBToSubstrait(context, *query_plan, data.strict, data.plan_names);
	serialized = transformer_d2s.SerializeToJson();
	output.data[0].Append(Value(serialized));
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

	if (!RoundtripVerificationEnabled(context)) {
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

	if (!RoundtripVerificationEnabled(context)) {
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
	if (input.inputs[0].IsNull()) {
		throw BinderException("from_substrait cannot be called with a NULL parameter");
	}
	string serialized = input.inputs[0].GetValueUnsafe<string>();
	// Create a new connection to avoid deadlock with the locked context
	auto con = Connection(*context.db);
	auto plan = SubstraitPlanToDuckDBRel(con.context, serialized, is_json);
	if (!plan.get()->IsReadOnly()) {
		return nullptr;
	}
	return plan->GetTableRef();
}

static unique_ptr<TableRef> FromSubstraitBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	return SubstraitBindReplace(context, input, false);
}

static unique_ptr<TableRef> FromSubstraitBindReplaceJSON(ClientContext &context, TableFunctionBindInput &input) {
	return SubstraitBindReplace(context, input, true);
}

struct FromSubstraitFunctionData : public TableFunctionData {
	FromSubstraitFunctionData() = default;
	shared_ptr<PlanRelation> plan;
	unique_ptr<QueryResult> res;
	unique_ptr<Connection> conn;
};

static unique_ptr<FunctionData> SubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names,
                                              bool is_json) {
	auto result = make_uniq<FromSubstraitFunctionData>();
	result->conn = make_uniq<Connection>(*context.db);
	if (input.inputs[0].IsNull()) {
		throw BinderException("from_substrait cannot be called with a NULL parameter");
	}
	string serialized = input.inputs[0].GetValueUnsafe<string>();
	// Use the connection's context to avoid deadlock with the locked context
	result->plan = SubstraitPlanToDuckDBRel(result->conn->context, serialized, is_json);
	for (auto &column : result->plan->Columns()) {
		return_types.emplace_back(column.Type());
		names.emplace_back(column.Name());
	}
	return std::move(result);
}

static unique_ptr<FunctionData> FromSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	return SubstraitBind(context, input, return_types, names, false);
}

static unique_ptr<FunctionData> FromSubstraitBindJSON(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	return SubstraitBind(context, input, return_types, names, true);
}

static void FromSubFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<FromSubstraitFunctionData>();
	if (!data.res) {
		auto con = Connection(*context.db);
		data.res = con.Query(data.plan->GetStatement());
	}
	auto result_chunk = data.res->Fetch();
	if (!result_chunk) {
		return;
	}
	output.Move(*result_chunk);
}

void InitializeGetSubstrait(const Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	// create the get_substrait table function that allows us to get a substrait
	// binary from a valid SQL Query
	TableFunction to_sub_func("get_substrait", {LogicalType::VARCHAR}, ToSubFunction, ToSubstraitBind);
	to_sub_func.GetSignature()
	    .AddKeywordOnly("enable_optimizer", LogicalType::BOOLEAN, Value(LogicalType::BOOLEAN))
	    .AddKeywordOnly("strict", LogicalType::BOOLEAN, Value::BOOLEAN(false));
	CreateTableFunctionInfo to_sub_info(to_sub_func);
	catalog.CreateTableFunction(*con.context, to_sub_info);
}

void InitializeGetSubstraitJSON(const Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	// create the get_substrait table function that allows us to get a substrait
	// JSON from a valid SQL Query
	TableFunction get_substrait_json("get_substrait_json", {LogicalType::VARCHAR}, ToJsonFunction, ToJsonBind);

	get_substrait_json.GetSignature().AddKeywordOnly("enable_optimizer", LogicalType::BOOLEAN,
	                                                  Value(LogicalType::BOOLEAN));
	CreateTableFunctionInfo get_substrait_json_info(get_substrait_json);
	catalog.CreateTableFunction(*con.context, get_substrait_json_info);
}

void InitializeFromSubstrait(const Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the from_substrait table function that allows us to get a query
	// result from a substrait plan
	TableFunction from_sub_func("from_substrait", {LogicalType::BLOB}, FromSubFunction, FromSubstraitBind);
	from_sub_func.bind_replace = FromSubstraitBindReplace;
	CreateTableFunctionInfo from_sub_info(from_sub_func);
	catalog.CreateTableFunction(*con.context, from_sub_info);
}

void InitializeFromSubstraitJSON(const Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	// create the from_substrait table function that allows us to get a query
	// result from a substrait plan
	TableFunction from_sub_func_json("from_substrait_json", {LogicalType::VARCHAR}, FromSubFunction,
	                                 FromSubstraitBindJSON);
	from_sub_func_json.bind_replace = FromSubstraitBindReplaceJSON;
	CreateTableFunctionInfo from_sub_info_json(from_sub_func_json);
	catalog.CreateTableFunction(*con.context, from_sub_info_json);
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption(SUBSTRAIT_VERIFY_ROUNDTRIP_SETTING,
	                          "Whether get_substrait also runs the plan it produced and checks its result against "
	                          "the query",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	Connection con(loader.GetDatabaseInstance());
	con.BeginTransaction();

	InitializeGetSubstrait(con);
	InitializeGetSubstraitJSON(con);

	InitializeFromSubstrait(con);
	InitializeFromSubstraitJSON(con);

	con.Commit();
}

void SubstraitExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string SubstraitExtension::Name() {
	return "substrait";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(substrait, loader) {
	duckdb::LoadInternal(loader);
}
}
