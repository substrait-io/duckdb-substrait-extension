#define DUCKDB_EXTENSION_MAIN

#include "substrait_extension.hpp"
#include "from_substrait.hpp"
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
		disabled_optimizers.insert(OptimizerType::IN_CLAUSE);
		disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
		disabled_optimizers.insert(OptimizerType::MATERIALIZED_CTE);
		// If error(varchar) gets implemented in substrait this can be removed
		context.config.scalar_subquery_error_on_multiple_rows = false;
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

shared_ptr<Relation> SubstraitPlanToDuckDBRel(shared_ptr<ClientContext> &context, const string &serialized,
                                              bool json = false, bool acquire_lock = false) {
	SubstraitToDuckDB transformer_s2d(context, serialized, json, acquire_lock);
	return transformer_s2d.TransformPlan();
}

//! This function matches results of substrait plans with direct Duckdb queries
//! Is only executed when pragma enable_verification = true
//! It creates extra connections to be able to execute the consumed DuckDB Plan
//! And the SQL query itself, ideally this wouldn't be necessary and won't
//! work for round-tripping tests over temporary objects.
static void VerifySubstraitRoundtrip(unique_ptr<LogicalOperator> &query_plan, ClientContext &context,
                                     ToSubstraitFunctionData &data, const string &serialized, bool is_json) {
	// We round-trip the generated json and verify if the result is the same
	auto con = Connection(*context.db);
	auto actual_result = con.Query(data.query);
	auto con_2 = Connection(*context.db);
	auto sub_relation = SubstraitPlanToDuckDBRel(con_2.context, serialized, is_json, true);
	auto substrait_result = sub_relation->Execute();
	substrait_result->names = actual_result->names;
	unique_ptr<MaterializedQueryResult> substrait_materialized;

	if (substrait_result->type == QueryResultType::STREAM_RESULT) {
		auto &stream_query = substrait_result->Cast<StreamQueryResult>();

		substrait_materialized = stream_query.Materialize();
	} else if (substrait_result->type == QueryResultType::MATERIALIZED_RESULT) {
		substrait_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(substrait_result));
	}
	auto &actual_col_coll = actual_result->Collection();
	auto &subs_col_coll = substrait_materialized->Collection();
	string error_message;
	if (!ColumnDataCollection::ResultEquals(actual_col_coll, subs_col_coll, error_message)) {
		query_plan->Print();
		sub_relation->Print();
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
	if (input.inputs[0].IsNull()) {
		throw BinderException("from_substrait cannot be called with a NULL parameter");
	}
	string serialized = input.inputs[0].GetValueUnsafe<string>();
	shared_ptr<ClientContext> c_ptr(&context, do_nothing);
	auto plan = SubstraitPlanToDuckDBRel(c_ptr, serialized, is_json);
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
	shared_ptr<Relation> plan;
	unique_ptr<QueryResult> res;
	unique_ptr<Connection> conn;
};

static unique_ptr<FunctionData> SubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names, bool is_json) {
	auto result = make_uniq<FromSubstraitFunctionData>();
	result->conn = make_uniq<Connection>(*context.db);
	if (input.inputs[0].IsNull()) {
		throw BinderException("from_substrait cannot be called with a NULL parameter");
	}
	string serialized = input.inputs[0].GetValueUnsafe<string>();
	shared_ptr<ClientContext> c_ptr(&context, do_nothing);
	result->plan = SubstraitPlanToDuckDBRel(c_ptr, serialized, is_json);
	for (auto &column : result->plan->Columns()) {
		return_types.emplace_back(column.Type());
		names.emplace_back(column.Name());
	}
	return std::move(result);
}

static unique_ptr<FunctionData> FromSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	return SubstraitBind(context, input, return_types, names, false);
}

static unique_ptr<FunctionData> FromSubstraitBindJSON(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return SubstraitBind(context, input, return_types, names, true);
}

static void FromSubFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<FromSubstraitFunctionData>();
	if (!data.res) {
		auto con = Connection(*context.db);
		data.plan->context = make_shared_ptr<ClientContextWrapper>(con.context);
		data.res = data.plan->Execute();
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

void SubstraitExtension::Load(DuckDB &db) {
	Connection con(db);
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

extern "C" {

DUCKDB_EXTENSION_API void substrait_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SubstraitExtension>();
}

DUCKDB_EXTENSION_API const char *substrait_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
