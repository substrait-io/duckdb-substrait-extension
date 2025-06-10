//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "custom_extensions/custom_extensions.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include <string>
#include <unordered_map>

namespace duckdb {
class DuckDBToSubstrait {
public:
	explicit DuckDBToSubstrait(ClientContext &context, LogicalOperator &dop, bool strict_p)
	    : context(context), strict(strict_p) {
		TransformPlan(dop);
	};

	~DuckDBToSubstrait() {
		plan.Clear();
	}
	//! Serializes the substrait plan to a string
	string SerializeToString() const;
	string SerializeToJson() const;

private:
	//! Transform DuckDB Plan to Substrait Plan
	void TransformPlan(LogicalOperator &dop);
	//! Registers a function
	uint64_t RegisterFunction(const std::string &name, vector<::substrait::Type> &args_types);
	//! Creates a reference to a table column
	static void CreateFieldRef(substrait::Expression *expr, uint64_t col_idx);
	//! In case of struct types we might we do DFS to get all names
	static vector<string> DepthFirstNames(const LogicalType &type);
	static void DepthFirstNamesRecurse(vector<string> &names, const LogicalType &type);
	static substrait::Expression_Literal ToExpressionLiteral(const substrait::Expression &expr);
	static void SetTableSchema(const TableCatalogEntry &table, substrait::NamedStruct *schema);
	static void SetNamedTable(const TableCatalogEntry &table, substrait::WriteRel *writeRel);

	//! Transforms Relation Root
	substrait::RelRoot *TransformRootOp(LogicalOperator &dop);

	//! Methods to Transform Logical Operators to Substrait Relations
	substrait::Rel *TransformOp(LogicalOperator &dop);
	substrait::Rel *TransformFilter(LogicalOperator &dop);
	substrait::Rel *TransformProjection(LogicalOperator &dop);
	substrait::Rel *TransformTopN(LogicalOperator &dop);
	substrait::Rel *TransformLimit(LogicalOperator &dop);
	substrait::Rel *TransformOrderBy(LogicalOperator &dop);
	substrait::Rel *TransformComparisonJoin(LogicalOperator &dop);
	substrait::Rel *TransformAggregateGroup(LogicalOperator &dop);
	substrait::Rel *TransformExpressionGet(LogicalOperator &dop);
	substrait::Rel *TransformGet(LogicalOperator &dop);
	substrait::Rel *TransformCrossProduct(LogicalOperator &dop);
	substrait::Rel *TransformUnion(LogicalOperator &dop);
	substrait::Rel *TransformDistinct(LogicalOperator &dop);
	substrait::Rel *TransformExcept(LogicalOperator &dop);
	substrait::Rel *TransformIntersect(LogicalOperator &dop);
	substrait::Rel *TransformCreateTable(LogicalOperator &dop);
	substrait::Rel *TransformInsertTable(LogicalOperator &dop);
	substrait::Rel *TransformDeleteTable(LogicalOperator &dop);
	static vector<LogicalType>::size_type GetColumnCount(LogicalOperator &dop);
	static substrait::Rel *TransformDummyScan();
	static substrait::RelCommon *CreateOutputMapping(vector<int32_t> vector);
	static bool IsPassthroughProjection(LogicalProjection &dproj, idx_t child_column_count, bool &needs_output_mapping);
	//! Methods to transform different LogicalGet Types (e.g., Table, Parquet)
	//! To Substrait;
	void TransformTableScanToSubstrait(LogicalGet &dget, substrait::ReadRel *sget) const;
	void TransformParquetScanToSubstrait(LogicalGet &dget, substrait::ReadRel *sget, BindInfo &bind_info,
	                                     const FunctionData &bind_data) const;

	//! Methods to transform DuckDBConstants to Substrait Expressions
	static void TransformConstant(const Value &dval, substrait::Expression &sexpr);
	static void TransformInteger(const Value &dval, substrait::Expression &sexpr);
	static void TransformDouble(const Value &dval, substrait::Expression &sexpr);
	static void TransformBigInt(const Value &dval, substrait::Expression &sexpr);
	static void TransformDate(const Value &dval, substrait::Expression &sexpr);
	static void TransformVarchar(const Value &dval, substrait::Expression &sexpr);
	static void TransformBoolean(const Value &dval, substrait::Expression &sexpr);
	static void TransformDecimal(const Value &dval, substrait::Expression &sexpr);
	static void TransformHugeInt(const Value &dval, substrait::Expression &sexpr);
	static void TransformSmallInt(const Value &dval, substrait::Expression &sexpr);
	static void TransformFloat(const Value &dval, substrait::Expression &sexpr);
	static void TransformTime(const Value &dval, substrait::Expression &sexpr);
	static void TransformInterval(const Value &dval, substrait::Expression &sexpr);
	static void TransformTimestamp(const Value &dval, substrait::Expression &sexpr);
	static void TransformEnum(const Value &dval, substrait::Expression &sexpr);

	//! Methods to transform a DuckDB Expression to a Substrait Expression
	void TransformExpr(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset = 0);
	static void TransformBoundRefExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformCastExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformFunctionExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	static void TransformConstantExpression(Expression &dexpr, substrait::Expression &sexpr);
	void TransformComparisonExpression(Expression &dexpr, substrait::Expression &sexpr);
	void TransformBetweenExpression(Expression &dexpr, substrait::Expression &sexpr);
	void TransformConjunctionExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformNotNullExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformIsNullExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformNotExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformCaseExpression(Expression &dexpr, substrait::Expression &sexpr);
	void TransformInExpression(Expression &dexpr, substrait::Expression &sexpr);

	//! Transforms a DuckDB Logical Type into a Substrait Type
	static substrait::Type DuckToSubstraitType(const LogicalType &type, BaseStatistics *column_statistics = nullptr,
	                                           bool not_null = false);

	//! Methods to transform DuckDB Filters to Substrait Expression
	substrait::Expression *TransformFilter(uint64_t col_idx, LogicalType &column_type, TableFilter &dfilter,
	                                       LogicalType &return_type);
	substrait::Expression *TransformIsNotNullFilter(uint64_t col_idx, const LogicalType &column_type,
	                                                TableFilter &dfilter, const LogicalType &return_type);
	substrait::Expression *TransformConjuctionAndFilter(uint64_t col_idx, LogicalType &column_type,
	                                                    TableFilter &dfilter, LogicalType &return_type);
	substrait::Expression *TransformConstantComparisonFilter(uint64_t col_idx, const LogicalType &column_type,
	                                                         TableFilter &dfilter, const LogicalType &return_type);
	substrait::Expression *TransformExpressionFilter(uint64_t col_idx, LogicalType &column_type,
	                                                 TableFilter &dfilter, LogicalType &return_type);

	//! Transforms DuckDB Join Conditions to Substrait Expression
	substrait::Expression *TransformJoinCond(const JoinCondition &dcond, uint64_t left_ncol);
	//! Transforms DuckDB Sort Order to Substrait Sort Order
	void TransformOrder(const BoundOrderByNode &dordf, substrait::SortField &sordf);

	static void AllocateFunctionArgument(substrait::Expression_ScalarFunction *scalar_fun,
	                                     substrait::Expression *value);
	static std::string &RemapFunctionName(std::string &function_name);
	static bool IsExtractFunction(const string &function_name);

	//! Creates a Conjunction
	template <typename T, typename FUNC>
	substrait::Expression *CreateConjunction(T &source, const FUNC f) {
		substrait::Expression *res = nullptr;
		for (auto &ele : source) {
			auto child_expression = f(ele);
			// Skip null expressions (filters that cannot be pushed down)
			if (!child_expression) {
				continue;
			}
			if (!res) {
				res = child_expression;
			} else {

				auto temp_expr = new substrait::Expression();
				auto scalar_fun = temp_expr->mutable_scalar_function();
				LogicalType boolean_type(LogicalTypeId::BOOLEAN);

				vector<::substrait::Type> args_types {DuckToSubstraitType(boolean_type),
				                                      DuckToSubstraitType(boolean_type)};

				scalar_fun->set_function_reference(RegisterFunction("and", args_types));
				*scalar_fun->mutable_output_type() = DuckToSubstraitType(boolean_type);
				AllocateFunctionArgument(scalar_fun, res);
				AllocateFunctionArgument(scalar_fun, child_expression);
				res = temp_expr;
			}
		}
		return res;
	}

	//! Variables used to register functions
	unordered_map<string, uint64_t> functions_map;
	unordered_map<string, uint64_t> extension_uri_map;

	//! Remapped DuckDB functions names to Substrait compatible function names
	static const unordered_map<std::string, std::string> function_names_remap;
	static const case_insensitive_set_t valid_extract_subfields;
	//! Variable that holds information about yaml function extensions
	static const SubstraitCustomFunctions custom_functions;
	uint64_t last_function_id = 1;
	uint64_t last_uri_id = 1;
	//! The substrait Plan
	substrait::Plan plan;
	ClientContext &context;
	//! If we are generating a query plan on strict mode we will error if
	//! things don't go perfectly shiny
	bool strict;
	string errors;
};
} // namespace duckdb
