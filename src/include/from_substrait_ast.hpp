//===----------------------------------------------------------------------===//
//                         DuckDB
//
// from_substrait_ast.hpp
//
// AST-based Substrait to DuckDB transformer (no Relation binding)
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>
#include "substrait/plan.pb.h"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! SubstraitToAST transforms Substrait plans directly to DuckDB AST nodes
//! without creating Relations. This avoids lock re-entrancy issues when
//! called during bind operations.
class SubstraitToAST {
public:
	SubstraitToAST(ClientContext &context, const string &serialized, bool json = false);

	//! Transforms Substrait Plan to DuckDB TableRef (pure AST, no binding)
	unique_ptr<TableRef> TransformPlanToTableRef();

private:
	//! Transforms Substrait Plan Root to a TableRef
	unique_ptr<TableRef> TransformRootOp(const substrait::RelRoot &sop);

	//! Transform Substrait Read operation to TableRef (base table only)
	unique_ptr<TableRef> TransformReadOp(const substrait::Rel &sop,
	                                     unique_ptr<ParsedExpression> *filter_out = nullptr);

	//! Transform Substrait Read operation with projection/filter handling via subquery wrapping
	unique_ptr<TableRef> TransformReadWithProjection(const substrait::Rel &sop);

	//! Transform Substrait Aggregate operation to TableRef
	unique_ptr<TableRef> TransformAggregateOp(const substrait::Rel &sop);

	//! Transform Substrait Join operation to TableRef
	unique_ptr<TableRef> TransformJoinOp(const substrait::Rel &sop);

	//! Transform Substrait Cross Product operation to TableRef
	unique_ptr<TableRef> TransformCrossProductOp(const substrait::Rel &sop);

	//! Transform Substrait Set operation to TableRef
	unique_ptr<TableRef> TransformSetOp(const substrait::Rel &sop);

	//! Transform Substrait SetOperationType to DuckDB SetOperationType
	static SetOperationType TransformSetOperationType(int32_t setop);

	//! Transform Substrait Sort field to DuckDB OrderByNode
	OrderByNode TransformOrder(const substrait::SortField &sordf);

	//! Transform Substrait Expressions to DuckDB ParsedExpressions
	unique_ptr<ParsedExpression> TransformExpr(const substrait::Expression &sexpr);
	static unique_ptr<ParsedExpression> TransformLiteralExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformSelectionExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformScalarFunctionExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformCastExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformIfThenExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformInExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformNested(const substrait::Expression &sexpr);

	//! Transform Substrait Type to DuckDB LogicalType
	static LogicalType SubstraitToDuckType(const substrait::Type &s_type);

	//! Helper to convert positional references to column references
	unique_ptr<ParsedExpression> ConvertPositionalToColumnRef(unique_ptr<ParsedExpression> expr,
	                                                           const substrait::NamedStruct &schema);

	unique_ptr<ParsedExpression> ConvertPositionalToColumnRef(unique_ptr<ParsedExpression> expr,
	                                                           const google::protobuf::RepeatedPtrField<std::string> &names);

	//! Helper functions
	string FindFunction(uint64_t id);
	static string RemoveFunctionExtension(const string &function_name);

	//! Transform literal value
	static Value TransformLiteralToValue(const substrait::Expression_Literal &literal);

	//! Client Context (not used for binding, just for configuration)
	ClientContext &context;

	//! Substrait Plan
	substrait::Plan plan;

	//! Function registry
	unordered_map<uint64_t, string> functions_map;
};

} // namespace duckdb
