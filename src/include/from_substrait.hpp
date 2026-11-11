//===----------------------------------------------------------------------===//
//                         DuckDB
//
// from_substrait.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>
#include "substrait/plan.pb.h"
#include "duckdb/main/connection.hpp"
#include "plan_relation.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

struct RootNameIterator {
	explicit RootNameIterator(const google::protobuf::RepeatedPtrField<std::string> *names) : names(names) {};
	string GetCurrentName() const {
		if (!names) {
			return "";
		}
		if (iterator >= names->size()) {
			throw InvalidInputException("Trying to access invalid root name at struct creation");
		}
		return (*names)[iterator];
	}
	void Next() {
		++iterator;
	}
	bool Unique(idx_t count) const {
		idx_t pos = iterator;
		set<string> values;
		for (idx_t i = 0; i < count; i++) {
			if (values.find((*names)[pos]) != values.end()) {
				return false;
			}
			values.insert((*names)[pos]);
			pos++;
		}
		return true;
	}
	bool Finished() const {
		if (!names) {
			return true;
		}
		return iterator >= names->size();
	}
	const google::protobuf::RepeatedPtrField<std::string> *names = nullptr;
	int iterator = 0;
};

class SubstraitToDuckDB {
public:
	SubstraitToDuckDB(shared_ptr<ClientContext> &context_p, const string &serialized, bool json = false);
	shared_ptr<PlanRelation> TransformPlan();

private:
	shared_ptr<PlanRelation> TransformRootOp(const substrait::RelRoot &sop);
	shared_ptr<PlanRelation> TransformOp(const substrait::Rel &sop,
	                                     const google::protobuf::RepeatedPtrField<std::string> *names = nullptr);
	shared_ptr<PlanRelation> TransformJoinOp(const substrait::Rel &sop);
	shared_ptr<PlanRelation> TransformLateralJoinOp(const substrait::Rel &sop);
	shared_ptr<PlanRelation> TransformCrossProductOp(const substrait::Rel &sop);
	shared_ptr<PlanRelation> TransformFetchOp(const substrait::Rel &sop,
	                                          const google::protobuf::RepeatedPtrField<std::string> *names = nullptr);
	shared_ptr<PlanRelation> TransformFilterOp(const substrait::Rel &sop);
	shared_ptr<PlanRelation> TransformProjectOp(const substrait::Rel &sop,
	                                            const google::protobuf::RepeatedPtrField<std::string> *names = nullptr);
	shared_ptr<PlanRelation> TransformAggregateOp(const substrait::Rel &sop);
	shared_ptr<PlanRelation> TransformWindowOp(const substrait::Rel &sop);
	shared_ptr<PlanRelation> TransformReadOp(const substrait::Rel &sop);
	shared_ptr<PlanRelation> GetValueRelationWithSingleBoolColumn();
	shared_ptr<PlanRelation>
	GetValuesExpression(const google::protobuf::RepeatedPtrField<substrait::Expression_Nested_Struct> &expression_rows);
	shared_ptr<PlanRelation> TransformSortOp(const substrait::Rel &sop,
	                                         const google::protobuf::RepeatedPtrField<std::string> *names = nullptr);
	shared_ptr<PlanRelation> TransformSetOp(const substrait::Rel &sop,
	                                        const google::protobuf::RepeatedPtrField<std::string> *names = nullptr);
	shared_ptr<PlanRelation> TransformWriteOp(const substrait::Rel &sop);
	static OnCreateConflict TransformCreateMode(substrait::WriteRel_CreateMode mode);
	shared_ptr<PlanRelation> TransformReferenceOp(const substrait::Rel &sop);

	//! Transform Substrait Expressions to DuckDB Expressions
	unique_ptr<ParsedExpression> TransformExpr(const substrait::Expression &sexpr,
	                                           RootNameIterator *iterator = nullptr);
	static unique_ptr<ParsedExpression> TransformLiteralExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformSelectionExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> ResolveOuterReference(
	    const substrait::Expression_FieldReference_OuterReference &outer_ref, int32_t field_idx);
	static JoinType TransformJoinType(substrait::JoinRel::JoinType stype, bool lateral_only);
	unique_ptr<ParsedExpression> TransformScalarFunctionExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformIfThenExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformCastExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformInExpr(const substrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformSubqueryExpr(const substrait::Expression &sexpr);
	//! Wraps a Substrait Rel as the SELECT statement of a DuckDB subquery expression
	unique_ptr<SelectStatement> SubqueryStatement(const substrait::Rel &rel);
	static ExpressionType TransformSetComparisonOp(
	    substrait::Expression_Subquery_SetComparison::ComparisonOp op);
	unique_ptr<ParsedExpression> TransformNested(const substrait::Expression &sexpr,
	                                             RootNameIterator *iterator = nullptr);

	static void VerifyCorrectExtractSubfield(const string &subfield);
	static string RemapFunctionName(const string &function_name);
	//! Folds an aggregate's variant selector -- carried either as a leading enumeration
	//! argument or as a `distribution` function option -- into the DuckDB function name.
	static string RemapEnumAggregate(const string &function_name, const vector<string> &enum_args,
	                                 const google::protobuf::RepeatedPtrField<substrait::FunctionOption> &options);
	static string RemoveExtension(const string &function_name);
	static LogicalType SubstraitToDuckType(const substrait::Type &s_type);
	static unique_ptr<ParsedExpression> TransformDivide(const substrait::Expression &sexpr, const string &signature,
	                                                    vector<unique_ptr<ParsedExpression>> children);
	//! Looks up for aggregation function in functions_map
	string FindFunction(uint64_t id);

	//! Transform Substrait Sort Order to DuckDB Order
	OrderByNode TransformOrder(const substrait::SortField &sordf);
	//! DuckDB Client Context
	shared_ptr<ClientContext> context;
	//! CTEs
	vector<shared_ptr<PlanRelation>> ctes;
	//! Substrait Plan
	substrait::Plan plan;
	//! Variable used to register functions
	unordered_map<uint64_t, string> functions_map;
	//! Tracks the left side's column names for each currently-enclosing LateralJoinRel,
	//! keyed by that LateralJoinRel's RelCommon.rel_anchor. Used to resolve OuterReference
	//! field references into a name-based ColumnRefExpression against the "left" alias.
	struct LateralScope {
		uint32_t rel_anchor;
		vector<string> left_column_names;
	};
	vector<LateralScope> lateral_scopes;
	//! Remapped functions with differing names to the correct DuckDB functions
	//! names
	static const unordered_map<std::string, std::string> function_names_remap;
	static const case_insensitive_set_t valid_extract_subfields;
	vector<ParsedExpression *> struct_expressions;
};
} // namespace duckdb
