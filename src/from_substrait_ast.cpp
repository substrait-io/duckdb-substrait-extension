#include "from_substrait_ast.hpp"

#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/common/exception.hpp"

#include "google/protobuf/util/json_util.h"

namespace duckdb {

// Ported from legacy from_substrait.cpp - maps Substrait function names to DuckDB equivalents
const std::unordered_map<std::string, std::string> function_names_remap = {
    {"modulus", "mod"},      {"std_dev", "stddev"},     {"starts_with", "prefix"},
    {"ends_with", "suffix"}, {"substring", "substr"},   {"char_length", "length"},
    {"is_nan", "isnan"},     {"is_finite", "isfinite"}, {"is_infinite", "isinf"},
    {"like", "~~"},          {"extract", "date_part"},  {"bitwise_and", "&"},
    {"bitwise_or", "|"},     {"bitwise_xor", "xor"},    {"octet_length", "strlen"}};

SubstraitToAST::SubstraitToAST(ClientContext &context_p, const string &serialized, bool json)
    : context(context_p) {
	if (!json) {
		if (!plan.ParseFromString(serialized)) {
			throw std::runtime_error("Could not parse binary Substrait plan");
		}
	} else {
		google::protobuf::util::Status status = google::protobuf::util::JsonStringToMessage(serialized, &plan);
		if (!status.ok()) {
			throw std::runtime_error("Could not parse JSON Substrait plan: " + status.ToString());
		}
	}

	// Register function extensions
	for (auto &sext : plan.extensions()) {
		if (!sext.has_extension_function()) {
			continue;
		}
		functions_map[sext.extension_function().function_anchor()] = sext.extension_function().name();
	}
}

Value SubstraitToAST::TransformLiteralToValue(const substrait::Expression_Literal &literal) {
	if (literal.has_null()) {
		return Value(LogicalType::SQLNULL);
	}
	switch (literal.literal_type_case()) {
	case substrait::Expression_Literal::LiteralTypeCase::kI8:
		return Value::TINYINT(static_cast<int8_t>(literal.i8()));
	case substrait::Expression_Literal::LiteralTypeCase::kI16:
		return Value::SMALLINT(static_cast<int16_t>(literal.i16()));
	case substrait::Expression_Literal::LiteralTypeCase::kI32:
		return Value::INTEGER(literal.i32());
	case substrait::Expression_Literal::LiteralTypeCase::kI64:
		return Value::BIGINT(literal.i64());
	case substrait::Expression_Literal::LiteralTypeCase::kFp32:
		return Value::FLOAT(literal.fp32());
	case substrait::Expression_Literal::LiteralTypeCase::kFp64:
		return Value::DOUBLE(literal.fp64());
	case substrait::Expression_Literal::LiteralTypeCase::kString:
		return {literal.string()};
	case substrait::Expression_Literal::LiteralTypeCase::kBoolean:
		return Value(literal.boolean());
	case substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
		const auto &substrait_decimal = literal.decimal();
		if (substrait_decimal.value().size() != 16) {
			throw InvalidInputException("Decimal value must have 16 bytes, but has " +
			                            std::to_string(substrait_decimal.value().size()));
		}
		auto raw_value = reinterpret_cast<const uint64_t *>(substrait_decimal.value().c_str());
		hugeint_t substrait_value {};
		substrait_value.lower = raw_value[0];
		substrait_value.upper = static_cast<int64_t>(raw_value[1]);
		Value val = Value::HUGEINT(substrait_value);
		auto decimal_type = LogicalType::DECIMAL(substrait_decimal.precision(), substrait_decimal.scale());
		// cast to correct value
		switch (decimal_type.InternalType()) {
		case PhysicalType::INT8:
			return Value::DECIMAL(val.GetValue<int8_t>(), substrait_decimal.precision(), substrait_decimal.scale());
		case PhysicalType::INT16:
			return Value::DECIMAL(val.GetValue<int16_t>(), substrait_decimal.precision(), substrait_decimal.scale());
		case PhysicalType::INT32:
			return Value::DECIMAL(val.GetValue<int32_t>(), substrait_decimal.precision(), substrait_decimal.scale());
		case PhysicalType::INT64:
			return Value::DECIMAL(val.GetValue<int64_t>(), substrait_decimal.precision(), substrait_decimal.scale());
		case PhysicalType::INT128:
			return Value::DECIMAL(substrait_value, substrait_decimal.precision(), substrait_decimal.scale());
		default:
			throw NotImplementedException("Unsupported internal type for decimal: %s", decimal_type.ToString());
		}
	}
	case substrait::Expression_Literal::LiteralTypeCase::kDate: {
		date_t date(literal.date());
		return Value::DATE(date);
	}
	case substrait::Expression_Literal::LiteralTypeCase::kTime: {
		dtime_t time(literal.time());
		return Value::TIME(time);
	}
	case substrait::Expression_Literal::LiteralTypeCase::kIntervalYearToMonth: {
		interval_t interval {};
		interval.months = literal.interval_year_to_month().months();
		interval.days = 0;
		interval.micros = 0;
		return Value::INTERVAL(interval);
	}
	case substrait::Expression_Literal::LiteralTypeCase::kIntervalDayToSecond: {
		interval_t interval {};
		interval.months = 0;
		interval.days = literal.interval_day_to_second().days();
		interval.micros = literal.interval_day_to_second().microseconds();
		return Value::INTERVAL(interval);
	}
	case substrait::Expression_Literal::LiteralTypeCase::kVarChar:
		return {literal.var_char().value()};
	case substrait::Expression_Literal::LiteralTypeCase::kFixedChar:
		return {literal.fixed_char()};
	default:
		throw NotImplementedException(
		    "Literal type not yet implemented in AST transformer: %s",
		    substrait::Expression_Literal::GetDescriptor()->FindFieldByNumber(literal.literal_type_case())->name());
	}
}

unique_ptr<ParsedExpression> SubstraitToAST::TransformLiteralExpr(const substrait::Expression &sexpr) {
	return make_uniq<ConstantExpression>(TransformLiteralToValue(sexpr.literal()));
}

unique_ptr<ParsedExpression> SubstraitToAST::TransformSelectionExpr(const substrait::Expression &sexpr) {
	// Substrait selection expressions reference columns via field positions
	if (!sexpr.selection().has_direct_reference() || !sexpr.selection().direct_reference().has_struct_field()) {
		throw SyntaxException("Can only have direct struct references in selections");
	}

	// Use positional reference (1-based in DuckDB)
	auto field_idx = sexpr.selection().direct_reference().struct_field().field();
	return make_uniq<PositionalReferenceExpression>(field_idx + 1);
}

string SubstraitToAST::FindFunction(uint64_t id) {
	if (functions_map.find(id) == functions_map.end()) {
		throw NotImplementedException("Could not find function with ID %s", to_string(id));
	}
	return functions_map[id];
}

string SubstraitToAST::RemoveFunctionExtension(const string &function_name) {
	// Remove extension prefix (e.g., "substrait:add" -> "add") and remap function names
	auto pos = function_name.find(':');
	string name = (pos != string::npos) ? function_name.substr(0, pos) : function_name;
	auto it = function_names_remap.find(name);
	if (it != function_names_remap.end()) {
		return it->second;
	}
	return name;
}

unique_ptr<ParsedExpression> SubstraitToAST::TransformScalarFunctionExpr(const substrait::Expression &sexpr) {
	auto &scalar_fun = sexpr.scalar_function();
	auto function_name = FindFunction(scalar_fun.function_reference());
	function_name = RemoveFunctionExtension(function_name);

	// Transform function arguments
	vector<unique_ptr<ParsedExpression>> children;
	for (auto &sarg : scalar_fun.arguments()) {
		if (sarg.has_value()) {
			children.push_back(TransformExpr(sarg.value()));
		} else if (sarg.has_enum_()) {
			// Enum arguments (like for EXTRACT function) - treat as string constants
			auto enum_val = sarg.enum_();
			children.push_back(make_uniq<ConstantExpression>(Value(enum_val)));
		} else {
			throw NotImplementedException("Unsupported scalar function argument type");
		}
	}

	// Handle special functions that map to operators
	if (function_name == "and") {
		return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(children));
	} else if (function_name == "or") {
		return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(children));
	} else if (function_name == "lt") {
		if (children.size() != 2) {
			throw InvalidInputException("lt function requires exactly 2 arguments");
		}
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHAN,
		                                       std::move(children[0]), std::move(children[1]));
	} else if (function_name == "lte") {
		if (children.size() != 2) {
			throw InvalidInputException("lte function requires exactly 2 arguments");
		}
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO,
		                                       std::move(children[0]), std::move(children[1]));
	} else if (function_name == "gt") {
		if (children.size() != 2) {
			throw InvalidInputException("gt function requires exactly 2 arguments");
		}
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN,
		                                       std::move(children[0]), std::move(children[1]));
	} else if (function_name == "gte") {
		if (children.size() != 2) {
			throw InvalidInputException("gte function requires exactly 2 arguments");
		}
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
		                                       std::move(children[0]), std::move(children[1]));
	} else if (function_name == "equal") {
		if (children.size() != 2) {
			throw InvalidInputException("equal function requires exactly 2 arguments");
		}
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
		                                       std::move(children[0]), std::move(children[1]));
	} else if (function_name == "not_equal") {
		if (children.size() != 2) {
			throw InvalidInputException("not_equal function requires exactly 2 arguments");
		}
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL,
		                                       std::move(children[0]), std::move(children[1]));
	} else if (function_name == "is_null") {
		if (children.size() != 1) {
			throw InvalidInputException("is_null function requires exactly 1 argument");
		}
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, std::move(children[0]));
	} else if (function_name == "is_not_null") {
		if (children.size() != 1) {
			throw InvalidInputException("is_not_null function requires exactly 1 argument");
		}
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(children[0]));
	} else if (function_name == "not") {
		if (children.size() != 1) {
			throw InvalidInputException("not function requires exactly 1 argument");
		}
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(children[0]));
	} else if (function_name == "is_not_distinct_from") {
		if (children.size() != 2) {
			throw InvalidInputException("is_not_distinct_from function requires exactly 2 arguments");
		}
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
		                                       std::move(children[0]), std::move(children[1]));
	}

	// For all other functions, create a FunctionExpression
	return make_uniq<FunctionExpression>(function_name, std::move(children));
}

LogicalType SubstraitToAST::SubstraitToDuckType(const substrait::Type &s_type) {
	switch (s_type.kind_case()) {
	case substrait::Type::KindCase::kBool:
		return {LogicalTypeId::BOOLEAN};
	case substrait::Type::KindCase::kI8:
		return {LogicalTypeId::TINYINT};
	case substrait::Type::KindCase::kI16:
		return {LogicalTypeId::SMALLINT};
	case substrait::Type::KindCase::kI32:
		return {LogicalTypeId::INTEGER};
	case substrait::Type::KindCase::kI64:
		return {LogicalTypeId::BIGINT};
	case substrait::Type::KindCase::kDecimal: {
		auto &s_decimal_type = s_type.decimal();
		return LogicalType::DECIMAL(s_decimal_type.precision(), s_decimal_type.scale());
	}
	case substrait::Type::KindCase::kDate:
		return {LogicalTypeId::DATE};
	case substrait::Type::KindCase::kTime:
		return {LogicalTypeId::TIME};
	case substrait::Type::KindCase::kVarchar:
	case substrait::Type::KindCase::kString:
		return {LogicalTypeId::VARCHAR};
	case substrait::Type::KindCase::kBinary:
		return {LogicalTypeId::BLOB};
	case substrait::Type::KindCase::kFp32:
		return {LogicalTypeId::FLOAT};
	case substrait::Type::KindCase::kFp64:
		return {LogicalTypeId::DOUBLE};
	case substrait::Type::KindCase::kTimestamp:
		return {LogicalTypeId::TIMESTAMP};
	case substrait::Type::KindCase::kList: {
		auto &s_list_type = s_type.list();
		auto element_type = SubstraitToDuckType(s_list_type.type());
		return LogicalType::LIST(element_type);
	}
	case substrait::Type::KindCase::kMap: {
		auto &s_map_type = s_type.map();
		auto key_type = SubstraitToDuckType(s_map_type.key());
		auto value_type = SubstraitToDuckType(s_map_type.value());
		return LogicalType::MAP(key_type, value_type);
	}
	case substrait::Type::KindCase::kStruct: {
		auto &s_struct_type = s_type.struct_();
		child_list_t<LogicalType> children;

		for (idx_t i = 0; i < (idx_t)s_struct_type.types_size(); i++) {
			auto field_name = "f" + std::to_string(i);
			auto field_type = SubstraitToDuckType(s_struct_type.types((int)i));
			children.push_back(make_pair(field_name, field_type));
		}

		return LogicalType::STRUCT(children);
	}
	case substrait::Type::KindCase::kUuid:
		return {LogicalTypeId::UUID};
	case substrait::Type::KindCase::kIntervalDay:
		return {LogicalTypeId::INTERVAL};
	default:
		throw NotImplementedException("Substrait type not yet supported: %s",
		                              substrait::Type::GetDescriptor()->FindFieldByNumber(s_type.kind_case())->name());
	}
}

unique_ptr<ParsedExpression> SubstraitToAST::TransformCastExpr(const substrait::Expression &sexpr) {
	const auto &scast = sexpr.cast();
	auto cast_type = SubstraitToDuckType(scast.type());
	auto cast_child = TransformExpr(scast.input());
	return make_uniq<CastExpression>(cast_type, std::move(cast_child));
}

unique_ptr<ParsedExpression> SubstraitToAST::TransformIfThenExpr(const substrait::Expression &sexpr) {
	// Transform CASE/WHEN statements
	const auto &scase = sexpr.if_then();
	auto dcase = make_uniq<CaseExpression>();

	// Transform each WHEN/THEN pair
	for (const auto &sif : scase.ifs()) {
		CaseCheck dif;
		dif.when_expr = TransformExpr(sif.if_());
		dif.then_expr = TransformExpr(sif.then());
		dcase->case_checks.push_back(std::move(dif));
	}

	// Transform ELSE clause
	dcase->else_expr = TransformExpr(scase.else_());

	return std::move(dcase);
}

unique_ptr<ParsedExpression> SubstraitToAST::TransformInExpr(const substrait::Expression &sexpr) {
	// Transform IN operations (e.g., x IN (1, 2, 3))
	const auto &substrait_in = sexpr.singular_or_list();

	// First value is the left side of IN, rest are the options
	vector<unique_ptr<ParsedExpression>> values;
	values.emplace_back(TransformExpr(substrait_in.value()));

	// Add all options from the IN list
	for (int32_t i = 0; i < substrait_in.options_size(); i++) {
		values.emplace_back(TransformExpr(substrait_in.options(i)));
	}

	return make_uniq<OperatorExpression>(ExpressionType::COMPARE_IN, std::move(values));
}

unique_ptr<ParsedExpression> SubstraitToAST::TransformNested(const substrait::Expression &sexpr) {
	// Transform nested structures: STRUCT, LIST, MAP
	auto &nested_expression = sexpr.nested();

	if (nested_expression.has_struct_()) {
		// STRUCT constructor (e.g., {'key': 'value', 'num': 42})
		auto &struct_expression = nested_expression.struct_();
		vector<unique_ptr<ParsedExpression>> children;
		for (auto &child : struct_expression.fields()) {
			children.emplace_back(TransformExpr(child));
		}
		// Use 'row' function for struct construction
		return make_uniq<FunctionExpression>("row", std::move(children));

	} else if (nested_expression.has_list()) {
		// LIST constructor (e.g., [1, 2, 3])
		auto &list_expression = nested_expression.list();
		vector<unique_ptr<ParsedExpression>> children;
		for (auto &child : list_expression.values()) {
			children.emplace_back(TransformExpr(child));
		}
		return make_uniq<FunctionExpression>("list_value", std::move(children));

	} else if (nested_expression.has_map()) {
		// MAP constructor (e.g., MAP(['k1', 'k2'], [1, 2]))
		auto &map_expression = nested_expression.map();
		vector<unique_ptr<ParsedExpression>> children;
		for (auto &key_value_pair : map_expression.key_values()) {
			children.emplace_back(TransformExpr(key_value_pair.key()));
			children.emplace_back(TransformExpr(key_value_pair.value()));
		}
		return make_uniq<FunctionExpression>("map", std::move(children));

	} else {
		throw NotImplementedException("Unsupported nested expression type");
	}
}

unique_ptr<ParsedExpression> SubstraitToAST::TransformExpr(const substrait::Expression &sexpr) {
	switch (sexpr.rex_type_case()) {
	case substrait::Expression::RexTypeCase::kLiteral:
		return TransformLiteralExpr(sexpr);
	case substrait::Expression::RexTypeCase::kSelection:
		return TransformSelectionExpr(sexpr);
	case substrait::Expression::RexTypeCase::kScalarFunction:
		return TransformScalarFunctionExpr(sexpr);
	case substrait::Expression::RexTypeCase::kCast:
		return TransformCastExpr(sexpr);
	case substrait::Expression::RexTypeCase::kIfThen:
		return TransformIfThenExpr(sexpr);
	case substrait::Expression::RexTypeCase::kSingularOrList:
		return TransformInExpr(sexpr);
	case substrait::Expression::RexTypeCase::kNested:
		return TransformNested(sexpr);
	default:
		throw NotImplementedException(
		    "Expression type not yet implemented in AST transformer: %s",
		    substrait::Expression::GetDescriptor()->FindFieldByNumber(sexpr.rex_type_case())->name());
	}
}

unique_ptr<TableRef> SubstraitToAST::TransformReadOp(const substrait::Rel &sop,
                                                      unique_ptr<ParsedExpression> *filter_out) {
	auto &sget = sop.read();

	// Extract filter if present (will be added as WHERE clause by caller)
	if (filter_out && sget.has_filter()) {
		*filter_out = TransformExpr(sget.filter());
	}

	unique_ptr<TableRef> base_ref;

	if (sget.has_virtual_table()) {
		// Handle VALUES clause (e.g., SELECT 1)
		if (!sget.virtual_table().values().empty()) {
			auto values_ref = make_uniq<ExpressionListRef>();
			auto literal_values = sget.virtual_table().values();

			// Transform each row of values
			for (auto &row : literal_values) {
				vector<unique_ptr<ParsedExpression>> expr_row;
				for (const auto &value : row.fields()) {
					auto literal_value = TransformLiteralToValue(value);
					expr_row.push_back(make_uniq<ConstantExpression>(literal_value));
				}
				values_ref->values.push_back(std::move(expr_row));
			}

			values_ref->alias = "values";
			base_ref = unique_ptr<TableRef>(values_ref.release());
		} else {
			throw NotImplementedException("Empty virtual table not supported");
		}
	} else if (sget.has_named_table()) {
		// Handle named tables
		auto table_ref = make_uniq<BaseTableRef>();
		table_ref->schema_name = DEFAULT_SCHEMA;
		table_ref->table_name = sget.named_table().names(0);
		base_ref = unique_ptr<TableRef>(table_ref.release());
	} else {
		throw NotImplementedException("Read operation type not yet supported in AST transformer");
	}

	return base_ref;
}

unique_ptr<TableRef> SubstraitToAST::TransformAggregateOp(const substrait::Rel &sop) {
	auto &sagg = sop.aggregate();
	auto select_node = make_uniq<SelectNode>();

	if (sagg.input().rel_type_case() == substrait::Rel::RelTypeCase::kRead) {
		select_node->from_table = TransformReadWithProjection(sagg.input());
	} else if (sagg.input().rel_type_case() == substrait::Rel::RelTypeCase::kFilter) {
		auto &filter = sagg.input().filter();
		auto &filter_input = filter.input();
		if (filter_input.rel_type_case() == substrait::Rel::RelTypeCase::kRead) {
			select_node->from_table = TransformReadWithProjection(filter_input);
			auto top_filter_expr = TransformExpr(filter.condition());
			select_node->where_clause = std::move(top_filter_expr);
		} else {
			substrait::RelRoot temp_root;
			temp_root.mutable_input()->CopyFrom(sagg.input());
			select_node->from_table = TransformRootOp(temp_root);
		}
	} else if (sagg.input().rel_type_case() == substrait::Rel::RelTypeCase::kAggregate) {
		substrait::RelRoot temp_root;
		temp_root.mutable_input()->CopyFrom(sagg.input());
		select_node->from_table = TransformRootOp(temp_root);
	} else if (sagg.input().rel_type_case() == substrait::Rel::RelTypeCase::kProject ||
	           sagg.input().rel_type_case() == substrait::Rel::RelTypeCase::kJoin ||
	           sagg.input().rel_type_case() == substrait::Rel::RelTypeCase::kSort ||
	           sagg.input().rel_type_case() == substrait::Rel::RelTypeCase::kFetch) {
		substrait::RelRoot temp_root;
		temp_root.mutable_input()->CopyFrom(sagg.input());
		select_node->from_table = TransformRootOp(temp_root);
	} else {
		throw NotImplementedException("Aggregate input type not yet supported: %s",
		                            substrait::Rel::GetDescriptor()
		                                ->FindFieldByNumber(sagg.input().rel_type_case())
		                                ->name());
	}

	if (sagg.groupings_size() > 0) {
		for (auto &sgrp : sagg.groupings()) {
			for (auto &sgrpexpr : sgrp.grouping_expressions()) {
				auto group_expr = TransformExpr(sgrpexpr);
				select_node->select_list.push_back(group_expr->Copy());
				select_node->groups.group_expressions.push_back(std::move(group_expr));
			}
		}
	}

	for (auto &smeas : sagg.measures()) {
		auto &s_aggr_function = smeas.measure();

		bool is_distinct = s_aggr_function.invocation() ==
		                   substrait::AggregateFunction_AggregationInvocation_AGGREGATION_INVOCATION_DISTINCT;

		vector<unique_ptr<ParsedExpression>> children;
		for (auto &sarg : s_aggr_function.arguments()) {
			if (sarg.has_value()) {
				auto arg_expr = TransformExpr(sarg.value());
				children.push_back(std::move(arg_expr));
			}
		}

		auto function_name = FindFunction(s_aggr_function.function_reference());
		function_name = RemoveFunctionExtension(function_name);

		if (function_name == "count" && children.empty()) {
			function_name = "count_star";
		}

		auto agg_expr = make_uniq<FunctionExpression>(function_name, std::move(children),
		                                               nullptr, nullptr, is_distinct);
		select_node->select_list.push_back(std::move(agg_expr));
	}

	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(select_node);
	auto subquery_ref = make_uniq<SubqueryRef>(std::move(select_stmt));

	static int agg_subquery_counter = 0;
	subquery_ref->alias = "agg_subquery_" + std::to_string(agg_subquery_counter++);

	return std::move(subquery_ref);
}

// Helper to convert PositionalReferenceExpression to ColumnRefExpression using schema
unique_ptr<ParsedExpression> SubstraitToAST::ConvertPositionalToColumnRef(unique_ptr<ParsedExpression> expr,
                                                                           const substrait::NamedStruct &schema) {
	if (!expr) {
		return expr;
	}

	switch (expr->type) {
	case ExpressionType::POSITIONAL_REFERENCE: {
		auto &pos_ref = expr->Cast<PositionalReferenceExpression>();
		// Convert 1-based position to 0-based index
		idx_t field_idx = pos_ref.index - 1;

		// Get column name from schema
		if (field_idx < (idx_t)schema.names_size()) {
			string col_name = schema.names((int)field_idx);
			return make_uniq<ColumnRefExpression>(col_name);
		}
		// If we can't find the name, return original
		return expr;
	}
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM: {
		auto &comp = expr->Cast<ComparisonExpression>();
		comp.left = ConvertPositionalToColumnRef(std::move(comp.left), schema);
		comp.right = ConvertPositionalToColumnRef(std::move(comp.right), schema);
		return expr;
	}
	case ExpressionType::CONJUNCTION_AND:
	case ExpressionType::CONJUNCTION_OR: {
		auto &conj = expr->Cast<ConjunctionExpression>();
		for (auto &child : conj.children) {
			child = ConvertPositionalToColumnRef(std::move(child), schema);
		}
		return expr;
	}
	case ExpressionType::OPERATOR_NOT:
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL: {
		auto &op = expr->Cast<OperatorExpression>();
		if (!op.children.empty()) {
			op.children[0] = ConvertPositionalToColumnRef(std::move(op.children[0]), schema);
		}
		return expr;
	}
	case ExpressionType::COMPARE_IN: {
		auto &op = expr->Cast<OperatorExpression>();
		for (auto &child : op.children) {
			child = ConvertPositionalToColumnRef(std::move(child), schema);
		}
		return expr;
	}
	case ExpressionType::FUNCTION: {
		auto &func = expr->Cast<FunctionExpression>();
		for (auto &child : func.children) {
			child = ConvertPositionalToColumnRef(std::move(child), schema);
		}
		return expr;
	}
	case ExpressionType::CAST: {
		auto &cast = expr->Cast<CastExpression>();
		cast.child = ConvertPositionalToColumnRef(std::move(cast.child), schema);
		return expr;
	}
	default:
		// For other expression types (constants, etc.), no conversion needed
		return expr;
	}
}


unique_ptr<ParsedExpression> SubstraitToAST::ConvertPositionalToColumnRef(unique_ptr<ParsedExpression> expr,
                                                                           const google::protobuf::RepeatedPtrField<std::string> &names) {
	if (!expr) {
		return expr;
	}

	switch (expr->type) {
	case ExpressionType::POSITIONAL_REFERENCE: {
		auto &pos_ref = expr->Cast<PositionalReferenceExpression>();
		idx_t field_idx = pos_ref.index - 1;
		if (field_idx < (idx_t)names.size()) {
			string col_name = names.Get((int)field_idx);
			return make_uniq<ColumnRefExpression>(col_name);
		}
		return expr;
	}
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM: {
		auto &comp = expr->Cast<ComparisonExpression>();
		comp.left = ConvertPositionalToColumnRef(std::move(comp.left), names);
		comp.right = ConvertPositionalToColumnRef(std::move(comp.right), names);
		return expr;
	}
	case ExpressionType::CONJUNCTION_AND:
	case ExpressionType::CONJUNCTION_OR: {
		auto &conj = expr->Cast<ConjunctionExpression>();
		for (auto &child : conj.children) {
			child = ConvertPositionalToColumnRef(std::move(child), names);
		}
		return expr;
	}
	case ExpressionType::FUNCTION: {
		auto &func = expr->Cast<FunctionExpression>();
		for (auto &child : func.children) {
			child = ConvertPositionalToColumnRef(std::move(child), names);
		}
		return expr;
	}
	default:
		return expr;
	}
}

unique_ptr<TableRef> SubstraitToAST::TransformReadWithProjection(const substrait::Rel &sop) {
	auto &sget = sop.read();

	bool has_projection = sget.has_projection();
	bool has_filter = sget.has_filter();

	if (!has_projection && !has_filter) {
		return TransformReadOp(sop);
	}

	auto select_node = make_uniq<SelectNode>();
	select_node->from_table = TransformReadOp(sop);

	if (has_filter && sget.has_base_schema()) {
		auto filter_expr = TransformExpr(sget.filter());
		filter_expr = ConvertPositionalToColumnRef(std::move(filter_expr), sget.base_schema());
		select_node->where_clause = std::move(filter_expr);
	}

	if (has_projection && sget.has_base_schema()) {
		const auto &projection = sget.projection().select();
		const auto &schema = sget.base_schema();

		for (int i = 0; i < projection.struct_items_size(); i++) {
			const auto &item = projection.struct_items(i);
			idx_t field_idx = item.field();

			if (field_idx < (idx_t)schema.names_size()) {
				string col_name = schema.names((int)field_idx);
				select_node->select_list.push_back(make_uniq<ColumnRefExpression>(col_name));
			} else {
				select_node->select_list.push_back(make_uniq<PositionalReferenceExpression>(field_idx + 1));
			}
		}
	} else {
		select_node->select_list.push_back(make_uniq<StarExpression>());
	}

	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(select_node);
	auto subquery = make_uniq<SubqueryRef>(std::move(select_stmt));

	static int subquery_counter = 0;
	subquery->alias = "subquery_" + std::to_string(subquery_counter++);

	return std::move(subquery);
}

unique_ptr<TableRef> SubstraitToAST::TransformJoinOp(const substrait::Rel &sop) {
	auto &sjoin = sop.join();

	// Map Substrait join type to DuckDB join type
	JoinType djointype;
	switch (sjoin.type()) {
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
		djointype = JoinType::INNER;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
		djointype = JoinType::LEFT;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
		djointype = JoinType::RIGHT;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SINGLE:
		djointype = JoinType::SINGLE;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
		djointype = JoinType::SEMI;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER:
		djointype = JoinType::OUTER;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI:
		djointype = JoinType::ANTI;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
		djointype = JoinType::RIGHT_SEMI;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT_ANTI:
		djointype = JoinType::RIGHT_ANTI;
		break;
	default:
		throw NotImplementedException("Unsupported join type: %s",
		                              substrait::JoinRel::GetDescriptor()->FindFieldByNumber(sjoin.type())->name());
	}

	// Build the join ref
	auto join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
	join_ref->type = djointype;

	// Transform left and right inputs
	if (sjoin.left().rel_type_case() == substrait::Rel::RelTypeCase::kRead) {
		join_ref->left = TransformReadWithProjection(sjoin.left());
	} else if (sjoin.left().rel_type_case() == substrait::Rel::RelTypeCase::kJoin) {
		join_ref->left = TransformJoinOp(sjoin.left());
	} else if (sjoin.left().rel_type_case() == substrait::Rel::RelTypeCase::kProject ||
	           sjoin.left().rel_type_case() == substrait::Rel::RelTypeCase::kFilter ||
	           sjoin.left().rel_type_case() == substrait::Rel::RelTypeCase::kAggregate ||
	           sjoin.left().rel_type_case() == substrait::Rel::RelTypeCase::kSort ||
	           sjoin.left().rel_type_case() == substrait::Rel::RelTypeCase::kFetch) {
		substrait::RelRoot temp_root;
		temp_root.mutable_input()->CopyFrom(sjoin.left());
		join_ref->left = TransformRootOp(temp_root);
	} else {
		throw NotImplementedException("Join left input type not yet supported: %s",
		                            substrait::Rel::GetDescriptor()
		                                ->FindFieldByNumber(sjoin.left().rel_type_case())
		                                ->name());
	}

	if (sjoin.right().rel_type_case() == substrait::Rel::RelTypeCase::kRead) {
		join_ref->right = TransformReadWithProjection(sjoin.right());
	} else if (sjoin.right().rel_type_case() == substrait::Rel::RelTypeCase::kJoin) {
		join_ref->right = TransformJoinOp(sjoin.right());
	} else if (sjoin.right().rel_type_case() == substrait::Rel::RelTypeCase::kProject ||
	           sjoin.right().rel_type_case() == substrait::Rel::RelTypeCase::kFilter ||
	           sjoin.right().rel_type_case() == substrait::Rel::RelTypeCase::kAggregate ||
	           sjoin.right().rel_type_case() == substrait::Rel::RelTypeCase::kSort ||
	           sjoin.right().rel_type_case() == substrait::Rel::RelTypeCase::kFetch) {
		substrait::RelRoot temp_root;
		temp_root.mutable_input()->CopyFrom(sjoin.right());
		join_ref->right = TransformRootOp(temp_root);
	} else {
		throw NotImplementedException("Join right input type not yet supported: %s",
		                            substrait::Rel::GetDescriptor()
		                                ->FindFieldByNumber(sjoin.right().rel_type_case())
		                                ->name());
	}

	// Transform join condition
	if (sjoin.has_expression()) {
		join_ref->condition = TransformExpr(sjoin.expression());
	}

	return unique_ptr<TableRef>(join_ref.release());
}

SetOperationType SubstraitToAST::TransformSetOperationType(int32_t setop) {
	switch (setop) {
	case substrait::SetRel_SetOp_SET_OP_UNION_ALL:
		return SetOperationType::UNION;
	case substrait::SetRel_SetOp_SET_OP_MINUS_PRIMARY:
		return SetOperationType::EXCEPT;
	case substrait::SetRel_SetOp_SET_OP_INTERSECTION_PRIMARY:
		return SetOperationType::INTERSECT;
	default:
		throw NotImplementedException("SetOperationType transform not implemented for SetRel_SetOp type %d", setop);
	}
}

unique_ptr<TableRef> SubstraitToAST::TransformCrossProductOp(const substrait::Rel &sop) {
	auto &scross = sop.cross();

	auto join_ref = make_uniq<JoinRef>(JoinRefType::CROSS);

	if (scross.left().rel_type_case() == substrait::Rel::RelTypeCase::kRead) {
		join_ref->left = TransformReadWithProjection(scross.left());
	} else {
		substrait::RelRoot temp_root;
		temp_root.mutable_input()->CopyFrom(scross.left());
		join_ref->left = TransformRootOp(temp_root);
	}

	if (scross.right().rel_type_case() == substrait::Rel::RelTypeCase::kRead) {
		join_ref->right = TransformReadWithProjection(scross.right());
	} else {
		substrait::RelRoot temp_root;
		temp_root.mutable_input()->CopyFrom(scross.right());
		join_ref->right = TransformRootOp(temp_root);
	}

	return std::move(join_ref);
}

unique_ptr<TableRef> SubstraitToAST::TransformSetOp(const substrait::Rel &sop) {
	auto &set = sop.set();
	auto set_op_type = set.op();
	auto duck_setop_type = TransformSetOperationType(set_op_type);

	// Check if this is UNION ALL (setop_all = true) or UNION DISTINCT (setop_all = false)
	// For Substrait, SET_OP_UNION_ALL means ALL modifier is present
	bool setop_all = (set_op_type == substrait::SetRel_SetOp_SET_OP_UNION_ALL);

	auto input_count = set.inputs_size();
	if (input_count != 2) {
		throw NotImplementedException("Set operations with %d inputs not supported (expected 2)", input_count);
	}

	// Transform left and right inputs recursively
	substrait::RelRoot left_root;
	left_root.mutable_input()->CopyFrom(set.inputs(0));
	auto left_ref = TransformRootOp(left_root);

	substrait::RelRoot right_root;
	right_root.mutable_input()->CopyFrom(set.inputs(1));
	auto right_ref = TransformRootOp(right_root);

	// Extract the query nodes from the SubqueryRef wrappers
	if (!left_ref || left_ref->type != TableReferenceType::SUBQUERY) {
		throw InternalException("Expected SubqueryRef for left input of set operation");
	}
	if (!right_ref || right_ref->type != TableReferenceType::SUBQUERY) {
		throw InternalException("Expected SubqueryRef for right input of set operation");
	}

	auto &left_subquery = left_ref->Cast<SubqueryRef>();
	auto &right_subquery = right_ref->Cast<SubqueryRef>();

	// Create the SetOperationNode
	auto setop_node = make_uniq<SetOperationNode>();
	setop_node->setop_type = duck_setop_type;
	setop_node->setop_all = setop_all;
	setop_node->left = std::move(left_subquery.subquery->node);
	setop_node->right = std::move(right_subquery.subquery->node);

	// Wrap in SelectStatement and SubqueryRef
	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(setop_node);

	return make_uniq<SubqueryRef>(std::move(select_stmt));
}

OrderByNode SubstraitToAST::TransformOrder(const substrait::SortField &sordf) {
	OrderType dordertype;
	OrderByNullType dnullorder;

	switch (sordf.direction()) {
	case substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
		dordertype = OrderType::ASCENDING;
		dnullorder = OrderByNullType::NULLS_FIRST;
		break;
	case substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
		dordertype = OrderType::ASCENDING;
		dnullorder = OrderByNullType::NULLS_LAST;
		break;
	case substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
		dordertype = OrderType::DESCENDING;
		dnullorder = OrderByNullType::NULLS_FIRST;
		break;
	case substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
		dordertype = OrderType::DESCENDING;
		dnullorder = OrderByNullType::NULLS_LAST;
		break;
	default:
		throw NotImplementedException(
		    "Unsupported ordering %s",
		    substrait::SortField::GetDescriptor()->FindFieldByNumber(sordf.direction())->name());
	}

	return {dordertype, dnullorder, TransformExpr(sordf.expr())};
}

unique_ptr<TableRef> SubstraitToAST::TransformRootOp(const substrait::RelRoot &sop) {
	auto &input = sop.input();

	// Extract top-level modifiers (Sort, Fetch) by unwrapping them
	vector<unique_ptr<ResultModifier>> modifiers;
	const substrait::Rel *current_rel = &input;

	// Process Sort and Fetch operations at the top level
	while (true) {
		if (current_rel->rel_type_case() == substrait::Rel::RelTypeCase::kSort) {
			// Extract ORDER BY
			auto order_modifier = make_uniq<OrderModifier>();
			for (auto &sordf : current_rel->sort().sorts()) {
				order_modifier->orders.push_back(TransformOrder(sordf));
			}
			modifiers.push_back(std::move(order_modifier));
			current_rel = &current_rel->sort().input();
		} else if (current_rel->rel_type_case() == substrait::Rel::RelTypeCase::kFetch) {
			// Extract LIMIT and OFFSET
			auto limit_modifier = make_uniq<LimitModifier>();
			auto &fetch = current_rel->fetch();

			// Handle limit (-1 means no limit)
			if (fetch.count() != -1) {
				limit_modifier->limit = make_uniq<ConstantExpression>(Value::BIGINT(fetch.count()));
			}

			// Handle offset
			if (fetch.offset() > 0) {
				limit_modifier->offset = make_uniq<ConstantExpression>(Value::BIGINT(fetch.offset()));
			}

			modifiers.push_back(std::move(limit_modifier));
			current_rel = &current_rel->fetch().input();
		} else {
			// No more Sort/Fetch operations to unwrap
			break;
		}
	}

	// Now process the remaining relation (Project, Filter, Read, etc.)
	unique_ptr<SelectNode> select_node;

	// Handle different relation types
	if (current_rel->rel_type_case() == substrait::Rel::RelTypeCase::kProject) {
		auto &project = current_rel->project();
		auto &project_input = project.input();

		select_node = make_uniq<SelectNode>();

		if (project_input.rel_type_case() == substrait::Rel::RelTypeCase::kRead) {
			select_node->from_table = TransformReadWithProjection(project_input);
		} else if (project_input.rel_type_case() == substrait::Rel::RelTypeCase::kFilter) {
			auto &filter = project_input.filter();
			auto &filter_input = filter.input();

			if (filter_input.rel_type_case() == substrait::Rel::RelTypeCase::kRead) {
				select_node->from_table = TransformReadWithProjection(filter_input);
				auto filter_expr = TransformExpr(filter.condition());
				select_node->where_clause = std::move(filter_expr);
			} else if (filter_input.rel_type_case() == substrait::Rel::RelTypeCase::kAggregate) {
				substrait::RelRoot temp_root;
				temp_root.mutable_input()->CopyFrom(project_input);
				for (int i = 0; i < sop.names_size(); i++) {
					temp_root.add_names(sop.names(i));
				}
				select_node->from_table = TransformRootOp(temp_root);
			} else {
				throw NotImplementedException("Filter input type not yet supported: %s",
				                            substrait::Rel::GetDescriptor()
				                                ->FindFieldByNumber(filter_input.rel_type_case())
				                                ->name());
			}
		} else if (project_input.rel_type_case() == substrait::Rel::RelTypeCase::kJoin) {
			select_node->from_table = TransformJoinOp(project_input);
		} else if (project_input.rel_type_case() == substrait::Rel::RelTypeCase::kCross) {
			select_node->from_table = TransformCrossProductOp(project_input);
		} else if (project_input.rel_type_case() == substrait::Rel::RelTypeCase::kAggregate) {
			select_node->from_table = TransformAggregateOp(project_input);
		} else if (project_input.rel_type_case() == substrait::Rel::RelTypeCase::kProject) {
		substrait::RelRoot temp_root;
		temp_root.mutable_input()->CopyFrom(project_input);
		for (int i = 0; i < sop.names_size(); i++) {
			temp_root.add_names(sop.names(i));
		}
		select_node->from_table = TransformRootOp(temp_root);
	} else if (project_input.rel_type_case() == substrait::Rel::RelTypeCase::kFetch ||
	           project_input.rel_type_case() == substrait::Rel::RelTypeCase::kSort) {
		substrait::RelRoot temp_root;
		temp_root.mutable_input()->CopyFrom(project_input);
		for (int i = 0; i < sop.names_size(); i++) {
			temp_root.add_names(sop.names(i));
		}
		select_node->from_table = TransformRootOp(temp_root);
		} else {
			throw NotImplementedException("Project input type not yet supported: %s",
			                            substrait::Rel::GetDescriptor()
			                                ->FindFieldByNumber(project_input.rel_type_case())
			                                ->name());
		}

		if (project.has_common() && project.common().has_emit() &&
		    project.common().emit().output_mapping_size() > 0) {
			auto &output_mapping = project.common().emit().output_mapping();

			vector<unique_ptr<ParsedExpression>> computed_exprs;
			for (auto &sexpr : project.expressions()) {
				auto expr = TransformExpr(sexpr);
				computed_exprs.push_back(std::move(expr));
			}

			idx_t num_inputs = 0;

			if (select_node->from_table && select_node->from_table->type == TableReferenceType::SUBQUERY) {
				auto &subquery_ref = select_node->from_table->Cast<SubqueryRef>();
				if (subquery_ref.subquery && subquery_ref.subquery->node) {
					auto &query_node = subquery_ref.subquery->node;
					if (query_node->type == QueryNodeType::SELECT_NODE) {
						auto &inner_select = query_node->Cast<SelectNode>();
						num_inputs = inner_select.select_list.size();
					}
				}
			}

			if (num_inputs == 0) {
				int32_t max_mapping_idx = 0;
				for (int i = 0; i < output_mapping.size(); i++) {
					max_mapping_idx = std::max(max_mapping_idx, output_mapping.Get(i));
				}
				idx_t num_computed = computed_exprs.size();
				num_inputs = (max_mapping_idx >= (int32_t)num_computed) ?
				             (max_mapping_idx - num_computed + 1) : 0;
			}

			for (int i = 0; i < output_mapping.size(); i++) {
				int32_t field_idx = output_mapping.Get(i);

				if ((idx_t)field_idx < num_inputs) {
					select_node->select_list.push_back(make_uniq<PositionalReferenceExpression>(field_idx + 1));
				} else {
					idx_t expr_idx = field_idx - num_inputs;
					if (expr_idx < computed_exprs.size()) {
						select_node->select_list.push_back(std::move(computed_exprs[expr_idx]));
						computed_exprs[expr_idx] = nullptr;
					}
				}
			}
		} else {
			for (auto &sexpr : project.expressions()) {
				auto expr = TransformExpr(sexpr);
				select_node->select_list.push_back(std::move(expr));
			}
		}

		if (select_node->select_list.empty()) {
			select_node->select_list.push_back(make_uniq<StarExpression>());
		}


		for (auto &modifier : modifiers) {
			select_node->modifiers.push_back(std::move(modifier));
		}

		auto select_stmt = make_uniq<SelectStatement>();
		select_stmt->node = std::move(select_node);

		return make_uniq<SubqueryRef>(std::move(select_stmt));
	} else if (current_rel->rel_type_case() == substrait::Rel::RelTypeCase::kFilter) {
		auto &filter = current_rel->filter();
		auto &filter_input = filter.input();

		if (filter_input.rel_type_case() == substrait::Rel::RelTypeCase::kAggregate) {
			auto agg_ref = TransformAggregateOp(filter_input);

			auto &agg_subquery = agg_ref->Cast<SubqueryRef>();
			auto *select_stmt_ptr = agg_subquery.subquery.get();
			auto *stmt = dynamic_cast<SelectStatement*>(select_stmt_ptr);
			if (!stmt || !stmt->node) {
				throw InternalException("Expected SelectStatement with SelectNode in aggregate operation");
			}
			auto *node_ptr = stmt->node.get();
			if (!node_ptr || node_ptr->type != QueryNodeType::SELECT_NODE) {
				throw InternalException("Expected SelectNode in aggregate SelectStatement");
			}
			select_node = unique_ptr<SelectNode>((SelectNode*)stmt->node.release());

			for (idx_t i = 0; i < select_node->select_list.size() && i < (idx_t)sop.names_size(); i++) {
				select_node->select_list[i]->alias = sop.names((int)i);
			}

			auto having_expr = TransformExpr(filter.condition());
			having_expr = ConvertPositionalToColumnRef(std::move(having_expr), sop.names());
			select_node->having = std::move(having_expr);

			for (auto &modifier : modifiers) {
				select_node->modifiers.push_back(std::move(modifier));
			}

			auto new_stmt = make_uniq<SelectStatement>();
			new_stmt->node = std::move(select_node);
			return make_uniq<SubqueryRef>(std::move(new_stmt));
		}

		select_node = make_uniq<SelectNode>();


		if (filter_input.rel_type_case() == substrait::Rel::RelTypeCase::kRead) {
			select_node->from_table = TransformReadWithProjection(filter_input);
		} else {
			throw NotImplementedException("Filter input type not yet supported");
		}

		auto top_filter_expr = TransformExpr(filter.condition());
		select_node->where_clause = std::move(top_filter_expr);


		select_node->select_list.push_back(make_uniq<StarExpression>());

		for (auto &modifier : modifiers) {
			select_node->modifiers.push_back(std::move(modifier));
		}

		auto select_stmt = make_uniq<SelectStatement>();
		select_stmt->node = std::move(select_node);

		return make_uniq<SubqueryRef>(std::move(select_stmt));
	} else if (current_rel->rel_type_case() == substrait::Rel::RelTypeCase::kRead) {
		select_node = make_uniq<SelectNode>();
		select_node->from_table = TransformReadWithProjection(*current_rel);
		select_node->select_list.push_back(make_uniq<StarExpression>());

		for (auto &modifier : modifiers) {
			select_node->modifiers.push_back(std::move(modifier));
		}

		auto select_stmt = make_uniq<SelectStatement>();
		select_stmt->node = std::move(select_node);

		return make_uniq<SubqueryRef>(std::move(select_stmt));
	} else if (current_rel->rel_type_case() == substrait::Rel::RelTypeCase::kAggregate) {
		auto agg_ref = TransformAggregateOp(*current_rel);

		if (!modifiers.empty()) {
			select_node = make_uniq<SelectNode>();
			select_node->from_table = std::move(agg_ref);
			select_node->select_list.push_back(make_uniq<StarExpression>());

			for (auto &modifier : modifiers) {
				select_node->modifiers.push_back(std::move(modifier));
			}

			auto select_stmt = make_uniq<SelectStatement>();
			select_stmt->node = std::move(select_node);
			return make_uniq<SubqueryRef>(std::move(select_stmt));
		}

		return agg_ref;
	} else if (current_rel->rel_type_case() == substrait::Rel::RelTypeCase::kJoin) {
		auto join_ref = TransformJoinOp(*current_rel);

		if (!modifiers.empty()) {
			select_node = make_uniq<SelectNode>();
			select_node->from_table = std::move(join_ref);
			select_node->select_list.push_back(make_uniq<StarExpression>());

			for (auto &modifier : modifiers) {
				select_node->modifiers.push_back(std::move(modifier));
			}

			auto select_stmt = make_uniq<SelectStatement>();
			select_stmt->node = std::move(select_node);
			return make_uniq<SubqueryRef>(std::move(select_stmt));
		}

		select_node = make_uniq<SelectNode>();
		select_node->from_table = std::move(join_ref);
		select_node->select_list.push_back(make_uniq<StarExpression>());

		auto select_stmt = make_uniq<SelectStatement>();
		select_stmt->node = std::move(select_node);
		return make_uniq<SubqueryRef>(std::move(select_stmt));
	} else if (current_rel->rel_type_case() == substrait::Rel::RelTypeCase::kCross) {
		auto cross_ref = TransformCrossProductOp(*current_rel);

		if (!modifiers.empty()) {
			select_node = make_uniq<SelectNode>();
			select_node->from_table = std::move(cross_ref);
			select_node->select_list.push_back(make_uniq<StarExpression>());

			for (auto &modifier : modifiers) {
				select_node->modifiers.push_back(std::move(modifier));
			}

			auto select_stmt = make_uniq<SelectStatement>();
			select_stmt->node = std::move(select_node);
			return make_uniq<SubqueryRef>(std::move(select_stmt));
		}

		select_node = make_uniq<SelectNode>();
		select_node->from_table = std::move(cross_ref);
		select_node->select_list.push_back(make_uniq<StarExpression>());

		auto select_stmt = make_uniq<SelectStatement>();
		select_stmt->node = std::move(select_node);
		return make_uniq<SubqueryRef>(std::move(select_stmt));
	} else if (current_rel->rel_type_case() == substrait::Rel::RelTypeCase::kSet) {
		auto set_ref = TransformSetOp(*current_rel);

		if (!modifiers.empty()) {
			select_node = make_uniq<SelectNode>();
			select_node->from_table = std::move(set_ref);
			select_node->select_list.push_back(make_uniq<StarExpression>());

			for (auto &modifier : modifiers) {
				select_node->modifiers.push_back(std::move(modifier));
			}

			auto select_stmt = make_uniq<SelectStatement>();
			select_stmt->node = std::move(select_node);
			return make_uniq<SubqueryRef>(std::move(select_stmt));
		}

		return set_ref;
	}

	throw NotImplementedException("Root operation type not yet supported: %s",
	                            substrait::Rel::GetDescriptor()->FindFieldByNumber(current_rel->rel_type_case())->name());
}

unique_ptr<TableRef> SubstraitToAST::TransformPlanToTableRef() {
	if (plan.relations().empty()) {
		throw InvalidInputException("Substrait Plan does not have a SELECT statement");
	}

	return TransformRootOp(plan.relations(0).root());
}

} // namespace duckdb
