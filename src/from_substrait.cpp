#include "from_substrait.hpp"

#include <cinttypes>
#include <cmath>

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"

#include "duckdb/parser/expression/comparison_expression.hpp"

#include "duckdb/main/client_data.hpp"
#include "google/protobuf/util/json_util.h"
#include "substrait/plan.pb.h"

#include "duckdb/main/table_description.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/helper.hpp"

#include "duckdb/main/relation.hpp"
#include "duckdb/main/relation/create_table_relation.hpp"
#include <duckdb/main/relation/delete_relation.hpp>
#include "duckdb/main/relation/table_relation.hpp"
#include "duckdb/main/relation/table_function_relation.hpp"
#include "duckdb/main/relation/value_relation.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/cross_product_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/setop_relation.hpp"

namespace duckdb {
const std::unordered_map<std::string, std::string> SubstraitToDuckDB::function_names_remap = {
    {"modulus", "mod"},      {"std_dev", "stddev"},     {"starts_with", "prefix"},
    {"ends_with", "suffix"}, {"substring", "substr"},   {"char_length", "length"},
    {"is_nan", "isnan"},     {"is_finite", "isfinite"}, {"is_infinite", "isinf"},
    {"like", "~~"},          {"extract", "date_part"},  {"bitwise_and", "&"},
    {"bitwise_or", "|"},     {"bitwise_xor", "xor"},    {"octet_length", "strlen"}};

const case_insensitive_set_t SubstraitToDuckDB::valid_extract_subfields = {
    "year",    "month",       "day",          "decade", "century", "millenium",
    "quarter", "microsecond", "milliseconds", "second", "minute",  "hour"};

string SubstraitToDuckDB::RemapFunctionName(const string &function_name) {
	// Let's first drop any extension id
	string name;
	for (auto &c : function_name) {
		if (c == ':') {
			break;
		}
		name += c;
	}
	auto it = function_names_remap.find(name);
	if (it != function_names_remap.end()) {
		name = it->second;
	}
	return name;
}

string SubstraitToDuckDB::RemoveExtension(const string &function_name) {
	// Lets first drop any extension id
	string name;
	for (auto &c : function_name) {
		if (c == ':') {
			break;
		}
		name += c;
	}
	return name;
}

SubstraitToDuckDB::SubstraitToDuckDB(shared_ptr<ClientContext> &context_p, const string &serialized, bool json,
                                     bool acquire_lock_p)
    : context(context_p), acquire_lock(acquire_lock_p) {
	if (!json) {
		if (!plan.ParseFromString(serialized)) {
			throw std::runtime_error("Was not possible to convert binary into Substrait plan");
		}
	} else {
		google::protobuf::util::Status status = google::protobuf::util::JsonStringToMessage(serialized, &plan);
		if (!status.ok()) {
			throw std::runtime_error("Was not possible to convert JSON into Substrait plan: " + status.ToString());
		}
	}

	for (auto &sext : plan.extensions()) {
		if (!sext.has_extension_function()) {
			continue;
		}
		functions_map[sext.extension_function().function_anchor()] = sext.extension_function().name();
	}
}

Value TransformLiteralToValue(const substrait::Expression_Literal &literal) {
	if (literal.has_null()) {
		return Value(LogicalType::SQLNULL);
	}
	switch (literal.literal_type_case()) {
	case substrait::Expression_Literal::LiteralTypeCase::kFp64:
		return Value::DOUBLE(literal.fp64());
	case substrait::Expression_Literal::LiteralTypeCase::kFp32:
		return Value::FLOAT(literal.fp32());
	case substrait::Expression_Literal::LiteralTypeCase::kString:
		return {literal.string()};
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
	case substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
		return Value(literal.boolean());
	}
	case substrait::Expression_Literal::LiteralTypeCase::kI8:
		return Value::TINYINT(static_cast<int8_t>(literal.i8()));
	case substrait::Expression_Literal::LiteralTypeCase::kI32:
		return Value::INTEGER(literal.i32());
	case substrait::Expression_Literal::LiteralTypeCase::kI64:
		return Value::BIGINT(literal.i64());
	case substrait::Expression_Literal::LiteralTypeCase::kDate: {
		date_t date(literal.date());
		return Value::DATE(date);
	}
	case substrait::Expression_Literal::LiteralTypeCase::kPrecisionTime: {
		dtime_t time(literal.precision_time().value());
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
		// Convert seconds to microseconds and add subseconds (with precision adjustment)
		int64_t seconds_in_micros = literal.interval_day_to_second().seconds() * Interval::MICROS_PER_SEC;
		int64_t subseconds = literal.interval_day_to_second().subseconds();
		int32_t precision = literal.interval_day_to_second().precision();
		// Adjust subseconds based on precision (e.g., precision 9 = nanoseconds, need to convert to microseconds)
		if (precision > 6) {
			// Convert from higher precision (e.g., nanoseconds) to microseconds
			subseconds = subseconds / (int64_t)pow(10, precision - 6);
		} else if (precision < 6) {
			// Convert from lower precision (e.g., milliseconds) to microseconds
			subseconds = subseconds * (int64_t)pow(10, 6 - precision);
		}
		interval.micros = seconds_in_micros + subseconds;
		return Value::INTERVAL(interval);
	}
	case substrait::Expression_Literal::LiteralTypeCase::kVarChar:
		return {literal.var_char().value()};
	default:
		throw NotImplementedException(
		    "literals of this type are not implemented: %s",
		    substrait::Expression_Literal::GetDescriptor()->FindFieldByNumber(literal.literal_type_case())->name());
	}
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformLiteralExpr(const substrait::Expression &sexpr) {
	return make_uniq<ConstantExpression>(TransformLiteralToValue(sexpr.literal()));
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformSelectionExpr(const substrait::Expression &sexpr) {
	if (!sexpr.selection().has_direct_reference() || !sexpr.selection().direct_reference().has_struct_field()) {
		throw SyntaxException("Can only have direct struct references in selections");
	}
	return make_uniq<PositionalReferenceExpression>(sexpr.selection().direct_reference().struct_field().field() + 1);
}

void SubstraitToDuckDB::VerifyCorrectExtractSubfield(const string &subfield) {
	D_ASSERT(SubstraitToDuckDB::valid_extract_subfields.count(subfield));
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformScalarFunctionExpr(const substrait::Expression &sexpr) {
	auto function_name = FindFunction(sexpr.scalar_function().function_reference());
	function_name = RemoveExtension(function_name);
	vector<unique_ptr<ParsedExpression>> children;
	vector<string> enum_expressions;
	auto &function_arguments = sexpr.scalar_function().arguments();
	for (auto &sarg : function_arguments) {
		if (sarg.has_value()) {
			// value expression
			children.push_back(TransformExpr(sarg.value()));
		} else if (sarg.has_type()) {
			// type expression
			throw NotImplementedException("Type arguments in Substrait expressions are not supported yet!");
		} else {
			// enum expression
			D_ASSERT(sarg.has_enum_());
			auto &enum_str = sarg.enum_();
			enum_expressions.push_back(enum_str);
		}
	}
	// string compare galore
	// TODO simplify this
	if (function_name == "and") {
		return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(children));
	} else if (function_name == "or") {
		return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(children));
	} else if (function_name == "lt") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "equal") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "not_equal") {
		D_ASSERT(children.size() == 2);
		// FIXME: We do a not_like if we are doing a string comparison
		// This is due to substrait not supporting !~~
		bool is_it_string = false;
		for (idx_t child_idx = 0; child_idx < 2; child_idx++) {
			if (children[child_idx]->GetExpressionClass() == ExpressionClass::CONSTANT) {
				auto &constant = children[child_idx]->Cast<ConstantExpression>();
				if (constant.value.type() == LogicalType::VARCHAR) {
					is_it_string = true;
				}
			}
		}
		if (is_it_string) {
			string not_equal = "!~~";
			return make_uniq<FunctionExpression>(not_equal, std::move(children));
		} else {
			return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, std::move(children[0]),
			                                       std::move(children[1]));
		}

	} else if (function_name == "lte") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "gte") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "gt") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "is_not_null") {
		D_ASSERT(children.size() == 1);
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(children[0]));
	} else if (function_name == "is_null") {
		D_ASSERT(children.size() == 1);
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, std::move(children[0]));
	} else if (function_name == "not") {
		D_ASSERT(children.size() == 1);
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(children[0]));
	} else if (function_name == "is_not_distinct_from") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "between") {
		D_ASSERT(children.size() == 3);
		return make_uniq<BetweenExpression>(std::move(children[0]), std::move(children[1]), std::move(children[2]));
	} else if (function_name == "coalesce") {
		// COALESCE is a variadic function that returns the first non-NULL value
		// Convert to DuckDB's OPERATOR_COALESCE
		D_ASSERT(children.size() >= 1);
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE, std::move(children));
	} else if (function_name == "extract") {
		D_ASSERT(enum_expressions.size() == 1);
		auto &subfield = enum_expressions[0];
		VerifyCorrectExtractSubfield(subfield);
		auto constant_expression = make_uniq<ConstantExpression>(Value(subfield));
		children.insert(children.begin(), std::move(constant_expression));
	}

	return make_uniq<FunctionExpression>(RemapFunctionName(function_name), std::move(children));
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformIfThenExpr(const substrait::Expression &sexpr) {
	const auto &scase = sexpr.if_then();
	auto dcase = make_uniq<CaseExpression>();
	for (const auto &sif : scase.ifs()) {
		CaseCheck dif;
		dif.when_expr = TransformExpr(sif.if_());
		dif.then_expr = TransformExpr(sif.then());
		dcase->case_checks.push_back(std::move(dif));
	}
	dcase->else_expr = TransformExpr(scase.else_());
	return std::move(dcase);
}

LogicalType SubstraitToDuckDB::SubstraitToDuckType(const substrait::Type &s_type) {
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
	case substrait::Type::KindCase::kPrecisionTime:
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
	case substrait::Type::KindCase::kPrecisionTimestamp: {
		auto &s_precision_timestamp = s_type.precision_timestamp();
		auto precision = s_precision_timestamp.precision();
		switch (precision) {
		case 0:
			return {LogicalTypeId::TIMESTAMP_SEC};
		case 3:
			return {LogicalTypeId::TIMESTAMP_MS};
		case 6:
			return {LogicalTypeId::TIMESTAMP};
		case 9:
			return {LogicalTypeId::TIMESTAMP_NS};
		default:
			throw NotImplementedException("Unsupported timestamp precision: %d", precision);
		}
	}
	case substrait::Type::KindCase::kPrecisionTimestampTz: {
		// DuckDB's TIMESTAMP_TZ is always microsecond precision (6)
		auto &s_precision_timestamp_tz = s_type.precision_timestamp_tz();
		auto precision = s_precision_timestamp_tz.precision();
		if (precision != 6) {
			throw NotImplementedException("DuckDB TIMESTAMP_TZ only supports microsecond precision (6), got: %d", precision);
		}
		return {LogicalTypeId::TIMESTAMP_TZ};
	}
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

		for (idx_t i = 0; i < s_struct_type.types_size(); i++) {
			auto field_name = "f" + std::to_string(i);
			auto field_type = SubstraitToDuckType(s_struct_type.types(i));
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

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformCastExpr(const substrait::Expression &sexpr) {
	const auto &scast = sexpr.cast();
	auto cast_type = SubstraitToDuckType(scast.type());
	auto cast_child = TransformExpr(scast.input());
	return make_uniq<CastExpression>(cast_type, std::move(cast_child));
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformInExpr(const substrait::Expression &sexpr) {
	const auto &substrait_in = sexpr.singular_or_list();

	vector<unique_ptr<ParsedExpression>> values;
	values.emplace_back(TransformExpr(substrait_in.value()));

	for (int32_t i = 0; i < substrait_in.options_size(); i++) {
		values.emplace_back(TransformExpr(substrait_in.options(i)));
	}

	return make_uniq<OperatorExpression>(ExpressionType::COMPARE_IN, std::move(values));
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformNested(const substrait::Expression &sexpr,
                                                                RootNameIterator *iterator) {
	auto &nested_expression = sexpr.nested();
	if (nested_expression.has_struct_()) {
		auto &struct_expression = nested_expression.struct_();
		vector<unique_ptr<ParsedExpression>> children;
		for (auto &child : struct_expression.fields()) {
			children.emplace_back(TransformExpr(child));
		}
		if (iterator && !iterator->Finished() && iterator->Unique(children.size())) {
			for (auto &child : children) {
				child->alias = iterator->GetCurrentName();
				iterator->Next();
			}
			return make_uniq<FunctionExpression>("struct_pack", std::move(children));
		} else {
			return make_uniq<FunctionExpression>("row", std::move(children));
		}

	} else if (nested_expression.has_list()) {
		auto &list_expression = nested_expression.list();
		vector<unique_ptr<ParsedExpression>> children;
		for (auto &child : list_expression.values()) {
			children.emplace_back(TransformExpr(child));
		}
		return make_uniq<FunctionExpression>("list_value", std::move(children));

	} else if (nested_expression.has_map()) {
		auto &map_expression = nested_expression.map();
		vector<unique_ptr<ParsedExpression>> children;
		for (auto &key_value_pair : map_expression.key_values()) {
			children.emplace_back(TransformExpr(key_value_pair.key()));
			children.emplace_back(TransformExpr(key_value_pair.value()));
		}
		return make_uniq<FunctionExpression>("map", std::move(children));

	} else {
		throw NotImplementedException("Substrait nested expression is not yet implemented.");
	}
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformExpr(const substrait::Expression &sexpr,
                                                              RootNameIterator *iterator) {
	if (iterator) {
		iterator->Next();
	}
	switch (sexpr.rex_type_case()) {
	case substrait::Expression::RexTypeCase::kLiteral:
		return TransformLiteralExpr(sexpr);
	case substrait::Expression::RexTypeCase::kSelection:
		return TransformSelectionExpr(sexpr);
	case substrait::Expression::RexTypeCase::kScalarFunction:
		return TransformScalarFunctionExpr(sexpr);
	case substrait::Expression::RexTypeCase::kIfThen:
		return TransformIfThenExpr(sexpr);
	case substrait::Expression::RexTypeCase::kCast:
		return TransformCastExpr(sexpr);
	case substrait::Expression::RexTypeCase::kSingularOrList:
		return TransformInExpr(sexpr);
	case substrait::Expression::RexTypeCase::kNested:
		return TransformNested(sexpr, iterator);
	case substrait::Expression::RexTypeCase::kSubquery:
	default:
		throw NotImplementedException(
		    "Unsupported expression type %s",
		    substrait::Expression::GetDescriptor()->FindFieldByNumber(sexpr.rex_type_case())->name());
	}
}

string SubstraitToDuckDB::FindFunction(uint64_t id) {
	if (functions_map.find(id) == functions_map.end()) {
		throw NotImplementedException("Could not find aggregate function %s", to_string(id));
	}
	return functions_map[id];
}

OrderByNode SubstraitToDuckDB::TransformOrder(const substrait::SortField &sordf) {

	OrderType dordertype;
	OrderByNullType dnullorder;

	switch (sordf.direction()) {
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
		dordertype = OrderType::ASCENDING;
		dnullorder = OrderByNullType::NULLS_FIRST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
		dordertype = OrderType::ASCENDING;
		dnullorder = OrderByNullType::NULLS_LAST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
		dordertype = OrderType::DESCENDING;
		dnullorder = OrderByNullType::NULLS_FIRST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
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

shared_ptr<Relation> SubstraitToDuckDB::TransformJoinOp(const substrait::Rel &sop) {
	auto &sjoin = sop.join();

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
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
		djointype = JoinType::RIGHT_SEMI;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_MARK:
		djointype = JoinType::MARK;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER:
		djointype = JoinType::OUTER;
		break;
	default:
		throw NotImplementedException("Unsupported join type: %s",
		                              substrait::JoinRel::GetDescriptor()->FindFieldByNumber(sjoin.type())->name());
	}
	unique_ptr<ParsedExpression> join_condition = TransformExpr(sjoin.expression());
	return make_shared_ptr<JoinRelation>(TransformOp(sjoin.left())->Alias("left"),
	                                     TransformOp(sjoin.right())->Alias("right"), std::move(join_condition),
	                                     djointype);
}

shared_ptr<Relation> SubstraitToDuckDB::TransformCrossProductOp(const substrait::Rel &sop) {
	auto &sub_cross = sop.cross();

	return make_shared_ptr<CrossProductRelation>(TransformOp(sub_cross.left())->Alias("left"),
	                                             TransformOp(sub_cross.right())->Alias("right"));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformFetchOp(const substrait::Rel &sop,
                                                         const google::protobuf::RepeatedPtrField<std::string> *names) {
	auto &slimit = sop.fetch();
	idx_t limit = slimit.count() == -1 ? NumericLimits<idx_t>::Maximum() : slimit.count();
	idx_t offset = slimit.offset();
	return make_shared_ptr<LimitRelation>(TransformOp(slimit.input(), names), limit, offset);
}

shared_ptr<Relation> SubstraitToDuckDB::TransformFilterOp(const substrait::Rel &sop) {
	auto &sfilter = sop.filter();
	return make_shared_ptr<FilterRelation>(TransformOp(sfilter.input()), TransformExpr(sfilter.condition()));
}

const substrait::RelCommon *GetCommon(const substrait::Rel &sop) {
	const substrait::RelCommon *common;
	switch (sop.rel_type_case()) {
	case substrait::Rel::RelTypeCase::kRead:
		return &sop.read().common();
	case substrait::Rel::RelTypeCase::kFilter:
		return &sop.filter().common();
	case substrait::Rel::RelTypeCase::kFetch:
		return &sop.fetch().common();
	case substrait::Rel::RelTypeCase::kAggregate:
		return &sop.aggregate().common();
	case substrait::Rel::RelTypeCase::kSort:
		return &sop.sort().common();
	case substrait::Rel::RelTypeCase::kJoin:
		return &sop.join().common();
	case substrait::Rel::RelTypeCase::kProject:
		return &sop.project().common();
	case substrait::Rel::RelTypeCase::kSet:
		return &sop.set().common();
	case substrait::Rel::RelTypeCase::kExtensionSingle:
		return &sop.extension_single().common();
	case substrait::Rel::RelTypeCase::kExtensionMulti:
		return &sop.extension_multi().common();
	case substrait::Rel::RelTypeCase::kExtensionLeaf:
		return &sop.extension_leaf().common();
	case substrait::Rel::RelTypeCase::kCross:
		return &sop.cross().common();
	case substrait::Rel::RelTypeCase::kHashJoin:
		return &sop.hash_join().common();
	case substrait::Rel::RelTypeCase::kMergeJoin:
		return &sop.merge_join().common();
	case substrait::Rel::RelTypeCase::kNestedLoopJoin:
		return &sop.nested_loop_join().common();
	case substrait::Rel::RelTypeCase::kWindow:
		return &sop.window().common();
	case substrait::Rel::RelTypeCase::kExchange:
		return &sop.exchange().common();
	case substrait::Rel::RelTypeCase::kExpand:
		return &sop.expand().common();
	case substrait::Rel::RelTypeCase::kWrite:
	case substrait::Rel::RelTypeCase::kUpdate:
	case substrait::Rel::RelTypeCase::kDdl:
	default:
		throw NotImplementedException("Unsupported relation type %s",
		                              substrait::Rel::GetDescriptor()->FindFieldByNumber(sop.rel_type_case())->name());
	}
}

const google::protobuf::RepeatedField<int32_t> &GetOutputMapping(const substrait::Rel &sop) {
	const substrait::RelCommon *common = GetCommon(sop);
	if (!common->has_emit()) {
		static google::protobuf::RepeatedField<int32_t> empty_mapping;
		return empty_mapping;
	}
	return common->emit().output_mapping();
}

shared_ptr<Relation>
SubstraitToDuckDB::TransformProjectOp(const substrait::Rel &sop,
                                      const google::protobuf::RepeatedPtrField<std::string> *names) {
	vector<unique_ptr<ParsedExpression>> expressions;
	RootNameIterator iterator(names);

	auto &input = sop.project().input();
	auto hasZeroColumnVirtualTable = false;
	shared_ptr<Relation> input_rel;
	size_t num_input_columns = 0;
	if (sop.project().input().rel_type_case() == substrait::Rel::RelTypeCase::kRead) {
		auto &sget = sop.project().input().read();
		if (sget.has_virtual_table()) {
			auto virtual_table = sget.virtual_table();
			if ((virtual_table.values().empty() && virtual_table.expressions().empty()) ||
			    (virtual_table.expressions().size() > 0 && virtual_table.expressions(0).fields().empty())) {
				hasZeroColumnVirtualTable = true;
				input_rel = GetValueRelationWithSingleBoolColumn();
			}
		}
	}
	if (!hasZeroColumnVirtualTable) {
		input_rel = TransformOp(input);
		num_input_columns = input_rel->Columns().size();
	}

	auto mapping = GetOutputMapping(sop);
	if (mapping.empty()) {
		for (int i = 1; i <= num_input_columns; i++) {
			expressions.push_back(make_uniq<PositionalReferenceExpression>(i));
		}

		for (auto &sexpr : sop.project().expressions()) {
			expressions.push_back(TransformExpr(sexpr, &iterator));
		}
	} else {
		expressions.resize(mapping.size());
		for (size_t i = 0; i < mapping.size(); i++) {
			if (mapping[i] < num_input_columns) {
				expressions[i] = make_uniq<PositionalReferenceExpression>(mapping[i] + 1);
			} else {
				auto expr_idx = mapping[i] - num_input_columns;
				if (expr_idx >= (size_t)sop.project().expressions_size()) {
					throw InvalidInputException(
					    "Project emit mapping references expression index %d, but only %d expressions exist "
					    "(possible schema mismatch: plan declares more columns than actual table)",
					    expr_idx, sop.project().expressions_size());
				}
				expressions[i] = TransformExpr(sop.project().expressions(expr_idx), &iterator);
			}
		}
	}

	vector<string> mock_aliases;
	for (size_t i = 0; i < expressions.size(); i++) {
		mock_aliases.push_back("expr_" + to_string(i));
	}
	return make_shared_ptr<ProjectionRelation>(input_rel, std::move(expressions), std::move(mock_aliases));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformAggregateOp(const substrait::Rel &sop) {
	vector<unique_ptr<ParsedExpression>> expressions;
	GroupByNode group_node;

	auto input_rel = TransformOp(sop.aggregate().input());

	if (sop.aggregate().groupings_size() > 0) {
		// First, add ALL grouping expressions to the GroupByNode
		for (int i = 0; i < sop.aggregate().grouping_expressions_size(); i++) {
			auto expr = TransformExpr(sop.aggregate().grouping_expressions(i));
			// Add to output expressions
			expressions.push_back(expr->Copy());
			// Add to group_node
			group_node.group_expressions.push_back(std::move(expr));
		}
		
		// Now convert each Substrait grouping to a DuckDB grouping set
		for (int g = 0; g < sop.aggregate().groupings_size(); g++) {
			auto &sgrp = sop.aggregate().groupings(g);
			GroupingSet grouping_set;
			// expression_references contains indices into the grouping_expressions array
			for (auto ref_idx : sgrp.expression_references()) {
				if (ref_idx < (uint32_t)sop.aggregate().grouping_expressions_size()) {
					grouping_set.insert(ref_idx);
				} else {
					throw InternalException("Invalid expression reference index in grouping");
				}
			}
			group_node.grouping_sets.push_back(grouping_set);
		}
	}

	for (auto &smeas : sop.aggregate().measures()) {
		vector<unique_ptr<ParsedExpression>> children;
		auto &s_aggr_function = smeas.measure();
		bool is_distinct = s_aggr_function.invocation() ==
		                   substrait::AggregateFunction_AggregationInvocation_AGGREGATION_INVOCATION_DISTINCT;
		auto function_name = FindFunction(s_aggr_function.function_reference());
		
		// Special handling for GROUPING() function
		// GROUPING() is not a regular aggregate function - it's a special operator
		if (function_name == "grouping") {
			// For GROUPING(), the arguments are field references to grouping columns
			// We need to resolve these to copies of the actual grouping expressions
			for (auto &sarg : s_aggr_function.arguments()) {
				auto arg_expr = TransformExpr(sarg.value());
				// Check if this is a positional reference
				if (arg_expr->GetExpressionClass() == ExpressionClass::POSITIONAL_REFERENCE) {
					auto &pos_ref = arg_expr->Cast<PositionalReferenceExpression>();
					// The position is 1-based, convert to 0-based index
					idx_t group_idx = pos_ref.index - 1;
					// Get the corresponding grouping expression and make a copy
					if (group_idx < group_node.group_expressions.size()) {
						children.push_back(group_node.group_expressions[group_idx]->Copy());
					} else {
						throw InternalException("GROUPING function references invalid grouping column index");
					}
				} else {
					// If it's not a positional reference, use it as-is
					children.push_back(std::move(arg_expr));
				}
			}
			// Create an OperatorExpression with GROUPING_FUNCTION type
			expressions.push_back(make_uniq<OperatorExpression>(ExpressionType::GROUPING_FUNCTION, std::move(children)));
		} else {
			for (auto &sarg : s_aggr_function.arguments()) {
				children.push_back(TransformExpr(sarg.value()));
			}
			if (function_name == "count" && children.empty()) {
				function_name = "count_star";
			}
			expressions.push_back(make_uniq<FunctionExpression>(RemapFunctionName(function_name), std::move(children),
			                                                    nullptr, nullptr, is_distinct));
		}
	}

	return make_shared_ptr<AggregateRelation>(input_rel, std::move(expressions), std::move(group_node));
}
unique_ptr<TableDescription> TableInfo(ClientContext &context, const string &schema_name, const string &table_name) {
	// obtain the table info
	auto table = Catalog::GetEntry<TableCatalogEntry>(context, INVALID_CATALOG, schema_name, table_name,
	                                                  OnEntryNotFound::RETURN_NULL);
	if (!table) {
		return {};
	}
	// write the table info to the result
	auto result = make_uniq<TableDescription>(INVALID_CATALOG, schema_name, table_name);
	for (auto &column : table->GetColumns().Logical()) {
		result->columns.emplace_back(column.Copy());
	}
	return result;
}

shared_ptr<Relation> SubstraitToDuckDB::TransformReadOp(const substrait::Rel &sop) {
	auto &sget = sop.read();
	shared_ptr<Relation> scan;
	auto context_wrapper = make_shared_ptr<RelationContextWrapper>(context);
	if (sget.has_named_table()) {
		auto table_name = sget.named_table().names(0);
		// If we can't find a table with that name, let's try a view.
		try {
			auto table_info = TableInfo(*context, DEFAULT_SCHEMA, table_name);
			if (!table_info) {
				throw CatalogException("Table '%s' does not exist!", table_name);
			}
			if (acquire_lock) {
				scan = make_shared_ptr<TableRelation>(context, std::move(table_info));

			} else {
				scan = make_shared_ptr<TableRelation>(context_wrapper, std::move(table_info));
			}
		} catch (...) {
			if (acquire_lock) {
				scan = make_shared_ptr<ViewRelation>(context, DEFAULT_SCHEMA, table_name);

			} else {
				scan = make_shared_ptr<ViewRelation>(context_wrapper, DEFAULT_SCHEMA, table_name);
			}
		}
	} else if (sget.has_local_files()) {
		vector<Value> parquet_files;
		auto local_file_items = sget.local_files().items();
		for (auto &current_file : local_file_items) {
			if (current_file.has_parquet()) {
				if (current_file.has_uri_file()) {
					parquet_files.emplace_back(current_file.uri_file());
				} else if (current_file.has_uri_path()) {
					parquet_files.emplace_back(current_file.uri_path());
				} else {
					throw NotImplementedException("Unsupported type for file path, Only uri_file and uri_path are "
					                              "currently supported");
				}
			} else {
				throw NotImplementedException("Unsupported type of local file for read operator on substrait");
			}
		}
		string name = "parquet_" + StringUtil::GenerateRandomName();
		named_parameter_map_t named_parameters({{"binary_as_string", Value::BOOLEAN(false)}});
		vector<Value> parameters {Value::LIST(parquet_files)};
		shared_ptr<TableFunctionRelation> scan_rel;
		if (acquire_lock) {
			scan_rel = make_shared_ptr<TableFunctionRelation>(context, "parquet_scan", parameters,
			                                                  std::move(named_parameters));
		} else {
			scan_rel = make_shared_ptr<TableFunctionRelation>(context_wrapper, "parquet_scan", parameters,
			                                                  std::move(named_parameters));
		}

		auto rel = static_cast<Relation *>(scan_rel.get());
		scan = rel->Alias(name);
	} else if (sget.has_virtual_table()) {
		// We need to handle a virtual table as a LogicalExpressionGet
		if (!sget.virtual_table().values().empty()) {
			auto literal_values = sget.virtual_table().values();
			vector<vector<Value>> expression_rows;
			for (auto &row : literal_values) {
				auto values = row.fields();
				vector<Value> expression_row;
				for (const auto &value : values) {
					expression_row.emplace_back(TransformLiteralToValue(value));
				}
				expression_rows.emplace_back(expression_row);
			}
			vector<string> column_names;
			if (acquire_lock) {
				scan = make_shared_ptr<ValueRelation>(context, expression_rows, column_names);

			} else {
				scan = make_shared_ptr<ValueRelation>(context_wrapper, expression_rows, column_names);
			}
		} else if (!sget.virtual_table().expressions().empty()) {
			scan = GetValuesExpression(sget.virtual_table().expressions());
		} else if (sget.has_base_schema() && sget.base_schema().names_size() > 0 && sget.base_schema().struct_().types_size() > 0) {
			// Empty virtual table represents an empty result (EMPTY_RESULT operator)
			// Extract schema from base_schema
			vector<string> column_names;
			vector<LogicalType> column_types;

			auto &base_schema = sget.base_schema();
			auto &struct_type = base_schema.struct_();

			// Extract column names and types from base_schema
			for (int i = 0; i < base_schema.names_size() && i < struct_type.types_size(); i++) {
				column_names.push_back(base_schema.names(i));
				column_types.push_back(SubstraitToDuckType(struct_type.types(i)));
			}

			// Create a ValueRelation with one row of NULLs to preserve schema, then filter with FALSE to get 0 rows
			// Using Filter with FALSE ensures the row is eliminated before any aggregations
			vector<vector<Value>> one_null_row;
			vector<Value> null_values;
			for (auto &type : column_types) {
				null_values.push_back(Value(type));
			}
			one_null_row.push_back(null_values);

			shared_ptr<Relation> values_rel;
			if (acquire_lock) {
				values_rel = make_shared_ptr<ValueRelation>(context, one_null_row, column_names);
			} else {
				values_rel = make_shared_ptr<ValueRelation>(context_wrapper, one_null_row, column_names);
			}
			// Filter with 1=0 to get empty result with schema (eliminates row before aggregation)
			scan = values_rel->Filter("1=0");
		} else {
			// Fallback: empty result with no schema
			vector<vector<Value>> empty_rows;
			vector<string> column_names;
			if (acquire_lock) {
				scan = make_shared_ptr<ValueRelation>(context, empty_rows, column_names);
			} else {
				scan = make_shared_ptr<ValueRelation>(context_wrapper, empty_rows, column_names);
			}
		}
	} else if (sget.has_iceberg_table()) {
		if (sget.iceberg_table().direct().metadata_uri().empty()) {
			throw InvalidInputException("Metadata file missing in iceberg table read in substrait");
		}
		string name = "iceberg_" + StringUtil::GenerateRandomName();
		named_parameter_map_t named_parameters({});
		vector<Value> parameters {sget.iceberg_table().direct().metadata_uri()};
		if (sget.iceberg_table().direct().has_snapshot_id()) {
			auto str = sget.iceberg_table().direct().snapshot_id();
			int64_t snapshot_id = strtoimax(str.c_str(), nullptr, 10);
			if (snapshot_id <= 0 || snapshot_id == std::numeric_limits<int64_t>::max()) {
				throw InvalidInputException("Invalid snapshot id: " + sget.iceberg_table().direct().snapshot_id());
			}
			named_parameters.emplace("snapshot_from_id", Value::UBIGINT(snapshot_id));
		} else if (sget.iceberg_table().direct().has_snapshot_timestamp()) {
			named_parameters.emplace("snapshot_from_timestamp",
				Value::TIMESTAMP(timestamp_t(sget.iceberg_table().direct().snapshot_timestamp())));
		}
		shared_ptr<TableFunctionRelation> scan_rel;
		if (acquire_lock) {
			scan_rel = make_shared_ptr<TableFunctionRelation>(context, "iceberg_scan", parameters,
			                                                  std::move(named_parameters));
		} else {
			scan_rel = make_shared_ptr<TableFunctionRelation>(context_wrapper, "iceberg_scan", parameters,
			                                                  std::move(named_parameters));
		}
		auto rel = static_cast<Relation *>(scan_rel.get());
		scan = rel->Alias(name);
	} else {
		throw NotImplementedException("Unsupported type of read operator for substrait");
	}

	// When a named table's physical schema has more columns than the plan's
	// baseSchema declares, add a projection to narrow down to only the declared
	// columns. Without this, downstream emit mappings (which assume baseSchema
	// column count) compute wrong expression indices.
	if (sget.has_named_table() && sget.has_base_schema() && !sget.has_projection()) {
		auto &base_schema = sget.base_schema();
		auto physical_cols = scan->Columns().size();
		auto declared_cols = (size_t)base_schema.names_size();
		if (declared_cols > 0 && declared_cols < physical_cols) {
			// Match baseSchema column names to physical column positions
			vector<unique_ptr<ParsedExpression>> proj_exprs;
			vector<string> proj_aliases;
			auto &scan_columns = scan->Columns();
			for (int i = 0; i < base_schema.names_size(); i++) {
				auto &col_name = base_schema.names(i);
				bool found = false;
				for (size_t j = 0; j < scan_columns.size(); j++) {
					if (StringUtil::CIEquals(scan_columns[j].Name(), col_name)) {
						proj_exprs.push_back(make_uniq<PositionalReferenceExpression>(j + 1));
						proj_aliases.push_back(col_name);
						found = true;
						break;
					}
				}
				if (!found) {
					throw InvalidInputException(
					    "baseSchema column '%s' not found in physical table (schema mismatch)", col_name);
				}
			}
			scan = make_shared_ptr<ProjectionRelation>(std::move(scan), std::move(proj_exprs), std::move(proj_aliases));
		}
	}

	if (sget.has_filter()) {
		scan = make_shared_ptr<FilterRelation>(std::move(scan), TransformExpr(sget.filter()));
	}

	if (sget.has_projection()) {
		vector<unique_ptr<ParsedExpression>> expressions;
		vector<string> aliases;
		idx_t expr_idx = 0;
		for (auto &sproj : sget.projection().select().struct_items()) {
			// FIXME how to get actually alias?
			aliases.push_back("expr_" + to_string(expr_idx++));
			// TODO make sure nothing else is in there
			expressions.push_back(make_uniq<PositionalReferenceExpression>(sproj.field() + 1));
		}
		scan = make_shared_ptr<ProjectionRelation>(std::move(scan), std::move(expressions), std::move(aliases));
	}

	return scan;
}

shared_ptr<Relation> SubstraitToDuckDB::GetValueRelationWithSingleBoolColumn() {
	vector<vector<unique_ptr<ParsedExpression>>> expressions;
	vector<unique_ptr<ParsedExpression>> expression_row;
	expressions.emplace_back(std::move(expression_row));
	Value result(LogicalType::BOOLEAN);
	expressions[0].emplace_back(make_uniq<ConstantExpression>(result));
	vector<string> column_names;
	shared_ptr<Relation> scan;
	if (acquire_lock) {
		scan = make_shared_ptr<ValueRelation>(context, std::move(expressions), column_names);
	} else {
		auto context_wrapper = make_shared_ptr<RelationContextWrapper>(context);
		scan = make_shared_ptr<ValueRelation>(context_wrapper, std::move(expressions), column_names);
	}
	return scan;
}

shared_ptr<Relation> SubstraitToDuckDB::GetValuesExpression(
    const google::protobuf::RepeatedPtrField<substrait::Expression_Nested_Struct> &expression_rows) {
	vector<vector<unique_ptr<ParsedExpression>>> expressions;
	for (auto &row : expression_rows) {
		vector<unique_ptr<ParsedExpression>> expression_row;
		for (const auto &expr : row.fields()) {
			expression_row.emplace_back(TransformExpr(expr));
		}
		expressions.emplace_back(std::move(expression_row));
	}
	vector<string> column_names;
	shared_ptr<Relation> scan;
	if (acquire_lock) {
		scan = make_shared_ptr<ValueRelation>(context, std::move(expressions), column_names);
	} else {
		auto context_wrapper = make_shared_ptr<RelationContextWrapper>(context);
		scan = make_shared_ptr<ValueRelation>(context_wrapper, std::move(expressions), column_names);
	}
	return scan;
}

shared_ptr<Relation> SubstraitToDuckDB::TransformSortOp(const substrait::Rel &sop,
                                                        const google::protobuf::RepeatedPtrField<std::string> *names) {
	vector<OrderByNode> order_nodes;
	for (auto &sordf : sop.sort().sorts()) {
		order_nodes.push_back(TransformOrder(sordf));
	}
	return make_shared_ptr<OrderRelation>(TransformOp(sop.sort().input(), names), std::move(order_nodes));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformWindowOp(const substrait::Rel &sop) {
	// Get the input relation
	auto input_rel = TransformOp(sop.window().input());
	
	// Build window expressions from the window functions
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<string> aliases;
	
	// First, add all input columns to preserve them in the output
	auto num_input_columns = input_rel->Columns().size();
	for (size_t i = 0; i < num_input_columns; i++) {
		expressions.push_back(make_uniq<PositionalReferenceExpression>(i + 1));
		aliases.push_back(""); // Empty alias, DuckDB will use the original column name
	}
	
	// Then add the window function expressions
	for (auto &window_func : sop.window().window_functions()) {
		// Get the function name
		auto function_name = FindFunction(window_func.function_reference());
		auto remapped_name = RemapFunctionName(function_name);
		
		// Determine the expression type based on the function name
		ExpressionType expr_type;
		if (remapped_name == "row_number") {
			expr_type = ExpressionType::WINDOW_ROW_NUMBER;
		} else if (remapped_name == "rank") {
			expr_type = ExpressionType::WINDOW_RANK;
		} else if (remapped_name == "dense_rank") {
			expr_type = ExpressionType::WINDOW_RANK_DENSE;
		} else if (remapped_name == "percent_rank") {
			expr_type = ExpressionType::WINDOW_PERCENT_RANK;
		} else if (remapped_name == "cume_dist") {
			expr_type = ExpressionType::WINDOW_CUME_DIST;
		} else if (remapped_name == "ntile") {
			expr_type = ExpressionType::WINDOW_NTILE;
		} else if (remapped_name == "lag") {
			expr_type = ExpressionType::WINDOW_LAG;
		} else if (remapped_name == "lead") {
			expr_type = ExpressionType::WINDOW_LEAD;
		} else if (remapped_name == "first_value") {
			expr_type = ExpressionType::WINDOW_FIRST_VALUE;
		} else if (remapped_name == "last_value") {
			expr_type = ExpressionType::WINDOW_LAST_VALUE;
		} else if (remapped_name == "nth_value") {
			expr_type = ExpressionType::WINDOW_NTH_VALUE;
		} else {
			// Default to WINDOW_AGGREGATE for aggregate functions used as window functions
			expr_type = ExpressionType::WINDOW_AGGREGATE;
		}
		
		// Create window expression
		auto window_expr = make_uniq<WindowExpression>(expr_type, "", "", remapped_name);
		
		// Add function arguments
		for (auto &arg : window_func.arguments()) {
			window_expr->children.push_back(TransformExpr(arg.value()));
		}
		
		// Add partition expressions
		for (auto &partition_expr : sop.window().partition_expressions()) {
			window_expr->partitions.push_back(TransformExpr(partition_expr));
		}
		
		// Add order by expressions
		for (auto &sort_field : sop.window().sorts()) {
			window_expr->orders.push_back(TransformOrder(sort_field));
		}
		
		// Handle window bounds
		if (window_func.has_lower_bound() || window_func.has_upper_bound()) {
			// Determine bounds type (ROWS or RANGE)
			bool is_rows = window_func.bounds_type() == substrait::Expression_WindowFunction_BoundsType_BOUNDS_TYPE_ROWS;
			
			// Transform lower bound
			if (window_func.has_lower_bound()) {
				auto &lower = window_func.lower_bound();
				if (lower.has_unbounded()) {
					window_expr->start = WindowBoundary::UNBOUNDED_PRECEDING;
				} else if (lower.has_current_row()) {
					window_expr->start = is_rows ? WindowBoundary::CURRENT_ROW_ROWS : WindowBoundary::CURRENT_ROW_RANGE;
				} else if (lower.has_preceding()) {
					window_expr->start_expr = make_uniq<ConstantExpression>(Value::BIGINT(lower.preceding().offset()));
					window_expr->start = is_rows ? WindowBoundary::EXPR_PRECEDING_ROWS : WindowBoundary::EXPR_PRECEDING_RANGE;
				} else if (lower.has_following()) {
					window_expr->start_expr = make_uniq<ConstantExpression>(Value::BIGINT(lower.following().offset()));
					window_expr->start = is_rows ? WindowBoundary::EXPR_FOLLOWING_ROWS : WindowBoundary::EXPR_FOLLOWING_RANGE;
				}
			}
			
			// Transform upper bound
			if (window_func.has_upper_bound()) {
				auto &upper = window_func.upper_bound();
				if (upper.has_unbounded()) {
					window_expr->end = WindowBoundary::UNBOUNDED_FOLLOWING;
				} else if (upper.has_current_row()) {
					window_expr->end = is_rows ? WindowBoundary::CURRENT_ROW_ROWS : WindowBoundary::CURRENT_ROW_RANGE;
				} else if (upper.has_preceding()) {
					window_expr->end_expr = make_uniq<ConstantExpression>(Value::BIGINT(upper.preceding().offset()));
					window_expr->end = is_rows ? WindowBoundary::EXPR_PRECEDING_ROWS : WindowBoundary::EXPR_PRECEDING_RANGE;
				} else if (upper.has_following()) {
					window_expr->end_expr = make_uniq<ConstantExpression>(Value::BIGINT(upper.following().offset()));
					window_expr->end = is_rows ? WindowBoundary::EXPR_FOLLOWING_ROWS : WindowBoundary::EXPR_FOLLOWING_RANGE;
				}
			}
		}
		
		// Handle invocation (DISTINCT, etc.)
		window_expr->distinct =
			window_func.invocation() == substrait::AggregateFunction_AggregationInvocation_AGGREGATION_INVOCATION_DISTINCT;
		
		expressions.push_back(std::move(window_expr));
		aliases.push_back(""); // Empty alias, DuckDB will generate one
	}
	
	// Return a projection with the window expressions
	// Window functions in DuckDB are handled as projections with window expressions
	return input_rel->Project(std::move(expressions), aliases);
}

static SetOperationType TransformSetOperationType(substrait::SetRel_SetOp setop) {
	switch (setop) {
	case substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_UNION_ALL: {
		return SetOperationType::UNION;
	}
	case substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_MINUS_PRIMARY: {
		return SetOperationType::EXCEPT;
	}
	case substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_INTERSECTION_PRIMARY: {
		return SetOperationType::INTERSECT;
	}
	default: {
		throw NotImplementedException("SetOperationType transform not implemented for SetRel_SetOp type %s",
		                              substrait::SetRel::GetDescriptor()->FindFieldByNumber(setop)->name());
	}
	}
}

shared_ptr<Relation> SubstraitToDuckDB::TransformSetOp(const substrait::Rel &sop,
                                                       const google::protobuf::RepeatedPtrField<std::string> *names) {
	D_ASSERT(sop.has_set());
	auto &set = sop.set();
	auto set_op_type = set.op();
	auto type = TransformSetOperationType(set_op_type);

	auto &inputs = set.inputs();
	auto input_count = set.inputs_size();
	if (input_count > 2) {
		throw NotImplementedException("The amount of inputs (%d) is not supported for this set operation", input_count);
	}
	auto lhs = TransformOp(inputs[0]);
	auto rhs = TransformOp(inputs[1], names);

	return make_shared_ptr<SetOpRelation>(std::move(lhs), std::move(rhs), type);
}

shared_ptr<Relation> SubstraitToDuckDB::TransformWriteOp(const substrait::Rel &sop) {
	auto &swrite = sop.write();
	auto &nobj = swrite.named_table();
	if (nobj.names_size() == 0) {
		throw InvalidInputException("Named object must have at least one name");
	}
	auto table_idx = nobj.names_size() - 1;
	auto table_name = nobj.names(table_idx);
	string schema_name;
	string catalog_name;
	if (table_idx > 0) {
		if (table_idx == 1) {
			schema_name = nobj.names(0);
		} else {
			catalog_name = nobj.names(0);
			schema_name = nobj.names(1);
		}
	}
	auto input = TransformOp(swrite.input());
	switch (swrite.op()) {
	case substrait::WriteRel::WriteOp::WriteRel_WriteOp_WRITE_OP_CTAS:
		return input->CreateRel(schema_name, table_name);
	case substrait::WriteRel::WriteOp::WriteRel_WriteOp_WRITE_OP_INSERT:
		return input->InsertRel(schema_name, table_name);
	case substrait::WriteRel::WriteOp::WriteRel_WriteOp_WRITE_OP_DELETE: {
		switch (input->type) {
		case RelationType::PROJECTION_RELATION: {
			auto project = std::move(input.get()->Cast<ProjectionRelation>());
			auto filter = std::move(project.child->Cast<FilterRelation>());
                        return make_shared_ptr<DeleteRelation>(filter.context, std::move(filter.condition), catalog_name, schema_name, table_name);
		}
		case RelationType::FILTER_RELATION: {
			auto filter = std::move(input.get()->Cast<FilterRelation>());
			return make_shared_ptr<DeleteRelation>(filter.context, std::move(filter.condition), catalog_name, schema_name, table_name);
		}
		default:
			throw NotImplementedException("Unsupported relation type for delete operation");
		}
	}
	default:
		throw NotImplementedException("Unsupported write operation %s",
		                              substrait::WriteRel::GetDescriptor()->FindFieldByNumber(swrite.op())->name());
	}
}

shared_ptr<Relation> SubstraitToDuckDB::TransformReferenceOp(const substrait::Rel &sop) {
	auto index = sop.reference().subtree_ordinal();
	if (index >= ctes.size()) {
		throw InvalidInputException("Reference made to non-existent top-level relation: %d", index);
	}
	auto &cte = ctes[index];
	return cte;
}

shared_ptr<Relation> SubstraitToDuckDB::TransformOp(const substrait::Rel &sop,
                                                    const google::protobuf::RepeatedPtrField<std::string> *names) {
	switch (sop.rel_type_case()) {
	case substrait::Rel::RelTypeCase::kJoin:
		return TransformJoinOp(sop);
	case substrait::Rel::RelTypeCase::kCross:
		return TransformCrossProductOp(sop);
	case substrait::Rel::RelTypeCase::kFetch:
		return TransformFetchOp(sop, names);
	case substrait::Rel::RelTypeCase::kFilter:
		return TransformFilterOp(sop);
	case substrait::Rel::RelTypeCase::kProject:
		return TransformProjectOp(sop, names);
	case substrait::Rel::RelTypeCase::kAggregate:
		return TransformAggregateOp(sop);
	case substrait::Rel::RelTypeCase::kRead:
		return TransformReadOp(sop);
	case substrait::Rel::RelTypeCase::kSort:
		return TransformSortOp(sop, names);
	case substrait::Rel::RelTypeCase::kWindow:
		return TransformWindowOp(sop);
	case substrait::Rel::RelTypeCase::kSet:
		return TransformSetOp(sop, names);
	case substrait::Rel::RelTypeCase::kWrite:
		return TransformWriteOp(sop);
	case substrait::Rel::RelTypeCase::kReference:
		return TransformReferenceOp(sop);
	default:
		throw NotImplementedException("Unsupported relation type %s",
		                              substrait::Rel::GetDescriptor()->FindFieldByNumber(sop.rel_type_case())->name());
	}
}

void SkipColumnNamesRecurse(int32_t &columns_to_skip, const LogicalType &type) {
	if (type.id() == LogicalTypeId::STRUCT) {
		idx_t struct_size = StructType::GetChildCount(type);
		columns_to_skip += static_cast<int32_t>(struct_size);
		for (auto &struct_type : StructType::GetChildTypes(type)) {
			SkipColumnNamesRecurse(columns_to_skip, struct_type.second);
		}
	}
}

int32_t SkipColumnNames(const LogicalType &type) {
	int32_t columns_to_skip = 0;
	SkipColumnNamesRecurse(columns_to_skip, type);
	return columns_to_skip;
}

Relation *GetProjection(Relation &relation) {
	switch (relation.type) {
	case RelationType::PROJECTION_RELATION:
		return &relation;
	case RelationType::LIMIT_RELATION:
		return GetProjection(*relation.Cast<LimitRelation>().child);
	case RelationType::ORDER_RELATION:
		return GetProjection(*relation.Cast<OrderRelation>().child);
	case RelationType::SET_OPERATION_RELATION:
		return GetProjection(*relation.Cast<SetOpRelation>().right);
	default:
		return nullptr;
	}
}

shared_ptr<Relation> SubstraitToDuckDB::TransformRootOp(const substrait::RelRoot &sop) {
	vector<string> aliases;
	const auto &column_names = sop.names();
	vector<unique_ptr<ParsedExpression>> expressions;
	int id = 1;
	auto child = TransformOp(sop.input(), &column_names);
	auto first_projection_or_table = GetProjection(*child);
	if (first_projection_or_table) {
		vector<ColumnDefinition> *column_definitions = &first_projection_or_table->Cast<ProjectionRelation>().columns;
		int32_t i = 0;
		if (column_definitions->size() > column_names.size()) {
			throw InvalidInputException("Number of column names less than number of column definitions");
		}
		for (auto &column : *column_definitions) {
			aliases.push_back(column_names[i++]);
			auto column_type = column.GetType();
			i += SkipColumnNames(column.GetType());
			expressions.push_back(make_uniq<PositionalReferenceExpression>(id++));
		}
	} else {
		for (auto &column_name : column_names) {
			aliases.push_back(column_name);
			expressions.push_back(make_uniq<PositionalReferenceExpression>(id++));
		}
	}

	if (sop.input().rel_type_case() == substrait::Rel::RelTypeCase::kWrite) {
		auto write = sop.input().write();
		switch (write.op()) {
		case substrait::WriteRel::WriteOp::WriteRel_WriteOp_WRITE_OP_CTAS: {
			const auto create_table = static_cast<CreateTableRelation *>(child.get());
			auto proj = make_shared_ptr<ProjectionRelation>(create_table->child, std::move(expressions), aliases);
			return proj->CreateRel(create_table->schema_name, create_table->table_name);
		}
		default:
			return child;
		}
	}

	return make_shared_ptr<ProjectionRelation>(child, std::move(expressions), aliases);
}

shared_ptr<Relation> SubstraitToDuckDB::TransformPlan() {
	if (plan.relations().empty()) {
		throw InvalidInputException("Substrait Plan does not have a SELECT statement");
	}
	ctes.clear();
	auto size = plan.relations().size();
	// The last relation is the root.  Others could be CTEs.
	for (auto i = 0; i < size - 1; i++) {
		auto cte = TransformOp(plan.relations(i).rel());
		ctes.push_back(cte);
	}
	return TransformRootOp(plan.relations(size - 1).root());
}

} // namespace duckdb
