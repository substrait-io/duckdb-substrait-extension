#include "to_substrait.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "google/protobuf/util/json_util.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

namespace duckdb {
const std::unordered_map<std::string, std::string> DuckDBToSubstrait::function_names_remap = {
    {"mod", "modulus"},
    {"stddev", "std_dev"},
    {"prefix", "starts_with"},
    {"suffix", "ends_with"},
    {"substr", "substring"},
    {"length", "char_length"},
    {"isnan", "is_nan"},
    {"isfinite", "is_finite"},
    {"isinf", "is_infinite"},
    {"sum_no_overflow", "sum"},
    {"count_star", "count"},
    {"~~", "like"},
    {"*", "multiply"},
    {"-", "subtract"},
    {"+", "add"},
    {"/", "divide"},
    {"//", "divide"},
    {"%", "modulus"},
    {"first", "any_value"},
    {"&", "bitwise_and"},
    {"|", "bitwise_or"},
    {"xor", "bitwise_xor"},
    {"strlen", "octet_length"},
    // functions_comparison declares each of these twice, differing only in what a null
    // argument does: `least`/`greatest` return null if ANY argument is null, while the
    // `_skip_null` pair return null only if ALL of them are. DuckDB's least(NULL, 1) is 1, so
    // the skip-null declarations are the ones that describe it -- the shorter names would
    // encode a plan that a conforming consumer evaluates to NULL instead.
    {"least", "least_skip_null"},
    {"greatest", "greatest_skip_null"}};

const case_insensitive_set_t DuckDBToSubstrait::valid_extract_subfields = {
    "year",    "month",       "day",          "decade", "century", "millenium",
    "quarter", "microsecond", "milliseconds", "second", "minute",  "hour"};

const SubstraitCustomFunctions DuckDBToSubstrait::custom_functions {};

std::string &DuckDBToSubstrait::RemapFunctionName(std::string &function_name) {
	auto it = function_names_remap.find(function_name);
	if (it != function_names_remap.end()) {
		function_name = it->second;
	}
	return function_name;
}

string DuckDBToSubstrait::SerializeToString() const {
	string serialized;
	if (!plan.SerializeToString(&serialized)) {
		throw InternalException("It was not possible to serialize the substrait plan");
	}
	return serialized;
}

string DuckDBToSubstrait::SerializeToJson() const {
	string serialized;
	auto success = google::protobuf::util::MessageToJsonString(plan, &serialized);
	if (!success.ok()) {
		throw InternalException("It was not possible to serialize the substrait plan");
	}
	return serialized;
}

void DuckDBToSubstrait::AllocateFunctionArgument(substrait::Expression_ScalarFunction *scalar_fun,
                                                 substrait::Expression *value) {
	auto function_argument = scalar_fun->mutable_arguments()->Add();
	function_argument->set_allocated_value(value);
}

string GetRawValue(hugeint_t value) {
	std::string str;
	str.reserve(16);
	auto byte = reinterpret_cast<const char *>(&value.lower);
	for (idx_t i = 0; i < 8; i++) {
		str.push_back(byte[i]);
	}
	byte = reinterpret_cast<const char *>(&value.upper);
	for (idx_t i = 0; i < 8; i++) {
		str.push_back(byte[i]);
	}

	return str;
}

void DuckDBToSubstrait::TransformDecimal(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	auto *allocated_decimal = sval.mutable_decimal();
	uint8_t scale, width;
	hugeint_t hugeint_value {};
	Value mock_value;
	// alright time for some dirty switcharoo
	switch (dval.type().InternalType()) {
	case PhysicalType::INT8: {
		auto internal_value = dval.GetValueUnsafe<int8_t>();
		mock_value = Value::TINYINT(internal_value);
		break;
	}

	case PhysicalType::INT16: {
		auto internal_value = dval.GetValueUnsafe<int16_t>();
		mock_value = Value::SMALLINT(internal_value);
		break;
	}
	case PhysicalType::INT32: {
		auto internal_value = dval.GetValueUnsafe<int32_t>();
		mock_value = Value::INTEGER(internal_value);
		break;
	}
	case PhysicalType::INT64: {
		auto internal_value = dval.GetValueUnsafe<int64_t>();
		mock_value = Value::BIGINT(internal_value);
		break;
	}
	case PhysicalType::INT128: {
		auto internal_value = dval.GetValueUnsafe<hugeint_t>();
		mock_value = Value::HUGEINT(internal_value);
		break;
	}
	default:
		throw InternalException("Not accepted internal type for decimal");
	}
	hugeint_value = mock_value.GetValue<hugeint_t>();
	auto raw_value = GetRawValue(hugeint_value);

	dval.type().GetDecimalProperties(width, scale);

	allocated_decimal->set_scale(scale);
	allocated_decimal->set_precision(width);
	allocated_decimal->set_value(raw_value);
}

void DuckDBToSubstrait::TransformTinyInt(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i8(dval.GetValue<int8_t>());
}

void DuckDBToSubstrait::TransformInteger(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i32(dval.GetValue<int32_t>());
}

void DuckDBToSubstrait::TransformSmallInt(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i16(dval.GetValue<int16_t>());
}

void DuckDBToSubstrait::TransformDouble(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_fp64(dval.GetValue<double>());
}

void DuckDBToSubstrait::TransformFloat(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_fp32(dval.GetValue<float>());
}

void DuckDBToSubstrait::TransformBigInt(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i64(dval.GetValue<int64_t>());
}

void DuckDBToSubstrait::TransformDate(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_date(dval.GetValue<date_t>().days);
}

void DuckDBToSubstrait::TransformTime(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	auto precision_time = sval.mutable_precision_time();
	precision_time->set_precision(6); // microseconds
	precision_time->set_value(dval.GetValue<dtime_t>().value);
}

void DuckDBToSubstrait::TransformTimestamp(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_string(dval.ToString());
}

void DuckDBToSubstrait::TransformTimestampTz(const Value &dval, substrait::Expression &sexpr) {
	auto tz_value = dval.GetValue<timestamp_tz_t>();
	if (!Value::IsFinite(tz_value)) {
		// DuckDB encodes +/-infinity as INT64_MAX/MIN micros. Emitting that raw would look
		// like a real instant near year 294241 to any other consumer, so fail loudly instead.
		throw NotImplementedException("Substrait has no representation for infinite TIMESTAMPTZ values");
	}
	auto &sval = *sexpr.mutable_literal();
	auto precision_timestamp_tz = sval.mutable_precision_timestamp_tz();
	precision_timestamp_tz->set_precision(6); // microseconds
	precision_timestamp_tz->set_value(tz_value.value);
}

void DuckDBToSubstrait::TransformInterval(const Value &dval, substrait::Expression &sexpr) {
	// Substrait supports two types of INTERVAL (interval_year and interval_day)
	// whereas DuckDB INTERVAL combines both in one type. Therefore intervals
	// containing both months and days or seconds will lose some data
	// unfortunately. This implementation opts to set the largest interval value.
	auto &sval = *sexpr.mutable_literal();
	auto months = dval.GetValue<interval_t>().months;
	if (months != 0) {
		auto interval_year = make_uniq<substrait::Expression_Literal_IntervalYearToMonth>();
		interval_year->set_months(months);
		sval.set_allocated_interval_year_to_month(interval_year.release());
	} else {
		auto interval_day = make_uniq<substrait::Expression_Literal_IntervalDayToSecond>();
		interval_day->set_days(dval.GetValue<interval_t>().days);
		interval_day->set_subseconds(dval.GetValue<interval_t>().micros);
		interval_day->set_precision(6); // microseconds precision
		sval.set_allocated_interval_day_to_second(interval_day.release());
	}
}

void DuckDBToSubstrait::TransformVarchar(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	string duck_str = dval.GetValue<string>();
	sval.set_string(dval.GetValue<string>());
}

void DuckDBToSubstrait::TransformBoolean(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_boolean(dval.GetValue<bool>());
}

void DuckDBToSubstrait::TransformHugeInt(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	auto *allocated_decimal = sval.mutable_decimal();
	// Checked cast so this also handles UBIGINT (stored as uint64, not hugeint),
	// which DuckToSubstraitType maps to the same decimal(38, 0) representation.
	auto hugeint = dval.GetValue<hugeint_t>();
	auto raw_value = GetRawValue(hugeint);
	allocated_decimal->set_scale(0);
	allocated_decimal->set_precision(38);
	allocated_decimal->set_value(raw_value);
}

void DuckDBToSubstrait::TransformEnum(const Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_string(dval.ToString());
}

void DuckDBToSubstrait::TransformConstant(const Value &dval, substrait::Expression &sexpr) {
	if (dval.IsNull()) {
		sexpr.mutable_literal()->mutable_null();
		return;
	}
	auto &duckdb_type = dval.type();
	switch (duckdb_type.id()) {
	case LogicalTypeId::DECIMAL:
		TransformDecimal(dval, sexpr);
		break;
	case LogicalTypeId::TINYINT:
		TransformTinyInt(dval, sexpr);
		break;
	case LogicalTypeId::INTEGER:
		TransformInteger(dval, sexpr);
		break;
	case LogicalTypeId::SMALLINT:
		TransformSmallInt(dval, sexpr);
		break;
	case LogicalTypeId::BIGINT:
		TransformBigInt(dval, sexpr);
		break;
	case LogicalTypeId::HUGEINT:
		TransformHugeInt(dval, sexpr);
		break;
		// Substrait has no unsigned integer types, so these are upcast to the
		// next-wider signed literal, mirroring DuckToSubstraitType's type mapping.
	case LogicalTypeId::UTINYINT:
		TransformSmallInt(dval, sexpr);
		break;
	case LogicalTypeId::USMALLINT:
		TransformInteger(dval, sexpr);
		break;
	case LogicalTypeId::UINTEGER:
		TransformBigInt(dval, sexpr);
		break;
	case LogicalTypeId::UBIGINT:
		TransformHugeInt(dval, sexpr);
		break;
	case LogicalTypeId::DATE:
		TransformDate(dval, sexpr);
		break;
	case LogicalTypeId::TIME:
		TransformTime(dval, sexpr);
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP:
		TransformTimestamp(dval, sexpr);
		break;
	case LogicalTypeId::TIMESTAMP_TZ:
		TransformTimestampTz(dval, sexpr);
		break;
	case LogicalTypeId::INTERVAL:
		TransformInterval(dval, sexpr);
		break;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		TransformVarchar(dval, sexpr);
		break;
	case LogicalTypeId::BOOLEAN:
		TransformBoolean(dval, sexpr);
		break;
	case LogicalTypeId::DOUBLE:
		TransformDouble(dval, sexpr);
		break;
	case LogicalTypeId::FLOAT:
		TransformFloat(dval, sexpr);
		break;
	case LogicalTypeId::ENUM:
		TransformEnum(dval, sexpr);
		break;
	default:
		throw NotImplementedException("Consuming a value of type %s is not supported yet", duckdb_type.ToString());
	}
}

void DuckDBToSubstrait::TransformBoundRefExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                    uint64_t col_offset) {
	auto &dref = dexpr.Cast<BoundReferenceExpression>();
	if (!filter_columns.empty()) {
		// Inside a table filter, references index the filter's columns.
		CreateFieldRef(&sexpr, filter_columns[dref.Index()]);
		return;
	}
	CreateFieldRef(&sexpr, dref.Index() + col_offset);
}

void DuckDBToSubstrait::TransformCastExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	auto &dcast = dexpr.Cast<BoundFunctionExpression>();
	auto scast = sexpr.mutable_cast();
	TransformExpr(*BoundCastExpression::ChildMutable(dcast), *scast->mutable_input(), col_offset);
	*scast->mutable_type() = DuckToSubstraitType(dcast.GetReturnType());
	scast->set_failure_behavior(BoundCastExpression::IsTryCast(dcast)
	                                ? substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_RETURN_NULL
	                                : substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION);
}

bool DuckDBToSubstrait::IsExtractFunction(const string &function_name) {
	return valid_extract_subfields.count(function_name);
}

static Value NormalizeDecimalConstant(const Value &value) {
	if (value.IsNull()) {
		return value;
	}
	auto unscaled = IntegralValue::Get(value);
	auto scale = DecimalType::GetScale(value.type());
	while (scale > 0 && unscaled % 10 == 0) {
		unscaled /= 10;
		scale--;
	}
	uint8_t precision = 0;
	auto digits = unscaled;
	do {
		precision++;
		digits /= 10;
	} while (digits != 0);
	precision = MaxValue(precision, scale);
	if (precision <= Decimal::MAX_WIDTH_INT64) {
		return Value::DECIMAL(Hugeint::Cast<int64_t>(unscaled), precision, scale);
	}
	return Value::DECIMAL(unscaled, precision, scale);
}

void DuckDBToSubstrait::TransformFunctionExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                    uint64_t col_offset) {
	auto &dfun = dexpr.Cast<BoundFunctionExpression>();

	auto function_name = dfun.Function().GetName().GetIdentifierName();

	if (function_name == "row") {
		auto nested_expression = sexpr.mutable_nested();
		auto struct_expression = nested_expression->mutable_struct_();
		for (auto &child : dfun.GetChildren()) {
			auto child_expression = struct_expression->add_fields();
			TransformExpr(*child, *child_expression, col_offset);
		}
		return;
	}
	if (function_name == "list_value" || function_name == "list_pack") {
		auto nested_expression = sexpr.mutable_nested();
		auto list_expression = nested_expression->mutable_list();
		for (auto &child : dfun.GetChildren()) {
			auto child_value = list_expression->add_values();
			TransformExpr(*child, *child_value, col_offset);
		}
		return;
	}
	if (function_name == "map") {
		auto nested_expression = sexpr.mutable_nested();
		auto map_expression = nested_expression->mutable_map();
		D_ASSERT(dfun.GetChildren().size() == 2);
		auto child_value = map_expression->add_key_values();
		auto key = child_value->mutable_key();
		auto value = child_value->mutable_value();
		TransformExpr(*dfun.GetChildren()[0], *key, col_offset);
		TransformExpr(*dfun.GetChildren()[1], *value, col_offset);
		return;
	}
	if (function_name == "!~~") {
		// Substrait has no not_like. DuckDB's NOT LIKE operator is emitted as the
		// composition not(like(x, pattern)); both halves are standard functions.
		auto not_fun = sexpr.mutable_scalar_function();
		auto not_arg = not_fun->add_arguments();
		auto like_fun = not_arg->mutable_value()->mutable_scalar_function();
		vector<substrait::Type> like_arg_types;
		for (auto &darg : dfun.GetChildren()) {
			auto like_arg = like_fun->add_arguments();
			TransformExpr(*darg, *like_arg->mutable_value(), col_offset);
			like_arg_types.emplace_back(DuckToSubstraitType(darg->GetReturnType()));
		}
		like_fun->set_function_reference(RegisterFunction("like", like_arg_types));
		*like_fun->mutable_output_type() = DuckToSubstraitType(LogicalType::BOOLEAN);

		vector<substrait::Type> not_arg_types;
		not_arg_types.emplace_back(DuckToSubstraitType(LogicalType::BOOLEAN));
		not_fun->set_function_reference(RegisterFunction("not", not_arg_types));
		*not_fun->mutable_output_type() = DuckToSubstraitType(dfun.GetReturnType());
		return;
	}
	auto sfun = sexpr.mutable_scalar_function();
	if (IsExtractFunction(function_name)) {
		// Change the name to 'extract', and add an Enum argument containing the subfield
		auto subfield = function_name;
		function_name = "extract";
		auto enum_arg = sfun->add_arguments();
		*enum_arg->mutable_enum_() = subfield;
	}
	// Substrait only defines substring/substr over i32 offsets and lengths, but DuckDB binds
	// those arguments as i64 (BIGINT). Emit them cast to i32 so the call resolves to the
	// standard substring signature instead of an unknown native function.
	// Offsets and lengths outside the i32 range cause the cast to fail. If Substrait adds
	// an i64 substring signature, emit the wider arguments directly.
	bool is_substring = function_name == "substring" || function_name == "substr";
	bool is_decimal_multiply = dfun.Function().GetName() == "*" && dfun.GetReturnType().id() == LogicalTypeId::DECIMAL;
	// Substrait's divide takes its zero-divisor behavior as an option, and DuckDB's operators take theirs from
	// session settings: / follows IEEE (infinity or NaN) while ieee_floating_point_ops is on, otherwise it, //
	// and divide() raise or return NULL as error_on_division_by_zero says. The option exists for the integer and
	// floating-point signatures only.
	string division_by_zero;
	if (function_name == "/" || function_name == "//" || function_name == "divide") {
		bool option_applies = true;
		for (auto &darg : dfun.GetChildren()) {
			auto &arg_type = darg->GetReturnType();
			option_applies &= arg_type.IsIntegral() || arg_type.id() == LogicalTypeId::FLOAT ||
			                  arg_type.id() == LogicalTypeId::DOUBLE;
		}
		if (option_applies) {
			if (function_name == "/" && Settings::Get<IeeeFloatingPointOpsSetting>(context)) {
				division_by_zero = "IEEE";
			} else {
				division_by_zero = Settings::Get<ErrorOnDivisionByZeroSetting>(context) ? "ERROR" : "NULL";
			}
		}
	}
	vector<substrait::Type> args_types;
	for (idx_t arg_idx = 0; arg_idx < dfun.GetChildren().size(); arg_idx++) {
		auto &darg = dfun.GetChildren()[arg_idx];
		auto sarg = sfun->add_arguments();
		if (is_decimal_multiply && darg->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		    darg->GetReturnType().id() == LogicalTypeId::DECIMAL) {
			// Bound casts can inflate a constant's precision/scale. Keep its exact value in the smallest decimal type.
			auto normalized = NormalizeDecimalConstant(darg->Cast<BoundConstantExpression>().GetValue());
			TransformConstant(normalized, *sarg->mutable_value());
			args_types.emplace_back(DuckToSubstraitType(normalized.type()));
		} else if (is_substring && arg_idx > 0 && darg->GetReturnType().id() == LogicalTypeId::BIGINT) {
			auto narrowed = LogicalType::INTEGER;
			auto scast = sarg->mutable_value()->mutable_cast();
			TransformExpr(*darg, *scast->mutable_input(), col_offset);
			*scast->mutable_type() = DuckToSubstraitType(narrowed);
			scast->set_failure_behavior(substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION);
			args_types.emplace_back(DuckToSubstraitType(narrowed));
		} else {
			TransformExpr(*darg, *sarg->mutable_value(), col_offset);
			args_types.emplace_back(DuckToSubstraitType(darg->GetReturnType()));
		}
	}
	sfun->set_function_reference(RegisterFunction(RemapFunctionName(function_name), args_types));
	if (!division_by_zero.empty()) {
		auto option = sfun->add_options();
		option->set_name("on_division_by_zero");
		option->add_preference(division_by_zero);
	}

	auto output_type = sfun->mutable_output_type();
	*output_type = DuckToSubstraitType(dfun.GetReturnType());
	if ((dfun.Function().GetName() != "+" && dfun.Function().GetName() != "-" && dfun.Function().GetName() != "*") ||
	    dfun.GetReturnType().id() != LogicalTypeId::DECIMAL || args_types.size() != 2 ||
	    !args_types[0].has_decimal() || !args_types[1].has_decimal()) {
		return;
	}

	// Derive the arithmetic type from the exported arguments, including DuckDB's bound casts.
	auto &left = args_types[0].decimal();
	auto &right = args_types[1].decimal();
	auto scale = is_decimal_multiply ? left.scale() + right.scale() : MaxValue(left.scale(), right.scale());
	auto input_scale = scale;
	auto precision = is_decimal_multiply
	                     ? left.precision() + right.precision() + 1
	                     : scale + MaxValue(left.precision() - left.scale(), right.precision() - right.scale()) + 1;
	if (precision > 38) {
		scale = MaxValue(scale - (precision - 38), MinValue<int32_t>(scale, 6));
		precision = 38;
	}
	auto arithmetic_type = LogicalType::DECIMAL(NumericCast<uint8_t>(precision), NumericCast<uint8_t>(scale));
	auto required_scale = is_decimal_multiply ? input_scale : DecimalType::GetScale(dfun.GetReturnType());
	if (scale != required_scale) {
		// A cast back cannot recover fractional digits removed by Substrait's scale reduction.
		auto operation =
		    is_decimal_multiply ? "multiplication" : (dfun.Function().GetName() == "+" ? "addition" : "subtraction");
		throw NotImplementedException("Decimal %s would lose scale in Substrait: %s instead of %s", operation,
		                              arithmetic_type.ToString(), dfun.GetReturnType().ToString());
	}
	*output_type = DuckToSubstraitType(arithmetic_type);
	auto overflow = sfun->add_options();
	overflow->set_name("overflow");
	overflow->add_preference("ERROR");
	if (arithmetic_type != dfun.GetReturnType()) {
		// Restore the SQL type before this expression becomes another function's argument.
		substrait::Expression arithmetic;
		arithmetic.Swap(&sexpr);
		auto cast = sexpr.mutable_cast();
		cast->mutable_input()->Swap(&arithmetic);
		*cast->mutable_type() = DuckToSubstraitType(dfun.GetReturnType());
		cast->set_failure_behavior(substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION);
	}
}

void DuckDBToSubstrait::TransformConstantExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &dconst = dexpr.Cast<BoundConstantExpression>();
	TransformConstant(dconst.GetValue(), sexpr);
}

void DuckDBToSubstrait::TransformComparisonExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                      uint64_t col_offset) {
	auto &dcomp = dexpr.Cast<BoundFunctionExpression>();

	string fname;
	switch (dexpr.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
		fname = "equal";
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		fname = "lt";
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		fname = "lte";
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		fname = "gt";
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		fname = "gte";
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		fname = "not_equal";
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		fname = "is_not_distinct_from";
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		fname = "is_distinct_from";
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.GetExpressionType()));
	}

	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<::substrait::Type> args_types;
	args_types.emplace_back(DuckToSubstraitType(BoundComparisonExpression::Left(dcomp).GetReturnType()));
	args_types.emplace_back(DuckToSubstraitType(BoundComparisonExpression::Right(dcomp).GetReturnType()));
	scalar_fun->set_function_reference(RegisterFunction(fname, args_types));
	auto sarg = scalar_fun->add_arguments();
	TransformExpr(*BoundComparisonExpression::LeftMutable(dcomp), *sarg->mutable_value(), col_offset);
	sarg = scalar_fun->add_arguments();
	TransformExpr(*BoundComparisonExpression::RightMutable(dcomp), *sarg->mutable_value(), col_offset);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dcomp.GetReturnType());
}

void DuckDBToSubstrait::TransformBetweenExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                   uint64_t col_offset) {
	auto &dcomp = dexpr.Cast<BoundFunctionExpression>();

	if (dexpr.GetExpressionType() != ExpressionType::COMPARE_BETWEEN) {
		throw InternalException("Not a between comparison expression");
	}

	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<::substrait::Type> args_types;
	args_types.emplace_back(DuckToSubstraitType(BoundBetweenExpression::Input(dcomp).GetReturnType()));
	args_types.emplace_back(DuckToSubstraitType(BoundBetweenExpression::LowerBound(dcomp).GetReturnType()));
	args_types.emplace_back(DuckToSubstraitType(BoundBetweenExpression::UpperBound(dcomp).GetReturnType()));
	scalar_fun->set_function_reference(RegisterFunction("between", args_types));

	auto sarg = scalar_fun->add_arguments();
	TransformExpr(*BoundBetweenExpression::InputMutable(dcomp), *sarg->mutable_value(), col_offset);
	sarg = scalar_fun->add_arguments();
	TransformExpr(*BoundBetweenExpression::LowerBoundMutable(dcomp), *sarg->mutable_value(), col_offset);
	sarg = scalar_fun->add_arguments();
	TransformExpr(*BoundBetweenExpression::UpperBoundMutable(dcomp), *sarg->mutable_value(), col_offset);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dcomp.GetReturnType());
}

void DuckDBToSubstrait::TransformConjunctionExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                       uint64_t col_offset) {
	auto &dconj = dexpr.Cast<BoundConjunctionExpression>();
	string fname;
	switch (dexpr.GetExpressionType()) {
	case ExpressionType::CONJUNCTION_AND:
		fname = "and";
		break;
	case ExpressionType::CONJUNCTION_OR:
		fname = "or";
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.GetExpressionType()));
	}

	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<::substrait::Type> args_types;
	for (auto &child : dconj.GetChildren()) {
		auto s_arg = scalar_fun->add_arguments();
		TransformExpr(*child, *s_arg->mutable_value(), col_offset);
		args_types.emplace_back(DuckToSubstraitType(child->GetReturnType()));
	}
	scalar_fun->set_function_reference(RegisterFunction(fname, args_types));

	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dconj.GetReturnType());
}

void DuckDBToSubstrait::TransformNotNullExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                   uint64_t col_offset) {
	auto &dop = dexpr.Cast<BoundOperatorExpression>();
	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<::substrait::Type> args_types;
	args_types.emplace_back(DuckToSubstraitType(dop.GetChildren()[0]->GetReturnType()));
	scalar_fun->set_function_reference(RegisterFunction("is_not_null", args_types));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dop.GetChildren()[0], *s_arg->mutable_value(), col_offset);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dop.GetReturnType());
}

void DuckDBToSubstrait::TransformCaseExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	auto &dcase = dexpr.Cast<BoundCaseExpression>();
	auto scase = sexpr.mutable_if_then();
	for (auto &dcheck : dcase.CaseChecks()) {
		auto sif = scase->mutable_ifs()->Add();
		TransformExpr(*dcheck.when_expr, *sif->mutable_if_(), col_offset);
		// Push a Cast
		auto scast = sif->mutable_then()->mutable_cast();
		TransformExpr(*dcheck.then_expr, *scast->mutable_input(), col_offset);
		*scast->mutable_type() = DuckToSubstraitType(dcase.GetReturnType());
		scast->set_failure_behavior(substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION);
	}
	// Push a Cast
	auto else_cast = scase->mutable_else_()->mutable_cast();
	TransformExpr(*dcase.ElseExpression(), *else_cast->mutable_input(), col_offset);
	*else_cast->mutable_type() = DuckToSubstraitType(dcase.GetReturnType());
	else_cast->set_failure_behavior(substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION);
}

void DuckDBToSubstrait::TransformInExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	auto &duck_in_op = dexpr.Cast<BoundOperatorExpression>();
	auto subs_in_op = sexpr.mutable_singular_or_list();

	// Get the expression
	TransformExpr(*duck_in_op.GetChildren()[0], *subs_in_op->mutable_value(), col_offset);

	// Get the values
	for (idx_t i = 1; i < duck_in_op.GetChildren().size(); i++) {
		subs_in_op->add_options();
		TransformExpr(*duck_in_op.GetChildren()[i], *subs_in_op->mutable_options(static_cast<int32_t>(i) - 1),
		              col_offset);
	}
}

void DuckDBToSubstrait::TransformIsNullExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                  uint64_t col_offset) {
	auto &dop = dexpr.Cast<BoundOperatorExpression>();
	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<substrait::Type> args_types;
	args_types.emplace_back(DuckToSubstraitType(dop.GetChildren()[0]->GetReturnType()));
	scalar_fun->set_function_reference(RegisterFunction("is_null", args_types));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dop.GetChildren()[0], *s_arg->mutable_value(), col_offset);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dop.GetReturnType());
}

void DuckDBToSubstrait::TransformNotExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	auto &dop = dexpr.Cast<BoundOperatorExpression>();
	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<::substrait::Type> args_types;
	args_types.emplace_back(DuckToSubstraitType(dop.GetChildren()[0]->GetReturnType()));
	scalar_fun->set_function_reference(RegisterFunction("not", args_types));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dop.GetChildren()[0], *s_arg->mutable_value(), col_offset);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dop.GetReturnType());
}

void DuckDBToSubstrait::TransformCoalesceExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                     uint64_t col_offset) {
	auto &dop = dexpr.Cast<BoundOperatorExpression>();
	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<::substrait::Type> args_types;
	
	// COALESCE is variadic - add all children as arguments
	for (auto &child : dop.GetChildren()) {
		auto s_arg = scalar_fun->add_arguments();
		TransformExpr(*child, *s_arg->mutable_value(), col_offset);
		args_types.emplace_back(DuckToSubstraitType(child->GetReturnType()));
	}
	
	scalar_fun->set_function_reference(RegisterFunction("coalesce", args_types));
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dop.GetReturnType());
}

void DuckDBToSubstrait::TransformExpr(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	switch (dexpr.GetExpressionType()) {
	case ExpressionType::BOUND_REF:
		TransformBoundRefExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_CAST:
		TransformCastExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::BOUND_FUNCTION:
		TransformFunctionExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::VALUE_CONSTANT:
		TransformConstantExpression(dexpr, sexpr);
		break;
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
	case ExpressionType::COMPARE_DISTINCT_FROM:
		TransformComparisonExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::COMPARE_BETWEEN:
		TransformBetweenExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::CONJUNCTION_AND:
	case ExpressionType::CONJUNCTION_OR:
		TransformConjunctionExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		TransformNotNullExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::CASE_EXPR:
		TransformCaseExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::COMPARE_IN:
		TransformInExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_IS_NULL:
		TransformIsNullExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_NOT:
		TransformNotExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_COALESCE:
		TransformCoalesceExpression(dexpr, sexpr, col_offset);
		break;
	default:
		throw NotImplementedException(ExpressionTypeToString(dexpr.GetExpressionType()));
	}
}

uint64_t DuckDBToSubstrait::RegisterFunction(const string &name, vector<::substrait::Type> &args_types) {
	if (name.empty()) {
		throw InternalException("Missing function name");
	}
	auto function = custom_functions.Get(name, args_types);
	// The name the function is declared under: its impl's compound name, or the bare name
	// when it resolved against no extension YAML. Every use below wants the same string.
	const auto declared_name = function.GetName();
	auto substrait_extensions = plan.mutable_extension_urns();
	if (!function.IsNative()) {
		auto extensionURN = function.GetExtensionURN();
		if (extensionURN.empty()) {
			// A non-native function must carry the URN declared by its extension YAML. An empty URN
			// would be serialized as an extensionUrns entry with no `urn` field, and because
			// extension_urn_map is keyed on the URN string, every such extension would collapse onto
			// a single shared anchor -- silently producing an unresolvable plan (see #205, #212).
			throw InternalException("Function \"%s\" resolved to an extension with an empty URN", declared_name);
		}
		auto it = extension_urn_map.find(extensionURN);
		if (it == extension_urn_map.end()) {
			// We have to add this extension
			extension_urn_map[extensionURN] = last_urn_id;
			auto urn = substrait_extensions->Add();
			urn->set_urn(extensionURN);
			urn->set_extension_urn_anchor(last_urn_id);
			last_urn_id++;
		}
	}
	auto function_entry = functions_map.find(declared_name);
	if (function_entry == functions_map.end()) {
		auto function_id = last_function_id++;
		auto sfun = plan.add_extensions()->mutable_extension_function();
		sfun->set_function_anchor(function_id);
		sfun->set_name(declared_name);
		if (!function.IsNative()) {
			// We only define URN if not native
			sfun->set_extension_urn_reference(extension_urn_map[function.GetExtensionURN()]);
		} else {
			// Function was not found in the yaml files
			sfun->set_extension_urn_reference(0);
			if (strict) {
				// Produce warning message
				std::ostringstream error;
				// Casting Error Message
				error << "Could not find function \"" << declared_name << "\" with argument types: (";
				auto types = SubstraitCustomFunctions::GetTypes(args_types);
				for (idx_t i = 0; i < types.size(); i++) {
					error << "\'" << types[i] << "\'";
					if (i != types.size() - 1) {
						error << ", ";
					}
				}
				error << ")" << std::endl;
				errors += error.str();
			}
		}
		functions_map[declared_name] = function_id;
		return function_id;
	}
	return function_entry->second;
}

void DuckDBToSubstrait::CreateFieldRef(substrait::Expression *expr, const ColumnIndex &column) {
	auto selection = expr->mutable_selection();
	selection->mutable_root_reference();
	auto segment = selection->mutable_direct_reference()->mutable_struct_field();
	segment->set_field(static_cast<int32_t>(column.GetPrimaryIndex()));
	for (auto index = &column; index->HasChildren(); index = &index->GetChildIndexes()[0]) {
		if (index->GetChildIndexes().size() != 1) {
			throw NotImplementedException("A table filter on more than one field of a struct column is not supported");
		}
		segment = segment->mutable_child()->mutable_struct_field();
		segment->set_field(static_cast<int32_t>(index->GetChildIndexes()[0].GetPrimaryIndex()));
	}
}

LogicalType DuckDBToSubstrait::ScanColumnType(const ColumnIndex &column, const vector<LogicalType> &types) {
	auto type = types[column.GetPrimaryIndex()];
	for (auto index = &column; index->HasChildren(); index = &index->GetChildIndexes()[0]) {
		if (index->GetChildIndexes().size() != 1) {
			throw NotImplementedException("A table filter on more than one field of a struct column is not supported");
		}
		type = StructType::GetChildType(type, index->GetChildIndexes()[0].GetPrimaryIndex());
	}
	return type;
}

void DuckDBToSubstrait::CreateFieldRef(substrait::Expression *expr, uint64_t col_idx) {
	auto selection = expr->mutable_selection();
	selection->mutable_direct_reference()->mutable_struct_field()->set_field(static_cast<int32_t>(col_idx));
	selection->mutable_root_reference();
	D_ASSERT(selection->root_type_case() == substrait::Expression_FieldReference::RootTypeCase::kRootReference);
	D_ASSERT(expr->has_selection());
}

vector<string> DuckDBToSubstrait::DepthFirstNames(const LogicalType &type) {
	vector<string> names;
	DepthFirstNamesRecurse(names, type);
	return names;
}

void DuckDBToSubstrait::DepthFirstNamesRecurse(vector<string> &names, const LogicalType &type) {
	if (type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::TUPLE) {
		// Recurse this; a TUPLE's members get the positional names DuckDB synthesizes for them.
		for (auto &child : TupleType::NamedChildren(type)) {
			names.emplace_back(child.first.GetIdentifierName());
			DepthFirstNamesRecurse(names, child.second);
		}
	}
}

substrait::Expression *DuckDBToSubstrait::TransformIsNotNullFilter(uint64_t col_idx, const LogicalType &column_type,
                                                                   const TableFilter &dfilter,
                                                                   const LogicalType &return_type) {
	auto s_expr = make_uniq<substrait::Expression>();
	auto scalar_fun = s_expr->mutable_scalar_function();
	vector<substrait::Type> args_types;

	args_types.emplace_back(DuckToSubstraitType(column_type));

	scalar_fun->set_function_reference(RegisterFunction("is_not_null", args_types));
	auto s_arg = scalar_fun->add_arguments();
	CreateFieldRef(s_arg->mutable_value(), col_idx);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(return_type);
	return s_expr.release();
}

substrait::Expression *DuckDBToSubstrait::TransformIsNullFilter(uint64_t col_idx, const LogicalType &column_type,
                                                                const TableFilter &dfilter,
                                                                const LogicalType &return_type) {
	auto s_expr = make_uniq<substrait::Expression>();
	auto scalar_fun = s_expr->mutable_scalar_function();
	vector<substrait::Type> args_types;

	args_types.emplace_back(DuckToSubstraitType(column_type));

	scalar_fun->set_function_reference(RegisterFunction("is_null", args_types));
	auto s_arg = scalar_fun->add_arguments();
	CreateFieldRef(s_arg->mutable_value(), col_idx);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(return_type);
	return s_expr.release();
}

substrait::Expression *DuckDBToSubstrait::TransformStructExtractFilter(uint64_t col_idx, const LogicalType &column_type, const TableFilter &dfilter, const LogicalType &return_type) {
	auto &struct_filter = dfilter.Cast<LegacyStructFilter>();

	// Apply the child filter to the struct's child field. The col_idx for the
	// recursive call is 0 because the child type becomes the "root" for the child filter.
	return TransformFilter(0, StructType::GetChildType(column_type, struct_filter.child_idx),
	                       *struct_filter.child_filter, return_type);
}

substrait::Expression *DuckDBToSubstrait::TransformConjunctionAndFilter(uint64_t col_idx, const LogicalType &column_type,
                                                                       const TableFilter &dfilter, const LogicalType &return_type) {
	auto &conjunction_filter = dfilter.Cast<LegacyConjunctionAndFilter>();
	return CreateConjunction(conjunction_filter.child_filters, [&](const unique_ptr<TableFilter> &in) {
		return TransformFilter(col_idx, column_type, *in, return_type);
	});
}

substrait::Expression *DuckDBToSubstrait::TransformConjunctionOrFilter(uint64_t col_idx, const LogicalType &column_type,
                                                                      const TableFilter &dfilter, const LogicalType &return_type) {
	auto &conjunction_filter = dfilter.Cast<LegacyConjunctionOrFilter>();
	return CreateConjunction(conjunction_filter.child_filters, [&](const unique_ptr<TableFilter> &in) {
		return TransformFilter(col_idx, column_type, *in, return_type);
	}, "or");
}

substrait::Expression *DuckDBToSubstrait::TransformConstantComparisonFilter(uint64_t col_idx,
                                                                            const LogicalType &column_type,
                                                                            const TableFilter &dfilter,
                                                                            const LogicalType &return_type) {
	auto s_expr = make_uniq<substrait::Expression>();
	auto s_scalar = s_expr->mutable_scalar_function();
	auto &constant_filter = dfilter.Cast<LegacyConstantFilter>();
	*s_scalar->mutable_output_type() = DuckToSubstraitType(LogicalTypeId::BOOLEAN);
	auto s_arg = s_scalar->add_arguments();
	CreateFieldRef(s_arg->mutable_value(), col_idx);
	s_arg = s_scalar->add_arguments();
	TransformConstant(constant_filter.constant, *s_arg->mutable_value());
	uint64_t function_id;
	vector<::substrait::Type> args_types;
	args_types.emplace_back(DuckToSubstraitType(column_type));

	args_types.emplace_back(DuckToSubstraitType(constant_filter.constant.type()));
	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		function_id = RegisterFunction("equal", args_types);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		function_id = RegisterFunction("not_equal", args_types);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		function_id = RegisterFunction("lte", args_types);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		function_id = RegisterFunction("lt", args_types);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		function_id = RegisterFunction("gt", args_types);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		function_id = RegisterFunction("gte", args_types);
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		function_id = RegisterFunction("is_not_distinct_from", args_types);
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		function_id = RegisterFunction("is_distinct_from", args_types);
		break;
	default:
		throw InternalException(ExpressionTypeToString(constant_filter.comparison_type));
	}
	s_scalar->set_function_reference(function_id);
	return s_expr.release();
}

substrait::Expression *DuckDBToSubstrait::TransformInFilter(uint64_t col_idx, const LogicalType &column_type,
                                                            const TableFilter &dfilter, const LogicalType &return_type) {
	auto s_expr = make_uniq<substrait::Expression>();
	auto &in_filter = dfilter.Cast<LegacyInFilter>();
	auto singular_or_list = s_expr->mutable_singular_or_list();

	// Set the input expression (the column being filtered)
	CreateFieldRef(singular_or_list->mutable_value(), col_idx);

	// Add the options (the values in the IN list)
	        for (auto &constant_value : in_filter.values) {
		TransformConstant(constant_value, *singular_or_list->add_options());
	}

	return s_expr.release();
}

substrait::Expression *DuckDBToSubstrait::TransformDynamicFilter(uint64_t col_idx, const LogicalType &column_type, const TableFilter &dfilter, const LogicalType &return_type) {
	// A dynamic filter compares against a constant that only becomes known at run time, as an
	// optimization: the query stays correct without it. Nothing to emit.
	return nullptr;
}

//! Drops the functions DuckDB's optimizer wraps around pushed-down filters to steer their execution:
//! optional and selectivity-optional filters, dynamic filters, bloom filters and prefix ranges. They
//! let a scan skip work but are not needed for a correct result, and no other system can bind them.
//! Returns nullptr when nothing is left of the expression.
static unique_ptr<Expression> StripTableFilterFunctions(unique_ptr<Expression> expr) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function = expr->Cast<BoundFunctionExpression>();
		if (TableFilterFunctions::IsTableFilterFunction(function.Function())) {
			return nullptr;
		}
	}
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conjunction = expr->Cast<BoundConjunctionExpression>();
		vector<unique_ptr<Expression>> kept;
		for (auto &child : conjunction.GetChildrenMutable()) {
			auto stripped = StripTableFilterFunctions(std::move(child));
			if (stripped) {
				kept.push_back(std::move(stripped));
			} else if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
				// Leaving a disjunct out would narrow the OR, so leave out the whole OR instead.
				return nullptr;
			}
		}
		if (kept.empty()) {
			return nullptr;
		}
		if (kept.size() == 1) {
			return std::move(kept[0]);
		}
		conjunction.GetChildrenMutable() = std::move(kept);
	}
	return expr;
}

substrait::Expression *DuckDBToSubstrait::TransformExpressionFilter(uint64_t col_idx, const LogicalType &column_type, const TableFilter &dfilter, const LogicalType &return_type) {
	auto &expr_filter = dfilter.Cast<ExpressionFilter>();

	// The placeholder stands for the filtered column: filter_columns[0] when the caller registered the
	// column (and the struct field it reaches into), the plain column otherwise.
	auto column_ref = make_uniq<BoundReferenceExpression>(column_type, filter_columns.empty() ? col_idx : 0);
	auto bound_expr = StripTableFilterFunctions(expr_filter.ToExpression(*column_ref));
	if (!bound_expr) {
		return nullptr;
	}

	// Transform the properly bound expression
	auto s_expr = make_uniq<substrait::Expression>();
	TransformExpr(*bound_expr, *s_expr);
	return s_expr.release();
}

//! A filter over several columns refers to them as BoundReferenceExpressions whose indexes point into the
//! filter's own column list, which in turn names scan columns. Register those scan columns as the filter's
//! columns so the references resolve to them, struct field paths included.
substrait::Expression *DuckDBToSubstrait::TransformMultiColumnFilter(const TableFilter &dfilter,
                                                                     const vector<ColumnIndex> &column_ids) {
	auto &expr_filter = dfilter.Cast<ExpressionFilter>();
	auto bound_expr = StripTableFilterFunctions(expr_filter.expr->Copy());
	if (!bound_expr) {
		return nullptr;
	}
	filter_columns.clear();
	for (auto &column_index : expr_filter.column_indexes) {
		filter_columns.push_back(column_ids[column_index]);
	}
	auto s_expr = make_uniq<substrait::Expression>();
	TransformExpr(*bound_expr, *s_expr);
	filter_columns.clear();
	return s_expr.release();
}

substrait::Expression *DuckDBToSubstrait::TransformFilter(uint64_t col_idx, const LogicalType &column_type,
                                                          const TableFilter &dfilter, const LogicalType &return_type) {
	switch (dfilter.filter_type) {
	case TableFilterType::LEGACY_CONJUNCTION_AND:
		return TransformConjunctionAndFilter(col_idx, column_type, dfilter, return_type);
	case TableFilterType::LEGACY_CONJUNCTION_OR:
		return TransformConjunctionOrFilter(col_idx, column_type, dfilter, return_type);
	case TableFilterType::LEGACY_CONSTANT_COMPARISON:
		return TransformConstantComparisonFilter(col_idx, column_type, dfilter, return_type);
	case TableFilterType::LEGACY_DYNAMIC_FILTER:
		return TransformDynamicFilter(col_idx, column_type, dfilter, return_type);
	case TableFilterType::EXPRESSION_FILTER:
		return TransformExpressionFilter(col_idx, column_type, dfilter, return_type);
	case TableFilterType::LEGACY_IN_FILTER:
		return TransformInFilter(col_idx, column_type, dfilter, return_type);
	case TableFilterType::LEGACY_IS_NOT_NULL:
		return TransformIsNotNullFilter(col_idx, column_type, dfilter, return_type);
	case TableFilterType::LEGACY_IS_NULL:
		return TransformIsNullFilter(col_idx, column_type, dfilter, return_type);
	case TableFilterType::LEGACY_OPTIONAL_FILTER:
		return nullptr;
	case TableFilterType::LEGACY_STRUCT_EXTRACT:
		return TransformStructExtractFilter(col_idx, column_type, dfilter, return_type);
        default:
		throw NotImplementedException("Unsupported table filter type: %s",
			EnumUtil::ToString(dfilter.filter_type));
	}
}

//! A join condition that is not a left-right comparison is an expression over the columns of both
//! sides, the left ones first: the layout of the join's output. When the join is emitted with its
//! children swapped, move the references accordingly.
static void SwapJoinReferences(Expression &expr, idx_t original_left_count, idx_t emitted_left_count) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_REF) {
		auto &ref = expr.Cast<BoundReferenceExpression>();
		ref.IndexMutable() = ref.Index() < original_left_count ? ref.Index() + emitted_left_count
		                                                        : ref.Index() - original_left_count;
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { SwapJoinReferences(child, original_left_count, emitted_left_count); });
}

substrait::Expression *DuckDBToSubstrait::TransformJoinCond(const JoinCondition &dcond, uint64_t left_ncol) {
	auto expr = make_uniq<substrait::Expression>();
	if (!dcond.IsComparison()) {
		TransformExpr(*dcond.JoinExpressionReference(), *expr, 0);
		return expr.release();
	}
	string join_comparision;
	switch (dcond.GetComparisonType()) {
	case ExpressionType::COMPARE_EQUAL:
		join_comparision = "equal";
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		join_comparision = "not_equal";
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		join_comparision = "gt";
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		join_comparision = "is_not_distinct_from";
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		join_comparision = "is_distinct_from";
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		join_comparision = "gte";
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		join_comparision = "lte";
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		join_comparision = "lt";
		break;
	default:
		throw NotImplementedException("Unsupported join comparison: " + ExpressionTypeToOperator(dcond.GetComparisonType()));
	}
	vector<::substrait::Type> args_types;
	auto scalar_fun = expr->mutable_scalar_function();
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dcond.LeftReference(), *s_arg->mutable_value());
	args_types.emplace_back(DuckToSubstraitType(dcond.GetLHS().GetReturnType()));

	s_arg = scalar_fun->add_arguments();
	TransformExpr(*dcond.RightReference(), *s_arg->mutable_value(), left_ncol);
	args_types.emplace_back(DuckToSubstraitType(dcond.GetRHS().GetReturnType()));

	LogicalType bool_type = LogicalType::BOOLEAN;
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(bool_type);
	scalar_fun->set_function_reference(RegisterFunction(join_comparision, args_types));

	return expr.release();
}

void DuckDBToSubstrait::TransformOrder(const BoundOrderByNode &dordf, substrait::SortField &sordf) {
	switch (dordf.type) {
	case OrderType::ASCENDING:
		switch (dordf.null_order) {
		case OrderByNullType::NULLS_FIRST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST);
			break;
		case OrderByNullType::NULLS_LAST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST);

			break;
		default:
			throw InternalException("Unsupported ordering type");
		}
		break;
	case OrderType::DESCENDING:
		switch (dordf.null_order) {
		case OrderByNullType::NULLS_FIRST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST);
			break;
		case OrderByNullType::NULLS_LAST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST);

			break;
		default:
			throw InternalException("Unsupported ordering type");
		}
		break;
	default:
		throw InternalException("Unsupported ordering type");
	}
	TransformExpr(*dordf.expression, *sordf.mutable_expr());
}

substrait::Rel *DuckDBToSubstrait::TransformFilter(LogicalOperator &dop) {

	auto &dfilter = dop.Cast<LogicalFilter>();

	unique_ptr<substrait::Rel> res(TransformOp(*dop.children[0]));

	if (!dfilter.expressions.empty()) {
		auto filter = make_uniq<substrait::Rel>();
		filter->mutable_filter()->set_allocated_input(res.release());
		filter->mutable_filter()->set_allocated_condition(
		    CreateConjunction(dfilter.expressions, [&](const unique_ptr<Expression> &in) {
			    auto expr = make_uniq<substrait::Expression>();
			    TransformExpr(*in, *expr);
			    return expr.release();
		    }));
		res = std::move(filter);
	}

	if (!dfilter.projection_map.empty()) {
		auto projection = make_uniq<substrait::Rel>();
		auto sproj = projection->mutable_project();
		sproj->set_allocated_input(res.release());
		auto child_column_count = GetColumnCount(*dop.children[0]);
		auto t_index = 0;
		vector<int32_t> output_mapping;
		for (auto col_idx : dfilter.projection_map) {
			CreateFieldRef(sproj->add_expressions(), col_idx);
			output_mapping.push_back(child_column_count + t_index);
			++t_index;
		}
		auto rel_common = CreateOutputMapping(output_mapping);
		sproj->set_allocated_common(rel_common);
		res = std::move(projection);
	}
	return res.release();
}

substrait::RelCommon *DuckDBToSubstrait::CreateOutputMapping(vector<int32_t> vector) {
	auto rel_common = make_uniq<substrait::RelCommon>();
	auto output_mapping = rel_common->mutable_emit()->mutable_output_mapping();
	for (auto &col_idx : vector) {
		output_mapping->Add(col_idx);
	}
	return rel_common.release();
}

bool DuckDBToSubstrait::IsPassthroughProjection(LogicalProjection &dproj, idx_t child_column_count,
                                                bool &needs_output_mapping) {
	// check if the projection is just pass through of input columns with no reordering
	needs_output_mapping = true;
	auto isPassThrough = true;
	if (child_column_count > dproj.expressions.size()) {
		return false;
	}
	idx_t exp_col_idx = 0;
	for (auto &dexpr : dproj.expressions) {
		if (dexpr->GetExpressionType() != ExpressionType::BOUND_REF) {
			isPassThrough = false;
			break;
		}
		auto &dref = dexpr.get()->Cast<BoundReferenceExpression>();
		if (dref.Index() != exp_col_idx) {
			isPassThrough = false;
			break;
		}
		exp_col_idx++;
	}
	needs_output_mapping = child_column_count != exp_col_idx;
	return child_column_count == exp_col_idx && isPassThrough;
}

substrait::Rel *DuckDBToSubstrait::TransformProjection(LogicalOperator &dop) {
	auto res = make_uniq<substrait::Rel>();
	auto &dproj = dop.Cast<LogicalProjection>();

	auto child_column_count = GetColumnCount(*dop.children[0]);
	auto need_output_mapping = true;
	if (IsPassthroughProjection(dproj, child_column_count, need_output_mapping)) {
		// skip the projection
		return TransformOp(*dop.children[0]);
	}

	auto sproj = res->mutable_project();
	sproj->set_allocated_input(TransformOp(*dop.children[0]));

	auto t_index = 0;
	vector<int32_t> output_mapping;
	for (auto &dexpr : dproj.expressions) {
		switch (dexpr->GetExpressionType()) {
		case ExpressionType::BOUND_REF: {
			auto &dref = dexpr.get()->Cast<BoundReferenceExpression>();
			output_mapping.push_back(dref.Index());
			break;
		}
		default:
			TransformExpr(*dexpr.get(), *sproj->add_expressions());
			output_mapping.push_back(child_column_count + t_index);
			t_index++;
		}
	}
	if (need_output_mapping) {
		if (sproj->expressions_size() == 0) {
			// atleast one expression should be there, add zeroth column as dummy expression
			CreateFieldRef(sproj->add_expressions(), 0);
		}
		auto rel_common = CreateOutputMapping(output_mapping);
		sproj->set_allocated_common(rel_common);
	}
	return res.release();
}

substrait::Rel *DuckDBToSubstrait::TransformTopN(LogicalOperator &dop) {
	auto &dtopn = dop.Cast<LogicalTopN>();
	auto res = make_uniq<substrait::Rel>();
	auto stopn = res->mutable_fetch();

	auto sord_rel = make_uniq<substrait::Rel>();
	auto sord = sord_rel->mutable_sort();
	sord->set_allocated_input(TransformOp(*dop.children[0]));

	for (auto &dordf : dtopn.orders) {
		TransformOrder(dordf, *sord->add_sorts());
	}

	stopn->set_allocated_input(sord_rel.release());

	// Set offset expression (always set, default is 0)
	auto offset_expr = make_uniq<substrait::Expression>();
	offset_expr->mutable_literal()->set_i64(static_cast<int64_t>(dtopn.offset));
	stopn->set_allocated_offset_expr(offset_expr.release());

	// Set count expression (TopN always has a limit)
	auto count_expr = make_uniq<substrait::Expression>();
	count_expr->mutable_literal()->set_i64(static_cast<int64_t>(dtopn.limit));
	stopn->set_allocated_count_expr(count_expr.release());

	return res.release();
}

substrait::Rel *DuckDBToSubstrait::TransformLimit(LogicalOperator &dop) {
	auto &dlimit = dop.Cast<LogicalLimit>();
	// figure out limit and offset of this node
	int32_t limit_val;
	int32_t offset_val;
	switch (dlimit.limit_val.Type()) {
	case LimitNodeType::CONSTANT_VALUE:
		limit_val = static_cast<int32_t>(dlimit.limit_val.GetConstantValue());
		break;
	case LimitNodeType::UNSET:
		limit_val = -1;
		break;
	default:
		throw InternalException("Unsupported limit value type");
	}
	switch (dlimit.offset_val.Type()) {
	case LimitNodeType::CONSTANT_VALUE:
		offset_val = static_cast<int32_t>(dlimit.offset_val.GetConstantValue());
		break;
	case LimitNodeType::UNSET:
		offset_val = 0;
		break;
	default:
		throw InternalException("Unsupported offset value type");
	}

	auto res = make_uniq<substrait::Rel>();
	auto stopn = res->mutable_fetch();
	stopn->set_allocated_input(TransformOp(*dop.children[0]));

	// Set offset expression (always set, default is 0)
	auto offset_expr = make_uniq<substrait::Expression>();
	offset_expr->mutable_literal()->set_i64(static_cast<int64_t>(offset_val));
	stopn->set_allocated_offset_expr(offset_expr.release());

	// Set count expression only if limit is set (not -1)
	if (limit_val >= 0) {
		auto count_expr = make_uniq<substrait::Expression>();
		count_expr->mutable_literal()->set_i64(static_cast<int64_t>(limit_val));
		stopn->set_allocated_count_expr(count_expr.release());
	}

	return res.release();
}

substrait::Rel *DuckDBToSubstrait::TransformOrderBy(LogicalOperator &dop) {
	auto res = make_uniq<substrait::Rel>();
	auto &dord = dop.Cast<LogicalOrder>();
	auto sord = res->mutable_sort();

	sord->set_allocated_input(TransformOp(*dop.children[0]));

	for (auto &dordf : dord.orders) {
		TransformOrder(dordf, *sord->add_sorts());
	}

	if (!dord.projection_map.empty()) {
		auto proj_rel = make_uniq<substrait::Rel>();
		auto projection = proj_rel->mutable_project();
		auto child_column_count = GetColumnCount(*dop.children[0]);
		for (auto &col_idx : dord.projection_map) {
			CreateFieldRef(projection->add_expressions(), col_idx);
		}
		vector<int32_t> output_mapping;
		for (idx_t i = 0; i < projection->expressions_size(); i++) {
			output_mapping.push_back(child_column_count + i);
		}
		auto rel_common = CreateOutputMapping(output_mapping);
		projection->set_allocated_common(rel_common);
		projection->set_allocated_input(res.release());
		return proj_rel.release();
	}

	return res.release();
}

substrait::Rel *DuckDBToSubstrait::TransformComparisonJoin(LogicalOperator &dop) {
	auto res = make_uniq<substrait::Rel>();
	auto sjoin = res->mutable_join();
	auto &djoin = dop.Cast<LogicalComparisonJoin>();
	
	// RIGHT_SEMI and RIGHT_ANTI are equivalent to their LEFT variants with swapped children.
	bool is_right_semi = djoin.join_type == JoinType::RIGHT_SEMI;
	bool is_right_anti = djoin.join_type == JoinType::RIGHT_ANTI;
	bool swap_children = is_right_semi || is_right_anti;
	idx_t left_child_idx = swap_children ? 1 : 0;
	idx_t right_child_idx = swap_children ? 0 : 1;

	// A duplicate eliminated join (what DuckDB abbreviates as a delim join) is a comparison
	// join that additionally feeds the distinct values of some of its data side's columns
	// (the duplicate eliminated columns) into duplicate eliminated get scans
	// (LOGICAL_DELIM_GET) within its other child. Substrait has no equivalent construct, so
	// each of those scans is expanded into an aggregation over a copy of the data side (see
	// TransformDuplicateEliminatedGet). The data side is registered here while the scan side
	// is transformed, and marked as a saved computation so consumers can recognize that the
	// copies (marked as loaded computations) do not need to be computed again.
	bool is_duplicate_eliminated = dop.type == LogicalOperatorType::LOGICAL_DELIM_JOIN;
	idx_t data_side_idx = djoin.delim_flipped ? 1 : 0;
	int32_t computation_id = is_duplicate_eliminated ? ++last_computation_id : 0;
	auto transform_child = [&](idx_t child_idx) -> substrait::Rel * {
		if (!is_duplicate_eliminated) {
			return TransformOp(*dop.children[child_idx]);
		}
		if (child_idx == data_side_idx) {
			auto data_side = TransformOp(*dop.children[child_idx]);
			auto common = GetRelCommon(*data_side);
			if (common) {
				auto saved = common->mutable_hint()->add_saved_computations();
				saved->set_computation_id(computation_id);
				saved->set_type(substrait::RelCommon_Hint_ComputationType_COMPUTATION_TYPE_HASHTABLE);
			}
			return data_side;
		}
		DuplicateEliminationSource source;
		source.data_side = dop.children[data_side_idx].get();
		for (auto &dexpr : djoin.duplicate_eliminated_columns) {
			if (dexpr->GetExpressionType() != ExpressionType::BOUND_REF) {
				throw NotImplementedException("Duplicate eliminated column must be a bound reference");
			}
			source.column_indices.push_back(dexpr->Cast<BoundReferenceExpression>().Index());
		}
		source.computation_id = computation_id;
		duplicate_elimination_sources.push_back(std::move(source));
		auto scan_side = TransformOp(*dop.children[child_idx]);
		duplicate_elimination_sources.pop_back();
		return scan_side;
	};
	sjoin->set_allocated_left(transform_child(left_child_idx));
	sjoin->set_allocated_right(transform_child(right_child_idx));

	auto left_col_count = dop.children[left_child_idx]->types.size();
	if (dop.children[left_child_idx]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    dop.children[left_child_idx]->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &child_join = dop.children[left_child_idx]->Cast<LogicalComparisonJoin>();
		if (child_join.join_type == JoinType::RIGHT_SEMI || child_join.join_type == JoinType::RIGHT_ANTI) {
			// A right semi/anti join is emitted with its children swapped and projects only its
			// right side, so its Substrait output has right_projection_map columns.
			left_col_count = child_join.right_projection_map.size();
		} else if (child_join.join_type == JoinType::SEMI || child_join.join_type == JoinType::ANTI) {
			left_col_count = child_join.left_projection_map.size();
		} else if (child_join.join_type == JoinType::MARK) {
			// A mark join outputs its left side plus the mark column.
			left_col_count = child_join.left_projection_map.size() + 1;
		} else {
			left_col_count = child_join.left_projection_map.size() + child_join.right_projection_map.size();
		}
	}
	
	// For RIGHT_SEMI, we need to swap the column references in join conditions
	auto right_col_count = dop.children[right_child_idx]->types.size();
	if (swap_children) {
		// For RIGHT_SEMI/RIGHT_ANTI, swap left and right expressions in conditions
		sjoin->set_allocated_expression(CreateConjunction(
		    djoin.conditions, [&](const JoinCondition &in) {
				// Create expression with swapped left/right
				auto expr = make_uniq<substrait::Expression>();
				if (!in.IsComparison()) {
					auto condition = in.JoinExpressionReference()->Copy();
					SwapJoinReferences(*condition, dop.children[0]->types.size(), left_col_count);
					TransformExpr(*condition, *expr, 0);
					return expr.release();
				}
				string join_comparision;
				switch (in.GetComparisonType()) {
				case ExpressionType::COMPARE_EQUAL:
					join_comparision = "equal";
					break;
				case ExpressionType::COMPARE_NOTEQUAL:
					join_comparision = "not_equal";
					break;
				case ExpressionType::COMPARE_GREATERTHAN:
					// Swap: left > right becomes right < left
					join_comparision = "lt";
					break;
				case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
					join_comparision = "is_not_distinct_from";
					break;
				case ExpressionType::COMPARE_DISTINCT_FROM:
					join_comparision = "is_distinct_from";
					break;
				case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
					// Swap: left >= right becomes right <= left
					join_comparision = "lte";
					break;
				case ExpressionType::COMPARE_LESSTHANOREQUALTO:
					// Swap: left <= right becomes right >= left
					join_comparision = "gte";
					break;
				case ExpressionType::COMPARE_LESSTHAN:
					// Swap: left < right becomes right > left
					join_comparision = "gt";
					break;
				default:
					throw NotImplementedException("Unsupported join comparison: " + ExpressionTypeToOperator(in.GetComparisonType()));
				}
				vector<::substrait::Type> args_types;
				auto scalar_fun = expr->mutable_scalar_function();
				
				// Swap: right expression first (no offset)
				auto s_arg = scalar_fun->add_arguments();
				TransformExpr(*in.RightReference(), *s_arg->mutable_value());
				args_types.emplace_back(DuckToSubstraitType(in.GetRHS().GetReturnType()));
				
				// Then left expression (with offset)
				s_arg = scalar_fun->add_arguments();
				TransformExpr(*in.LeftReference(), *s_arg->mutable_value(), left_col_count);
				args_types.emplace_back(DuckToSubstraitType(in.GetLHS().GetReturnType()));
				
				LogicalType bool_type = LogicalType::BOOLEAN;
				*scalar_fun->mutable_output_type() = DuckToSubstraitType(bool_type);
				scalar_fun->set_function_reference(RegisterFunction(join_comparision, args_types));
				
				return expr.release();
			}));
	} else {
		sjoin->set_allocated_expression(CreateConjunction(
		    djoin.conditions, [&](const JoinCondition &in) { return TransformJoinCond(in, left_col_count); }));
	}
	if (!sjoin->has_expression()) {
		// A join without conditions pairs every row; say so rather than leave the condition out.
		TransformConstant(Value::BOOLEAN(true), *sjoin->mutable_expression());
	}

	switch (djoin.join_type) {
	case JoinType::INNER:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_INNER);
		break;
	case JoinType::LEFT:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT);
		break;
	case JoinType::RIGHT:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT);
		break;
	case JoinType::SINGLE:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SINGLE);
		break;
	case JoinType::SEMI:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI);
		break;
	case JoinType::RIGHT_SEMI:
		// Convert RIGHT_SEMI to LEFT_SEMI since we swapped the children
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI);
		break;
	case JoinType::ANTI:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI);
		break;
	case JoinType::RIGHT_ANTI:
		// Convert RIGHT_ANTI to LEFT_ANTI since we swapped the children
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI);
		break;
	case JoinType::MARK:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_MARK);
		break;
	case JoinType::OUTER:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER);
		break;
	default:
		throw NotImplementedException("Unsupported join type " + JoinTypeToString(djoin.join_type));
	}
	// somewhat odd semantics on our side
	if (djoin.left_projection_map.empty()) {
		for (uint64_t i = 0; i < dop.children[0]->types.size(); i++) {
			djoin.left_projection_map.push_back(ProjectionIndex(i));
		}
	}
	if (djoin.right_projection_map.empty()) {
		for (uint64_t i = 0; i < dop.children[1]->types.size(); i++) {
			djoin.right_projection_map.push_back(ProjectionIndex(i));
		}
	}
	// TODO this projection seems redundant but from_substrait does not work without it
	auto proj_rel = make_uniq<substrait::Rel>();
	auto projection = proj_rel->mutable_project();
	auto child_column_count = GetColumnCount(*dop.children[left_child_idx]);
	
	// For RIGHT_SEMI/RIGHT_ANTI (now converted to their LEFT variants with swapped children),
	// use right_projection_map. For SEMI/ANTI, use left_projection_map.
	auto &projection_map = swap_children ? djoin.right_projection_map : djoin.left_projection_map;
	for (auto idx : projection_map) {
		CreateFieldRef(projection->add_expressions(), idx);
	}
	if (djoin.join_type == JoinType::MARK) {
		// A mark join outputs its left side and one boolean mark column, which Substrait places
		// right after the left columns. None of the right side's columns come out.
		child_column_count += 1;
		CreateFieldRef(projection->add_expressions(), left_col_count);
	} else if (djoin.join_type != JoinType::SEMI && djoin.join_type != JoinType::RIGHT_SEMI &&
	           djoin.join_type != JoinType::ANTI && djoin.join_type != JoinType::RIGHT_ANTI) {
		child_column_count += GetColumnCount(*dop.children[right_child_idx]);
		for (auto right_idx : djoin.right_projection_map) {
			CreateFieldRef(projection->add_expressions(), right_idx + left_col_count);
		}
	}

	vector<int32_t> output_mapping;
	for (idx_t i = 0; i < projection->expressions_size(); i++) {
		output_mapping.push_back(child_column_count + i);
	}
	auto rel_common = CreateOutputMapping(output_mapping);
	projection->set_allocated_common(rel_common);
	projection->set_allocated_input(res.release());
	return proj_rel.release();
}

substrait::Rel *DuckDBToSubstrait::TransformDuplicateEliminatedGet(LogicalOperator &dop) {
	auto &dget = dop.Cast<LogicalDelimGet>();
	if (duplicate_elimination_sources.empty()) {
		throw NotImplementedException("Found a duplicate eliminated get without an enclosing "
		                              "duplicate eliminated join");
	}
	// A duplicate eliminated get scans the distinct values of the duplicate eliminated
	// columns of its innermost enclosing duplicate eliminated join. Substrait has no
	// equivalent construct, so emit a copy of the join's data side aggregated on those
	// columns, which produces exactly the rows the get scans.
	auto source = duplicate_elimination_sources.back();
	if (source.column_indices.size() != dget.chunk_types.size()) {
		throw InternalException("Duplicate eliminated get scans %llu columns but the enclosing "
		                        "duplicate eliminated join eliminates duplicates over %llu columns",
		                        dget.chunk_types.size(), source.column_indices.size());
	}
	// The data side may itself contain duplicate eliminated gets, but they belong to outer
	// duplicate eliminated joins; hide this join's entry while the copy is emitted so they
	// resolve against the correct join.
	duplicate_elimination_sources.pop_back();
	auto data_side = TransformOp(*source.data_side);
	duplicate_elimination_sources.push_back(source);
	// Mark the copy as a load of the computation saved on the join's data side.
	auto common = GetRelCommon(*data_side);
	if (common) {
		auto loaded = common->mutable_hint()->add_loaded_computations();
		loaded->set_computation_id_reference(source.computation_id);
		loaded->set_type(substrait::RelCommon_Hint_ComputationType_COMPUTATION_TYPE_HASHTABLE);
	}

	auto res = new substrait::Rel();
	auto aggregate = res->mutable_aggregate();
	aggregate->set_allocated_input(data_side);
	auto grouping = aggregate->add_groupings();
	for (idx_t i = 0; i < source.column_indices.size(); i++) {
		CreateFieldRef(aggregate->add_grouping_expressions(), source.column_indices[i]);
		grouping->add_expression_references(i);
	}
	return res;
}

uint64_t
DuckDBToSubstrait::RegisterAggregateFunction(const BoundAggregateFunction &function,
                                             optional_ptr<FunctionData> bind_data,
                                             google::protobuf::RepeatedPtrField<substrait::FunctionArgument> &arguments,
                                             vector<substrait::Type> &arg_types) {
	auto name = function.GetName().GetIdentifierName();
	if (name != "string_agg" && name != "group_concat" && name != "listagg") {
		return RegisterFunction(RemapFunctionName(name), arg_types);
	}
	if (arguments.size() >= 2) {
		// DuckDB 2.0 keeps the separator argument in the expression tree, so it is already emitted.
		return RegisterFunction("string_agg", arg_types);
	}
	if (!bind_data || !function.HasSerializationCallbacks()) {
		throw NotImplementedException("Cannot export string_agg without its bound separator");
	}
	// Without a separator argument the default comes from the bind data, where DuckDB stores it in field 100
	// of its serialized form.
	MemoryStream stream;
	BinarySerializer serializer(stream);
	serializer.Begin();
	function.GetSerializeCallback()(serializer, bind_data, function);
	serializer.End();
	stream.Rewind();
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto separator = deserializer.ReadProperty<string>(100, "separator");
	deserializer.End();
	arguments.Add()->mutable_value()->mutable_literal()->set_string(separator);
	arg_types.emplace_back(DuckToSubstraitType(LogicalType::VARCHAR));
	return RegisterFunction("string_agg", arg_types);
}

substrait::Rel *DuckDBToSubstrait::TransformAggregateGroup(LogicalOperator &dop) {
	auto res = make_uniq<substrait::Rel>();
	auto &daggr = dop.Cast<LogicalAggregate>();
	auto saggr = res->mutable_aggregate();
	saggr->set_allocated_input(TransformOp(*dop.children[0]));
	
	// In v0.89.0, grouping expressions are stored at the AggregateRel level
	// and groupings reference them by index
	for (auto &dgrp : daggr.groups) {
		// Add the expression to the AggregateRel's grouping_expressions array
		// This supports both simple column references (BOUND_REF) and complex expressions
		auto grouping_expr = saggr->add_grouping_expressions();
		TransformExpr(*dgrp, *grouping_expr);
	}
	
	// Handle multiple grouping sets (for ROLLUP, CUBE, GROUPING SETS)
	if (daggr.grouping_sets.empty()) {
		// No explicit grouping sets - create a single grouping set with all groups
		auto sgrp = saggr->add_groupings();
		for (idx_t i = 0; i < daggr.groups.size(); i++) {
			sgrp->add_expression_references(i);
		}
	} else {
		// Multiple grouping sets - convert each one
		for (auto &grouping_set : daggr.grouping_sets) {
			auto sgrp = saggr->add_groupings();
			for (auto &group_idx : grouping_set) {
				sgrp->add_expression_references(group_idx);
			}
		}
	}
	for (auto &dmeas : daggr.expressions) {
		auto measure = saggr->add_measures();
		auto smeas = measure->mutable_measure();
		if (dmeas->GetExpressionType() != ExpressionType::BOUND_AGGREGATE) {
			// TODO push projection or push substrait, too
			throw NotImplementedException("No non-aggregate expressions in measures yet");
		}
		auto &daexpr = dmeas->Cast<BoundAggregateExpression>();

		*smeas->mutable_output_type() = DuckToSubstraitType(daexpr.GetReturnType());
		smeas->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
		if (daexpr.GetOrderBys()) {
			for (auto &order : daexpr.GetOrderBys()->orders) {
				TransformOrder(order, *smeas->add_sorts());
			}
		}
		vector<::substrait::Type> args_types;
		for (auto &darg : daexpr.GetChildren()) {
			auto s_arg = smeas->add_arguments();
			args_types.emplace_back(DuckToSubstraitType(darg->GetReturnType()));
			TransformExpr(*darg, *s_arg->mutable_value());
		}
		if (daexpr.GetFilter()) {
			TransformExpr(*daexpr.GetFilter(), *measure->mutable_filter());
		}
		smeas->set_function_reference(RegisterAggregateFunction(daexpr.Function(), daexpr.BindInfo().get(),
		                                                        *smeas->mutable_arguments(), args_types));
		if (daexpr.IsDistinct()) {
			smeas->set_invocation(substrait::AggregateFunction_AggregationInvocation_AGGREGATION_INVOCATION_DISTINCT);
		}
	}

	// Transform GROUPING() function calls
	// Each grouping function is represented as an aggregate function measure
	// that takes field references to the grouping columns it references
	for (auto &grouping_func : daggr.grouping_functions) {
		auto smeas = saggr->add_measures()->mutable_measure();
		smeas->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);

		// Build argument types - each argument is a reference to a grouping column
		vector<::substrait::Type> args_types;
		for (auto &group_idx : grouping_func) {
			// Each argument is a field reference to a grouping expression
			if (group_idx >= daggr.groups.size()) {
				throw InternalException("Grouping index out of bounds");
			}
			auto s_arg = smeas->add_arguments();
			auto field_ref = s_arg->mutable_value();
			CreateFieldRef(field_ref, group_idx);
			
			// Get the type of the grouping column
			args_types.emplace_back(DuckToSubstraitType(daggr.groups[group_idx]->GetReturnType()));
		}
		
		// Register the "grouping" function as an aggregate function
		smeas->set_function_reference(RegisterFunction("grouping", args_types));
		
		// GROUPING() returns BIGINT in DuckDB
		*smeas->mutable_output_type() = DuckToSubstraitType(LogicalType::BIGINT);
	}

	// With more than one grouping set, the spec appends an i32 column holding
	// the grouping set index. DuckDB's output has no such column, so leave it out.
	if (saggr->groupings_size() > 1) {
		vector<int32_t> output_mapping;
		int32_t output_count = saggr->grouping_expressions_size() + saggr->measures_size();
		for (int32_t i = 0; i < output_count; i++) {
			output_mapping.push_back(i);
		}
		saggr->set_allocated_common(CreateOutputMapping(output_mapping));
	}

	return res.release();
}

substrait::Rel *DuckDBToSubstrait::TransformWindow(LogicalOperator &dop) {
	auto &dwindow = dop.Cast<LogicalWindow>();
	
	struct WindowSpec {
		const BoundWindowExpression &window;
		vector<idx_t> expression_indices;

		bool Matches(const BoundWindowExpression &other) const {
			if (!Expression::ListEquals(window.Partitions(), other.Partitions()) ||
			    window.OrderBy().size() != other.OrderBy().size()) {
				return false;
			}
			for (idx_t i = 0; i < window.OrderBy().size(); i++) {
				if (!window.OrderBy()[i].Equals(other.OrderBy()[i])) {
					return false;
				}
			}
			return true;
		}
	};
	
	vector<WindowSpec> window_specs;
	
	// Group expressions by their window specifications
	for (idx_t i = 0; i < dwindow.expressions.size(); i++) {
		auto &dexpr = dwindow.expressions[i];
		if (dexpr->GetExpressionClass() != ExpressionClass::BOUND_WINDOW) {
			throw NotImplementedException("Only window expressions are supported in window operator");
		}
		
		auto &dwin_expr = dexpr->Cast<BoundWindowExpression>();
		if (dwin_expr.Filter()) {
			throw NotImplementedException("Substrait export does not support FILTER on window functions");
		}
		if (!dwin_expr.ArgOrders().empty()) {
			throw NotImplementedException("ORDER BY in window function arguments is not supported");
		}
		
		bool found = false;
		for (auto &spec : window_specs) {
			if (spec.Matches(dwin_expr)) {
				spec.expression_indices.push_back(i);
				found = true;
				break;
			}
		}
		
		if (!found) {
			window_specs.push_back({dwin_expr, {i}});
		}
	}
	
	// Now create chained window relations, one for each unique spec
	substrait::Rel *current_input = TransformOp(*dop.children[0]);
	auto child_column_count = GetColumnCount(*dop.children[0]);
	vector<int32_t> output_mapping(child_column_count + dwindow.expressions.size());
	for (idx_t i = 0; i < child_column_count; i++) {
		output_mapping[i] = i;
	}
	idx_t output_idx = child_column_count;

	for (auto &spec : window_specs) {
		auto res = make_uniq<substrait::Rel>();
		auto swindow = res->mutable_window();
		
		// Set the input (either the original input or the previous window relation)
		swindow->set_allocated_input(current_input);
		
		// Set partition expressions at relation level
		for (auto &dpart : spec.window.Partitions()) {
			TransformExpr(*dpart, *swindow->add_partition_expressions());
		}
		
		// Set sort specifications at relation level
		for (auto &dorder : spec.window.OrderBy()) {
			TransformOrder(dorder, *swindow->add_sorts());
		}
		
		// Process each window function expression in this group
		for (auto expr_idx : spec.expression_indices) {
			output_mapping[child_column_count + expr_idx] = output_idx++;
			auto &dexpr = dwindow.expressions[expr_idx];
			auto &dwin_expr = dexpr->Cast<BoundWindowExpression>();
		auto swin_func = swindow->add_window_functions();
		swin_func->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);

		// Set output type
		*swin_func->mutable_output_type() = DuckToSubstraitType(dwin_expr.GetReturnType());
		
		// Determine function name and add arguments
		string function_name;
		vector<::substrait::Type> args_types;
		
		if (dwin_expr.GetExpressionType() == ExpressionType::WINDOW_AGGREGATE) {
			// This is an aggregate function used as a window function
			if (!dwin_expr.AggregateFunction()) {
				throw InternalException("Window aggregate expression missing aggregate function");
			}
			function_name = dwin_expr.AggregateFunction()->GetName().GetIdentifierName();
			
			// Handle DISTINCT aggregates
			if (dwin_expr.Distinct()) {
				swin_func->set_invocation(substrait::AggregateFunction_AggregationInvocation_AGGREGATION_INVOCATION_DISTINCT);
			}
		} else {
			// This is a window-specific function (ROW_NUMBER, RANK, DENSE_RANK, etc.)
			switch (dwin_expr.GetExpressionType()) {
			case ExpressionType::WINDOW_ROW_NUMBER:
				function_name = "row_number";
				break;
			case ExpressionType::WINDOW_RANK:
				function_name = "rank";
				break;
			case ExpressionType::WINDOW_RANK_DENSE:
				function_name = "dense_rank";
				break;
			case ExpressionType::WINDOW_PERCENT_RANK:
				function_name = "percent_rank";
				break;
			case ExpressionType::WINDOW_CUME_DIST:
				function_name = "cume_dist";
				break;
			case ExpressionType::WINDOW_NTILE:
				function_name = "ntile";
				break;
			case ExpressionType::WINDOW_LAG:
				function_name = "lag";
				break;
			case ExpressionType::WINDOW_LEAD:
				function_name = "lead";
				break;
			case ExpressionType::WINDOW_FIRST_VALUE:
				function_name = "first_value";
				break;
			case ExpressionType::WINDOW_LAST_VALUE:
				function_name = "last_value";
				break;
			case ExpressionType::WINDOW_NTH_VALUE:
				function_name = "nth_value";
				break;
			default:
				throw NotImplementedException("Unsupported window function type: " +
				                              ExpressionTypeToString(dwin_expr.GetExpressionType()));
			}
		}
		
		// Add function arguments. For LAG/LEAD the children also carry the offset and default.
		for (auto &darg : dwin_expr.GetChildren()) {
			auto s_arg = swin_func->add_arguments();
			args_types.emplace_back(DuckToSubstraitType(darg->GetReturnType()));
			TransformExpr(*darg, *s_arg->mutable_value());
		}
		
		if (dwin_expr.AggregateFunction()) {
			swin_func->set_function_reference(RegisterAggregateFunction(*dwin_expr.AggregateFunction(),
			                                                           dwin_expr.BindInfo().get(),
			                                                           *swin_func->mutable_arguments(), args_types));
		} else {
			swin_func->set_function_reference(RegisterFunction(RemapFunctionName(function_name), args_types));
		}
		
		// Set window frame bounds
		// Offset-free GROUPS frames use the same peer groups as RANGE.
		substrait::Expression_WindowFunction_BoundsType bounds_type;
		
		switch (dwin_expr.WindowStart()) {
		case WindowBoundary::CURRENT_ROW_ROWS:
		case WindowBoundary::EXPR_PRECEDING_ROWS:
		case WindowBoundary::EXPR_FOLLOWING_ROWS:
			bounds_type = substrait::Expression_WindowFunction_BoundsType_BOUNDS_TYPE_ROWS;
			break;
		case WindowBoundary::CURRENT_ROW_GROUPS:
		case WindowBoundary::CURRENT_ROW_RANGE:
		case WindowBoundary::EXPR_PRECEDING_RANGE:
		case WindowBoundary::EXPR_FOLLOWING_RANGE:
			bounds_type = substrait::Expression_WindowFunction_BoundsType_BOUNDS_TYPE_RANGE;
			break;
		default:
			// Check end boundary
			switch (dwin_expr.WindowEnd()) {
			case WindowBoundary::CURRENT_ROW_ROWS:
			case WindowBoundary::EXPR_PRECEDING_ROWS:
			case WindowBoundary::EXPR_FOLLOWING_ROWS:
				bounds_type = substrait::Expression_WindowFunction_BoundsType_BOUNDS_TYPE_ROWS;
				break;
			default:
				// Default to RANGE
				bounds_type = substrait::Expression_WindowFunction_BoundsType_BOUNDS_TYPE_RANGE;
				break;
			}
			break;
		}

		swin_func->set_bounds_type(bounds_type);

		// Helper function to transform window boundaries
		auto TransformWindowBoundary = [this, &dwin_expr](
			                               WindowBoundary boundary_type, const unique_ptr<Expression> &boundary_expr,
			                               substrait::Expression_WindowFunction_Bound *bound, bool is_start_bound) {
			switch (boundary_type) {
			case WindowBoundary::UNBOUNDED_PRECEDING:
			case WindowBoundary::UNBOUNDED_FOLLOWING:
				bound->mutable_unbounded();
				break;
			case WindowBoundary::CURRENT_ROW_ROWS:
			case WindowBoundary::CURRENT_ROW_RANGE:
			case WindowBoundary::CURRENT_ROW_GROUPS:
				bound->mutable_current_row();
				break;
			case WindowBoundary::EXPR_PRECEDING_GROUPS:
			case WindowBoundary::EXPR_FOLLOWING_GROUPS:
				throw NotImplementedException("GROUPS window frames with offsets are not supported");
			case WindowBoundary::EXPR_PRECEDING_ROWS:
			case WindowBoundary::EXPR_PRECEDING_RANGE:
			case WindowBoundary::EXPR_FOLLOWING_ROWS:
			case WindowBoundary::EXPR_FOLLOWING_RANGE: {
				bool preceding = boundary_type == WindowBoundary::EXPR_PRECEDING_ROWS ||
					             boundary_type == WindowBoundary::EXPR_PRECEDING_RANGE;
				bool is_range = boundary_type == WindowBoundary::EXPR_PRECEDING_RANGE ||
					            boundary_type == WindowBoundary::EXPR_FOLLOWING_RANGE;
				if (!boundary_expr) {
					throw InternalException("Window boundary expression missing for %s",
						                    preceding ? "PRECEDING" : "FOLLOWING");
				}
				auto unwrap_casts = [](Expression &expr) -> Expression & {
					auto result = &expr;
					while (BoundCastExpression::IsCast(*result)) {
						result = BoundCastExpression::ChildMutable(result->Cast<BoundFunctionExpression>()).get();
					}
					return *result;
				};
				auto offset_expr = boundary_expr.get();
				if (is_range) {
					// The binder rewrites RANGE offsets as ORDER +/- offset, reversing arithmetic for DESC.
					auto &boundary = unwrap_casts(*boundary_expr);
					if (dwin_expr.OrderBy().size() != 1 ||
						boundary.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
						throw NotImplementedException("Only constant offsets are supported for window bounds");
					}
					auto &arithmetic = boundary.Cast<BoundFunctionExpression>();
					auto &order = dwin_expr.OrderBy()[0];
					bool subtract = preceding == (order.type == OrderType::ASCENDING);
					if (arithmetic.Function().GetName() != (subtract ? "-" : "+") || arithmetic.GetChildren().size() != 2 ||
						!unwrap_casts(*arithmetic.GetChildren()[0]).Equals(unwrap_casts(*order.expression))) {
						throw NotImplementedException("Unsupported RANGE window boundary expression");
					}
					offset_expr = arithmetic.GetChildren()[1].get();
				}
				if (unwrap_casts(*offset_expr).GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					throw NotImplementedException("Only constant offsets are supported for window bounds");
				}
				// Evaluate only the constant offset and its casts, never the ORDER arithmetic.
				auto offset = ExpressionExecutor::EvaluateScalar(context, *offset_expr);
				if (offset.IsNull()) {
					throw NotImplementedException("NULL window bound offsets are not supported");
				}
				bool zero;
				if (is_range && offset.type().id() == LogicalTypeId::INTERVAL) {
					auto interval = offset.GetValue<interval_t>();
					if (interval.months != 0 && (interval.days != 0 || interval.micros != 0)) {
						throw NotImplementedException(
							"Mixed month and day-time intervals are not supported for RANGE window bounds");
					}
					if (interval < interval_t {}) {
						throw NotImplementedException("Negative window bound offsets are not supported");
					}
					zero = interval.months == 0 && interval.days == 0 && interval.micros == 0;
				} else {
					if (!is_range) {
						offset = Value::BIGINT(offset.GetValue<int64_t>());
					}
					if (!offset.type().IsNumeric()) {
						throw NotImplementedException(
							"Only numeric or interval offsets are supported for RANGE window bounds");
					}
					auto zero_value = Value::Numeric(offset.type(), 0);
					if (offset < zero_value) {
						throw NotImplementedException("Negative window bound offsets are not supported");
					}
					zero = offset == zero_value;
				}
				if (zero) {
					bound->mutable_current_row();
					break;
				}
				auto serialized_offset = make_uniq<substrait::Expression>();
				TransformConstant(offset, *serialized_offset);
				optional<Value> legacy_offset;
				bool has_legacy_offset = false;
				if (offset.type().IsNumeric()) {
					legacy_offset = offset.TryCastAs(context, LogicalType::BIGINT);
					if (legacy_offset) {
						auto restored_offset = legacy_offset->TryCastAs(context, offset.type());
						has_legacy_offset = restored_offset && offset == *restored_offset;
					}
				}
				if (preceding) {
					auto preceding_bound = bound->mutable_preceding();
					preceding_bound->set_allocated_offset_expr(serialized_offset.release());
					if (has_legacy_offset) {
						preceding_bound->set_offset(legacy_offset->GetValue<int64_t>());
					}
				} else {
					auto following_bound = bound->mutable_following();
					following_bound->set_allocated_offset_expr(serialized_offset.release());
					if (has_legacy_offset) {
						following_bound->set_offset(legacy_offset->GetValue<int64_t>());
					}
				}
				break;
			}
			default:
				// Default to UNBOUNDED PRECEDING for start, CURRENT ROW for end
				if (is_start_bound) {
					bound->mutable_unbounded();
				} else {
					bound->mutable_current_row();
				}
				break;
			}
		};

		// Transform start bound
		auto lower_bound = swin_func->mutable_lower_bound();
		TransformWindowBoundary(dwin_expr.WindowStart(), dwin_expr.StartExpr(), lower_bound, true);

		// Transform end bound
		auto upper_bound = swin_func->mutable_upper_bound();
		TransformWindowBoundary(dwin_expr.WindowEnd(), dwin_expr.EndExpr(), upper_bound, false);
		}

		// Update current_input to chain the next window relation
		current_input = res.release();
	}

	// Grouping interleaved specifications changes the order in which window columns are appended.
	if (window_specs.size() > 1) {
		current_input->mutable_window()->set_allocated_common(CreateOutputMapping(output_mapping));
	}
	return current_input;
}

int32_t GetTimestampPrecision(LogicalTypeId type) {
	switch (type) {
	case LogicalTypeId::TIMESTAMP_SEC:
		return 0;
	case LogicalTypeId::TIMESTAMP_MS:
		return 3;
	case LogicalTypeId::TIMESTAMP:
		return 6;
	case LogicalTypeId::TIMESTAMP_NS:
		return 9;
	default:
		throw InternalException("Only timestamp values can have a timestamp precision");
	}
}

substrait::Type DuckDBToSubstrait::DuckToSubstraitType(const LogicalType &type, BaseStatistics *column_statistics,
                                                       bool not_null) {
	substrait::Type s_type;
	substrait::Type_Nullability type_nullability;
	if (not_null) {
		type_nullability = substrait::Type_Nullability::Type_Nullability_NULLABILITY_REQUIRED;
	} else {
		type_nullability = substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		auto bool_type = s_type.mutable_bool_();
		bool_type->set_nullability(type_nullability);
		return s_type;
	}

	case LogicalTypeId::TINYINT: {
		auto integral_type = s_type.mutable_i8();
		integral_type->set_nullability(type_nullability);
		return s_type;
	}
		// Substrait ppl think unsigned types are not common, so we have to upcast
		// these beauties Which completely borks the optimization they are created
		// for
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::SMALLINT: {
		auto integral_type = s_type.mutable_i16();
		integral_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::INTEGER: {
		auto integral_type = s_type.mutable_i32();
		integral_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::BIGINT: {
		auto integral_type = s_type.mutable_i64();
		integral_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT: {
		// FIXME: Support for hugeint types?
		auto s_decimal = s_type.mutable_decimal();
		s_decimal->set_scale(0);
		s_decimal->set_precision(38);
		s_decimal->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::DATE: {
		auto date_type = s_type.mutable_date();
		date_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIME: {
		auto time_type = s_type.mutable_precision_timestamp();
		time_type->set_precision(6); // microseconds
		time_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC: {
		auto timestamp_type = s_type.mutable_precision_timestamp();
		timestamp_type->set_precision(GetTimestampPrecision(type.id()));
		timestamp_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto timestamp_type = s_type.mutable_precision_timestamp_tz();
		// Timestamp tz is always 'us'
		timestamp_type->set_precision(6);
		timestamp_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::INTERVAL: {
		auto interval_type = s_type.mutable_interval_day();
		interval_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::FLOAT: {
		auto float_type = s_type.mutable_fp32();
		float_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::DOUBLE: {
		auto double_type = s_type.mutable_fp64();
		double_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::DECIMAL: {
		auto decimal_type = s_type.mutable_decimal();
		decimal_type->set_nullability(type_nullability);
		decimal_type->set_precision(DecimalType::GetWidth(type));
		decimal_type->set_scale(DecimalType::GetScale(type));
		return s_type;
	}
	case LogicalTypeId::VARCHAR: {
		auto string_type = s_type.mutable_string();
		string_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::BLOB: {
		auto binary_type = s_type.mutable_binary();
		binary_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::UUID: {
		auto uuid_type = s_type.mutable_uuid();
		uuid_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::ENUM: {
		auto enum_type = s_type.mutable_user_defined();
		enum_type->set_nullability(type_nullability);
		return s_type;
	}
	case LogicalTypeId::TUPLE:
	case LogicalTypeId::STRUCT: {
		// Substrait structs are positional, so a TUPLE (an unnamed struct) is the same thing.
		auto struct_type = s_type.mutable_struct_();
		struct_type->set_nullability(type_nullability);
		// ok lets get the children of our struct
		auto children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			auto new_type = struct_type->add_types();
			*new_type = DuckToSubstraitType(child.second, column_statistics, not_null);
		}
		return s_type;
	}
	case LogicalTypeId::MAP: {
		auto map_type = s_type.mutable_map();
		map_type->set_nullability(type_nullability);

		auto key_type = MapType::KeyType(type);
		auto value_type = MapType::ValueType(type);

		*map_type->mutable_key() = DuckToSubstraitType(key_type, column_statistics, not_null);
		*map_type->mutable_value() = DuckToSubstraitType(value_type, column_statistics, not_null);

		return s_type;
	}
	case LogicalTypeId::LIST: {
		auto list_type = s_type.mutable_list();
		list_type->set_nullability(type_nullability);

		auto child_type = ListType::GetChildType(type);

		*list_type->mutable_type() = DuckToSubstraitType(child_type, column_statistics, not_null);

		return s_type;
	}
	default:
		throw NotImplementedException("Logical Type " + type.ToString() +
		                              " not implemented as Substrait Schema Result.");
	}
}

static unique_ptr<BaseStatistics> GetColumnStatistics(ClientContext &context, const BaseTableFunction &function,
                                                      const FunctionData *bind_data, idx_t col_idx) {
	if (function.statistics_extended) {
		TableFunctionGetStatisticsInput input(bind_data, ColumnIndex(col_idx));
		return function.statistics_extended(context, input);
	}
	if (function.statistics) {
		return function.statistics(context, bind_data, col_idx);
	}
	return nullptr;
}

set<idx_t> GetNotNullConstraintCol(const TableCatalogEntry &tbl) {
	set<idx_t> not_null;
	for (auto &constraint : tbl.GetConstraints()) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null_constrait = constraint->Cast<NotNullConstraint>();
			not_null.insert(not_null_constrait.index.index);
		}
	}
	return not_null;
}

void DuckDBToSubstrait::TransformTableScanToSubstrait(LogicalGet &dget, substrait::ReadRel *sget,
                                                      TableCatalogEntry &table) const {
	sget->mutable_named_table()->add_names(table.name.GetIdentifierName());
	auto base_schema = make_uniq<substrait::NamedStruct>();
	auto type_info = make_uniq<substrait::Type_Struct>();
	type_info->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
	auto not_null_constraint = GetNotNullConstraintCol(table);
	for (idx_t i = 0; i < dget.names.size(); i++) {
		auto cur_type = dget.returned_types[i];
		base_schema->add_names(dget.names[i].GetIdentifierName());
		auto depth_names = DepthFirstNames(cur_type);
		for (auto &name : depth_names) {
			base_schema->add_names(name);
		}
		auto column_statistics = GetColumnStatistics(context, dget.function, dget.bind_data.get(), i);
		bool not_null = not_null_constraint.find(i) != not_null_constraint.end();
		auto new_type = type_info->add_types();
		*new_type = DuckToSubstraitType(cur_type, column_statistics.get(), not_null);
	}
	base_schema->set_allocated_struct_(type_info.release());
	sget->set_allocated_base_schema(base_schema.release());
}

void DuckDBToSubstrait::TransformParquetScanToSubstrait(LogicalGet &dget, substrait::ReadRel *sget, BindInfo &bind_info,
                                                        const FunctionData &bind_data) const {
	auto files_path = bind_info.GetOptionList<string>("file_path");
	for (auto &file_path : files_path) {
		auto parquet_item = sget->mutable_local_files()->add_items();
		// FIXME: should this be uri or file ogw
		parquet_item->set_uri_file(file_path);
		parquet_item->mutable_parquet();
	}

	auto base_schema = make_uniq<substrait::NamedStruct>();
	auto type_info = make_uniq<substrait::Type_Struct>();
	type_info->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
	for (idx_t i = 0; i < dget.names.size(); i++) {
		auto cur_type = dget.returned_types[i];
		base_schema->add_names(dget.names[i].GetIdentifierName());
		auto depth_names = DepthFirstNames(cur_type);
		for (auto &name : depth_names) {
			base_schema->add_names(name);
		}
		auto column_statistics = GetColumnStatistics(context, dget.function, &bind_data, i);
		auto new_type = type_info->add_types();
		*new_type = DuckToSubstraitType(cur_type, column_statistics.get(), false);
	}
	base_schema->set_allocated_struct_(type_info.release());
	sget->set_allocated_base_schema(base_schema.release());
}

substrait::Rel *DuckDBToSubstrait::TransformDummyScan() {
	// I just have to turn the dummy scan to emit one garbage row, the projection will take care of the rest
	auto get_rel = make_uniq<substrait::Rel>();
	auto sget = get_rel->mutable_read();
	auto virtual_table = sget->mutable_virtual_table();

	// Add a dummy value to emit one row
	auto dummy_struct = virtual_table->add_expressions();
	auto dummy_field = dummy_struct->add_fields();
	dummy_field->mutable_literal()->set_i32(42);
	return get_rel.release();
}

substrait::Rel *DuckDBToSubstrait::TransformEmptyResult(LogicalOperator &dop) {
	// Create an empty virtual table to represent an empty result
	// An empty virtual table (no rows) naturally represents an empty result
	auto get_rel = make_uniq<substrait::Rel>();
	auto sget = get_rel->mutable_read();
	sget->mutable_virtual_table();
	// Don't add any expressions - this creates an empty virtual table with no rows

	// Add base_schema to preserve the schema information
	auto &empty_result = dop.Cast<LogicalEmptyResult>();
	auto base_schema = make_uniq<substrait::NamedStruct>();
	auto type_info = make_uniq<substrait::Type_Struct>();
	type_info->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
	
	for (idx_t i = 0; i < empty_result.return_types.size(); i++) {
		auto cur_type = empty_result.return_types[i];
		// Use generic column names since LogicalEmptyResult doesn't have column names
		base_schema->add_names("col" + std::to_string(i));
		auto depth_names = DepthFirstNames(cur_type);
		for (auto &name : depth_names) {
			base_schema->add_names(name);
		}
		auto new_type = type_info->add_types();
		*new_type = DuckToSubstraitType(cur_type, nullptr, false);
	}
	base_schema->set_allocated_struct_(type_info.release());
	sget->set_allocated_base_schema(base_schema.release());

	return get_rel.release();
}

substrait::Rel *DuckDBToSubstrait::TransformGet(LogicalOperator &dop) {
	auto get_rel = make_uniq<substrait::Rel>();
	auto &dget = dop.Cast<LogicalGet>();

	// A scan of a DuckDB table no longer describes itself through get_bind_info; the table is on the
	// operator. Other scans (parquet) still do, and the bound function leaves that callback on the
	// definition it was bound from.
	// Attached catalogs (postgres, mysql, ...) also return a table entry, but with their own bind data.
	auto scanned_table = dget.GetTable();
	bool duck_table_scan = scanned_table && scanned_table->IsDuckTable();
	BindInfo bind_info(duck_table_scan ? ScanType::TABLE : ScanType::EXTERNAL);
	if (!duck_table_scan) {
		auto &function_definition = *dget.function.GetDefinition();
		if (!function_definition.get_bind_info) {
			throw NotImplementedException("Scans of table function %s cannot be exported: it has no bind info",
			                              dget.function.GetName().GetIdentifierName());
		}
		TableFunctionGetBindInfoInput bind_input(context, dget.bind_data.get());
		bind_info = function_definition.get_bind_info(bind_input);
		// DuckDB 2.0 builds its parquet scan on a single-file reader. The multi-file function around it
		// describes itself as an external scan and leaves the parquet-ness to the per-file reader.
		if (bind_info.type == ScanType::EXTERNAL &&
		    (dget.function.GetName() == "read_parquet" || dget.function.GetName() == "parquet_scan")) {
			bind_info.type = ScanType::PARQUET;
		}
	}
	auto sget = get_rel->mutable_read();

	if (dget.table_filters.HasFilters() || dget.table_filters.HasMultiColumnFilters()) {
		// Pushdown filter
		// Table filters name scan columns by their position among the columns the scan reads; the
		// ReadRel's base schema is the whole table, so translate to table column indexes.
		const auto &table_filters = dget.table_filters;
		auto &column_ids = dget.GetColumnIds();
		vector<substrait::Expression *> filters;
		for (auto &in : table_filters) {
			// The column may reach into a struct field (a pushed-down extract); the filter tests that field.
			auto &column = column_ids[in.GetIndex()];
			auto field_type = ScanColumnType(column, dget.returned_types);
			filter_columns.assign(1, column);
			filters.push_back(TransformFilter(column.GetPrimaryIndex(), field_type, in.Filter(), field_type));
			filter_columns.clear();
		}
		for (auto &multi_column_filter : table_filters.GetMultiColumnFilters()) {
			filters.push_back(TransformMultiColumnFilter(*multi_column_filter, column_ids));
		}
		auto filter = CreateConjunction(filters, [](substrait::Expression *in) { return in; });
		if (filter) {
			sget->set_allocated_filter(filter);
		}
	}

	// Collect the scan's output columns in output order
	auto &column_ids = dget.GetColumnIds();
	vector<const ColumnIndex *> output_columns;
	if (!dget.projection_ids.empty()) {
		for (auto proj_idx : dget.projection_ids) {
			output_columns.push_back(&column_ids[proj_idx]);
		}
	} else {
		for (auto &column_id : column_ids) {
			output_columns.push_back(&column_id);
		}
	}

	bool has_pushdown_extract = false;
	for (auto column : output_columns) {
		if (column->IsPushdownExtract()) {
			has_pushdown_extract = true;
			break;
		}
	}

	// For pushdown extract: base columns to read and each output column's position within them
	vector<idx_t> read_columns;
	vector<idx_t> output_positions;
	if (has_pushdown_extract) {
		// The scan extracts struct fields and emits them directly. Substrait reads
		// cannot express that, so read the full columns and wrap the read in a
		// projection that performs the extraction.
		for (auto column : output_columns) {
			if (column->IsRowIdColumn()) {
				throw NotImplementedException("Row id columns combined with struct extract pushdown are not "
				                              "supported in the to_substrait function");
			}
		}
		// Read each referenced base column once, in order of first occurrence
		auto read_position = [&](idx_t primary_idx) -> idx_t {
			for (idx_t pos = 0; pos < read_columns.size(); pos++) {
				if (read_columns[pos] == primary_idx) {
					return pos;
				}
			}
			read_columns.push_back(primary_idx);
			return read_columns.size() - 1;
		};
		for (auto column : output_columns) {
			output_positions.push_back(read_position(column->GetPrimaryIndex()));
		}

		auto projection = make_uniq<substrait::Expression_MaskExpression>();
		projection->set_maintain_singular_struct(true);
		auto select = projection->mutable_select();
		for (auto col_idx : read_columns) {
			auto struct_item = select->add_struct_items();
			struct_item->set_field(static_cast<int32_t>(col_idx));
		}
		sget->set_allocated_projection(projection.release());
	} else if (!output_columns.empty()) {
		auto projection = make_uniq<substrait::Expression_MaskExpression>();
		// fixme: whatever this means
		projection->set_maintain_singular_struct(true);
		auto select = projection->mutable_select();
		for (auto column : output_columns) {
			// The two branches below are intentionally asymmetric for row id columns.
			// When the scan has a pushed-down projection (projection_ids) -- e.g. under
			// late materialization, where a TopN/Limit scan reads the sort column plus a
			// row id and re-fetches the rest by row id -- the mask must contain one struct
			// item per projected column to stay positionally aligned with the projection.
			// A row id is not a real base-schema column, so it gets a placeholder item
			// with the default field (0); dropping it misaligns downstream references and
			// breaks round-tripping (see test_substrait_topn / test_limit). Without a
			// projection the mask is built only from the real columns, so row ids are
			// skipped entirely.
			if (!dget.projection_ids.empty()) {
				auto struct_item = select->add_struct_items();
				if (!column->IsRowIdColumn()) {
					struct_item->set_field(static_cast<int32_t>(column->GetPrimaryIndex()));
				}
			} else if (!column->IsRowIdColumn()) {
				auto struct_item = select->add_struct_items();
				struct_item->set_field(static_cast<int32_t>(column->GetPrimaryIndex()));
			}
		}
		if (select->struct_items_size() != 0) {
			sget->set_allocated_projection(projection.release());
		}
	}

	// Add Table Schema
	switch (bind_info.type) {
	case ScanType::TABLE:
		TransformTableScanToSubstrait(dget, sget, *scanned_table);
		break;
	case ScanType::PARQUET:
		TransformParquetScanToSubstrait(dget, sget, bind_info, *dget.bind_data);
		break;
	default: {
		string options;
		for (auto &option : bind_info.options) {
			options += (options.empty() ? "" : ", ") + option.first;
		}
		throw NotImplementedException("Scan type %s of table function %s (options: %s) is not yet implemented for the "
		                              "to_substrait function",
		                              EnumUtil::ToString(bind_info.type), dget.function.GetName().GetIdentifierName(),
		                              options);
	}
	}

	if (has_pushdown_extract) {
		// Resolve the struct field path for each output column first, so any
		// unsupported case throws before we allocate protobuf objects (which
		// would otherwise leak on the exception path).
		vector<vector<int32_t>> extract_paths;
		for (auto column : output_columns) {
			vector<int32_t> path;
			auto current = column;
			while (current->HasChildren()) {
				D_ASSERT(current->ChildIndexCount() == 1);
				auto &child = current->GetChildIndex(0);
				if (!child.HasPrimaryIndex()) {
					throw NotImplementedException("Field-name based struct extract pushdown is not supported in "
					                              "the to_substrait function");
				}
				path.push_back(static_cast<int32_t>(child.GetPrimaryIndex()));
				current = &child;
			}
			extract_paths.push_back(std::move(path));
		}

		// Wrap the read in a projection performing the struct extraction the scan
		// would have done, emitting only the extracted values.
		auto proj_rel = make_uniq<substrait::Rel>();
		auto sproj = proj_rel->mutable_project();
		sproj->set_allocated_input(get_rel.release());
		vector<int32_t> output_mapping;
		auto read_column_count = static_cast<int32_t>(read_columns.size());
		for (idx_t i = 0; i < output_columns.size(); i++) {
			auto selection = sproj->add_expressions()->mutable_selection();
			selection->mutable_root_reference();
			auto segment = selection->mutable_direct_reference()->mutable_struct_field();
			segment->set_field(static_cast<int32_t>(output_positions[i]));
			for (auto field : extract_paths[i]) {
				segment = segment->mutable_child()->mutable_struct_field();
				segment->set_field(field);
			}
			output_mapping.push_back(read_column_count + static_cast<int32_t>(i));
		}
		sproj->set_allocated_common(CreateOutputMapping(output_mapping));
		return proj_rel.release();
	}

	return get_rel.release();
}

substrait::Rel *DuckDBToSubstrait::TransformExpressionGet(LogicalOperator &dop) {
	auto get_rel = make_uniq<substrait::Rel>();
	auto &dget = dop.Cast<LogicalExpressionGet>();

	auto sget = get_rel->mutable_read();
	auto virtual_table = sget->mutable_virtual_table();

	for (auto &row : dget.expressions) {
		auto row_item = virtual_table->add_expressions();
		for (auto &expr : row) {
			TransformExpr(*expr, *row_item->add_fields());
		}
	}
	return get_rel.release();
}

substrait::Rel *DuckDBToSubstrait::TransformCrossProduct(LogicalOperator &dop) {
	auto rel = make_uniq<substrait::Rel>();
	auto sub_cross_prod = rel->mutable_cross();
	auto &djoin = dop.Cast<LogicalCrossProduct>();
	sub_cross_prod->set_allocated_left(TransformOp(*dop.children[0]));
	sub_cross_prod->set_allocated_right(TransformOp(*dop.children[1]));
	auto bindings = djoin.GetColumnBindings();
	return rel.release();
}

substrait::Rel *DuckDBToSubstrait::TransformUnion(LogicalOperator &dop) {
	if (dop.children.size() == 1) {
		// The optimizer folded the other inputs away; what is left of the union is this input.
		return TransformOp(*dop.children[0]);
	}
	auto rel = make_uniq<substrait::Rel>();

	auto set_op = rel->mutable_set();
	auto &dunion = dop.Cast<LogicalSetOperation>();
	D_ASSERT(dunion.type == LogicalOperatorType::LOGICAL_UNION);

	set_op->set_op(substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_UNION_ALL);
	auto inputs = set_op->mutable_inputs();

	// Set operations can have more than two children
	for (auto &child : dop.children) {
		inputs->AddAllocated(TransformOp(*child));
	}
	auto bindings = dunion.GetColumnBindings();
	return rel.release();
}

substrait::Rel *DuckDBToSubstrait::CreateSetOperation(LogicalOperator &child_op,
                                                       substrait::SetRel_SetOp set_op_type) {
	if (child_op.children.size() == 1) {
		// The optimizer folded the other input away; what is left of the set operation is this input.
		return TransformOp(*child_op.children[0]);
	}
	auto rel = make_uniq<substrait::Rel>();
	auto set_op = rel->mutable_set();
	set_op->set_op(set_op_type);
	auto &set_operation = child_op.Cast<LogicalSetOperation>();
	auto inputs = set_op->mutable_inputs();
	inputs->AddAllocated(TransformOp(*set_operation.children[0]));
	inputs->AddAllocated(TransformOp(*set_operation.children[1]));
	return rel.release();
}

substrait::Rel *DuckDBToSubstrait::TransformDistinct(LogicalOperator &dop) {
	D_ASSERT(dop.children.size() == 1);
	auto &child_op = dop.children[0];

	// Check if this is a DISTINCT used with set operations (EXCEPT/INTERSECT)
	// or a standalone DISTINCT operation
	switch (child_op->type) {
	case LogicalOperatorType::LOGICAL_EXCEPT:
		// DISTINCT with EXCEPT - use SetRel
		return CreateSetOperation(*child_op, substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_MINUS_PRIMARY);
	case LogicalOperatorType::LOGICAL_INTERSECT:
		// DISTINCT with INTERSECT - use SetRel
		return CreateSetOperation(*child_op, substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_INTERSECTION_PRIMARY);
	default: {
		// Standalone DISTINCT operation - use AggregateRel with grouping but no measures
		// This handles cases like: SELECT DISTINCT col1, col2 FROM table
		auto rel = make_uniq<substrait::Rel>();
		auto saggr = rel->mutable_aggregate();
		
		// Set the input relation
		saggr->set_allocated_input(TransformOp(*child_op));
		
		// Get the column bindings from the DISTINCT operator
		auto bindings = dop.GetColumnBindings();
		
		// Add all columns as grouping expressions
		// In a standalone DISTINCT, all output columns become grouping keys
		for (idx_t i = 0; i < bindings.size(); i++) {
			auto grouping_expr = saggr->add_grouping_expressions();
			auto field_ref = grouping_expr->mutable_selection()->mutable_direct_reference()->mutable_struct_field();
			field_ref->set_field(i);
		}
		
		// Create a single grouping set with all columns
		auto sgrp = saggr->add_groupings();
		for (idx_t i = 0; i < bindings.size(); i++) {
			sgrp->add_expression_references(i);
		}
		
		// No measures needed for DISTINCT - it's just grouping
		return rel.release();
	}
	}
}

substrait::Rel *DuckDBToSubstrait::TransformExcept(LogicalOperator &dop) {
	auto rel = make_uniq<substrait::Rel>();
	auto set_op = rel->mutable_set();
	set_op->set_op(substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_MINUS_PRIMARY);
	auto &set_operation = dop.Cast<LogicalSetOperation>();
	auto inputs = set_op->mutable_inputs();
	// MINUS_PRIMARY subtracts all secondary inputs from the primary input,
	// matching DuckDB's semantics for a multi-child EXCEPT
	for (auto &child : set_operation.children) {
		inputs->AddAllocated(TransformOp(*child));
	}
	auto bindings = dop.GetColumnBindings();
	return rel.release();
}

substrait::Rel *DuckDBToSubstrait::TransformIntersect(LogicalOperator &dop) {
	auto &set_operation = dop.Cast<LogicalSetOperation>();
	if (set_operation.children.size() != 2) {
		// DuckDB's multi-child INTERSECT semantics (present in every child) do not
		// match INTERSECTION_PRIMARY (present in any secondary input). Check before
		// allocating so we don't leak on the exception path.
		throw NotImplementedException("INTERSECT with more than two children is not yet supported in the "
		                              "to_substrait function");
	}
	auto rel = make_uniq<substrait::Rel>();
	auto set_op = rel->mutable_set();
	set_op->set_op(substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_INTERSECTION_PRIMARY);
	auto inputs = set_op->mutable_inputs();
	inputs->AddAllocated(TransformOp(*set_operation.children[0]));
	inputs->AddAllocated(TransformOp(*set_operation.children[1]));
	auto bindings = dop.GetColumnBindings();
	return rel.release();
}

substrait::WriteRel_CreateMode DuckDBToSubstrait::TransformOnCreateConflict(OnCreateConflict on_conflict) {
	switch (on_conflict) {
	case OnCreateConflict::ERROR_ON_CONFLICT:
		return substrait::WriteRel::CREATE_MODE_ERROR_IF_EXISTS;
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		return substrait::WriteRel::CREATE_MODE_IGNORE_IF_EXISTS;
	case OnCreateConflict::REPLACE_ON_CONFLICT:
		return substrait::WriteRel::CREATE_MODE_REPLACE_IF_EXISTS;
	default:
		throw NotImplementedException("Unsupported OnCreateConflict type " + EnumUtil::ToString(on_conflict));
	}
}

substrait::Rel *DuckDBToSubstrait::TransformCreateTable(LogicalOperator &dop) {
	auto rel = make_uniq<substrait::Rel>();
	auto &create_table = dop.Cast<LogicalCreateTable>();
	auto &create_info = create_table.info.get()->Base();
	if (create_table.children.size() != 1) {
		if (create_table.children.size() == 0) {
			throw NotImplementedException("Create table without children not implemented");
		}
		throw InternalException("Create table with more than one child is not supported");
	}

	auto schema = make_uniq<substrait::NamedStruct>();
	auto type_info = make_uniq<substrait::Type_Struct>();
	for (auto &name : create_info.columns.GetColumnNames()) {
		schema->add_names(name);
	}
	for (auto &col_type : create_info.columns.GetColumnTypes()) {
		auto s_type = DuckToSubstraitType(col_type, nullptr, false);
		*type_info->add_types() = s_type;
	}
	schema->set_allocated_struct_(type_info.release());

	// This is CreateTableAsSelect
	substrait::Rel *input = TransformOp(*create_table.children[0]);
	auto write = rel->mutable_write();
	write->set_allocated_table_schema(schema.release());
	write->set_allocated_input(input);
	write->set_op(substrait::WriteRel::WriteOp::WriteRel_WriteOp_WRITE_OP_CTAS);
	auto named_table = write->mutable_named_table();
	named_table->add_names(create_info.GetQualifiedName().Schema().GetIdentifierName());
	named_table->add_names(create_info.GetTableName().GetIdentifierName());
	write->set_create_mode(TransformOnCreateConflict(create_info.on_conflict));

	return rel.release();
}

void DuckDBToSubstrait::SetTableSchema(const TableCatalogEntry &table, substrait::NamedStruct *schema) {
	for (auto &name : table.GetColumns().GetColumnNames()) {
		schema->add_names(name);
	}
	auto type_info = make_uniq<substrait::Type_Struct>();
	type_info->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
	for (auto &col_type : table.GetColumns().GetColumnTypes()) {
		auto s_type = DuckToSubstraitType(col_type, nullptr, false);
		*type_info->add_types() = s_type;
	}
	schema->set_allocated_struct_(type_info.release());
}

void DuckDBToSubstrait::SetNamedTable(const TableCatalogEntry &table, substrait::WriteRel *writeRel) {
	auto named_table = writeRel->mutable_named_table();
	named_table->add_names(table.schema.name.GetIdentifierName());
	named_table->add_names(table.name.GetIdentifierName());
}

substrait::Rel *DuckDBToSubstrait::TransformInsertTable(LogicalOperator &dop) {
	auto rel = make_uniq<substrait::Rel>();
	auto &insert_table = dop.Cast<LogicalInsert>();
	if (insert_table.children.size() != 1) {
		throw InternalException("insert table expected one child, found " + to_string(insert_table.children.size()));
	}

	auto writeRel = rel->mutable_write();
	writeRel->set_op(substrait::WriteRel::WriteOp::WriteRel_WriteOp_WRITE_OP_INSERT);
	writeRel->set_output(substrait::WriteRel::OUTPUT_MODE_NO_OUTPUT);

	SetNamedTable(insert_table.table, writeRel);
	auto schema = make_uniq<substrait::NamedStruct>();
	SetTableSchema(insert_table.table, schema.get());
	writeRel->set_allocated_table_schema(schema.release());

	substrait::Rel *input = TransformOp(*insert_table.children[0]);
	writeRel->set_allocated_input(input);
	return rel.release();
}

substrait::Rel *DuckDBToSubstrait::TransformDeleteTable(LogicalOperator &dop) {
	auto rel = make_uniq<substrait::Rel>();
	auto &logical_delete = dop.Cast<LogicalDelete>();
	auto &table = logical_delete.table;
	if (logical_delete.children.size() != 1) {
		throw InternalException("Delete table expected one child, found " + to_string(logical_delete.children.size()));
	}

	auto writeRel = rel->mutable_write();
	writeRel->set_op(substrait::WriteRel::WriteOp::WriteRel_WriteOp_WRITE_OP_DELETE);
	writeRel->set_output(substrait::WriteRel::OUTPUT_MODE_NO_OUTPUT);

	auto named_table = writeRel->mutable_named_table();
	named_table->add_names(table.schema.catalog.GetName().GetIdentifierName());
	named_table->add_names(table.schema.name.GetIdentifierName());
	named_table->add_names(table.name.GetIdentifierName());

	SetNamedTable(logical_delete.table, writeRel);
	auto schema = make_uniq<substrait::NamedStruct>();
	SetTableSchema(logical_delete.table, schema.get());
	writeRel->set_allocated_table_schema(schema.release());

	substrait::Rel *input = TransformOp(*logical_delete.children[0]);
	writeRel->set_allocated_input(input);
	return rel.release();
}

substrait::Rel *DuckDBToSubstrait::TransformCTERef(LogicalOperator &dop) {
	auto rel = make_uniq<substrait::Rel>();
	auto &cte_ref = dop.Cast<LogicalCTERef>();
	auto it = find(cte_indices.begin(), cte_indices.end(), cte_ref.cte_index);
	if (it == cte_indices.end()) {
		throw InternalException("CTE reference index not found: " + to_string(cte_ref.cte_index.index));
	}
	auto index = it - cte_indices.begin();
	auto ref_rel = rel->mutable_reference();
	ref_rel->set_subtree_ordinal(index);
	return rel.release();
}

vector<LogicalType>::size_type DuckDBToSubstrait::GetColumnCount(LogicalOperator &dop) {
	return dop.types.size();
}

substrait::RelCommon *DuckDBToSubstrait::GetRelCommon(substrait::Rel &rel) {
	switch (rel.rel_type_case()) {
	case substrait::Rel::RelTypeCase::kRead:
		return rel.mutable_read()->mutable_common();
	case substrait::Rel::RelTypeCase::kFilter:
		return rel.mutable_filter()->mutable_common();
	case substrait::Rel::RelTypeCase::kFetch:
		return rel.mutable_fetch()->mutable_common();
	case substrait::Rel::RelTypeCase::kAggregate:
		return rel.mutable_aggregate()->mutable_common();
	case substrait::Rel::RelTypeCase::kSort:
		return rel.mutable_sort()->mutable_common();
	case substrait::Rel::RelTypeCase::kJoin:
		return rel.mutable_join()->mutable_common();
	case substrait::Rel::RelTypeCase::kLateralJoin:
		// Defensive only: to_substrait.cpp never constructs a LateralJoinRel (see comment on
		// TransformOp's switch below) — DuckDB always flattens/decorrelates dependent joins during
		// planning, so LOGICAL_DEPENDENT_JOIN can never reach TransformOp. This case exists only
		// for interface completeness.
		return rel.mutable_lateral_join()->mutable_common();
	case substrait::Rel::RelTypeCase::kProject:
		return rel.mutable_project()->mutable_common();
	case substrait::Rel::RelTypeCase::kSet:
		return rel.mutable_set()->mutable_common();
	case substrait::Rel::RelTypeCase::kCross:
		return rel.mutable_cross()->mutable_common();
	case substrait::Rel::RelTypeCase::kWindow:
		return rel.mutable_window()->mutable_common();
	default:
		return nullptr;
	}
}

substrait::Rel *DuckDBToSubstrait::TransformOp(LogicalOperator &dop) {
	switch (dop.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		return TransformFilter(dop);
	case LogicalOperatorType::LOGICAL_TOP_N:
		return TransformTopN(dop);
	case LogicalOperatorType::LOGICAL_LIMIT:
		return TransformLimit(dop);
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return TransformOrderBy(dop);
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return TransformProjection(dop);
	// Note: there is intentionally no case for LogicalOperatorType::LOGICAL_DEPENDENT_JOIN here.
	// DuckDB's Planner::CreatePlan unconditionally runs FlattenDependentJoins::DecorrelateIndependent
	// (duckdb/src/planner/planner.cpp:98) BEFORE this extension's TransformPlan/TransformOp ever sees
	// the LogicalOperator tree (see substrait_extension.cpp's ExtractPlan, which only gates the
	// separate Optimizer::Optimize step on enable_optimizer, not planning/flattening). DuckDB's own
	// physical_plan_generator.cpp throws NotImplementedException if LOGICAL_DEPENDENT_JOIN ever
	// reaches physical plan generation, confirming it never survives past planning. A dependent join
	// arriving via TransformLateralJoinOp's consumer-side Relation construction goes through this
	// exact same Planner when later executed/re-extracted, so it too becomes a LOGICAL_DELIM_JOIN
	// (handled by TransformComparisonJoin below) by the time to_substrait ever runs. Consequently,
	// a LateralJoinRel-emitting function on the producer side would be unreachable dead code.
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return TransformComparisonJoin(dop);
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		// A duplicate eliminated join is a comparison join with extra bookkeeping for the
		// duplicate eliminated get scans it feeds; TransformComparisonJoin handles both.
		return TransformComparisonJoin(dop);
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		return TransformDuplicateEliminatedGet(dop);
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return TransformAggregateGroup(dop);
	case LogicalOperatorType::LOGICAL_WINDOW:
		return TransformWindow(dop);
	case LogicalOperatorType::LOGICAL_GET:
		return TransformGet(dop);
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		return TransformExpressionGet(dop);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return TransformCrossProduct(dop);
	case LogicalOperatorType::LOGICAL_UNION:
		return TransformUnion(dop);
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return TransformDistinct(dop);
	case LogicalOperatorType::LOGICAL_EXCEPT:
		return TransformExcept(dop);
	case LogicalOperatorType::LOGICAL_INTERSECT:
		return TransformIntersect(dop);
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		return TransformDummyScan();
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return TransformEmptyResult(dop);
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
		return TransformCreateTable(dop);
	case LogicalOperatorType::LOGICAL_INSERT:
		return TransformInsertTable(dop);
	case LogicalOperatorType::LOGICAL_DELETE:
		return TransformDeleteTable(dop);
	case LogicalOperatorType::LOGICAL_CTE_REF:
		return TransformCTERef(dop);
	default:
		throw NotImplementedException(LogicalOperatorToString(dop.type));
	}
}

static bool IsRowModificationOperator(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_DELETE:
	case LogicalOperatorType::LOGICAL_UPDATE:
		return true;
	default:
		return false;
	}
}

substrait::RelRoot *DuckDBToSubstrait::TransformRootOp(LogicalOperator &dop) {
	auto root_rel = make_uniq<substrait::RelRoot>();
	root_rel->set_allocated_input(TransformOp(dop));

	if (IsRowModificationOperator(dop)) {
		return root_rel.release();
	}

	if (dop.type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
		auto &create_table = dop.Cast<LogicalCreateTable>();
		auto &create_info = create_table.info.get()->Base();
		auto col_names = create_info.columns.GetColumnNames();
		auto col_types = create_info.columns.GetColumnTypes();
		for (idx_t i = 0; i < col_names.size(); i++) {
			root_rel->add_names(col_names[i]);
			for (auto &sub_name : DepthFirstNames(col_types[i])) {
				root_rel->add_names(sub_name);
			}
		}
	} else {
		D_ASSERT(plan_names.size() == dop.types.size());
		for (idx_t i = 0; i < plan_names.size(); i++) {
			root_rel->add_names(plan_names[i].GetIdentifierName());
			if (i < dop.types.size()) {
				for (auto &sub_name : DepthFirstNames(dop.types[i])) {
					root_rel->add_names(sub_name);
				}
			}
		}
	}

	return root_rel.release();
}

LogicalOperator *DuckDBToSubstrait::TransformCTE(LogicalMaterializedCTE &dop) {
	D_ASSERT(dop.children.size() == 2);
	cte_indices.push_back(dop.table_index);
	auto cte = dop.children[0].get();
	auto root = dop.children[1].get();
	plan.add_relations()->set_allocated_rel(TransformOp(*cte));
	if (root->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		return TransformCTE(root->Cast<LogicalMaterializedCTE>());
	}
	return root;
}

void DuckDBToSubstrait::TransformPlan(LogicalOperator &dop) {
	if (dop.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		// DuckDB now handles CTEs differently...
		// https://duckdb.org/2024/09/09/announcing-duckdb-110#automatic-cte-materialization
		auto &lmc = dop.Cast<LogicalMaterializedCTE>();
		auto root = TransformCTE(lmc);
		plan.add_relations()->set_allocated_root(TransformRootOp(*root));
	} else {
		plan.add_relations()->set_allocated_root(TransformRootOp(dop));
	}
	if (strict && !errors.empty()) {
		throw InvalidInputException("Strict Mode is set to true, and the following warnings/errors happened. \n" +
		                            errors);
	}
	auto version = plan.mutable_version();
	version->set_major_number(0);
	version->set_minor_number(102);
	version->set_patch_number(0);
	version->set_producer("DuckDB");
}
} // namespace duckdb
