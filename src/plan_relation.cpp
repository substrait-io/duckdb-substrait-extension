#include "plan_relation.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

static const char *PlanRelationTypeName(PlanRelationType type) {
	switch (type) {
	case PlanRelationType::TABLE:
		return "TABLE_RELATION";
	case PlanRelationType::VIEW:
		return "VIEW_RELATION";
	case PlanRelationType::TABLE_FUNCTION:
		return "TABLE_FUNCTION_RELATION";
	case PlanRelationType::VALUES:
		return "VALUE_LIST_RELATION";
	case PlanRelationType::PROJECTION:
		return "PROJECTION_RELATION";
	case PlanRelationType::FILTER:
		return "FILTER_RELATION";
	case PlanRelationType::AGGREGATE:
		return "AGGREGATE_RELATION";
	case PlanRelationType::JOIN:
		return "JOIN_RELATION";
	case PlanRelationType::CROSS_PRODUCT:
		return "CROSS_PRODUCT_RELATION";
	case PlanRelationType::LIMIT:
		return "LIMIT_RELATION";
	case PlanRelationType::ORDER:
		return "ORDER_RELATION";
	case PlanRelationType::SET_OPERATION:
		return "SET_OPERATION_RELATION";
	case PlanRelationType::ALIAS:
		return "SUBQUERY_RELATION";
	case PlanRelationType::CREATE_TABLE:
		return "CREATE_TABLE_RELATION";
	case PlanRelationType::INSERT:
		return "INSERT_RELATION";
	case PlanRelationType::DELETE:
		return "DELETE_RELATION";
	}
	return "INVALID_RELATION";
}

static string CreatePlanAlias(PlanRelationType type, const string &alias) {
	if (!alias.empty()) {
		return alias;
	}
	return StringUtil::Format("%s_%s", PlanRelationTypeName(type), StringUtil::GenerateRandomName());
}

static bool IsOverJoin(PlanRelation &child) {
	auto current = &child;
	while (current->InheritsColumnBindings()) {
		current = current->ChildRelation();
	}
	return current->type == PlanRelationType::JOIN;
}

static unique_ptr<QueryNode> SelectStarFrom(unique_ptr<TableRef> ref) {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = std::move(ref);
	return std::move(result);
}

// A projection or aggregate over a join goes into the SELECT of the join, so its expressions see both sides.
static unique_ptr<QueryNode> SelectFrom(PlanRelation &child) {
	if (IsOverJoin(child)) {
		return child.GetQueryNode();
	}
	auto select = make_uniq<SelectNode>();
	select->from_table = child.GetTableRef();
	return std::move(select);
}

PlanRelation::PlanRelation(shared_ptr<ClientContext> context_p, PlanRelationType type_p, const string &alias_p)
    : context(std::move(context_p)), type(type_p), alias(CreatePlanAlias(type_p, alias_p)) {
}

unique_ptr<TableRef> PlanRelation::GetTableRef() {
	auto select = make_uniq<SelectStatement>();
	select->node = GetQueryNode();
	return make_uniq<SubqueryRef>(std::move(select), GetAlias());
}

unique_ptr<SQLStatement> PlanRelation::GetStatement() {
	auto select = make_uniq<SelectStatement>();
	select->node = GetQueryNode();
	return std::move(select);
}

Identifier PlanRelation::GetAlias() {
	return Identifier(alias);
}

void PlanRelation::Bind(vector<ColumnDefinition> &result_columns) {
	// ClientContext::BindStatement plans after it binds in DuckDB 2.0.0, which costs time on every node and refuses
	// what only planning refuses, so the binder is called directly.
	context->RunFunctionInTransaction([&]() {
		auto binder = Binder::CreateBinder(*context);
		auto statement = GetStatement();
		auto bound = binder->Bind(*statement);
		for (idx_t i = 0; i < bound.names.size(); i++) {
			result_columns.emplace_back(bound.names[i], bound.types[i]);
		}
	});
}

shared_ptr<PlanRelation> PlanRelation::Alias(const string &alias_p) {
	return make_shared_ptr<PlanAlias>(shared_from_this(), alias_p);
}

shared_ptr<PlanRelation> PlanRelation::Filter(const string &expression) {
	auto expression_list = Parser(*context).ParseExpressionList(expression);
	if (expression_list.size() != 1) {
		throw ParserException("Expected a single expression as filter condition");
	}
	return make_shared_ptr<PlanFilter>(shared_from_this(), std::move(expression_list[0]));
}

shared_ptr<PlanRelation> PlanRelation::Project(vector<unique_ptr<ParsedExpression>> expressions,
                                               const vector<string> &aliases) {
	return make_shared_ptr<PlanProjection>(shared_from_this(), std::move(expressions), StringsToIdentifiers(aliases));
}

shared_ptr<PlanRelation> PlanRelation::CreateRel(const Identifier &schema_name, const Identifier &table_name,
                                                 bool temporary, OnCreateConflict on_conflict) {
	return make_shared_ptr<PlanCreateTable>(shared_from_this(), Identifier::InvalidCatalog(), schema_name, table_name,
	                                        temporary, on_conflict);
}

shared_ptr<PlanRelation> PlanRelation::InsertRel(const Identifier &schema_name, const Identifier &table_name) {
	return make_shared_ptr<PlanInsert>(shared_from_this(), Identifier::InvalidCatalog(), schema_name, table_name);
}

PlanTable::PlanTable(shared_ptr<ClientContext> context_p, unique_ptr<TableDescription> description_p)
    : PlanRelation(std::move(context_p), PlanRelationType::TABLE), description(std::move(description_p)) {
}

const vector<ColumnDefinition> &PlanTable::Columns() {
	return description->columns;
}

unique_ptr<QueryNode> PlanTable::GetQueryNode() {
	return SelectStarFrom(GetTableRef());
}

unique_ptr<TableRef> PlanTable::GetTableRef() {
	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->SetQualifiedName(description->qualified_name);
	return std::move(table_ref);
}

Identifier PlanTable::GetAlias() {
	return description->qualified_name.Name();
}

PlanView::PlanView(shared_ptr<ClientContext> context_p, Identifier schema_name_p, Identifier view_name_p)
    : PlanRelation(std::move(context_p), PlanRelationType::VIEW), schema_name(std::move(schema_name_p)),
      view_name(std::move(view_name_p)) {
	Bind(columns);
}

PlanView::PlanView(shared_ptr<ClientContext> context_p, unique_ptr<TableRef> ref, const string &view_name_p)
    : PlanRelation(std::move(context_p), PlanRelationType::VIEW), view_name(view_name_p),
      premade_tableref(std::move(ref)) {
	Bind(columns);
	premade_tableref->alias = Identifier(view_name_p);
}

const vector<ColumnDefinition> &PlanView::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanView::GetQueryNode() {
	return SelectStarFrom(GetTableRef());
}

unique_ptr<TableRef> PlanView::GetTableRef() {
	if (premade_tableref) {
		return premade_tableref->Copy();
	}
	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->SetQualifiedName(QualifiedName(table_ref->GetQualifiedName().Catalog(), schema_name, view_name));
	return std::move(table_ref);
}

Identifier PlanView::GetAlias() {
	return view_name;
}

PlanTableFunction::PlanTableFunction(shared_ptr<ClientContext> context_p, string name_p, vector<Value> parameters_p,
                                     named_parameter_map_t named_parameters_p)
    : PlanRelation(std::move(context_p), PlanRelationType::TABLE_FUNCTION), name(std::move(name_p)),
      parameters(std::move(parameters_p)), named_parameters(std::move(named_parameters_p)) {
	Bind(columns);
}

const vector<ColumnDefinition> &PlanTableFunction::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanTableFunction::GetQueryNode() {
	return SelectStarFrom(GetTableRef());
}

unique_ptr<TableRef> PlanTableFunction::GetTableRef() {
	vector<unique_ptr<ParsedExpression>> children;
	for (auto &parameter : parameters) {
		children.push_back(ConstantExpression::FromValue(parameter));
	}
	for (auto &parameter : named_parameters) {
		// The table function binder reads a named parameter as a comparison of a column of that name with its value.
		auto column_ref = make_uniq<ColumnRefExpression>(parameter.first);
		auto constant_value = ConstantExpression::FromValue(parameter.second);
		children.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(column_ref),
		                                                   std::move(constant_value)));
	}
	auto table_function = make_uniq<TableFunctionRef>();
	table_function->function = make_uniq<FunctionExpression>(Identifier(name), std::move(children));
	return std::move(table_function);
}

Identifier PlanTableFunction::GetAlias() {
	return Identifier(name);
}

PlanValues::PlanValues(shared_ptr<ClientContext> context_p, const vector<vector<Value>> &values, vector<string> names_p)
    : PlanRelation(std::move(context_p), PlanRelationType::VALUES), names(std::move(names_p)) {
	for (auto &row : values) {
		vector<unique_ptr<ParsedExpression>> row_expressions;
		for (auto &value : row) {
			row_expressions.push_back(ConstantExpression::FromValue(value));
		}
		expressions.push_back(std::move(row_expressions));
	}
	QueryResult::DeduplicateColumns(names);
	Bind(columns);
}

PlanValues::PlanValues(shared_ptr<ClientContext> context_p, vector<vector<unique_ptr<ParsedExpression>>> expressions_p,
                       vector<string> names_p)
    : PlanRelation(std::move(context_p), PlanRelationType::VALUES), expressions(std::move(expressions_p)) {
	D_ASSERT(!expressions.empty());
	if (names_p.empty()) {
		for (auto &expr : expressions[0]) {
			names_p.emplace_back(expr->GetName());
		}
	}
	names = std::move(names_p);
	QueryResult::DeduplicateColumns(names);
	Bind(columns);
}

const vector<ColumnDefinition> &PlanValues::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanValues::GetQueryNode() {
	return SelectStarFrom(GetTableRef());
}

unique_ptr<TableRef> PlanValues::GetTableRef() {
	auto table_ref = make_uniq<ExpressionListRef>();
	// Once bound, the reference carries the bound types, so a later bind in another connection casts to them.
	if (columns.empty()) {
		for (auto &name : names) {
			table_ref->expected_names.push_back(Identifier(name));
		}
	} else {
		for (auto &column : columns) {
			table_ref->expected_names.emplace_back(column.Name());
			table_ref->expected_types.push_back(column.Type());
		}
	}
	for (auto &row : expressions) {
		vector<unique_ptr<ParsedExpression>> copied_row;
		copied_row.reserve(row.size());
		for (auto &expr : row) {
			copied_row.push_back(expr->Copy());
		}
		table_ref->values.push_back(std::move(copied_row));
	}
	table_ref->alias = GetAlias();
	return std::move(table_ref);
}

Identifier PlanValues::GetAlias() {
	return Identifier("values");
}

PlanProjection::PlanProjection(shared_ptr<PlanRelation> child_p, vector<unique_ptr<ParsedExpression>> expressions_p,
                               vector<Identifier> aliases)
    : PlanRelation(child_p->context, PlanRelationType::PROJECTION), expressions(std::move(expressions_p)),
      child(std::move(child_p)) {
	if (!aliases.empty()) {
		if (aliases.size() != expressions.size()) {
			throw ParserException("Aliases list length must match expression list length!");
		}
		for (idx_t i = 0; i < aliases.size(); i++) {
			expressions[i]->SetAlias(aliases[i]);
		}
	}
	Bind(columns);
}

const vector<ColumnDefinition> &PlanProjection::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanProjection::GetQueryNode() {
	auto result = SelectFrom(*child);
	auto &select_node = result->Cast<SelectNode>();
	select_node.aggregate_handling = AggregateHandling::NO_AGGREGATES_ALLOWED;
	select_node.select_list.clear();
	for (auto &expr : expressions) {
		select_node.select_list.push_back(expr->Copy());
	}
	return result;
}

Identifier PlanProjection::GetAlias() {
	return child->GetAlias();
}

PlanFilter::PlanFilter(shared_ptr<PlanRelation> child_p, unique_ptr<ParsedExpression> condition_p)
    : PlanRelation(child_p->context, PlanRelationType::FILTER), condition(std::move(condition_p)),
      child(std::move(child_p)) {
	vector<ColumnDefinition> unused_columns;
	Bind(unused_columns);
}

// The bound output of the filter renames duplicate columns, and a lateral join resolves outer references by the
// names its left input reports, so the answer has to stay the child's.
const vector<ColumnDefinition> &PlanFilter::Columns() {
	return child->Columns();
}

unique_ptr<QueryNode> PlanFilter::GetQueryNode() {
	if (IsOverJoin(*child)) {
		auto child_node = child->GetQueryNode();
		auto &select_node = child_node->Cast<SelectNode>();
		if (!select_node.where_clause) {
			select_node.where_clause = condition->Copy();
		} else {
			select_node.where_clause = make_uniq<ConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, std::move(select_node.where_clause), condition->Copy());
		}
		return child_node;
	}
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = child->GetTableRef();
	result->where_clause = condition->Copy();
	return std::move(result);
}

Identifier PlanFilter::GetAlias() {
	return child->GetAlias();
}

PlanAggregate::PlanAggregate(shared_ptr<PlanRelation> child_p, vector<unique_ptr<ParsedExpression>> expressions_p,
                             GroupByNode groups_p)
    : PlanRelation(child_p->context, PlanRelationType::AGGREGATE), expressions(std::move(expressions_p)),
      groups(std::move(groups_p)), child(std::move(child_p)) {
	Bind(columns);
}

const vector<ColumnDefinition> &PlanAggregate::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanAggregate::GetQueryNode() {
	auto result = SelectFrom(*child);
	auto &select_node = result->Cast<SelectNode>();
	if (!groups.group_expressions.empty()) {
		select_node.aggregate_handling = AggregateHandling::STANDARD_HANDLING;
		select_node.groups = groups.Copy();
	} else {
		select_node.aggregate_handling = AggregateHandling::FORCE_AGGREGATES;
	}
	select_node.select_list.clear();
	for (auto &expr : expressions) {
		select_node.select_list.push_back(expr->Copy());
	}
	return result;
}

Identifier PlanAggregate::GetAlias() {
	return child->GetAlias();
}

PlanJoin::PlanJoin(shared_ptr<PlanRelation> left_p, shared_ptr<PlanRelation> right_p,
                   unique_ptr<ParsedExpression> condition_p, JoinType join_type_p, JoinRefType join_ref_type_p)
    : PlanRelation(left_p->context, PlanRelationType::JOIN), left(std::move(left_p)), right(std::move(right_p)),
      condition(std::move(condition_p)), join_type(join_type_p), join_ref_type(join_ref_type_p) {
	Bind(columns);
}

const vector<ColumnDefinition> &PlanJoin::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanJoin::GetQueryNode() {
	return SelectStarFrom(GetTableRef());
}

unique_ptr<TableRef> PlanJoin::GetTableRef() {
	auto join_ref = make_uniq<JoinRef>(join_ref_type);
	join_ref->left = left->GetTableRef();
	join_ref->right = right->GetTableRef();
	if (condition) {
		join_ref->condition = condition->Copy();
	}
	join_ref->type = join_type;
	return std::move(join_ref);
}

PlanCrossProduct::PlanCrossProduct(shared_ptr<PlanRelation> left_p, shared_ptr<PlanRelation> right_p)
    : PlanRelation(left_p->context, PlanRelationType::CROSS_PRODUCT), left(std::move(left_p)),
      right(std::move(right_p)) {
	Bind(columns);
}

const vector<ColumnDefinition> &PlanCrossProduct::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanCrossProduct::GetQueryNode() {
	return SelectStarFrom(GetTableRef());
}

unique_ptr<TableRef> PlanCrossProduct::GetTableRef() {
	auto cross_product_ref = make_uniq<JoinRef>(JoinRefType::CROSS);
	cross_product_ref->left = left->GetTableRef();
	cross_product_ref->right = right->GetTableRef();
	return std::move(cross_product_ref);
}

PlanLimit::PlanLimit(shared_ptr<PlanRelation> child_p, int64_t limit_p, int64_t offset_p)
    : PlanRelation(child_p->context, PlanRelationType::LIMIT), limit(limit_p), offset(offset_p),
      child(std::move(child_p)) {
}

const vector<ColumnDefinition> &PlanLimit::Columns() {
	return child->Columns();
}

unique_ptr<QueryNode> PlanLimit::GetQueryNode() {
	auto child_node = child->GetQueryNode();
	auto limit_node = make_uniq<LimitModifier>();
	if (limit >= 0) {
		limit_node->limit = ConstantExpression::Integer(limit);
	}
	if (offset > 0) {
		limit_node->offset = ConstantExpression::Integer(offset);
	}
	child_node->modifiers.push_back(std::move(limit_node));
	return child_node;
}

Identifier PlanLimit::GetAlias() {
	return child->GetAlias();
}

PlanOrder::PlanOrder(shared_ptr<PlanRelation> child_p, vector<OrderByNode> orders_p)
    : PlanRelation(child_p->context, PlanRelationType::ORDER), orders(std::move(orders_p)), child(std::move(child_p)) {
	Bind(columns);
}

const vector<ColumnDefinition> &PlanOrder::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanOrder::GetQueryNode() {
	auto select = make_uniq<SelectNode>();
	select->from_table = child->GetTableRef();
	select->select_list.push_back(make_uniq<StarExpression>());
	auto order_node = make_uniq<OrderModifier>();
	for (auto &order : orders) {
		order_node->orders.emplace_back(order.type, order.null_order, order.expression->Copy());
	}
	select->modifiers.push_back(std::move(order_node));
	return std::move(select);
}

Identifier PlanOrder::GetAlias() {
	return child->GetAlias();
}

PlanSetOp::PlanSetOp(shared_ptr<PlanRelation> left_p, shared_ptr<PlanRelation> right_p, SetOperationType setop_type_p,
                     bool setop_all_p)
    : PlanRelation(left_p->context, PlanRelationType::SET_OPERATION), left(std::move(left_p)),
      right(std::move(right_p)), setop_type(setop_type_p), setop_all(setop_all_p) {
	Bind(columns);
}

const vector<ColumnDefinition> &PlanSetOp::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanSetOp::GetQueryNode() {
	auto result = make_uniq<SetOperationNode>();
	if (!setop_all) {
		result->modifiers.push_back(make_uniq<DistinctModifier>());
	}
	result->children.push_back(left->GetQueryNode());
	result->children.push_back(right->GetQueryNode());
	result->setop_type = setop_type;
	result->setop_all = setop_all;
	return std::move(result);
}

Identifier PlanSetOp::GetAlias() {
	return left->GetAlias();
}

PlanAlias::PlanAlias(shared_ptr<PlanRelation> child_p, const string &alias_p)
    : PlanRelation(child_p->context, PlanRelationType::ALIAS, alias_p), child(std::move(child_p)) {
	vector<ColumnDefinition> unused_columns;
	Bind(unused_columns);
}

const vector<ColumnDefinition> &PlanAlias::Columns() {
	return child->Columns();
}

unique_ptr<QueryNode> PlanAlias::GetQueryNode() {
	return child->GetQueryNode();
}

PlanCreateTable::PlanCreateTable(shared_ptr<PlanRelation> child_p, Identifier catalog_name_p, Identifier schema_name_p,
                                 Identifier table_name_p, bool temporary_p, OnCreateConflict on_conflict_p)
    : PlanRelation(child_p->context, PlanRelationType::CREATE_TABLE), child(std::move(child_p)),
      catalog_name(std::move(catalog_name_p)), schema_name(std::move(schema_name_p)),
      table_name(std::move(table_name_p)), temporary(temporary_p), on_conflict(on_conflict_p) {
	Bind(columns);
}

const vector<ColumnDefinition> &PlanCreateTable::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanCreateTable::GetQueryNode() {
	throw InternalException("Cannot create a query node from a create table relation");
}

unique_ptr<SQLStatement> PlanCreateTable::GetStatement() {
	auto select = make_uniq<SelectStatement>();
	select->node = child->GetQueryNode();
	auto statement = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTableInfo>();
	info->SetQualifiedName(QualifiedName(catalog_name, schema_name, table_name));
	info->query = std::move(select);
	info->on_conflict = on_conflict;
	info->temporary = temporary;
	statement->info = std::move(info);
	return std::move(statement);
}

PlanInsert::PlanInsert(shared_ptr<PlanRelation> child_p, Identifier catalog_name_p, Identifier schema_name_p,
                       Identifier table_name_p)
    : PlanRelation(child_p->context, PlanRelationType::INSERT), child(std::move(child_p)),
      catalog_name(std::move(catalog_name_p)), schema_name(std::move(schema_name_p)),
      table_name(std::move(table_name_p)) {
	Bind(columns);
}

const vector<ColumnDefinition> &PlanInsert::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanInsert::GetQueryNode() {
	throw InternalException("Cannot create a query node from an insert relation");
}

unique_ptr<SQLStatement> PlanInsert::GetStatement() {
	auto statement = make_uniq<InsertStatement>();
	auto select = make_uniq<SelectStatement>();
	select->node = child->GetQueryNode();
	statement->node->qualified_name = QualifiedName(catalog_name, schema_name, table_name);
	statement->node->select_statement = std::move(select);
	return std::move(statement);
}

PlanDelete::PlanDelete(shared_ptr<ClientContext> context_p, unique_ptr<ParsedExpression> condition_p,
                       Identifier catalog_name_p, Identifier schema_name_p, Identifier table_name_p)
    : PlanRelation(std::move(context_p), PlanRelationType::DELETE), condition(std::move(condition_p)),
      catalog_name(std::move(catalog_name_p)), schema_name(std::move(schema_name_p)),
      table_name(std::move(table_name_p)) {
	Bind(columns);
}

const vector<ColumnDefinition> &PlanDelete::Columns() {
	return columns;
}

unique_ptr<QueryNode> PlanDelete::GetQueryNode() {
	throw InternalException("Cannot create a query node from a delete relation");
}

unique_ptr<SQLStatement> PlanDelete::GetStatement() {
	auto basetable = make_uniq<BaseTableRef>();
	basetable->SetQualifiedName(QualifiedName(catalog_name, schema_name, table_name));
	auto statement = make_uniq<DeleteStatement>();
	statement->node->condition = condition ? condition->Copy() : nullptr;
	statement->node->table = std::move(basetable);
	return std::move(statement);
}

} // namespace duckdb
