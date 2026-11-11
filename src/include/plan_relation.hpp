#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

enum class PlanRelationType : uint8_t {
	TABLE,
	VIEW,
	TABLE_FUNCTION,
	VALUES,
	PROJECTION,
	FILTER,
	AGGREGATE,
	JOIN,
	CROSS_PRODUCT,
	LIMIT,
	ORDER,
	SET_OPERATION,
	ALIAS,
	CREATE_TABLE,
	INSERT,
	DELETE
};

//! One node of a translated Substrait plan. A node that adds something to validate binds its query when it is
//! constructed, so a defect in any part of a plan is found, also in a part the final statement does not contain.
class PlanRelation : public enable_shared_from_this<PlanRelation> {
public:
	PlanRelation(shared_ptr<ClientContext> context, PlanRelationType type, const string &alias = "");
	virtual ~PlanRelation() = default;

	shared_ptr<ClientContext> context;
	PlanRelationType type;
	const string alias;

public:
	virtual const vector<ColumnDefinition> &Columns() = 0;
	virtual unique_ptr<QueryNode> GetQueryNode() = 0;
	virtual unique_ptr<TableRef> GetTableRef();
	virtual unique_ptr<SQLStatement> GetStatement();
	virtual Identifier GetAlias();
	virtual bool IsReadOnly() {
		return true;
	}
	virtual bool InheritsColumnBindings() {
		return false;
	}
	virtual PlanRelation *ChildRelation() {
		return nullptr;
	}

	shared_ptr<PlanRelation> Alias(const string &alias);
	shared_ptr<PlanRelation> Filter(const string &expression);
	shared_ptr<PlanRelation> Project(vector<unique_ptr<ParsedExpression>> expressions, const vector<string> &aliases);
	shared_ptr<PlanRelation> CreateRel(const Identifier &schema_name, const Identifier &table_name, bool temporary,
	                                   OnCreateConflict on_conflict);
	shared_ptr<PlanRelation> InsertRel(const Identifier &schema_name, const Identifier &table_name);

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

protected:
	void Bind(vector<ColumnDefinition> &result_columns);
};

class PlanTable : public PlanRelation {
public:
	PlanTable(shared_ptr<ClientContext> context, unique_ptr<TableDescription> description);
	unique_ptr<TableDescription> description;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;
	Identifier GetAlias() override;
};

class PlanView : public PlanRelation {
public:
	PlanView(shared_ptr<ClientContext> context, Identifier schema_name, Identifier view_name);
	PlanView(shared_ptr<ClientContext> context, unique_ptr<TableRef> ref, const string &view_name);
	Identifier schema_name;
	Identifier view_name;
	vector<ColumnDefinition> columns;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;
	Identifier GetAlias() override;

private:
	unique_ptr<TableRef> premade_tableref;
};

class PlanTableFunction : public PlanRelation {
public:
	PlanTableFunction(shared_ptr<ClientContext> context, string name, vector<Value> parameters,
	                  named_parameter_map_t named_parameters);
	string name;
	vector<Value> parameters;
	named_parameter_map_t named_parameters;
	vector<ColumnDefinition> columns;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;
	Identifier GetAlias() override;
};

class PlanValues : public PlanRelation {
public:
	PlanValues(shared_ptr<ClientContext> context, const vector<vector<Value>> &values, vector<string> names);
	PlanValues(shared_ptr<ClientContext> context, vector<vector<unique_ptr<ParsedExpression>>> expressions,
	           vector<string> names);
	vector<vector<unique_ptr<ParsedExpression>>> expressions;
	vector<string> names;
	vector<ColumnDefinition> columns;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;
	Identifier GetAlias() override;
};

class PlanProjection : public PlanRelation {
public:
	PlanProjection(shared_ptr<PlanRelation> child, vector<unique_ptr<ParsedExpression>> expressions,
	               vector<Identifier> aliases);
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<ColumnDefinition> columns;
	shared_ptr<PlanRelation> child;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	Identifier GetAlias() override;
};

class PlanFilter : public PlanRelation {
public:
	PlanFilter(shared_ptr<PlanRelation> child, unique_ptr<ParsedExpression> condition);
	unique_ptr<ParsedExpression> condition;
	shared_ptr<PlanRelation> child;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	Identifier GetAlias() override;
	bool InheritsColumnBindings() override {
		return true;
	}
	PlanRelation *ChildRelation() override {
		return child.get();
	}
};

class PlanAggregate : public PlanRelation {
public:
	PlanAggregate(shared_ptr<PlanRelation> child, vector<unique_ptr<ParsedExpression>> expressions, GroupByNode groups);
	vector<unique_ptr<ParsedExpression>> expressions;
	GroupByNode groups;
	vector<ColumnDefinition> columns;
	shared_ptr<PlanRelation> child;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	Identifier GetAlias() override;
};

class PlanJoin : public PlanRelation {
public:
	PlanJoin(shared_ptr<PlanRelation> left, shared_ptr<PlanRelation> right, unique_ptr<ParsedExpression> condition,
	         JoinType join_type, JoinRefType join_ref_type = JoinRefType::REGULAR);
	shared_ptr<PlanRelation> left;
	shared_ptr<PlanRelation> right;
	unique_ptr<ParsedExpression> condition;
	JoinType join_type;
	JoinRefType join_ref_type;
	vector<ColumnDefinition> columns;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;
};

class PlanCrossProduct : public PlanRelation {
public:
	PlanCrossProduct(shared_ptr<PlanRelation> left, shared_ptr<PlanRelation> right);
	shared_ptr<PlanRelation> left;
	shared_ptr<PlanRelation> right;
	vector<ColumnDefinition> columns;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;
};

class PlanLimit : public PlanRelation {
public:
	PlanLimit(shared_ptr<PlanRelation> child, int64_t limit, int64_t offset);
	int64_t limit;
	int64_t offset;
	shared_ptr<PlanRelation> child;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	Identifier GetAlias() override;
	bool InheritsColumnBindings() override {
		return true;
	}
	PlanRelation *ChildRelation() override {
		return child.get();
	}
};

class PlanOrder : public PlanRelation {
public:
	PlanOrder(shared_ptr<PlanRelation> child, vector<OrderByNode> orders);
	vector<OrderByNode> orders;
	shared_ptr<PlanRelation> child;
	vector<ColumnDefinition> columns;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	Identifier GetAlias() override;
	bool InheritsColumnBindings() override {
		return true;
	}
	PlanRelation *ChildRelation() override {
		return child.get();
	}
};

class PlanSetOp : public PlanRelation {
public:
	PlanSetOp(shared_ptr<PlanRelation> left, shared_ptr<PlanRelation> right, SetOperationType setop_type,
	          bool setop_all);
	shared_ptr<PlanRelation> left;
	shared_ptr<PlanRelation> right;
	SetOperationType setop_type;
	bool setop_all;
	vector<ColumnDefinition> columns;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	Identifier GetAlias() override;
};

class PlanAlias : public PlanRelation {
public:
	PlanAlias(shared_ptr<PlanRelation> child, const string &alias);
	shared_ptr<PlanRelation> child;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	bool InheritsColumnBindings() override {
		return child->InheritsColumnBindings();
	}
	PlanRelation *ChildRelation() override {
		return child->ChildRelation();
	}
};

class PlanCreateTable : public PlanRelation {
public:
	PlanCreateTable(shared_ptr<PlanRelation> child, Identifier catalog_name, Identifier schema_name,
	                Identifier table_name, bool temporary, OnCreateConflict on_conflict);
	shared_ptr<PlanRelation> child;
	Identifier catalog_name;
	Identifier schema_name;
	Identifier table_name;
	vector<ColumnDefinition> columns;
	bool temporary;
	OnCreateConflict on_conflict;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<SQLStatement> GetStatement() override;
	bool IsReadOnly() override {
		return false;
	}
};

class PlanInsert : public PlanRelation {
public:
	PlanInsert(shared_ptr<PlanRelation> child, Identifier catalog_name, Identifier schema_name, Identifier table_name);
	shared_ptr<PlanRelation> child;
	Identifier catalog_name;
	Identifier schema_name;
	Identifier table_name;
	vector<ColumnDefinition> columns;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<SQLStatement> GetStatement() override;
	bool IsReadOnly() override {
		return false;
	}
};

class PlanDelete : public PlanRelation {
public:
	PlanDelete(shared_ptr<ClientContext> context, unique_ptr<ParsedExpression> condition, Identifier catalog_name,
	           Identifier schema_name, Identifier table_name);
	unique_ptr<ParsedExpression> condition;
	Identifier catalog_name;
	Identifier schema_name;
	Identifier table_name;
	vector<ColumnDefinition> columns;

	const vector<ColumnDefinition> &Columns() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<SQLStatement> GetStatement() override;
	bool IsReadOnly() override {
		return false;
	}
};

} // namespace duckdb
