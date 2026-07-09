//===----------------------------------------------------------------------===//
//                         DuckDB
//
// custom_extensions/substrait_custom_extensions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/hash.hpp"
#include <substrait/type.pb.h>
#include <unordered_map>

namespace duckdb {

struct SubstraitCustomFunction {
	SubstraitCustomFunction(string name_p, vector<string> arg_types_p)
	    : name(std::move(name_p)), arg_types(std::move(arg_types_p)) {};

	SubstraitCustomFunction() = default;
	bool operator==(const SubstraitCustomFunction &other) const {
		return name == other.name && arg_types == other.arg_types;
	}
	string GetName();
	string name;
	vector<string> arg_types;
};
//! Here we define function extensions
class SubstraitFunctionExtensions {
public:
	SubstraitFunctionExtensions(SubstraitCustomFunction function_p, string extension_path_p, string canonical_name_p)
	    : function(std::move(function_p)), extension_path(std::move(extension_path_p)),
	      canonical_name(std::move(canonical_name_p)) {};
	SubstraitFunctionExtensions(SubstraitCustomFunction function_p, string extension_path_p)
	    : function(std::move(function_p)), extension_path(std::move(extension_path_p)),
	      canonical_name(function.GetName()) {};
	SubstraitFunctionExtensions() = default;

	string GetExtensionURN() const;
	bool IsNative() const;
	const string &GetCanonicalName() const { return canonical_name; }

	SubstraitCustomFunction function;
	string extension_path;
	string canonical_name;
};

struct HashSubstraitFunctions {
	size_t operator()(SubstraitCustomFunction const &custom_function) const noexcept {
		// Hash Name
		auto hash_name = Hash(custom_function.name.c_str());
		// Hash Input Types
		auto &i_types = custom_function.arg_types;
		auto hash_type = Hash(i_types[0].c_str());
		for (idx_t i = 1; i < i_types.size(); i++) {
			hash_type = CombineHash(hash_type, Hash(i_types[i].c_str()));
		}
		// Combine name and inputs
		return CombineHash(hash_name, hash_type);
	}
};

struct HashSubstraitFunctionsName {
	size_t operator()(SubstraitCustomFunction const &custom_function) const noexcept {
		// Hash Name
		return Hash(custom_function.name.c_str());
	}
};

class SubstraitCustomFunctions {
public:
	SubstraitCustomFunctions();
	SubstraitFunctionExtensions Get(const string &name, const vector<substrait::Type> &types) const;
	static vector<string> GetTypes(const vector<substrait::Type> &types);
	void Initialize();

private:
	// For Regular Functions
	std::unordered_map<SubstraitCustomFunction, SubstraitFunctionExtensions, HashSubstraitFunctions> custom_functions;
	// For * Functions
	std::unordered_map<SubstraitCustomFunction, SubstraitFunctionExtensions, HashSubstraitFunctionsName>
	    any_arg_functions;
	// For ? Functions
	// When we have an argument ending with ? it means this argument can repeat many times
	std::unordered_map<SubstraitCustomFunction, SubstraitFunctionExtensions, HashSubstraitFunctions> many_arg_functions;

	void InsertCustomFunction(string name_p, vector<string> types_p, string file_path);
	void InsertAllFunctions(const vector<vector<string>> &all_types, vector<idx_t> &indices, int depth, string &name_p,
	                        string &file_path, const string &canonical_name);
};

} // namespace duckdb