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
	//! The compound name of this declared impl (name + ':' + declared arg short names).
	string GetCompoundName() const;
	string name;
	vector<string> arg_types;
};
//! Here we define function extensions
class SubstraitFunctionExtensions {
public:
	SubstraitFunctionExtensions(SubstraitCustomFunction function_p, string extension_path_p)
	    : function(std::move(function_p)), extension_path(std::move(extension_path_p)) {};
	SubstraitFunctionExtensions() = default;

	//! The name to declare this function under: the impl's compound name, or the bare
	//! function name when it resolved against no extension YAML.
	string GetName() const;
	string GetExtensionURN() const;
	bool IsNative() const;

	SubstraitCustomFunction function;
	string extension_path;
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

	void InsertCustomFunction(const string &name, const vector<string> &types, const string &file_path);
	void InsertAllFunctions(const vector<vector<string>> &all_types, const vector<string> &declared_types,
	                        vector<idx_t> &indices, int depth, const string &name, const string &file_path);
};

} // namespace duckdb