//===----------------------------------------------------------------------===//
//                         DuckDB
//
// custom_extensions/substrait_custom_extensions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_idx.hpp"
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

//! A variadic overload: the extension that declares it, plus the fewest arguments a call
//! site may pass. Shares its leading `function` member with SubstraitFunctionExtensions so
//! that InsertOverload can resolve declaration specificity for either map.
struct SubstraitVariadicFunction {
	SubstraitCustomFunction function;
	string extension_path;
	//! Minimum call-site arity, derived from the declared arity and the YAML's `variadic.min`.
	idx_t min_arguments;
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
	// For functions the extension YAML declares `variadic`, meaning their final argument may
	// repeat. Keyed on that one repeatable type rather than on an expanded argument list,
	// because a call site can pass any number of it -- which is also how the Substrait
	// signature grammar names them: the variadic argument appears once, so the variadic `and`
	// is `and:bool` however many arguments the call site passes.
	std::unordered_map<SubstraitCustomFunction, SubstraitVariadicFunction, HashSubstraitFunctions> variadic_functions;

	void InsertCustomFunction(const string &name, const vector<string> &types, const string &file_path);
	//! Registers an impl whose final declared argument may repeat. `variadic_min` counts
	//! occurrences of that argument, verbatim from the YAML's `variadic.min`.
	void InsertVariadicCustomFunction(const string &name, const vector<string> &types, const string &file_path,
	                                  idx_t variadic_min);
	//! `variadic_min_arguments` is the minimum call-site arity for a variadic impl, or invalid
	//! for a fixed-arity one.
	void InsertFunction(const string &name, const vector<string> &types, const string &file_path,
	                    optional_idx variadic_min_arguments);
	void InsertAllFunctions(const vector<vector<string>> &all_types, const vector<string> &declared_types,
	                        vector<idx_t> &indices, int depth, const string &name, const string &file_path,
	                        optional_idx variadic_min_arguments);
};

} // namespace duckdb