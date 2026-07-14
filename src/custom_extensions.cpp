#include "custom_extensions/custom_extensions.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// Returns the name of the Type's set "kind" oneof field (e.g. "i32", "decimal",
// "bool"). We use reflection rather than parsing DebugString(): modern protobuf
// deliberately redacts DebugString() output (e.g. inserting "goo.gle/debugonly"),
// which would otherwise be mistaken for the type name.
string TransformTypes(const substrait::Type &type) {
	auto *reflection = type.GetReflection();
	auto *kind = type.GetDescriptor()->FindOneofByName("kind");
	if (!kind) {
		return "";
	}
	auto *field = reflection->GetOneofFieldDescriptor(type, kind);
	if (!field) {
		return "";
	}
	return string(field->name());
}

vector<string> GetAllTypes() {
	return {{"bool"},
	        {"i8"},
	        {"i16"},
	        {"i32"},
	        {"i64"},
	        {"fp32"},
	        {"fp64"},
	        {"string"},
	        {"binary"},
	        {"timestamp"},
	        {"date"},
	        {"time"},
	        {"interval_year"},
	        {"interval_day"},
	        {"timestamp_tz"},
	        {"uuid"},
	        {"varchar"},
	        {"fixed_binary"},
	        {"decimal"},
	        {"precision_timestamp"},
	        {"precision_timestamp_tz"}};
}

// Recurse over the whole shebang
void SubstraitCustomFunctions::InsertAllFunctions(const vector<vector<string>> &all_types, vector<idx_t> &indices,
                                                  int depth, string &name, string &file_path) {
	if (depth == indices.size()) {
		vector<string> types;
		for (idx_t i = 0; i < indices.size(); i++) {
			auto type = all_types[i][indices[i]];
			type = StringUtil::Replace(type, "boolean", "bool");
			types.push_back(type);
		}
		if (types.empty()) {
			any_arg_functions[{name, types}] = {{name, types}, std::move(file_path)};
		} else {
			bool many_arg = false;
			string type = types[0];
			for (auto &t : types) {
				if (!t.empty() && t[t.size() - 1] == '?') {
					// If all types are equal and they end with ? we have a many_argument function
					many_arg = type == t;
				}
			}
			if (many_arg) {
				many_arg_functions[{name, types}] = {{name, types}, std::move(file_path)};
			} else {
				custom_functions[{name, types}] = {{name, types}, std::move(file_path)};
			}
		}

		return;
	}
	for (int i = 0; i < all_types[depth].size(); ++i) {
		indices[depth] = i;
		InsertAllFunctions(all_types, indices, depth + 1, name, file_path);
	}
}

void SubstraitCustomFunctions::InsertCustomFunction(string name_p, vector<string> types_p, string file_path) {
	auto types = std::move(types_p);
	vector<vector<string>> all_types;
	for (auto &t : types) {
		if (t == "any1" || t == "unknown" || t == "any") {
			all_types.emplace_back(GetAllTypes());
		} else {
			all_types.push_back({t});
		}
	}
	// Get the number of dimensions
	idx_t num_arguments = all_types.size();

	// Create a vector to hold the indices
	vector<idx_t> idx(num_arguments, 0);

	// Call the helper function with initial depth 0
	InsertAllFunctions(all_types, idx, 0, name_p, file_path);
}

string SubstraitCustomFunction::GetName() {
	if (arg_types.empty()) {
		return name;
	}
	string function_signature = name + ":";
	for (auto &type : arg_types) {
		function_signature += type + "_";
	}
	function_signature.pop_back();
	return function_signature;
}

string SubstraitFunctionExtensions::GetExtensionURN() const {
	if (IsNative()) {
		return "";
	}
	return "extension:io.substrait:" + extension_path;
}

bool SubstraitFunctionExtensions::IsNative() const {
	return extension_path == "native";
}

SubstraitCustomFunctions::SubstraitCustomFunctions() {
	Initialize();
};

vector<string> SubstraitCustomFunctions::GetTypes(const vector<substrait::Type> &types) {
	vector<string> transformed_types;
	for (auto &type : types) {
		transformed_types.emplace_back(TransformTypes(type));
	}
	return transformed_types;
}

// FIXME: We might have to do DuckDB extensions at some point
SubstraitFunctionExtensions SubstraitCustomFunctions::Get(const string &name,
                                                          const vector<::substrait::Type> &types) const {
	vector<string> transformed_types;
	if (types.empty()) {
		SubstraitCustomFunction custom_function {name, {}};
		auto it = any_arg_functions.find(custom_function);
		if (it != custom_functions.end()) {
			// We found it in our substrait custom map, return that
			return it->second;
		}
		return {{name, {}}, "native"};
	}

	for (auto &type : types) {
		transformed_types.emplace_back(TransformTypes(type));
		if (transformed_types.back().empty()) {
			// If it is empty it means we did not find a yaml extension, we return the function name
			return {{name, {}}, "native"};
		}
	}
	{
		SubstraitCustomFunction custom_function {name, {transformed_types}};
		auto it = custom_functions.find(custom_function);
		if (it != custom_functions.end()) {
			// We found it in our substrait custom map, return that
			return it->second;
		}
	}

	// check if it's a many argument fit
	bool possibly_many_arg = true;
	string type = transformed_types[0];
	for (auto &t : transformed_types) {
		possibly_many_arg = possibly_many_arg && type == t;
	}
	if (possibly_many_arg) {
		type += '?';
		SubstraitCustomFunction custom_many_function {name, {{type}}};
		auto many_it = many_arg_functions.find(custom_many_function);
		if (many_it != many_arg_functions.end()) {
			return many_it->second;
		}
	}
	// TODO: check if this should also print the arg types or not
	// we did not find it, return it as a native substrait function
	return {{name, {}}, "native"};
}

} // namespace duckdb