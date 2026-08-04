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

// Concrete types over which an `any`/`any1` argument is expanded when
// pre-building the overload maps. Each token is a protobuf Type.kind field name
// (what TransformTypes() derives from a concrete argument), so the expanded
// overloads match at lookup -- these are proto kind names, not the abbreviated
// signature short names GetCompoundName() emits. This is the curated set of kinds that
// occur as arguments, not an exhaustive list of every proto kind.
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
	        {"date"},
	        {"interval_year"},
	        {"interval_day"},
	        {"uuid"},
	        {"varchar"},
	        {"fixed_binary"},
	        {"decimal"},
	        {"precision_timestamp"},
	        {"precision_timestamp_tz"}};
}

// True for the parameterized type placeholders `any`, `any1`, `any2`, ...
static bool IsAnyType(const string &type) {
	if (!StringUtil::StartsWith(type, "any")) {
		return false;
	}
	for (idx_t i = 3; i < type.size(); i++) {
		if (!StringUtil::CharacterIsDigit(type[i])) {
			return false;
		}
	}
	return true;
}

// Rewrites a type as declared by an extension YAML into the canonical Substrait
// type name, which is what the protobuf Type.kind field names use (e.g.
// fixedchar -> fixed_char), and drops the nullability marker.
static string CanonicalizeDeclaredType(const string &declared_type) {
	auto type = StringUtil::Replace(declared_type, "boolean", "bool");
	type = StringUtil::Replace(type, "fixedchar", "fixed_char");
	type = StringUtil::Replace(type, "fixedbinary", "fixed_binary");
	// functions_arithmetic_decimal spells the aggregate sum/avg argument
	// `DECIMAL` (uppercase) while its scalar functions and every other extension
	// use lowercase `decimal`, so canonicalize the case too.
	type = StringUtil::Replace(type, "DECIMAL", "decimal");
	// A trailing `?` declares the argument nullable. Nullability is not part of an
	// overload's identity -- TransformTypes() derives a bare kind name from a call
	// site, and a compound name encodes only the short type name -- so strip it
	// here and keep it out of both the map keys and the declared signature.
	if (!type.empty() && type.back() == '?') {
		type.pop_back();
	}
	return type;
}

// How many arguments of a declared signature are placeholders. Fewer placeholders
// means a more specific declaration.
static idx_t CountAnyTypes(const vector<string> &types) {
	idx_t count = 0;
	for (auto &type : types) {
		if (IsAnyType(type)) {
			count++;
		}
	}
	return count;
}

// Claims an overload for a declared signature, unless a more specific declaration
// already holds it.
//
// Two extensions can declare an impl that covers the same concrete arguments: for
// example functions_datetime declares lt(date, date) while functions_comparison
// declares lt(any1, any1). Both want the date/date slot, so resolve by
// specificity rather than by ingestion order, and let the concrete declaration
// win. Equally specific declarations keep last-one-wins.
//
// The mapped value is passed in rather than built here, so that this serves both the
// fixed-arity and the variadic map. Both value types lead with the same `function` member,
// which is all the specificity comparison needs.
template <class MAP, class VALUE>
static void InsertOverload(MAP &overloads, const SubstraitCustomFunction &key, VALUE value) {
	auto it = overloads.find(key);
	if (it != overloads.end() &&
	    CountAnyTypes(it->second.function.arg_types) < CountAnyTypes(value.function.arg_types)) {
		return;
	}
	overloads[key] = std::move(value);
}

// Recurse over the whole shebang
// `name`, `declared_types` and `file_path` are deliberately const references: every leaf of this
// recursion needs the same values, so moving out of them would empty the string for all subsequent
// leaves (see #205).
void SubstraitCustomFunctions::InsertAllFunctions(const vector<vector<string>> &all_types,
                                                  const vector<string> &declared_types, vector<idx_t> &indices,
                                                  int depth, const string &name, const string &file_path,
                                                  optional_idx variadic_min_arguments) {
	if (depth == indices.size()) {
		vector<string> types;
		for (idx_t i = 0; i < indices.size(); i++) {
			types.push_back(all_types[i][indices[i]]);
		}
		// The map key holds the concrete argument types, because lookup happens with
		// the types TransformTypes() derives from a call site. The mapped function
		// instead keeps the declared argument types, because a compound function name
		// encodes the signature of the impl the extension declares -- so a call to
		// equal(i64, i64) is keyed on i64/i64 but named equal:any_any.
		if (types.empty()) {
			InsertOverload(any_arg_functions, {name, types},
			               SubstraitFunctionExtensions {{name, declared_types}, file_path});
		} else if (variadic_min_arguments.IsValid()) {
			// Only the final declared argument repeats, so that one type is the whole key: a
			// call site passing it n times has to find the same entry for every n.
			//
			// Dropping the rest of the declared arguments from the key is only sound because
			// every declared argument of a variadic impl has the same type. The generator
			// enforces that (validate_variadic_impl), because a key that cannot tell
			// f(i32, string...) from f(string...) would answer for calls the impl does not
			// cover. Should a placeholder ever expand a non-final argument, several leaves of
			// this recursion collapse onto one key -- harmless, since declared_types is the
			// only part of the value that varies with the leaf, and it does not vary at all.
			InsertOverload(
			    variadic_functions, {name, {types.back()}},
			    SubstraitVariadicFunction {{name, declared_types}, file_path, variadic_min_arguments.GetIndex()});
		} else {
			InsertOverload(custom_functions, {name, types},
			               SubstraitFunctionExtensions {{name, declared_types}, file_path});
		}

		return;
	}
	for (int i = 0; i < all_types[depth].size(); ++i) {
		indices[depth] = i;
		InsertAllFunctions(all_types, declared_types, indices, depth + 1, name, file_path, variadic_min_arguments);
	}
}

void SubstraitCustomFunctions::InsertCustomFunction(const string &name, const vector<string> &types,
                                                    const string &file_path) {
	InsertFunction(name, types, file_path, optional_idx::Invalid());
}

void SubstraitCustomFunctions::InsertVariadicCustomFunction(const string &name, const vector<string> &types,
                                                            const string &file_path, idx_t variadic_min) {
	// `variadic_min` counts occurrences of the final declared argument -- the repeatable one --
	// not total arguments. concat_ws declares (string, string) with min 1, so its separator
	// plus one repetition means two arguments at the fewest, not one. The empty case only
	// keeps the subtraction from wrapping: a declaration with no arguments has none to repeat,
	// and InsertAllFunctions files it as a no-argument overload regardless.
	InsertFunction(name, types, file_path, types.empty() ? variadic_min : types.size() - 1 + variadic_min);
}

void SubstraitCustomFunctions::InsertFunction(const string &name, const vector<string> &types, const string &file_path,
                                              optional_idx variadic_min_arguments) {
	vector<string> declared_types;
	vector<vector<string>> all_types;
	for (auto &t : types) {
		auto type = CanonicalizeDeclaredType(t);
		if (IsAnyType(type)) {
			// A placeholder accepts every type, so key the overload on each concrete
			// type it can bind to.
			all_types.emplace_back(GetAllTypes());
		} else {
			all_types.push_back({type});
		}
		declared_types.push_back(std::move(type));
	}
	// Get the number of dimensions
	idx_t num_arguments = all_types.size();

	// Create a vector to hold the indices
	vector<idx_t> idx(num_arguments, 0);

	// Call the helper function with initial depth 0
	InsertAllFunctions(all_types, declared_types, idx, 0, name, file_path, variadic_min_arguments);
}

// Maps a canonical Substrait type name (the protobuf `Type.kind` oneof field
// name, e.g. "string", "decimal", "precision_timestamp") to the abbreviated
// "Type Short Name" that compound function signatures must use, per
// https://substrait.io/extensions/#function-signature-compound-names. Types
// whose short name is identical to their name (i8/i16/i32/i64, fp32/fp64, bool,
// date, uuid, struct/list/map, func) are absent from the table and pass through
// unchanged.
static string TypeShortName(const string &type) {
	// The short name of every parameterized placeholder is `any`, so the digit that
	// distinguishes one placeholder from another is dropped (any1 -> any).
	if (IsAnyType(type)) {
		return "any";
	}
	static const std::unordered_map<string, string> SHORT_NAMES = {
	    {"string", "str"},
	    {"binary", "vbin"},
	    {"decimal", "dec"},
	    {"varchar", "vchar"},
	    {"fixed_char", "fchar"},
	    {"fixed_binary", "fbin"},
	    {"interval_year", "iyear"},
	    {"interval_day", "iday"},
	    {"interval_compound", "icompound"},
	    {"precision_time", "pt"},
	    {"precision_timestamp", "pts"},
	    {"precision_timestamp_tz", "ptstz"},
	};
	auto it = SHORT_NAMES.find(type);
	return it == SHORT_NAMES.end() ? type : it->second;
}

// Builds the compound name of a declared impl: the function name, a ':', and the
// short names of the declared argument types joined by '_'.
//
// An impl declared with no arguments keeps the ':' with nothing after it --
// `count:`, not `count`. The signature grammar cannot actually express that case
// (`argument-signature` requires at least one short-arg-type, filed upstream as
// substrait-io/substrait#1162), so `name:` is a de-facto convention rather than a
// normative one. It is however what the reference producers emit and what plan-side
// function lookup expects of them, so a bare `count` fails to resolve. #258 has the
// inventory and the reasoning.
string SubstraitCustomFunction::GetCompoundName() const {
	string function_signature = name + ":";
	for (auto &type : arg_types) {
		function_signature += TypeShortName(type) + "_";
	}
	if (!arg_types.empty()) {
		// Drop the separator the last argument appended. Guarded, because a
		// zero-argument impl never ran the loop and popping would eat the ':'.
		function_signature.pop_back();
	}
	return function_signature;
}

// The name this function is declared under in the plan's extension list.
//
// A function that resolved against no extension YAML is emitted under its bare
// DuckDB name, with no signature part at all: there is no declared impl to name it
// after. That case is not distinguishable from the arg_types of the function alone,
// because Get() reports it by returning empty arg_types no matter how many
// arguments the call site had -- so the check has to happen here, where the
// extension path is in scope. Hence the zero-argument native `random()` stays
// `random` while the zero-argument declared `row_number()` becomes `row_number:`.
string SubstraitFunctionExtensions::GetName() const {
	if (IsNative()) {
		return function.name;
	}
	return function.GetCompoundName();
}

string SubstraitFunctionExtensions::GetExtensionURN() const {
	if (IsNative()) {
		return "";
	}
	// extension_path holds the full Substrait URN (extension:<owner>:<id>) as
	// declared by the extension YAML, so return it verbatim.
	return extension_path;
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
		if (auto it = any_arg_functions.find(custom_function); it != any_arg_functions.end()) {
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

	// Check whether a variadic declaration covers the call. A variadic entry is keyed on its
	// one repeatable type, so it can only answer for a call whose arguments all share a type --
	// which is exactly the calls a variadic impl covers, given that all of its declared
	// arguments have that one type too (enforced by the generator, see
	// validate_variadic_impl). A call of mixed types is therefore not a near miss to be
	// widened later; no variadic declaration accepts one.
	bool possibly_variadic = true;
	const string &type = transformed_types[0];
	for (auto &t : transformed_types) {
		possibly_variadic = possibly_variadic && type == t;
	}
	if (possibly_variadic) {
		SubstraitCustomFunction custom_variadic_function {name, {type}};
		auto variadic_it = variadic_functions.find(custom_variadic_function);
		// An arity below the declared minimum is not an impl the extension offers, so leave it
		// to `native` rather than name it after a signature that does not cover it. DuckDB's
		// least(x) is the reachable case: functions_comparison declares least variadic with a
		// minimum of two. A minimum of 0 (`and`, `or`) can never reject anything -- the
		// zero-argument call it would admit is answered by any_arg_functions above, before this
		// branch is reached.
		if (variadic_it != variadic_functions.end() && transformed_types.size() >= variadic_it->second.min_arguments) {
			return {variadic_it->second.function, variadic_it->second.extension_path};
		}
	}
	// TODO: check if this should also print the arg types or not
	// we did not find it, return it as a native substrait function
	return {{name, {}}, "native"};
}

} // namespace duckdb