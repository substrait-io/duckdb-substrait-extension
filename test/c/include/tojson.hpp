// This file is copied from the https://github.com/mircodz/tojson repository.
// Source: https://github.com/mircodz/tojson/blob/main/include/tojson.hpp
// It is not packaged as a library, so this single header is included directly.
// This copy is stripped down to contain only the code required to load a YAML file
// and convert it to JSON (used for schema validation).

#pragma once

#include <yaml-cpp/yaml.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <stdexcept>

#if __has_cpp_attribute(nodiscard)
#define TOJSON_NODISCARD [[nodiscard]]
#else
#define TOJSON_NODISCARD
#endif

namespace tojson {
namespace detail {

inline nlohmann::json parse_scalar(const YAML::Node &node) {
	int i;
	double d;
	bool b;
	std::string s;

	// string tag will be !
	if (node.Tag() != "!") {
		if (YAML::convert<int>::decode(node, i)) return i;
		if (YAML::convert<double>::decode(node, d)) return d;
		if (YAML::convert<bool>::decode(node, b)) return b;
	}
	if (YAML::convert<std::string>::decode(node, s)) return s;
	
	return nullptr;
}

/// \todo refactor and pass nlohmann::json down by reference instead of returning it
inline nlohmann::json yaml2json(const YAML::Node &root) {
	nlohmann::json j{};

	switch (root.Type()) {
	case YAML::NodeType::Null: break;
	case YAML::NodeType::Scalar: return parse_scalar(root);
	case YAML::NodeType::Sequence:
		j = nlohmann::json::array();
		for (auto &&node : root)
			j.emplace_back(yaml2json(node));
		break;
	case YAML::NodeType::Map:
		j = nlohmann::json::object();
		for (auto &&it : root)
			j[it.first.as<std::string>()] = yaml2json(it.second);
		break;
	default: break;
	}
	return j;
}

inline std::string repr(const nlohmann::json &j) {
	if (j.is_number()) return std::to_string(j.get<int>());
	if (j.is_boolean()) return j.get<bool>() ? "true" : "false";
	if (j.is_number_float()) return std::to_string(j.get<double>());
	if (j.is_string()) return j.get<std::string>();
	throw std::runtime_error("invalid type");
}

}  // namespace detail

/// \brief Convert YAML string to JSON.
TOJSON_NODISCARD inline nlohmann::json yaml2json(const std::string &str) {
	YAML::Node root = YAML::Load(str);
	return detail::yaml2json(root);
}

/// \brief Load a YAML file to JSON.
TOJSON_NODISCARD inline nlohmann::json loadyaml(const std::string &filepath) {
	YAML::Node root = YAML::LoadFile(filepath);
	return detail::yaml2json(root);
}

}  // namespace tojson
