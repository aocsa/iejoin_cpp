#pragma once
#include <std/core.hpp>
#include "field.h"

namespace datafusionx {

// Define Schema class
struct Schema {
  std::vector<Field> fields;

  Schema(const std::vector<Field>& fields) : fields(fields) {}

  DEFAULT_CONSTRUCTOR(Schema)
  DEFAULT_ASSIGNMENT(Schema)
  
  // Project schema to a subset of fields
  Schema project(const std::vector<int>& indices) const {
    std::vector<Field> projectedFields;
    for (const auto& index : indices) {
      projectedFields.push_back(fields.at(index));
    }
    return Schema(projectedFields);
  }

  std::vector<std::string> columnNames() const {
    std::vector<std::string> names;
    for (const auto& field : fields) {
      names.push_back(field.name);
    }
    return names;
  }

  std::vector<ArrowType> columnTypes() const {
    std::vector<ArrowType> types;
    for (const auto& field : fields) {
      types.push_back(field.dataType);
    }
    return types;
  }

  Schema merge(const Schema& other) const {
    std::vector<Field> mergedFields = fields;
    for (const auto& field : other.fields) {
      mergedFields.push_back(field);
    }
    return Schema(mergedFields);
  }

  // Select schema fields by name
  Schema select(const std::vector<std::string>& names) const {
    std::vector<Field> selectedFields;
    for (const auto& name : names) {
      auto matches = std::ranges::views::filter(
          fields, [name](const auto& field) { return field.name == name; });
      if (std::ranges::distance(matches) == 1) {
        selectedFields.push_back(*matches.begin());
      } else {
        throw std::invalid_argument("Field " + name + " not found or ambiguous");
      }
    }
    return Schema(selectedFields);
  }

  int indexOfFirst(std::string col_name) {
    for (int i = 0; i < fields.size(); i++) {
      if (fields[i].name == col_name) {
        return i;
      }
    }
    return -1;
  }

  std::string toString() const {
    std::string str;
    for (const auto& field : fields) {
      str += field.toString() + "\n";
    }
    return str;
  }
};

}  // namespace datafusionx