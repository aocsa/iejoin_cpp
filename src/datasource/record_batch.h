#pragma once
#include <std/core.hpp>
#include "column_vector.h"
#include "schema.h"

namespace datacore {

class RecordBatch {
 public:
  RecordBatch(const Schema& schema,
              const std::vector<std::shared_ptr<ColumnVector>>& fields)
      : schema_(schema), fields_(fields) {}

  int rowCount() { return fields_.size() == 0 ? 0 : fields_[0]->size(); }

  int columnCount() { return fields_.size(); }

  std::shared_ptr<ColumnVector> field(int i) { return fields_[i]; }

  std::string toCSV() {
    std::stringstream ss;
    int columnCount = schema_.fields.size();
    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      if (columnIndex > 0) {
        ss << ",";
      }
      ss << schema_.fields[columnIndex].name;
    }
    ss << "\n";

    for (int rowIndex = 0; rowIndex < rowCount(); rowIndex++) {
      for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        if (columnIndex > 0) {
          ss << ",";
        }
        auto v = fields_[columnIndex];
        auto value = v->getValue(rowIndex);

        // TODO: Use std::visit to handle different types
        // std::variant<std::monostate, int, std::string> my_variant;
        // Initialize with monostate (null state)
        // my_variant = std::monostate{};
        ss << to_string(value);
      }
      ss << "\n";
    }
    return ss.str();
  }

  std::string toString() { return toCSV(); }

  Schema schema() { return schema_; }

 private:
  Schema schema_;
  std::vector<std::shared_ptr<ColumnVector>> fields_;
};

}  // namespace datacore