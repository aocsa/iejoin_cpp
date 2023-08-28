#pragma once
#include <std/core.hpp>
#include "column_vector.h"
#include "schema.h"

namespace datafusionx {

class RecordBatch {
 public:
  RecordBatch(const Schema& schema,
              const std::vector<std::shared_ptr<ColumnVector>>& fields);

  int rowCount();
  int columnCount();
  std::shared_ptr<ColumnVector> field(int i);
  
  std::string toCSV();
  std::string toString();
  Schema schema();

  Table toTable() {
    Table df = Table::make_empty(rowCount());
    for (int i = 0; i < columnCount(); i++) {
      auto column = field(i);
      df.insert(schema_.fields[i].name, column->columnArray());
    }
    return df;
  }

 private:
  Schema schema_;
  std::vector<std::shared_ptr<ColumnVector>> fields_;
};


RecordBatch::RecordBatch(const Schema& schema,
                         const std::vector<std::shared_ptr<ColumnVector>>& fields)
    : schema_(schema), fields_(fields) {}

int RecordBatch::rowCount() {
  return fields_.size() == 0 ? 0 : fields_[0]->size();
}

int RecordBatch::columnCount() {
  return fields_.size();
}

std::shared_ptr<ColumnVector> RecordBatch::field(int i) {
  return fields_[i];
}

std::string RecordBatch::toCSV() {
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
      ss << to_string(value);
    }
    ss << "\n";
  }
  return ss.str();
}

std::string RecordBatch::toString() {
  return toCSV();
}

Schema RecordBatch::schema() {
  return schema_;
}

}  // namespace datafusionx