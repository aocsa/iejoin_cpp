#pragma once

#include "arrow_types.h"
#include "dataframe/dataframe.h"

namespace datafusionx {

using Table = frame::Dataframe<frame::VariantType>;
using DataType = Table::DataType;
using RowArray = Table::RowArray;
using ColumnArray = Table::ColumnArray;

ArrowType get_field_type(DataType value);
std::string to_string(DataType value);

class ColumnVector {
 public:
  virtual ArrowType getType() = 0;
  virtual DataType getValue(int i) = 0;
  virtual ColumnArray columnArray() = 0;
  virtual int size() = 0;
};

class ArrowFieldVector : public ColumnVector {
 public:
  ArrowFieldVector(const ColumnArray& column_array) : column_array(column_array) {}
  virtual ArrowType getType() override {
    assert(size() > 0);
    return get_field_type(getValue(0));
  }
  virtual DataType getValue(int i) override { return column_array[i]; }
  virtual int size() override { return column_array.size(); }

  virtual ColumnArray columnArray() override {
    return column_array; 
  }

public:
  ColumnArray column_array;
};

class LiteralValueVector : public ColumnVector {
 public:
  LiteralValueVector(ArrowType dtype, DataType value, int num_rows)
    : dtype(dtype), value(value), num_rows(num_rows)
  {}

  virtual ArrowType getType() override {
    return dtype;
  }
  virtual DataType getValue(int i) override { return value; }
  virtual int size() override { return num_rows; }

  virtual ColumnArray columnArray() override {
    ColumnArray result(size());
    result.fill(value);
    return result; 
  }

 private:
  ArrowType dtype;
  DataType value;
  int num_rows;
};

ArrowType get_field_type(DataType value) {
  if (std::holds_alternative<char>(value)) {
    return ArrowType::BOOL;
  } else if (std::holds_alternative<int>(value)) {
    return ArrowType::INT32;
  } else if (std::holds_alternative<long int>(value)) {
    return ArrowType::INT64;
  } else if (std::holds_alternative<float>(value)) {
    return ArrowType::FLOAT;
  } else if (std::holds_alternative<double>(value)) {
    return ArrowType::DOUBLE;
  } else if (std::holds_alternative<std::string>(value)) {
    return ArrowType::STRING;
  } else {
    // You should decide how to handle the case when none of the types match.
    throw std::invalid_argument("Invalid type in variant");
  }
}

std::string to_string(DataType value) {
    if (std::holds_alternative<char>(value)) {
        return std::to_string(std::get<char>(value));
    } else if (std::holds_alternative<int>(value)) {
        return std::to_string(std::get<int>(value));
    } else if (std::holds_alternative<long int>(value)) {
        return std::to_string(std::get<long int>(value));
    } else if (std::holds_alternative<double>(value)) {
        return std::to_string(std::get<double>(value));
    } else if (std::holds_alternative<float>(value)) {
        return std::to_string(std::get<float>(value));
    } else if (std::holds_alternative<std::string>(value)) {
        return std::get<std::string>(value);
    } else {
        return {}; // or throw an exception or whatever error handling you want
    }
}

// overload operator << for std::variant
std::ostream& operator<<(std::ostream& os, const DataType& value) {
    if (std::holds_alternative<char>(value)) {
        os << std::get<char>(value);
    } else if (std::holds_alternative<int>(value)) {
        os << std::get<int>(value);
    } else if (std::holds_alternative<long int>(value)) {
        os << std::get<long int>(value);
    } else if (std::holds_alternative<double>(value)) {
        os << std::get<double>(value);
    } else if (std::holds_alternative<float>(value)) {
        os << std::get<float>(value);
    } else if (std::holds_alternative<std::string>(value)) {
        os << std::get<std::string>(value);
    } else {
        os << "Invalid type in variant";
    }
    return os;
}

}  // namespace datafusionx