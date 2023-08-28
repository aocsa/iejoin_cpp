
#pragma once 
#include <std/core.hpp>
#include "arrow_types.h"

#include <parquet/types.h>

namespace datafusionx {

#define DEFAULT_CONSTRUCTOR(ClassName) \
  ClassName() = default;               \
  ClassName(const ClassName&) = default;\
  ClassName(ClassName&&) = default;

#define DEFAULT_ASSIGNMENT(ClassName) \
  ClassName& operator=(const ClassName&) = default;\
  ClassName& operator=(ClassName&&) = default;


std::string arrowTypeToString(ArrowType type){
  // ArrowType enum value to std::string
  switch (type) {
    case ArrowType::BOOL:
      return "BOOL";
    case ArrowType::INT32:
      return "INT32";
    case ArrowType::INT64:
      return "INT64";
    case ArrowType::DOUBLE:
      return "DOUBLE";
    case ArrowType::FLOAT:
      return "FLOAT";
    case ArrowType::STRING:
      return "STRING";
    default:
      throw std::invalid_argument("Invalid ArrowType");
  } 
}

struct Field {
  std::string name;
  ArrowType dataType;
  Field(std::string name, ArrowType dataType) : name(name), dataType(dataType) {}
  Field(std::string name, parquet::Type::type dataType) : name(name){

    // switch data type from parquet to arrowType
    switch (dataType) {
      case parquet::Type::BOOLEAN:
        this->dataType = ArrowType::BOOL;
        break;
      case parquet::Type::INT32:
        this->dataType = ArrowType::INT32;
        break;
      case parquet::Type::INT64:
        this->dataType = ArrowType::INT64;
        break;
      case parquet::Type::DOUBLE:
        this->dataType = ArrowType::DOUBLE;
        break;
      case parquet::Type::FLOAT:
        this->dataType = ArrowType::FLOAT;
        break;
      case parquet::Type::BYTE_ARRAY:
        this->dataType = ArrowType::STRING;
        break;
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        this->dataType = ArrowType::STRING;
        break;
      default:
        throw std::invalid_argument("Invalid ParquetType");
    }
  }

  DEFAULT_CONSTRUCTOR(Field)
  DEFAULT_ASSIGNMENT(Field)

  std::string toString() const {
    return name + " " + arrowTypeToString(dataType);
  }
};

}