
#pragma once 
#include <std/core.hpp>
#include "arrow_types.h"

namespace datacore {

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

  DEFAULT_CONSTRUCTOR(Field)
  DEFAULT_ASSIGNMENT(Field)

  std::string toString() const {
    return name + " " + arrowTypeToString(dataType);
  }
};

}