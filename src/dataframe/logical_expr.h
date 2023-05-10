#pragma once

#include <iostream>
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <vector>

// Forward declaration of LogicalPlan
class LogicalPlan;
class Schema;

// Define ArrowType enum
enum class ArrowType { INT32, INT64, FLOAT, DOUBLE, STRING, BOOL };

// Define Field class
struct Field {
  std::string name;
  ArrowType dataType;

  Field(std::string name, ArrowType dataType)
      : name(std::move(name)), dataType(dataType) {}
};

// Define Schema class
class Schema {
 public:
  std::vector<Field> fields;

  Schema(std::vector<Field> fields) : fields(std::move(fields)) {}

  // Project schema to a subset of fields
  Schema project(const std::vector<int>& indices) const {
    std::vector<Field> projectedFields;
    for (const auto& index : indices) {
      projectedFields.push_back(fields.at(index));
    }
    return Schema(projectedFields);
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
        throw std::invalid_argument("Field " + name +
                                    " not found or ambiguous");
      }
    }
    return Schema(selectedFields);
  }
};

// Define LogicalPlan interface
class LogicalPlan {
 public:
  virtual Schema schema() const = 0;
  virtual std::vector<std::shared_ptr<LogicalPlan>> children() const = 0;
  virtual std::string toString() const = 0;
  virtual ~LogicalPlan() = default;
};

// Format a logical plan in human-readable form
std::string format(const LogicalPlan& plan, int indent = 0) {
  std::string indentation(indent, '\t');
  std::string result = indentation + plan.toString() + "\n";
  for (const auto& child : plan.children()) {
    result += format(*child, indent + 1);
  }
  return result;
}

// Forward declaration of LogicalPlan
class LogicalPlan;


// Define LogicalExpr interface
class LogicalExpr {
 public:
  virtual Field toField(const LogicalPlan& input) const = 0;
  virtual ~LogicalExpr() = default;
};

// Define Column class
class Column : public LogicalExpr {
 public:
  explicit Column(std::string name) : name_(std::move(name)) {}

  Field toField(const LogicalPlan& input) const override {
    const auto& fields = input.schema().fields;
    auto field = std::ranges::find_if(
        fields, [this](const auto& field) { return field.name == name_; });
    if (field != fields.end()) {
      return *field;
    } else {
      throw std::invalid_argument("No column named '" + name_ + "' in schema");
    }
  }

  std::string toString() const { return "#" + name_; }

 private:
  std::string name_;
};

// Convenience function to create a Column reference
std::shared_ptr<Column> col(std::string name) {
  return std::make_shared<Column>(std::move(name));
}

// Define ColumnIndex class
class ColumnIndex : public LogicalExpr {
 public:
  explicit ColumnIndex(int i) : i_(i) {}

  Field toField(const LogicalPlan& input) const override {
    if (i_ < 0 || i_ >= input.schema().fields.size()) {
      throw std::invalid_argument("Column index " + std::to_string(i_) +
                                  " out of range");
    }
    return input.schema().fields[i_];
  }

  std::string toString() const { return "#" + std::to_string(i_); }

 private:
  int i_;
};

ArrowType ParseType(const std::string& str) {
  ArrowType type;
  std::istringstream iss(str);
  int i;
  float f;
  double d;

  // Try parsing as integer
  if (iss >> i) {
    type = ArrowType::INT32;
    return type;
  }

  // Try parsing as float
  iss.clear();
  iss.seekg(0);
  if (iss >> f) {
    type = ArrowType::FLOAT;
    return type;
  }

  // Try parsing as double
  iss.clear();
  iss.seekg(0);
  if (iss >> d) {
    type = ArrowType::DOUBLE;
    return type;
  }
  // If none of the above worked, assume it is a string
  return ArrowType::STRING;
}

// Define Literal class
class Literal : public LogicalExpr {
 public:
  explicit Literal(std::string str) : str_(std::move(str)) {
    type_ = ParseType(str_);
  }

  Field toField(const LogicalPlan& input) const override {
    return Field(str_, type_);
  }

  std::string toString() const { return "'" + str_ + "'"; }

 private:
  std::string str_;
  ArrowType type_;
};

// Convenience function to create a LiteralString
std::shared_ptr<Literal> lit(std::string value) {
  return std::make_shared<Literal>(std::move(value));
}

// Define BinaryExpr class
class BinaryExpr : public LogicalExpr {
 public:
  BinaryExpr(std::string name, std::string op, std::shared_ptr<LogicalExpr> l,
             std::shared_ptr<LogicalExpr> r)
      : name_(std::move(name)),
        op_(std::move(op)),
        l_(std::move(l)),
        r_(std::move(r)) {}

  Field toField(const LogicalPlan& input) const override {
    return Field(name_, ArrowType::BOOL);
  }

 protected:
  std::string name_;
  std::string op_;
  std::shared_ptr<LogicalExpr> l_;
  std::shared_ptr<LogicalExpr> r_;
};

// Define BooleanBinaryExpr class
class BooleanBinaryExpr : public BinaryExpr {
 public:
  BooleanBinaryExpr(std::string name, std::string op,
                    std::shared_ptr<LogicalExpr> l,
                    std::shared_ptr<LogicalExpr> r)
      : BinaryExpr(std::move(name), std::move(op), std::move(l), std::move(r)) {
  }

  Field toField(const LogicalPlan& input) const {
    return Field(name_, ArrowType::BOOL);
  }
};

// Define And class
class And : public BooleanBinaryExpr {
 public:
  And(std::shared_ptr<LogicalExpr> l, std::shared_ptr<LogicalExpr> r)
      : BooleanBinaryExpr("and", "AND", std::move(l), std::move(r)) {}
};

// Define Or class
class Or : public BooleanBinaryExpr {
 public:
  Or(std::shared_ptr<LogicalExpr> l, std::shared_ptr<LogicalExpr> r)
      : BooleanBinaryExpr("or", "OR", std::move(l), std::move(r)) {}
};

// Define Eq class
class Eq : public BooleanBinaryExpr {
 public:
  Eq(std::shared_ptr<LogicalExpr> l, std::shared_ptr<LogicalExpr> r)
      : BooleanBinaryExpr("eq", "=", std::move(l), std::move(r)) {}
};

// Define Neq class
class Neq : public BooleanBinaryExpr {
 public:
  Neq(std::shared_ptr<LogicalExpr> l, std::shared_ptr<LogicalExpr> r)
      : BooleanBinaryExpr("neq", "!=", std::move(l), std::move(r)) {}
};

// Define Gt class
class Gt : public BooleanBinaryExpr {
 public:
  Gt(std::shared_ptr<LogicalExpr> l, std::shared_ptr<LogicalExpr> r)
      : BooleanBinaryExpr("gt", ">", std::move(l), std::move(r)) {}
};

// Define GtEq class
class GtEq : public BooleanBinaryExpr {
 public:
  GtEq(std::shared_ptr<LogicalExpr> l, std::shared_ptr<LogicalExpr> r)
      : BooleanBinaryExpr("gteq", ">=", std::move(l), std::move(r)) {}
};

// Define Lt class
class Lt : public BooleanBinaryExpr {
 public:
  Lt(std::shared_ptr<LogicalExpr> l, std::shared_ptr<LogicalExpr> r)
      : BooleanBinaryExpr("lt", "<", std::move(l), std::move(r)) {}
};

// Define LtEq class
class LtEq : public BooleanBinaryExpr {
 public:
  LtEq(std::shared_ptr<LogicalExpr> l, std::shared_ptr<LogicalExpr> r)
      : BooleanBinaryExpr("lteq", "<=", std::move(l), std::move(r)) {}
};

// Equality
std::shared_ptr<Eq> eq(const std::shared_ptr<LogicalExpr>& lhs,
                       const std::shared_ptr<LogicalExpr>& rhs) {
  return std::make_shared<Eq>(lhs, rhs);
}

// Inequality
std::shared_ptr<Neq> neq(const std::shared_ptr<LogicalExpr>& lhs,
                         const std::shared_ptr<LogicalExpr>& rhs) {
  return std::make_shared<Neq>(lhs, rhs);
}

// Greater than
std::shared_ptr<Gt> gt(const std::shared_ptr<LogicalExpr>& lhs,
                       const std::shared_ptr<LogicalExpr>& rhs) {
  return std::make_shared<Gt>(lhs, rhs);
}

// Greater than or equal
std::shared_ptr<GtEq> gte(const std::shared_ptr<LogicalExpr>& lhs,
                          const std::shared_ptr<LogicalExpr>& rhs) {
  return std::make_shared<GtEq>(lhs, rhs);
}

// Less than
std::shared_ptr<Lt> lt(const std::shared_ptr<LogicalExpr>& lhs,
                       const std::shared_ptr<LogicalExpr>& rhs) {
  return std::make_shared<Lt>(lhs, rhs);
}

// Less than or equal
std::shared_ptr<LtEq> lte(const std::shared_ptr<LogicalExpr>& lhs,
                          const std::shared_ptr<LogicalExpr>& rhs) {
  return std::make_shared<LtEq>(lhs, rhs);
}