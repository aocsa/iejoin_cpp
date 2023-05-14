#pragma once

#include <iostream>
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <vector>

#include "datasource/column_vector.h"
#include "datasource/datasource.h"
#include "datasource/field.h"
#include "datasource/schema.h"

namespace datacore {
// Forward declaration of LogicalPlan

// Define LogicalPlan interface
class LogicalPlan {
 public:
  virtual Schema schema() const = 0;
  virtual std::vector<std::shared_ptr<LogicalPlan>> children() const = 0;
  virtual std::string toString() const = 0;
  virtual ~LogicalPlan() = default;
};

// Format a logical plan in human-readable form
std::string format(const std::shared_ptr<LogicalPlan>& plan, int indent = 0) {
  std::string indentation(indent, '\t');
  std::string result = indentation + plan->toString() + "\n";
  for (const auto& child : plan->children()) {
    result += format(child, indent + 1);
  }
  return result;
}

// Forward declaration of LogicalPlan
class LogicalPlan;

// Define LogicalExpr interface
class LogicalExpr {
 public:
  virtual Field toField(const std::shared_ptr<LogicalPlan>& input) const = 0;
  virtual std::string toString() const = 0;
};

// Define Column class
class Column : public LogicalExpr {
 public:
  explicit Column(std::string name) : name(std::move(name)) {}

  Field toField(const std::shared_ptr<LogicalPlan>& input) const override {
    const auto& fields = input->schema().fields;
    auto field = std::ranges::find_if(
        fields, [this](const auto& field) { return field.name == name; });
    if (field != fields.end()) {
      return *field;
    } else {
      throw std::invalid_argument("No column named '" + name + "' in schema");
    }
  }

  std::string toString() const override { return "#" + name; }

 public:
  std::string name;
};

// Convenience function to create a Column reference
std::shared_ptr<Column> col(std::string name) {
  return std::make_shared<Column>(std::move(name));
}


// Define ColumnIndex class
class ColumnIndex : public LogicalExpr {
 public:
  explicit ColumnIndex(int i) : index(i) {}

  Field toField(const std::shared_ptr<LogicalPlan>& input) const override {
    if (index < 0 || index >= input->schema().fields.size()) {
      throw std::invalid_argument("Column index " + std::to_string(index) + " out of range");
    }
    return input->schema().fields[index];
  }

  std::string toString() const override { return "#" + std::to_string(index); }

public:
  int index;
};

// Define Literal class
class Literal : public LogicalExpr {
 public:
  explicit Literal(VariantType value) : value(value) { type = get_field_type(value); }

  Field toField(const std::shared_ptr<LogicalPlan>& input) const override { return Field("lit", type); }

  std::string toString() const override { return to_string(value); }

public:
  VariantType value;
  ArrowType type;
};

// Convenience function to create a LiteralString
std::shared_ptr<Literal> lit(VariantType value) {
  return std::make_shared<Literal>(value);
}

// Define BinaryExpr class
class BinaryExpr : public LogicalExpr {
 public:
  BinaryExpr(std::string name, std::string op, std::shared_ptr<LogicalExpr> l,
             std::shared_ptr<LogicalExpr> r)
      : name_(std::move(name)), op_(std::move(op)), l_(std::move(l)), r_(std::move(r)) {}

  Field toField(const std::shared_ptr<LogicalPlan>& input) const override {
    return Field(name_, ArrowType::BOOL);
  }

 public:
  std::string name_;
  std::string op_;
  std::shared_ptr<LogicalExpr> l_;
  std::shared_ptr<LogicalExpr> r_;
};

// Define BooleanBinaryExpr class
class BooleanBinaryExpr : public BinaryExpr {
 public:
  BooleanBinaryExpr(std::string name, std::string op, std::shared_ptr<LogicalExpr> l,
                    std::shared_ptr<LogicalExpr> r)
      : BinaryExpr(std::move(name), std::move(op), std::move(l), std::move(r)) {}

  Field toField(const std::shared_ptr<LogicalPlan>& input) const override final { return Field(name_, ArrowType::BOOL); }

  std::string toString() const override { return "(" + l_->toString() + " " + op_ + " " + r_->toString() + ")"; }
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



class Scan : public LogicalPlan {
public:
    std::string path;
    std::shared_ptr<DataSource> dataSource;
    std::vector<std::string> projection;
    Schema schema_;

   Schema deriveSchema() {
       Schema schema = dataSource->schema();
        if (projection.empty()) {
            return schema;
        } else {
            return schema.select(projection);
        }
    }

public:
    Scan(const std::string& path, std::shared_ptr<DataSource> dataSource, const std::vector<std::string>& projection)
        : path(path), dataSource(dataSource), projection(projection), schema_(deriveSchema()) {}

    Schema schema() const override {
        return schema_;
    }

    std::vector<std::shared_ptr<LogicalPlan>> children() const override {
        return std::vector<std::shared_ptr<LogicalPlan>>{};
    }

    std::string toString() const override {
        if (projection.empty()) {
            return "Scan: " + path + "; projection=None";
        } else {
            std::string proj = "Scan: " + path + "; projection=";
            for (const auto& p : projection) {
                proj += p + " ";
            }
            return proj;
        }
    }
};


class Selection : public LogicalPlan {
public:
    std::shared_ptr<LogicalPlan> input;
    std::shared_ptr<LogicalExpr> expr;

public:
    Selection(std::shared_ptr<LogicalPlan> input, std::shared_ptr<LogicalExpr> expr)
        : input(input), expr(expr) {}

    Schema schema() const override {
        return input->schema();
    }

    std::vector<std::shared_ptr<LogicalPlan>> children() const override {
        // selection does not change the schema of the input
        return std::vector<std::shared_ptr<LogicalPlan>>{input};
    }

    std::string toString() const override {
        return "Selection: " + expr->toString();
    }
};


class Projection : public LogicalPlan {
public:
    std::shared_ptr<LogicalPlan> input;
    std::vector<std::shared_ptr<LogicalExpr>> expr;

public:
    Projection(std::shared_ptr<LogicalPlan> input, std::vector<std::shared_ptr<LogicalExpr>> expr)
        : input(input), expr(expr) {}

    Schema schema() const override {
        std::vector<Field> fields;
        std::transform(expr.begin(), expr.end(), std::back_inserter(fields),
            [this](const std::shared_ptr<LogicalExpr>& e) { return e->toField(input); });
        return Schema(fields);
    }

    std::vector<std::shared_ptr<LogicalPlan>> children() const override {
        return std::vector<std::shared_ptr<LogicalPlan>>{input};
    }

    std::string toString() const override {
        std::stringstream ss;
        ss << "Projection: ";
        for (auto it = expr.begin(); it != expr.end(); ++it) {
            if (it != expr.begin()) {
                ss << ", ";
            }
            ss << (*it)->toString();
        }
        return ss.str();
    }
};

}  // namespace datacore
