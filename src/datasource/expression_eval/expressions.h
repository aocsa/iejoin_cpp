#pragma once

#include "datasource/csv_datasource.h"
#include "datasource/logical_expr.h"
#include "std/core.hpp"

namespace datafusionx {

// Conversion helper functions
std::string toString(const DataType& v);

bool toBool(const DataType& v);

// Interface for a physical expression
class Expression {
 public:
  virtual std::shared_ptr<ColumnVector> evaluate(
      const std::shared_ptr<RecordBatch>& input) = 0;
  virtual std::string toString() = 0;
};

class LiteralExpression : public Expression {
 public:
  LiteralExpression(DataType value) : value(value) {}

  std::shared_ptr<ColumnVector> evaluate(
      const std::shared_ptr<RecordBatch>& input) override {
    auto dtype = get_field_type(value);
    return std::make_shared<LiteralValueVector>(dtype, value,
                                                input->rowCount());
  }
  std::string toString() override { return to_string(value); }

 private:
  DataType value;
};

class ColumnExpression : public Expression {
 public:
  explicit ColumnExpression(int i) : index(i) {}

  std::shared_ptr<ColumnVector> evaluate(
      const std::shared_ptr<RecordBatch>& input) override {
    return input->field(index);
  }

  std::string toString() override { return "#" + std::to_string(index); }

 private:
  int index;
};

class BooleanExpression : public Expression {
 public:
  BooleanExpression(std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : l(l), r(r) {}

  std::shared_ptr<ColumnVector> evaluate(
      const std::shared_ptr<RecordBatch>& input) override {
    auto ll = l->evaluate(input);
    auto rr = r->evaluate(input);
    if (ll->getType() != rr->getType()) {
      std::cerr << (
          "maybe cannot compare values of different type: " + arrowTypeToString(ll->getType()) +
          " != " + arrowTypeToString(rr->getType()));
    }
    return compare(ll, rr);
  }

  virtual bool evaluate(const DataType& l, const DataType& r,
                        const ArrowType& arrowType) = 0;

  std::string toString()  override { return "#" + std::string("boolean op"); }

 protected:
  std::shared_ptr<ColumnVector> compare(const std::shared_ptr<ColumnVector>& l,
                                        const std::shared_ptr<ColumnVector>& r) {
    ColumnArray v;
    for (int i = 0; i < l->size(); ++i) {
      auto value = evaluate(l->getValue(i), r->getValue(i), l->getType());
      char filter = value ? 1 : 0;
      v.emplace_back(filter);
    }
    return std::make_shared<ArrowFieldVector>(v);
  }

  std::shared_ptr<Expression> l;
  std::shared_ptr<Expression> r;
};

class AndExpression : public BooleanExpression {
 public:
  AndExpression(std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : BooleanExpression(l, r) {}

  bool evaluate(const DataType& l, const DataType& r,
                const ArrowType& arrowType) override {
    return toBool(l) && toBool(r);
  }
};

class OrExpression : public BooleanExpression {
 public:
  OrExpression(std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : BooleanExpression(l, r) {}

  bool evaluate(const DataType& l, const DataType& r,
                const ArrowType& arrowType) override {
    return toBool(l) || toBool(r);
  }
};

// Additional expressions (EqExpression, NeqExpression, LtExpression, LtEqExpression,
// GtExpression, GtEqExpression) would follow the same pattern.

class EqExpression : public BooleanExpression {
 public:
  EqExpression(std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : BooleanExpression(l, r) {}

  bool evaluate(const DataType& l, const DataType& r,
                const ArrowType& arrowType) override {
    return l == r;
  }
};

class NeqExpression : public BooleanExpression {
 public:
  NeqExpression(std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : BooleanExpression(l, r) {}

  bool evaluate(const DataType& l, const DataType& r,
                const ArrowType& arrowType) override {
    return l != r;
  }
};

class LtExpression : public BooleanExpression {
 public:
  LtExpression(std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : BooleanExpression(l, r) {}

  bool evaluate(const DataType& l, const DataType& r,
                const ArrowType& arrowType) override {
    return l < r;
  }
};

class LtEqExpression : public BooleanExpression {
 public:
  LtEqExpression(std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : BooleanExpression(l, r) {}

  bool evaluate(const DataType& l, const DataType& r,
                const ArrowType& arrowType) override {
    return l <= r;
  }
};

class GtExpression : public BooleanExpression {
 public:
  GtExpression(std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : BooleanExpression(l, r) {}

  bool evaluate(const DataType& l, const DataType& r,
                const ArrowType& arrowType) override {
    return l > r;
  }
};

class GtEqExpression : public BooleanExpression {
 public:
  GtEqExpression(std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : BooleanExpression(l, r) {}

  bool evaluate(const DataType& l, const DataType& r,
                const ArrowType& arrowType) override {
    return l >= r;
  }
};

// Conversion helper functions
std::string toString(const DataType& v) { return to_string(v); }

bool toBool(const DataType& v) {
  if (std::holds_alternative<char>(v)) {
    return std::get<char>(v);
  } else if (std::holds_alternative<long int>(v)) {
    return std::get<long int>(v) == 1;
  } else {
    throw std::logic_error("Unsupported type conversion to bool");
  }
}

}  // namespace datafusionx