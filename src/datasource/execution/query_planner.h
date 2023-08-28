#pragma once

#include "datasource/csv_datasource.h"
#include "datasource/execution/dataframe_impl.h"
#include "datasource/expression_eval/expressions.h"
#include "datasource/logical_expr.h"
#include "std/core.hpp"

namespace datafusionx {

class PhysicalPlan;


class PhysicalPlan {
 public:
  virtual ~PhysicalPlan() = default;

  virtual Schema schema() = 0;

  virtual std::vector<std::shared_ptr<RecordBatch>> execute() = 0;

  virtual std::vector<std::shared_ptr<PhysicalPlan>> children() = 0;

  virtual std::string pretty() { return format_physical_plan(this, 0); }

  virtual std::string toString()  = 0;

  // Format a physical plan in human-readable form
  std::string format_physical_plan(PhysicalPlan* plan, int indent) {
    std::string indentation(indent, '\t');
    std::string result = indentation + plan->toString() + "\n";
    for (const auto& child : plan->children()) {
      result += format_physical_plan(child.get(), indent + 1);
    }
    return result;
  }
};


class ScanExec : public PhysicalPlan {
 public:
  ScanExec(const std::shared_ptr<DataSource>& ds,
           const std::vector<std::string>& projection)
      : ds(ds), projection(projection) {}

  Schema schema() override { return ds->schema().select(projection); }

  std::vector<std::shared_ptr<PhysicalPlan>> children() override {
    return std::vector<std::shared_ptr<PhysicalPlan>>{};
  }

  std::vector<std::shared_ptr<RecordBatch>> execute() override {
    return ds->scan(projection);
  }
  std::string toString() override {
    return "ScanExec(" + ds->toString() + ", " + toString(projection) + ")";
  }

 private:
  std::shared_ptr<DataSource> ds;
  std::vector<std::string> projection;

  // Helper function to convert vector of strings to a single string
  std::string toString(const std::vector<std::string>& vec) {
    std::string result = "[";
    for (const auto& str : vec) {
      result += str + ", ";
    }
    if (!vec.empty()) {
      result.pop_back();  // remove the last comma
      result.back() = ']';
    } else {
      result += ']';
    }
    return result;
  }
};

class SelectionExec : public PhysicalPlan {
 public:
  SelectionExec(const std::shared_ptr<PhysicalPlan>& input,
                const std::shared_ptr<Expression>& expr)
      : input(input), expr(expr) {}

  Schema schema() override { return input->schema(); }

  std::vector<std::shared_ptr<PhysicalPlan>> children() override { return {input}; }

  std::vector<std::shared_ptr<RecordBatch>> execute() override {
    auto inputExec = input->execute();
    std::vector<std::shared_ptr<RecordBatch>> output;

    for (auto& batch : inputExec) {
      auto result = expr->evaluate(batch);
      auto& column_array =
          std::dynamic_pointer_cast<ArrowFieldVector>(result)->column_array;
      auto bitVecResult = column_array.get_as<bool>();

      auto schema = batch->schema();
      int columnCount = schema.fields.size();
      std::vector<std::shared_ptr<ColumnVector>> filteredFields;
      for (int i = 0; i < columnCount; i++) {
        auto batch_filtered = filter(batch->field(i), bitVecResult);
        filteredFields.push_back(batch_filtered);
      }
      auto record_batch = std::make_shared<RecordBatch>(schema, filteredFields);
      if (record_batch->rowCount() > 0){
        output.emplace_back(record_batch);
      }
    }

    return output;
  }
  std::string toString() override {
    return "SelectionExec(" + input->toString() + ", " + expr->toString() + ")";
  }

 private:
  std::shared_ptr<ArrowFieldVector> filter(const std::shared_ptr<ColumnVector>& v,
                                           const std::vector<bool>& selection) {
    ColumnArray column_array;
    for (int i = 0; i < selection.size(); i++) {
      if (selection.at(i) == true) {
        column_array.emplace_back(v->getValue(i));
      }
    }
    return std::make_shared<ArrowFieldVector>(column_array);
    ;
  }

public:
  std::shared_ptr<PhysicalPlan> input;
  std::shared_ptr<Expression> expr;
};

class ProjectionExec : public PhysicalPlan {
 public:
  ProjectionExec(std::shared_ptr<PhysicalPlan> input, Schema schema,
                 std::vector<std::shared_ptr<Expression>> expr)
      : input(std::move(input)), schema_(std::move(schema)), expr(std::move(expr)) {}

  Schema schema() override { return schema_; }

  std::vector<std::shared_ptr<PhysicalPlan>> children() override { return {input}; }

  std::string toString() override {
    return "ProjectionExec(" + input->toString() + ", " + schema_.toString() + ")";
  }

  std::vector<std::shared_ptr<RecordBatch>> execute() override {
    std::vector<std::shared_ptr<RecordBatch>> batches = input->execute();
    std::vector<std::shared_ptr<RecordBatch>> result;

    for (const auto& batch : batches) {
      std::vector<std::shared_ptr<ColumnVector>> columns;
      for (const auto& e : expr) {
        columns.push_back(e->evaluate(batch));
      }
      result.push_back(std::make_shared<RecordBatch>(schema_, columns));
    }

    return result;
  }

  std::string toString() const {
    std::ostringstream oss;
    oss << "ProjectionExec: ";
    for (const auto& e : expr) {
      oss << e->toString() << " ";
    }
    return oss.str();
  }

 private:
  std::shared_ptr<PhysicalPlan> input;
  Schema schema_;
  std::vector<std::shared_ptr<Expression>> expr;
};

class QueryPlanner {
 public:
  static std::shared_ptr<PhysicalPlan> createPhysicalPlan(
      const std::shared_ptr<LogicalPlan>& plan) {
    if (auto scan = std::dynamic_pointer_cast<Scan>(plan)) {
      return std::make_shared<ScanExec>(scan->dataSource, scan->projection);
    } else if (auto selection = std::dynamic_pointer_cast<Selection>(plan)) {
      auto input = createPhysicalPlan(selection->input);
      auto filterExpr = createPhysicalExpr(selection->expr, selection->input);
      return std::make_shared<SelectionExec>(input, filterExpr);
    } else if (auto projection = std::dynamic_pointer_cast<Projection>(plan)) {
      auto input = createPhysicalPlan(projection->input);
      std::vector<std::shared_ptr<Expression>> projectionExpr;
      std::transform(projection->expr.begin(), projection->expr.end(),
                     std::back_inserter(projectionExpr), [&](const auto& expr) {
                       return createPhysicalExpr(expr, projection->input);
                     });
      std::vector<Field> fields;
      std::transform(projection->expr.begin(), projection->expr.end(), std::back_inserter(fields), 
                    [&projection](const auto& expr) { 
                      return expr->toField(projection->input); 
                    });
      Schema projectionSchema(fields);
      return std::make_shared<ProjectionExec>(input, projectionSchema, projectionExpr);
    }
    // else if (auto aggregate = std::dynamic_pointer_cast<Aggregate>(plan)) {
    //     auto input = createPhysicalPlan(aggregate->input);
    //     std::vector<std::shared_ptr<Expression>> groupExpr;
    //     std::transform(aggregate->groupExpr.begin(), aggregate->groupExpr.end(),
    //     std::back_inserter(groupExpr), [&](const auto& expr){ return
    //     createPhysicalExpr(expr, aggregate->input); });
    //     std::vector<std::shared_ptr<Expression>> aggregateExpr;
    //     for (const auto& expr : aggregate->aggregateExpr) {
    //         if (auto max = std::dynamic_pointer_cast<Max>(expr)) {
    //             aggregateExpr.push_back(std::make_shared<MaxExpression>(createPhysicalExpr(max->expr,
    //             aggregate->input)));
    //         }
    //         else if (auto min = std::dynamic_pointer_cast<Min>(expr)) {
    //             aggregateExpr.push_back(std::make_shared<MinExpression>(createPhysicalExpr(min->expr,
    //             aggregate->input)));
    //         }
    //         else if (auto sum = std::dynamic_pointer_cast<Sum>(expr)) {
    //             aggregateExpr.push_back(std::make_shared<SumExpression>(createPhysicalExpr(sum->expr,
    //             aggregate->input)));
    //         }
    //         else {
    //             throw std::invalid_argument("Unsupported aggregate function");
    //         }
    //     }
    //     return std::make_shared<HashAggregateExec>(input, groupExpr, aggregateExpr,
    //     aggregate->schema());
    // }
    else {
      throw std::invalid_argument("Unsupported logical plan");
    }
  }

  static std::shared_ptr<Expression> createPhysicalExpr(std::shared_ptr<LogicalExpr> expr, std::shared_ptr<LogicalPlan> input) {
    if (auto le = std::dynamic_pointer_cast<Literal>(expr)) {
        return std::make_shared<LiteralExpression>(le->value);
    }
    else if (auto column_index = std::dynamic_pointer_cast<ColumnIndex>(expr)) {
        return std::make_shared<ColumnExpression>(column_index->index);
    } 
    else if (auto column = std::dynamic_pointer_cast<Column>(expr)) {
        int index = input->schema().indexOfFirst(column->name);
         if (index == -1) {
            throw std::runtime_error("No column named: " + column->name);
        }
        return std::make_shared<ColumnExpression>(index);
    } else if (auto be = std::dynamic_pointer_cast<BinaryExpr>(expr)) {
        auto l = createPhysicalExpr(be->l_, input);
        auto r = createPhysicalExpr(be->r_, input);

        if (auto eq = std::dynamic_pointer_cast<Eq>(expr)) {
            return std::make_shared<EqExpression>(l, r);
        }
        else if (auto neq = std::dynamic_pointer_cast<Neq>(expr)) {
            return std::make_shared<NeqExpression>(l, r);
        }
        else if (auto gt = std::dynamic_pointer_cast<Gt>(expr)) {
            return std::make_shared<GtExpression>(l, r);
        }
        else if (auto gte = std::dynamic_pointer_cast<GtEq>(expr)) {
            return std::make_shared<GtEqExpression>(l, r);
        }
        else if (auto lt = std::dynamic_pointer_cast<Lt>(expr)) {
            return std::make_shared<LtExpression>(l, r);
        }
        else if (auto lte = std::dynamic_pointer_cast<LtEq>(expr)) {
            return std::make_shared<LtEqExpression>(l, r);
        }
        else if (auto andExpr = std::dynamic_pointer_cast<And>(expr)) {
            return std::make_shared<AndExpression>(l, r);
        }
        else if (auto orExpr = std::dynamic_pointer_cast<Or>(expr)) {
            return std::make_shared<OrExpression>(l, r);
        }
    }
  }
};

}  // namespace datafusionx