#pragma once

#include "datasource/csv_datasource.h"
#include "datasource/logical_expr.h"
#include "std/core.hpp"

namespace datafusionx {

class DataFrame {
 public:
  virtual ~DataFrame() = default;

  virtual std::shared_ptr<DataFrame> project(
      const std::vector<std::shared_ptr<LogicalExpr>>& expr) = 0;

  virtual std::shared_ptr<DataFrame> filter(const std::shared_ptr<LogicalExpr>& expr) = 0;

  // virtual std::shared_ptr<DataFrame> aggregate(const
  // std::vector<std::shared_ptr<LogicalExpr>>& groupBy, const std::vector<AggregateExpr>&
  // aggregateExpr) = 0;

  virtual Schema schema() = 0;

  virtual std::shared_ptr<LogicalPlan> logicalPlan() = 0;
};

class DataFrameImpl : public DataFrame {
 private:
  std::shared_ptr<LogicalPlan> plan;

 public:
  explicit DataFrameImpl(std::shared_ptr<LogicalPlan> plan) : plan(std::move(plan)) {}

  std::shared_ptr<DataFrame> project(
      const std::vector<std::shared_ptr<LogicalExpr>>& expr) override {
    return std::make_shared<DataFrameImpl>(std::make_shared<Projection>(plan, expr));
  }

  std::shared_ptr<DataFrame> filter(const std::shared_ptr<LogicalExpr>& expr) override {
    return std::make_shared<DataFrameImpl>((std::make_shared<Selection>(plan, expr)));
  }

  // std::shared_ptr<DataFrame> aggregate(const std::vector<std::shared_ptr<LogicalExpr>>&
  // groupBy, const std::vector<AggregateExpr>& aggregateExpr) override {
  //     return std::make_shared<DataFrameImpl>((std::make_shared<Aggregate>(plan,
  //     groupBy, aggregateExpr));
  // }

  Schema schema() override { return plan->schema(); }

  std::shared_ptr<LogicalPlan> logicalPlan() override { return plan; }
};

}  // namespace datafusionx