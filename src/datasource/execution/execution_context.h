#pragma once

#include "datasource/csv_datasource.h"
#include "datasource/execution/dataframe_impl.h"
#include "datasource/execution/query_planner.h"
#include "datasource/logical_expr.h"
#include "std/core.hpp"

namespace datafusionx {
class ExecutionContext {
 private:
  std::map<std::string, std::string> settings;
  std::map<std::string, std::shared_ptr<DataFrame>> tables;
  int batchSize;

 public:
  ExecutionContext(const std::map<std::string, std::string>& settings)
      : settings(settings) {
    batchSize = std::stoi(
        this->settings.try_emplace("ballista.csv.batchSize", "1024").first->second);
  }

  std::shared_ptr<DataFrame> csv(const std::string& filename) {
    return std::make_shared<DataFrameImpl>(std::make_shared<Scan>(
        filename, CsvDataSource(filename, std::nullopt, true, batchSize),
        std::vector<int>()));
  }

  void register_table(const std::string& tablename,
                      const std::shared_ptr<DataFrame>& df) {
    tables[tablename] = df;
  }

  void registerDataSource(const std::string& tablename, const DataSource& datasource) {
    register_table(tablename, std::make_shared<DataFrameImpl>(std::make_shared<Scan>(
                                  tablename, datasource, std::vector<int>())));
  }

  void registerCsv(const std::string& tablename, const std::string& filename) {
    register_table(tablename, csv(filename));
  }

  std::vector<std::shared_ptr<RecordBatch>> execute(
      const std::shared_ptr<DataFrame>& df) {
    return execute(df->logicalPlan());
  }

  std::vector<std::shared_ptr<RecordBatch>> execute(
      const std::shared_ptr<LogicalPlan>& plan) {
    // auto optimizedPlan = Optimizer().optimize(plan);
    auto physicalPlan = QueryPlanner::createPhysicalPlan(plan);
    return physicalPlan->execute();
  }
};

}  // namespace datafusionx