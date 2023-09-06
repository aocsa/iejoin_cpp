// Created by Alexander Ocsa on 11/05/23.
//  
#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "utils/pprint.h"  

#include "datasource/csv_datasource.h"
#include "datasource/parquet_datasource.h"
#include "datasource/logical_expr.h"
#include "datasource/execution/query_planner.h"

using namespace datafusionx;


int main() {
  const auto parquet_file_path = "/Users/aocsa/git/iejoin/employees.parquet";
  const auto csv_file_path = "/Users/aocsa/git/iejoin/employees10k.csv";

  // create a plan to represent the data source
  auto data_source = std::make_shared<CsvDataSource>(csv_file_path, std::nullopt, true, 1000);
  // auto data_source = std::make_shared<ParquetDataSource>(parquet_file_path);

  // create a plan to represent the scan of the data source (FROM)
  std::vector<std::string> emptyList;
  auto scan_plan = std::make_shared<Scan>("employee", data_source, emptyList);  

  // QUERY:
  // SELECT * from employee WHERE salary < 500 OR tax = 99640

  // create a plan to represent the selection (WHERE)
  auto salary_filter_expression = std::make_shared<Lt>(col("salary"), lit(500L));
  auto tax_filter_expression = std::make_shared<Eq>(col("tax"), lit(99640L));
  auto filter_expression = std::make_shared<Or>(salary_filter_expression, tax_filter_expression);
  auto selection_plan = std::make_shared<Selection>(scan_plan, filter_expression);

  // create a plan to represent the projection (SELECT)
  std::vector<std::shared_ptr<LogicalExpr>> projection_columns = {col("id"),  col("salary"), col("tax"), col("name")};
  auto query_plan = std::make_shared<Projection>(selection_plan, projection_columns);  

  
  auto physicalPlan = QueryPlanner::createPhysicalPlan(query_plan);
  std::cerr << physicalPlan->pretty() << std::endl;

  auto ret =  physicalPlan->execute();

  for (auto r : ret) {
      std::cerr << r->toString() << std::endl;
      std::cerr << std::endl;
  }
  return 0;
}
