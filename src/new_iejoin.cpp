#include <iostream>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <glog/logging.h>
#include "utils/pprint.h"


#include "datasource/csv_datasource.h"
#include "datasource/parquet_datasource.h"
#include "datasource/logical_expr.h"
#include "datasource/execution/query_planner.h"

/*
        SELECT r.id, s.id
        FROM employees r, employees s
        WHERE r.salary < s.salary AND r.tax > s.tax
*/

using namespace datafusionx;


int main(int argc, char *argv[]) {
  const auto parquet_file_path = "/Users/aocsa/git/iejoin/employees.parquet";
  const auto csv_file_path = "/Users/aocsa/git/iejoin/new_employees1000k.csv";

// auto left_data_source = std::make_shared<ParquetDataSource>(parquet_file_path);
// auto right_data_source = std::make_shared<ParquetDataSource>(parquet_file_path);

   auto left_data_source = std::make_shared<CsvDataSource>(csv_file_path, std::nullopt, true, 10000);
   auto right_data_source = std::make_shared<CsvDataSource>(csv_file_path, std::nullopt, true, 10000);

  std::vector<std::string> emptyList;
  auto left_scan_plan = std::make_shared<Scan>("employee1", left_data_source, emptyList);  
  auto right_scan_plan = std::make_shared<Scan>("employee2", right_data_source, emptyList);  

  auto left_sort_and_sample = std::make_shared<GlobalSort>(left_scan_plan, std::initializer_list<std::shared_ptr<ColumnIndex>>{col_index(3)});
  auto right_sort_and_sample = std::make_shared<GlobalSort>(right_scan_plan,std::initializer_list<std::shared_ptr<ColumnIndex>>{col_index(3)});
  
   auto repartition_01 = std::make_shared<Repartition>(left_sort_and_sample, 10);
   auto repartition_02 = std::make_shared<Repartition>(right_sort_and_sample, 10);

  // AND(<($1, $4), >($2, $5))
  auto left_op =  std::make_shared<Lt>(col_index(3), col_index(8));
  auto right_op =  std::make_shared<Gt>(col_index(4), col_index(9));
  auto join_condition = std::make_shared<And>(left_op, right_op);
  auto query_plan = std::make_shared<IEJoinMethod2>(repartition_01, repartition_02, join_condition);  
  
  auto physicalPlan = QueryPlanner::createPhysicalPlan(query_plan);
  std::cerr << physicalPlan->pretty() << std::endl;
  auto ret =  physicalPlan->execute();
  for (auto r : ret) {
      std::cerr << r->toString() << std::endl;
      std::cerr << std::endl;
  }
  return 0;
}