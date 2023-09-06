#include <iostream>
#include <string>
#include <vector>

#include "dataframe/dataframe.h"
#include "dataframe/iejoin.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include "utils/pprint.h"

using namespace iejoin;
/*
        SELECT r.id, s.id
        FROM employees r, employees s
        WHERE r.salary < s.salary AND r.tax > s.tax
*/

void scalable_iejoin_employees(std::string_view csv_file_path) {

  // create an empty Dataframe object
  DataFrame employees;
  // recreate a Dataframe object from csv file or another
  employees.read_csv(csv_file_path);
  employees.select({"salary", "tax"});
 
  std::vector<Predicate> preds = {{"op1", kOperator::kLess, "salary", "salary"},
                                  {"op2", kOperator::kGreater, "tax", "tax"}};

  auto actual = ScalableIEJoinUsingGlobalSort(employees, employees, preds);
  // LoopJoin.sz: 101
  // IEJoin.sz: 101
  // ScalableIEJoin.sz: 101
  std::cerr << "ScalableIEJoin.sz: " << actual.size() << std::endl;
}

void scalable_loop_join_employees(std::string_view filename) {

  // create an empty Dataframe object
  DataFrame employees;
  // recreate a Dataframe object from csv file or another
  employees.read_csv(filename);

  //  std::cout << employees << std::endl;

  // std::cout << employees << std::endl;
  Predicate pred = {"op1", kOperator::kEqual, "salary", "tax"};

  std::vector<Predicate> preds = {{"op1", kOperator::kLess, "salary", "salary"},
                                  {"op2", kOperator::kGreater, "tax", "tax"}};


  auto actual = ScalableLoopJoin(employees, employees, preds, 1);
  //    virtual_cross_join: 2 x_name 2
  //    cross_join_result.sz: 2
  //    ScalableIEJoin.sz: 47?

  // ScalableIEJoin.sz: 97  expected!!!
  std::cerr << "ScalableIEJoin.sz: " << actual.size() << std::endl;
}

int main(int argc, char *argv[]) {
   if (argc == 2 || argc == 3) {
     std::cout << "filename "  << ": " << argv[1] << std::endl;
     std::string_view csv_file_path = argv[1];
     std::string_view test_name = argc == 3 ? argv[2] : "scalable_iejoin";
     std::cout << "test_name "  << ": " << test_name << std::endl;
     if (csv_file_path.find(".csv") != std::string::npos){
      if (test_name == "iejoin") {
          test_iejoin_employees(csv_file_path);
      } else if (test_name == "scalable_iejoin") {
          scalable_iejoin_employees(csv_file_path);
      } else if (test_name == "scalable_loop_join_employees") {
          scalable_loop_join_employees(csv_file_path);
      } else {
          std::cerr << "scalable_iejoin_employees: \n";
          scalable_iejoin_employees(csv_file_path);
      }
     }
   }
   return 0;
}

