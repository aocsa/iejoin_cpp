#include <cinttypes>
#include <iostream>

#include "dataframe/dataframe.h"
#include "dataframe/iejoin.h"

#include <array>
#include <iostream>
#include <map>
#include <string>

#include <boost/dynamic_bitset.hpp>
#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <vector>

void distributed_iejoin_employees(std::string_view csv_file_path) {

  // create an empty Dataframe object
  DataFrame employees;
  // recreate a Dataframe object from csv file or another
  employees.read_csv(csv_file_path);

  //  std::cout << employees << std::endl;

  // std::cout << employees << std::endl;
  std::vector<Predicate> preds = {{"op1", kOperator::kLess, "salary", "salary"},
                                  {"op2", kOperator::kGreater, "tax", "tax"}};

  auto actual = ScalableIEJoin(employees, employees, preds);
  // LoopJoin.sz: 101
  // IEJoin.sz: 101
  // ScalableIEJoin.sz: ??
  std::cerr << "ScalableIEJoin.sz: " << actual.size() << std::endl;
}

void distributed_loop_join_employees(std::string_view filename) {

  // create an empty Dataframe object
  DataFrame employees;
  // recreate a Dataframe object from csv file or another
  employees.read_csv(filename);

  //  std::cout << employees << std::endl;

  // std::cout << employees << std::endl;
  Predicate pred = {"op1", kOperator::kEqual, "salary", "tax"};

  auto actual = ScalableLoopJoin(employees, employees, {pred}, 1);
  //    virtual_cross_join: 2 x 2
  //    cross_join_result.sz: 2
  //    ScalableIEJoin.sz: 47?

  // ScalableIEJoin.sz: 97  expected!!!
  std::cerr << "ScalableIEJoin.sz: " << actual.size() << std::endl;
}

int main(int argc, char *argv[]) {
  // Initialize Google’s logging library.


   // print the other arguments
   if (argc == 2 || argc == 3) {
     std::cout << "filename "  << ": " << argv[1] << std::endl;
     std::string_view csv_file_path = argv[1];
     std::string_view test_name = argc == 3 ? argv[2] : "distributed_iejoin_employees";
     std::cout << "test_name "  << ": " << test_name << std::endl;
     if (csv_file_path.contains(".csv")){
      if (test_name == "iejoin") {
          test_iejoin_employees(csv_file_path);
      } else if (test_name == "distributed_iejoin") {
          distributed_iejoin_employees(csv_file_path);
      } else if (test_name == "distributed_loop_join_employees"){
          distributed_loop_join_employees(csv_file_path);
      } else {
          std::cerr << "distributed_iejoin_employees: \n";
          distributed_iejoin_employees(csv_file_path);
      }
     }
   }

   return 0;
}