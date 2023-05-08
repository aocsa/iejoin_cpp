#include <cinttypes>
#include <iostream>

#include "dataframe.h"
#include "logical_expr.h"
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

using DataType = int;

using DataFrame = frame::Dataframe<DataType>;
using RowArray = DataFrame::RowArray;
using ColumnArray = DataFrame::ColumnArray;
using StringArray = std::vector<std::string>;

// PrintArray function
void PrintArray(std::string_view name, const DataFrame &L) {
  std::cout << name << ":" << std::endl;
  std::cout << L << std::endl;
}

// PrintArray function
void PrintArray(std::string_view name, const ColumnArray &L) {
  std::cout << name << ":" << std::endl;
  for (const auto &row : L) {
    // print variant value
//    std::visit([](auto &&arg) { std::cout << arg << " "; }, row);
  }
  std::cout << std::endl;
}

enum kOperator {
  kLess,
  kLessEqual,
  kGreater,
  kGreaterEqual,
  kEqual,
  kNotEqual,
};

std::function<bool(DataType, DataType)> get_operator_fn(const kOperator op) {
  // return a lambda function that takes two ints and returns a bool
  switch (op) {
  case kLess:
    return [](DataType a, DataType b) { return a < b; };
  case kLessEqual:
    return [](DataType a, DataType b) { return a <= b; };
  case kGreater:
    return [](DataType a, DataType b) { return a > b; };
  case kGreaterEqual:
    return [](DataType a, DataType b) { return a >= b; };
  case kEqual:
    return [](DataType a, DataType b) { return a == b; };
  case kNotEqual:
    return [](DataType a, DataType b) { return a != b; };
  default:
    throw std::runtime_error("Unknown operator");
  }
}

struct Predicate {
  std::string operator_ref;
  kOperator operator_name;
  std::string lhs;
  std::string rhs;

  std::function<bool(DataType , DataType)> condition() const {
    return get_operator_fn(operator_name);
  }
};

// create a view  and the <iota | view>
DataFrame ArrayOf(const DataFrame &table, const StringArray &cols) {
  // Project the predicate columns and a row id as tuples: (rid, X, ...)
  DataFrame result = DataFrame::create_empty_dataframe(table.num_rows());
  // Add a row id column
  result.create_row_index();

  for (auto col_name : cols) {
    auto index = table.col_index(col_name);

    DataFrame::ColumnArray column = table.get_column(index);
    result.insert(col_name, column);
  }
  return result;
}

void Mark(DataFrame &L) {
  // Add a marked column that will become P
  // [ ... P]
  std::vector<DataFrame::DataType> p_values;
  p_values.reserve(L.num_rows());
  for (int p = 0; p < L.num_rows(); p++) {
    p_values.push_back(p);
  }
  L.insert("p", p_values);
}

ColumnArray ExtractColumn(const DataFrame &table, const int &column) {
  return table.get_column(column);
}

std::vector<std::tuple<int, int>>
LoopJoin(const DataFrame &left, const DataFrame &right,
         const std::vector<Predicate> &preds) {
  std::vector<std::tuple<int, int>> result;

  for (size_t i = 0; i < left.num_rows(); i++) {
    for (size_t j = 0; j < right.num_rows(); j++) {
      const RowArray &left_row = left.get_row(i);
      const RowArray &right_row = right.get_row(j);
      bool matching = true;
      for (const Predicate &pred : preds) {
        auto left_row_index = left.col_index(pred.lhs);
        auto right_row_index = right.col_index(pred.rhs);
        if (!pred.condition()(left_row[left_row_index],
                              right_row[right_row_index])) {
          matching = false;
          break;
        }
      }
      if (matching) {
        result.emplace_back(i, j);
      }
    }
  }
  return result;
}

std::vector<std::pair<int, int>>
IESelfJoin(const DataFrame &T, const std::vector<Predicate> &preds, int trace = 0) {
  auto op1 = preds[0].condition();
  auto X = preds[0].lhs;

  auto op2 = preds[1].condition();
  auto Y = preds[1].lhs;
  int n = T.num_rows();
  auto op_name1 = preds[0].operator_name;
  auto op_name2 = preds[1].operator_name;

  // 1. let L1 (resp. L2) be the array of column X (resp. Y )
  DataFrame L = ArrayOf(T, {X, Y});

  // L:  [[0, 100, 6], [1, 140, 11], [2, 80, 10], [3, 90, 5]]
  if (trace)
    PrintArray("L", L);

  // 2. if (op1 ∈ {>, ≥}) sort L1 in descending order
  // 3.  else if (op1 ∈ {<, ≤}) sort L1 in ascending order
  bool descending1 = (op_name1 == kOperator::kGreater) ||
                     (op_name1 == kOperator::kGreaterEqual);

  //  0 100 6
  //  1 140 11
  //  2 80 10
  //  3 90 5

  //  std::sort(L.begin(), L.end(),
  //            [&descending1, &X](const auto &a, const auto &b) {
  //              auto first = std::make_pair(a[1], a[0]);
  //              auto second = std::make_pair(b[1], b[0]);
  //              return descending1 ? first > second : first < second;
  //            });
  L = L.sort_by(X, descending1);
  if (trace)
    PrintArray("sortLx", L);
  ColumnArray L1 = ExtractColumn(L, 1);

  Mark(L);
  if (trace)
    PrintArray("L", L);

  auto Li = ExtractColumn(L, 0);

  // 4. if (op2 ∈ {>, ≥}) sort L2 in ascending order
  // 5.  else if (op2 ∈ {<, ≤}) sort L2 in descending order
  bool descending2 =
      (op_name2 == kOperator::kLess) || (op_name2 == kOperator::kLessEqual);

  //  std::sort(L.begin(), L.end(),
  //            [&descending2, &Y](const auto &a, const auto &b) {
  //              auto first = std::make_pair(a[2], a[0]);
  //              auto second = std::make_pair(b[2], b[0]);
  //              return descending2 ? first > second : first < second;
  //            });
  //
  L = L.sort_by(Y, descending2);
  if (trace)
    PrintArray("sortLY", L);

  ColumnArray L2 = ExtractColumn(L, 2);

  // 6. compute the permutation array P of L2 w.r.t. L1
  ColumnArray P = ExtractColumn(L, 3);

  // 7. initialize bit-array B (|B| = n), and set all bits to 0
  boost::dynamic_bitset<> B(n);

  // 8. initialize join result as an empty list for tuple pairs
  std::vector<std::pair<int, int>> join_result;
  if (trace) {
    //  L1: [140, 100, 90, 80]
    PrintArray("L1", L1);

    //  L2: [11, 10, 6, 5]
    PrintArray("L2", L2);

    //  Li: [1, 0, 3, 2]
    PrintArray("Li", Li);

    //  P: [0, 3, 1, 2]
    PrintArray("P", P);
  }
  std::cout << "how many rows: " << n << std::endl;
  // 11. for(i←1 to n) do
  int off2 = 0;
  boost::dynamic_bitset<> one_bit(1, 1);

  for (int i = 0; i < n; ++i) {
    // 16. B[pos] ← 1
    // This has to come first or we will never join the first tuple.
    while (off2 < n) {
      if (!op2(L2[i], L2[off2])) {
        break;
      }
      B.set(P[off2], true);
      off2 += 1;
    }
    if (trace) {
      //   std::cerr << "B: " << i << " " << B.to_ulong() << std::endl;
    }

    // 12. pos ← P[i]
    int pos = P[i];

    // 9.  if (op1 ∈ {≤,≥} and op2 ∈ {≤,≥}) eqOff = 0
    // 10. else eqOff = 1
    // No, because there could be more than one equal value.
    // Scan the neighborhood instead
    int off1 = pos;
    while (op1(L1[off1], L1[pos]) && off1 > 0) {
      off1 -= 1;
    }
    while (off1 < n && !op1(L1[pos], L1[off1])) {
      off1 += 1;
    }

    // 13. for (j ← pos+eqOff to n) do
    while (true) {
      // 14. if B[j] = 1 then
      int j = B.find_next(off1 - 1);

      if (j >= n or j == -1) {
        break;
      }

      // 15. add tuples w.r.t. (L1[j], L1[i]) to join result
      if (trace) {
        std::cerr << "j,i': " << j << "," << i << std::endl;
      }
      auto t = std::make_pair(Li[pos], Li[j]);
      join_result.emplace_back(t);
      off1 = j + 1;
    }
    // std::cerr << "i: " << i << std::endl;
  }
  return join_result;
}

// See dataframe interface reference
// https://arrow.apache.org/datafusion-python/generated/datafusion.DataFrame.html#datafusion.DataFrame.filter
void test_employees() {

  // create an empty Dataframe object
  DataFrame employees;
  // recreate a Dataframe object from csv file or another
  employees.read_csv(
      "/Users/aocsa/git/iejoin/employees10k.csv");

//  std::cout << employees << std::endl;

  // std::cout << employees << std::endl;
  std::vector<Predicate> preds = {{"op1", kOperator::kLess, "salary", "salary"},
                                  {"op2", kOperator::kGreater, "tax", "tax"}};

  // auto sort_df = employees.sort_by("name");
  // std::cout << sort_df << std::endl;

  // auto select_df = employees.select({"name", "salary"});
  // std::cout << "select_df:" << select_df << std::endl;

  // auto parts = employees.partition(2);
  // for (auto &part : parts) {
  //   std::cout << part << std::endl;
  // }

  auto actual = IESelfJoin(employees, preds, 0);

  std::cerr << "actual.sz: " << actual.size() << std::endl;
  // for (const auto &[l, r] : actual) {
  //   printf("({%d}, {%d})\n", l, r);
  // }

  // auto a = std::make_shared<Column>("name");
  // auto b = std::make_shared<Literal>("John");
  // auto logical_expr = eq(a, b);

  //    auto filter_df = employees.filter(logical_expr);
}

DataFrame transform(const std::vector<std::map<std::string, int>> &data) {
  DataFrame table;

  std::vector<std::string> header;
  for (const auto &row_data : data) {
    std::vector<DataType> values;
    for (const auto &[key, value] : row_data) {
      header.push_back(key);
      values.push_back(value);
    }
    if (table.num_cols() == 0) {
      table.column_paste(header);
    }
    table.append(values);
  }
  return table;
}

void test_west() {
  std::vector<std::map<std::string, int>> west_dict = {
      {{{"row_index", 0}, {"t_id", 404}, {"time", 100}, {"cost", 6}, {"cores", 4}},
       {{"row_index", 1}, {"t_id", 498}, {"time", 140}, {"cost", 11}, {"cores", 2}},
       {{"row_index", 2}, {"t_id", 676}, {"time", 80}, {"cost", 10}, {"cores", 1}},
       {{"row_index", 3}, {"t_id", 742}, {"time", 90}, {"cost", 5}, {"cores", 4}}}};

  DataFrame west = transform(west_dict);

  std::cout << west << std::endl;
  std::vector<Predicate> preds = {{"op1", kOperator::kGreater, "time", "time"},
                                  {"op2", kOperator::kLess, "cost", "cost"}};

  auto expected = LoopJoin(west, west, preds);

  std::cerr << "expected.sz: " << expected.size() << std::endl;
  for (const auto &[l, r] : expected) {
    printf("({%d}, {%d})\n", l, r);
  }

  auto actual = IESelfJoin(west, preds, 0);

  std::cerr << "actual.sz: " << actual.size() << std::endl;
  for (const auto &[l, r] : actual) {
    printf("({%d}, {%d})\n", l, r);
  }
}

int main(int argc, char *argv[]) {
  // Initialize Google’s logging library.
//  test_west();

  test_employees();

//  std::map<int, std::string> m = {{1, "one"}, {2, "two"}, {3, "three"}};
//
//  bool contains_key = m.contains(1); // contains_key = true
//
//  std::vector<int> nums = {1, 2, 3, 4, 5};
//
//  auto tail_nums = nums | std::views::filter([](int i) { return i > 3; }) |
//                   std::views::transform([](int i) { return i * 2; }) |
//                   std::views::take(2) | std::views::reverse;
//  for (const auto& num : tail_nums) {
//    std::cout << num << " ";
//  }
//  std::cout << '\n';

  return 0;
}