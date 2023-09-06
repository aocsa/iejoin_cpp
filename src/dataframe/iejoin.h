#pragma once
#include <cinttypes>
#include <iostream>

#include "dataframe.h"

#include <array>
#include <iostream>
#include <map>
#include <string>

#include <boost/dynamic_bitset.hpp>
#include <deque>
#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <vector>
#include "utils/pprint.h"

namespace iejoin{
  
using DataType = int;

using DataFrame = frame::Dataframe<DataType>;
using RowArray = DataFrame::RowArray;
using ColumnArray = DataFrame::ColumnArray;
using StringArray = std::vector<std::string>;

struct Metadata {
  std::string col_name;
  long int min;
  long int max;
};

// PrintArray function
void PrintArray(std::string_view name, const DataFrame& L) {
  std::cout << name << ":" << std::endl;
  std::cout << L << std::endl;
}

// PrintArray function
template <typename Container>
void PrintArray(std::string_view name, const Container& L) {
  std::cout << name << ":" << std::endl;
  for (const auto& row : L) {
    std::cout << row << " ";
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
  std::string predicate_id;
  kOperator operator_type;
  std::string lhs;
  std::string rhs;

  std::function<bool(DataType, DataType)> condition_fn() const {
    return get_operator_fn(operator_type);
  }
};

// create a view  and the <iota | view>
DataFrame Project(const DataFrame& table, const StringArray& cols) {
  // Project the predicate columns and a row id as tuples: (rid, X, ...)
  DataFrame result = DataFrame::make_empty(table.num_rows());

  for (auto col_name : cols) {
    auto index = table.col_index(col_name);

    DataFrame::ColumnArray column = table.get_column(index);
    result.insert(col_name, column);
  }
  return result;
}

void Mark(DataFrame& L) {
  // Add a marked column that will become P
  // [ ... P]
  std::vector<DataFrame::DataType> p_values;
  p_values.reserve(L.num_rows());
  for (int p = 0; p < L.num_rows(); p++) {
    p_values.push_back(p);
  }
  L.insert("p", p_values);
}

ColumnArray ExtractColumn(const DataFrame& table, const int& column) {
  return table.get_column(column);
}

std::vector<std::tuple<int, int>> LoopJoin(const DataFrame& left, const DataFrame& right,
                                           const std::vector<Predicate>& preds,
                                           int trace = 0) {
  std::vector<std::tuple<int, int>> result;

  for (size_t i = 0; i < left.num_rows(); i++) {
    for (size_t j = 0; j < right.num_rows(); j++) {
      const RowArray& left_row = left.get_row(i);
      const RowArray& right_row = right.get_row(j);
      bool matching = true;
      for (const Predicate& pred : preds) {
        auto left_row_index = left.col_index(pred.lhs);
        auto right_row_index = right.col_index(pred.rhs);
        if (!pred.condition_fn()(left_row[left_row_index], right_row[right_row_index])) {
          matching = false;
          break;
        }
      }
      if (matching) {
        // TODO: add id in read_csv
        //        assert(left.col_index("row_index") == 0);
        //        assert(right.col_index("row_index") == 0);
        result.emplace_back(left_row[0],
                            right_row[0]);  // get id from column row_index
      }
    }
  }
  return result;
}

// // implement a hash-join algorithm for two tables
// // TODO: improve api... this algorithm assumes that the first column is the id
// std::vector<std::tuple<int, int>> HashJoin(const DataFrame &left, // should be a
// ColumnArray??
//                                            const DataFrame &right,
//                                            const std::vector<Predicate> &preds,
//                                            int trace = 0) {
//   std::unordered_map<int, RowArray> hashMap;
//   for (size_t i = 0; i < left.num_rows(); i++) {
//     const RowArray &left_row = left.get_row(i);
//     auto lhs_id = left_row[0];
//     hashMap[lhs_id] = left_row;
//   }
//   std::vector<std::tuple<int, int>> result;
//   for (size_t i = 0; i < right.num_rows(); i++) {
//     const RowArray &right_row = right.get_row(i);
//     auto rhs_id = right_row[0];
//     if (hashMap.find(rhs_id) != hashMap.end()) {
//       result.emplace_back(hashMap[rhs_id][0], rhs_id);
//     }
//   }
//   return result;
// }

std::vector<std::pair<int, int>> IESelfJoin(const DataFrame& T,
                                            const std::vector<Predicate>& preds,
                                            int trace = 0) {
  auto op1 = preds[0].condition_fn();
  auto X = preds[0].lhs;

  auto op2 = preds[1].condition_fn();
  auto Y = preds[1].lhs;
  int n = T.num_rows();
  auto op_name1 = preds[0].operator_type;
  auto op_name2 = preds[1].operator_type;

  // 1. let L1 (resp. L2) be the array of column X (resp. Y )
  DataFrame L = Project(T, {X, Y});

  // L:  [[0, 100, 6], [1, 140, 11], [2, 80, 10], [3, 90, 5]]
  if (trace) PrintArray("L", L);

  // 2. if (op1 ∈ {>, ≥}) sort L1 in descending order
  // 3.  else if (op1 ∈ {<, ≤}) sort L1 in ascending order
  bool descending1 =
      (op_name1 == kOperator::kGreater) || (op_name1 == kOperator::kGreaterEqual);
  L = L.sort_by(X, descending1);
  if (trace) PrintArray("sortLx", L);
  ColumnArray L1 = ExtractColumn(L, 1);

  Mark(L);
  if (trace) PrintArray("L", L);

  assert(L.col_index("row_index") == 0);
  auto Li = ExtractColumn(L, 0);

  // 4. if (op2 ∈ {>, ≥}) sort L2 in ascending order
  // 5.  else if (op2 ∈ {<, ≤}) sort L2 in descending order
  bool descending2 =
      (op_name2 == kOperator::kLess) || (op_name2 == kOperator::kLessEqual);

  L = L.sort_by(Y, descending2);
  if (trace) PrintArray("sortLY", L);

  ColumnArray L2 = ExtractColumn(L, 2);

  // 6. compute the permutation array P of L2 w.r.t. L1
  ColumnArray P = ExtractColumn(L, 3);

  // 7. initialize bit-array B (|B| = n), and set all bits to 0
  boost::dynamic_bitset<> B(n);
  //
  //  // 8. initialize join result as an empty list for tuple pairs
  std::vector<std::pair<int, int>> join_result;

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
  }
  return join_result;
}

std::vector<int> OffsetArray(const ColumnArray& L, const ColumnArray& Lr,
                             std::function<bool(DataType, DataType)> op) {
  std::vector<int> O(L.size(), Lr.size());
  size_t l_ = 0;
  for (size_t l = 0; l < L.size(); ++l) {
    while (l_ < Lr.size()) {
      if (op(L[l], Lr[l_])) {
        O[l] = l_;
        break;
      }
      l_++;
    }
  }
  return O;
}

void join_lists(int n, int m, const std::vector<int>& Li, const std::vector<int>& Lk,
                const std::vector<int>& L2, const std::vector<int>& L_2,
                const std::vector<int>& O1, const std::vector<int>& P,
                const std::vector<int>& Pr, bool (*op2)(int, int),
                std::vector<std::pair<int, int>>& join_result) {
  boost::dynamic_bitset<> B(n);

  int off2 = 0;
  for (int i = 0; i < m; ++i) {
    while (off2 < n) {
      if (not op2(L2[i], L_2[off2])) {
        break;
      }
      B.set(Pr[off2], true);
      off2 += 1;
    }
    int pos = P[i];
    int off1 = O1[pos];

    while (true) {
      int k = B.find_next(off1 - 1);

      if (k >= n or k == -1) {
        break;
      }
      auto t = std::make_pair(Li[i], Lk[k]);
      join_result.emplace_back(t);
      off1 = k + 1;
    }
  }
}

std::vector<std::pair<int, int>> IEJoin(const DataFrame& T, const DataFrame& Tr,
                                        const std::vector<Predicate>& preds,
                                        int trace = 0) {
  auto op1 = preds[0].condition_fn();
  auto X = preds[0].lhs;
  auto Xr = preds[0].rhs;

  auto op2 = preds[1].condition_fn();
  auto Y = preds[1].lhs;
  auto Yr = preds[1].rhs;

  int m = T.num_rows();
  int n = Tr.num_rows();

  if (trace) {
    std::cerr << "n:" << n << "|"
              << "m:" << m << std::endl;
  }

  auto op_name1 = preds[0].operator_type;
  auto op_name2 = preds[1].operator_type;

  /////////////////////////////
  DataFrame L = Project(T, {X, Y});
  DataFrame Lr = Project(Tr, {Xr, Yr});
  bool descending1 =
      (op_name1 == kOperator::kGreater) || (op_name1 == kOperator::kGreaterEqual);
  L = L.sort_by(X, descending1);
  ColumnArray L1 = ExtractColumn(L, 1);

  if (trace) PrintArray("L1:", L1);

  Mark(L);
  ////////////////////////////////
  Lr = Lr.sort_by(Xr, descending1);
  ColumnArray Lr1 = ExtractColumn(Lr, 1);
  Mark(Lr);

  if (trace) PrintArray("Lr1:", Lr1);

  ////////////////////////////////
  bool descending2 = (op_name2 == kOperator::kLess || op_name2 == kOperator::kLessEqual);
  L = L.sort_by(Y, descending2);
  auto L2 = ExtractColumn(L, 2);
  if (trace) PrintArray("L2:", L2);

  ////////////////////////////////
  assert(L.col_index("id") == 0);
  auto Li = ExtractColumn(L, 0);
  auto Lk = ExtractColumn(Lr, 0);

  Lr = Lr.sort_by(Yr, descending2);
  auto L_2 = ExtractColumn(Lr, 2);
  if (trace) PrintArray("L_2:", L_2);

  auto P = ExtractColumn(L, 3);
  auto Pr = ExtractColumn(Lr, 3);
  if (trace) {
    PrintArray("P:", P);
    PrintArray("Pr:", Pr);
  }

  auto O1 = OffsetArray(L1, Lr1, op1);
  if (trace) {
    PrintArray("O1:", O1);
  }

  // 7. initialize bit-array B (|B| = n), and set all bits to 0
  boost::dynamic_bitset<> B(n);
  //
  //  // 8. initialize join result as an empty list for tuple pairs
  std::vector<std::pair<int, int>> join_result;

  boost::dynamic_bitset<> one_bit(1, 1);

  int off2 = 0;
  for (int i = 0; i < m; ++i) {
    while (off2 < n) {
      if (not op2(L2[i], L_2[off2])) {
        break;
      }
      B.set(Pr[off2], true);
      off2 += 1;
    }
    int pos = P[i];
    int off1 = O1[pos];

    while (true) {
      int k = B.find_next(off1 - 1);

      if (k >= n or k == -1) {
        break;
      }
      auto t = std::make_pair(Li[i], Lk[k]);
      join_result.emplace_back(t);
      off1 = k + 1;
    }
  }
  return join_result;
}
// See dataframe interface reference
// https://arrow.apache.org/datafusion-python/generated/datafusion.DataFrame.html#datafusion.DataFrame.filter
void test_iejoin_employees(std::string_view filename) {
  // create an empty Dataframe object
  DataFrame employees;
  // recreate a Dataframe object from csv file or another
  employees.read_csv(filename);

  //  std::cout << employees << std::endl;

  // std::cout << employees << std::endl;
  std::vector<Predicate> preds = {{"op1", kOperator::kLess, "salary", "salary"},
                                  {"op2", kOperator::kGreater, "tax", "tax"}};

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

DataFrame transform(const std::vector<std::map<std::string, int>>& data) {
  DataFrame table;

  std::vector<std::string> header;
  for (const auto& row_data : data) {
    std::vector<DataType> values;
    for (const auto& [key, value] : row_data) {
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

bool has_intersection_values(int min_1, int max_1, int min_2, int max_2) {
  bool ret = max_1 >= min_2 && max_2 >= min_1;
  return ret;
}

struct Partition {
  int id;
  Metadata metadata_x;
  Metadata metadata_y;
};

bool has_intersection(const Metadata& min_max_1, const Metadata& min_max_2) {
  return has_intersection_values(min_max_1.min, min_max_1.max, min_max_2.min,
                                 min_max_2.max);
}

std::vector<std::pair<int, int>> virtual_cross_join_eq(std::vector<Partition>& lhs,
                                                       std::vector<Partition>& rhs,
                                                       std::string X, std::string Y,
                                                       bool trace = 1) {
  std::vector<std::pair<int, int>> result;
  for (auto& lhs_metadata : lhs) {
    for (auto& rhs_metadata : rhs) {
      if (has_intersection(lhs_metadata.metadata_x, rhs_metadata.metadata_y) and
          has_intersection(lhs_metadata.metadata_x, rhs_metadata.metadata_y)) {
        std::pair<int, int> t(lhs_metadata.id, rhs_metadata.id);
        if (trace) {
          std::cout << "has_intersection>> (" << std::get<0>(t) << ", " << std::get<1>(t)
                    << ")\n";
        }
        result.push_back(t);
      }
    }
  }
  return result;
}

std::vector<std::pair<int, int>> virtual_cross_join(std::vector<Partition>& lhs,
                                                    std::vector<Partition>& rhs,
                                                    std::string X, std::string Y,
                                                    bool trace = 1) {
  std::cerr << "virtual_cross_join: " << rhs.size() << " x_name " << lhs.size() << "\n";
  std::vector<std::pair<int, int>> result;
  for (auto& left : lhs) {
    for (auto& right : rhs) {
      if (has_intersection(left.metadata_x, right.metadata_x) and
          has_intersection(left.metadata_y, right.metadata_y)) {
        std::pair<int, int> t(left.id, right.id);
        if (trace) {
          std::cout << "has_intersection>> (" << std::get<0>(t) << ", " << std::get<1>(t)
                    << ")\n";
        }
        result.push_back(t);
      }
    }
  }
  return result;
}

void PrintMinMaxMetadata(std::vector<Partition>& lhs, std::vector<Partition>& rhs,
                         std::string X, std::string Y) {
  for (auto& lhs_metadata : lhs) {
    for (auto& rhs_metadata : rhs) {
      // Print partition
      // std::cout << "Partition: " << col_name << "|" << min << "," << max << std::endl;

      std::cout << "(" << lhs_metadata.metadata_x.min << " - "
                << lhs_metadata.metadata_x.max << ") | (" << lhs_metadata.metadata_y.min
                << " - " << lhs_metadata.metadata_y.max << ")\n";
    }
  }
}

auto CreateBatches(DataFrame& df) {
  static constexpr size_t kBucketSize = 10000;

  int num_parts =
      df.num_rows() > kBucketSize ? static_cast<int>(df.num_rows() / kBucketSize) : 2;
  return df.partition(num_parts);
}

// compute min_max values
Metadata min_max_on_sorted_column(DataFrame& df, std::string col_name) {
  Metadata result;
  auto index = df.col_index(col_name);
  auto c = df.get_column(index);
  assert(c.size() > 0);
  auto min = c[0];
  auto max = c[c.size() - 1];
  return Metadata{.col_name = col_name, .min = min, .max = max};
}

Metadata min_max(DataFrame& df, std::string col_name) {
  Metadata result;
  auto index = df.col_index(col_name);
  auto c = df.get_column(index);
  assert(c.size() > 0);
  auto min = *std::min_element(c.begin(), c.end());
  auto max = *std::max_element(c.begin(), c.end());
  return Metadata{.col_name = col_name, .min = min, .max = max};
}

std::vector<std::pair<int, int>> ScalableIEJoinUsingGlobalLocalSort(
    const DataFrame& left, const DataFrame& right, const std::vector<Predicate>& preds,
    int trace = 0) {
  auto X = preds[0].lhs;
  auto Y = preds[1].lhs;

  // insert row_index column to both dataframes
  auto lhs = Project(left, {X, Y});
  auto rhs = Project(right, {X, Y});

  auto lsh_parts = CreateBatches(lhs);
  auto rhs_parts = CreateBatches(rhs);
  auto lhs_num_parts = lsh_parts.size();
  auto rhs_num_parts = rhs_parts.size();

  auto generate_min_max_metadata = [](std::vector<DataFrame>& batches,
                                      std::string join_key) {
    DataFrame samples;
    samples.column_paste(batches[0].column_names());
    for (auto& batch : batches) {
      auto join_key_col = Project(batch, {join_key});
      size_t n_samples = (size_t)(std::ceil(join_key_col.num_rows() * 0.1));
      samples.merge(join_key_col.sample(n_samples));
    }
    samples.sort_by(join_key);
    auto partition_plan = samples.sample(batches.size());
    return partition_plan[join_key];
  };
  auto partition_plan_x_tmp = generate_min_max_metadata(lsh_parts, X);
  auto partition_plan_y_tmp = generate_min_max_metadata(rhs_parts, Y);

  std::deque<int> partition_plan_x(partition_plan_x_tmp.begin(),
                                   partition_plan_x_tmp.end());
  std::deque<int> partition_plan_y(partition_plan_y_tmp.begin(),
                                   partition_plan_y_tmp.end());

  // push front the minimum value for x_name
  partition_plan_x.push_front(std::numeric_limits<int>::min());
  // push back the maximum value for x_name
  partition_plan_x.push_back(std::numeric_limits<int>::max());

  assert(lhs_num_parts == partition_plan_x.size() - 2);
  std::vector<Partition> partitions_lhs;
  for (int i = 0; i < lhs_num_parts; ++i) {
    auto min = i < 1 ? partition_plan_x[0] : partition_plan_x[i - 1];
    auto max = partition_plan_x[i];

    auto metadata_x = Metadata{.col_name = X, .min = min, .max = max};
    auto metadata_y = min_max(lsh_parts[i], Y);
    partitions_lhs.emplace_back(
        Partition{.id = i, .metadata_x = metadata_x, .metadata_y = metadata_y});
  }

  std::vector<Partition> partitions_rhs;
  for (int i = 0; i < rhs_num_parts; ++i) {
    auto metadata_x = min_max(rhs_parts[i], X);
    auto metadata_y = min_max(rhs_parts[i], Y);
    partitions_rhs.emplace_back(
        Partition{.id = i, .metadata_x = metadata_x, .metadata_y = metadata_y});
  }
  PrintMinMaxMetadata(partitions_lhs, partitions_rhs, X, Y);
  std::vector<std::pair<int, int>> result;
  auto cross_join_result =
      virtual_cross_join(partitions_lhs, partitions_rhs, X, Y, trace);
  std::cout << "cross_join_result.sz: " << cross_join_result.size() << std::endl;
  for (int index = 0; index < cross_join_result.size(); index++) {
    auto [lhs_part_index, rhs_part_index] = cross_join_result[index];
    std::cout << "partition: [ " << lhs_part_index << "," << rhs_part_index << "]"
              << std::endl;
    auto expected =
        IEJoin(lsh_parts[lhs_part_index], rhs_parts[rhs_part_index], preds, trace);
    for (const auto& [x, y] : expected) {
      // std::cerr << "expected: (" << x_name << ", " << y_name << ")\n";
      result.emplace_back(std::make_pair(x, y));
    }
  }
  // materialize results
  return result;
}

std::vector<std::pair<int, int>> ScalableIEJoinUsingGlobalSort(
    const DataFrame& left, const DataFrame& right, const std::vector<Predicate>& preds,
    int trace = 0) {
  auto op1 = preds[0].condition_fn();
  auto X = preds[0].lhs;

  auto op2 = preds[1].condition_fn();
  auto Y = preds[1].lhs;
  auto op_name1 = preds[0].operator_type;
  auto op_name2 = preds[1].operator_type;

  // insert row_index column to both dataframes
  auto lhs = Project(left, {X, Y});
  auto rhs = Project(right, {X, Y});

  lhs = lhs.sort_by(X);
  rhs = rhs.sort_by(Y);

  auto lsh_parts = CreateBatches(lhs);
  auto rhs_parts = CreateBatches(rhs);
  auto lhs_num_parts = lsh_parts.size();
  auto rhs_num_parts = rhs_parts.size();

  std::vector<Partition> partitions_lhs;
  for (int i = 0; i < lhs_num_parts; ++i) {
    auto metadata_x = min_max_on_sorted_column(lsh_parts[i], X);
    auto metadata_y = min_max(lsh_parts[i], Y);
    partitions_lhs.emplace_back(
        Partition{.id = i, .metadata_x = metadata_x, .metadata_y = metadata_y});
  }

  std::vector<Partition> partitions_rhs;
  for (int i = 0; i < rhs_num_parts; ++i) {
    auto metadata_x = min_max(rhs_parts[i], X);
    auto metadata_y = min_max_on_sorted_column(rhs_parts[i], Y);
    partitions_rhs.emplace_back(
        Partition{.id = i, .metadata_x = metadata_x, .metadata_y = metadata_y});
  }

//  PrintMinMaxMetadata(partitions_lhs, partitions_rhs, X, Y);
  std::vector<std::pair<int, int>> result;
  auto cross_join_result =
      virtual_cross_join(partitions_lhs, partitions_rhs, X, Y, trace);
  std::cout << "cross_join_result.sz: " << cross_join_result.size() << std::endl;
  size_t left_sz = 0;
  size_t right_sz = 0;

  for (int index = 0; index < cross_join_result.size(); index++) {
    auto [lhs_part_index, rhs_part_index] = cross_join_result[index];
    std::cout << "partition: [ " << lhs_part_index << "," << rhs_part_index << "]"
              << std::endl;
    auto expected =
        IEJoin(lsh_parts[lhs_part_index], rhs_parts[rhs_part_index], preds, trace);
    for (const auto& [x, y] : expected) {
      // std::cerr << "expected: (" << x_name << ", " << y_name << ")\n";
      result.emplace_back(std::make_pair(x, y));
    }
    left_sz += lsh_parts[lhs_part_index].num_rows();
    right_sz += rhs_parts[rhs_part_index].num_rows();
  }
  std::cerr <<  " IEJOIN( " << left_sz << " x " << right_sz << ")" << std::endl;

  // materialize results
  return result;
}

std::vector<std::pair<int, int>> ScalableLoopJoin(const DataFrame &left,
                                                  const DataFrame &right,
                                                  std::vector<Predicate> &preds,
                                                  int trace = 0) {
  auto op1 = preds[0].condition_fn();
  auto X = preds[0].lhs;

  auto op2 = preds[1].condition_fn();
  auto Y = preds[1].lhs;
  auto op_name1 = preds[0].operator_type;
  auto op_name2 = preds[1].operator_type;

  // insert row_index column to both dataframes
  auto lhs = Project(left, {X, Y});
  auto rhs = Project(right, {X, Y});

  lhs = lhs.sort_by(X);
  rhs = rhs.sort_by(Y);

  auto lsh_parts = CreateBatches(lhs);
  auto rhs_parts = CreateBatches(rhs);
  auto lhs_num_parts = lsh_parts.size();
  auto rhs_num_parts = rhs_parts.size();

  std::vector<Partition> partitions_lhs;
  for (int i = 0; i < lhs_num_parts; ++i) {
    auto metadata_x = min_max_on_sorted_column(lsh_parts[i], X);
    auto metadata_y = min_max(lsh_parts[i], Y);
    partitions_lhs.emplace_back(
        Partition{.id = i, .metadata_x = metadata_x, .metadata_y = metadata_y});
  }

  std::vector<Partition> partitions_rhs;
  for (int i = 0; i < rhs_num_parts; ++i) {
    auto metadata_x = min_max(rhs_parts[i], X);
    auto metadata_y = min_max_on_sorted_column(rhs_parts[i], Y);
    partitions_rhs.emplace_back(
        Partition{.id = i, .metadata_x = metadata_x, .metadata_y = metadata_y});
  }

  PrintMinMaxMetadata(partitions_lhs, partitions_rhs, X, Y);
  std::vector<std::pair<int, int>> result;
  auto cross_join_result =
      virtual_cross_join(partitions_lhs, partitions_rhs, X, Y, trace);
  std::cout << "cross_join_result.sz: " << cross_join_result.size()
            << std::endl;
  for (int index = 0; index < cross_join_result.size(); index++) {
    auto [lhs_part_index, rhs_part_index] = cross_join_result[index];
    auto expected = LoopJoin(lsh_parts[lhs_part_index],
                             rhs_parts[rhs_part_index], preds, trace);
    for (const auto &[x, y] : expected) {
      result.emplace_back(std::make_pair(x, y));
    }
  }
  return result;
}


std::vector<std::pair<int, int>> ScalableLoopJoin(const DataFrame &left,
                                                  const DataFrame &right,
                                                  Predicate &pred,
                                                  int trace = 0) {
  auto op1 = pred.condition_fn();
  auto X = pred.lhs;
  auto Y = pred.rhs;
  auto op_name1 = pred.operator_type;

  // insert row_index column to both dataframes
  auto lhs = Project(left, {X, Y});
  auto rhs = Project(right, {X, Y});

  // why do we need to sort? so we can apply hash join
  //  lhs = lhs.sort_by(X);
  //  rhs = rhs.sort_by(Y);

  // optimize partition sort
  auto lsh_parts = CreateBatches(lhs);
  auto rhs_parts = CreateBatches(rhs);
  auto lhs_num_parts = lsh_parts.size();
  auto rhs_num_parts = rhs_parts.size();

  std::vector<Partition> partitions_lhs;
  for (int i = 0; i < lhs_num_parts; ++i) {
    partitions_lhs.emplace_back(
        Partition{.id = i, .metadata_x = min_max(lsh_parts[i], X), .metadata_y = min_max(lsh_parts[i], Y)});
  }

  std::vector<Partition> partitions_rhs;
  for (int i = 0; i < rhs_num_parts; ++i) {
    partitions_rhs.emplace_back(
        Partition{.id = i, .metadata_x = min_max(rhs_parts[i], X), .metadata_y = min_max(rhs_parts[i], Y)});
  }

  std::vector<std::pair<int, int>> result;
  auto cross_join_result =
      virtual_cross_join_eq(partitions_lhs, partitions_rhs, X, Y, trace);
  std::cout << "cross_join_result.sz: " << cross_join_result.size()
            << std::endl;
  for (int index = 0; index < cross_join_result.size(); index++) {
    auto [lhs_part_index, rhs_part_index] = cross_join_result[index];
    auto expected = LoopJoin(lsh_parts[lhs_part_index],
                             rhs_parts[rhs_part_index], {pred}, trace);
    for (const auto &[x, y] : expected) {
      result.emplace_back(std::make_pair(x, y));
    }
  }
  return result;
}


}