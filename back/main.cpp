#include <array>
#include <iostream>
#include <map>
#include <string>

#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <vector>
#include <boost/dynamic_bitset.hpp>

#include "read_csv.h"

struct Predicate {
  std::string operator_type;
  std::function<bool(int, int)> op_function;
  std::string lhs;
  std::string rhs;
};


using Row = std::map<std::string, int>;
using Table = std::vector<Row>;
using IntTable = std::vector<std::vector<int>>;
using ColumnNames = std::vector<std::string>;
using Identifier = std::function<int(int)>;

std::vector<int> ExtractColumn(const IntTable &table, const int &column) {
  std::vector<int> result;

  for (const auto &row : table) {
    result.push_back(row.at(column));
  }
  return result;
}

IntTable Project(
    const Table &table, const ColumnNames &cols,
    Identifier identifier = [](int rid) { return rid; }) {
  // Project the predicate columns and a row id as tuples: (rid, X, ...)
  IntTable result;

  int rid = 0;
  for (const auto &row : table) {
    std::vector<int> row_result = {identifier(rid)};

    for (const auto &name : cols) {
      row_result.push_back(row.at(name));
    }
    result.push_back(row_result);
    ++rid;
  }
  return result;
}

IntTable Mark(IntTable &L) {
  // Add a marked column that will become P
  // [ ... P]
  int p = 0;
  for (auto &l : L) {
    l.push_back(p);
    ++p;
  }
  return L;
}

std::vector<std::tuple<int, int>>
MakePairs(const std::vector<std::tuple<Row, Row>> &pairs) {
  std::vector<std::tuple<int, int>> result;

  for (const auto &pair : pairs) {
    const Row &l = std::get<0>(pair);
    const Row &r = std::get<1>(pair);
    result.emplace_back(l.at("row"), r.at("row"));
  }

  return result;
}

std::vector<std::tuple<Row, Row>>
LoopJoin(const std::vector<Row> &left, const std::vector<Row> &right,
         const std::vector<Predicate> &preds) {
  std::vector<std::tuple<Row, Row>> result;

  for (const Row &left_row : left) {
    for (const Row &right_row : right) {
      bool matching = true;
      for (const Predicate &pred : preds) {

        if (!pred.op_function(left_row.at(pred.lhs), right_row.at(pred.rhs))) {
          matching = false;
          break;
        }
      }
      if (matching) {
        result.emplace_back(left_row, right_row);
      }
    }
  }

  return result;
}

// PrintArray function
void PrintArray(std::string_view name, const IntTable &L) {
  std::cout << name << ":" << std::endl;
  for (const auto &row : L) {
    for (const auto &col : row) {
      std::cout << col << " ";
    }
    std::cout << std::endl;
  }
}

// PrintArray function
void PrintArray(std::string_view name, const std::vector<int> &L) {
  std::cout << name << ":" << std::endl;
  for (const auto &row : L) {
    std::cout << row << " ";
  }
  std::cout << std::endl;
}

//
//int find_bit(const boost::dynamic_bitset<> &bits, const  boost::dynamic_bitset<> &pattern, int start_index = 0) {
//
//  for (int i = start_index; i < bits.size() - pattern.size() + 1; ++i) {
//    bool match = true;
//    for (int j = 0; j < pattern.size(); ++j) {
//      if (bits[i + j] != pattern[j]) {
//        match = false;
//        break;
//      }
//    }
//    if (match) return i;
//  }
//  return -1;
//}

std::vector<std::pair<int, int>>
IESelfJoin(const Table &T, const std::vector<Predicate> &preds, int trace = 0) {
  auto op1 = preds[0].op_function;
  auto op_name1 = preds[0].operator_type;
  auto X = preds[0].lhs;

  auto op2 = preds[1].op_function;
  auto op_name2 = preds[1].operator_type;
  auto Y = preds[1].lhs;
  int n = T.size();

  // 1. let L1 (resp. L2) be the array of column X (resp. Y )
  IntTable L = Project(T, {X, Y});

  // L:  [[0, 100, 6], [1, 140, 11], [2, 80, 10], [3, 90, 5]]
  if (trace)
    PrintArray("L", L);

  // 2. if (op1 ∈ {>, ≥}) sort L1 in descending order
  // 3.  else if (op1 ∈ {<, ≤}) sort L1 in ascending order
  bool descending1 = (op_name1 == ">") || (op_name1 == ">=");
  std::sort(L.begin(), L.end(),
            [&descending1, &X](const auto &a, const auto &b) {
              auto first = std::make_pair(a[1], a[0]);
              auto second = std::make_pair(b[1], b[0]);
              return descending1 ? first < second : first > second;
            });
  std::vector<int> L1 = ExtractColumn(L, 1);

  L = Mark(L);
  auto Li = ExtractColumn(L, 0);

  // 4. if (op2 ∈ {>, ≥}) sort L2 in ascending order
  // 5.  else if (op2 ∈ {<, ≤}) sort L2 in descending order
  bool descending2 = (op_name2 == "<") || (op_name2 == "<=");
  std::sort(L.begin(), L.end(),
            [&descending2, &Y](const auto &a, const auto &b) {
              auto first = std::make_pair(a[2], a[0]);
              auto second = std::make_pair(b[2], b[0]);
              return descending2 ? first < second : first > second;
            });
  std::vector<int> L2 = ExtractColumn(L, 2);

  // 6. compute the permutation array P of L2 w.r.t. L1
  std::vector<int> P = ExtractColumn(L, 3);

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

  // 11. for(i←1 to n) do
  int off2 = 0;
  boost::dynamic_bitset<> one_bit(1,1);

  for (int i = 0; i < n; ++i) {
    // 16. B[pos] ← 1
    // This has to come first or we will never join the first tuple.
    while (off2 < n) {
      if (!op2(L2[i], L2[off2])) {
        break;
      }
      B.set(P[off2]);
      off2 += 1;
    }
    if (trace) {
      std::cerr << "B: " << i << " " << B.to_ulong() << std::endl;
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
//      if (trace) {
//        std::cerr << "search since: " << off1 << " " << B.to_string() << std::endl;
//      }
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
    std::cerr << "i: " << i << std::endl;

  }
  return join_result;
}

void sample_west() {
  std::vector<Row> west = {
      {{{"row", 0}, {"t_id", 404}, {"time", 100}, {"cost", 6}, {"cores", 4}},
       {{"row", 1}, {"t_id", 498}, {"time", 140}, {"cost", 11}, {"cores", 2}},
       {{"row", 2}, {"t_id", 676}, {"time", 80}, {"cost", 10}, {"cores", 1}},
       {{"row", 3}, {"t_id", 742}, {"time", 90}, {"cost", 5}, {"cores", 4}}}};

  std::vector<Predicate> preds = {{"op1", std::greater<int>(), "time", "time"},
                                  {"op2", std::less<int>(), "cost", "cost"}};

  auto expected = MakePairs(LoopJoin(west, west, preds));

  for (const auto &[l, r] : expected) {
    printf("({%d}, {%d})\n", l, r);
  }

  auto actual = IESelfJoin(west, preds, 1);

  std::cerr << "actual.sz: " << actual.size() << std::endl;
  for (const auto &[l, r] : actual) {
    printf("({%d}, {%d})\n", l, r);
  }
}

void sample_employees() {

  std::vector<Row> employees =  read_csv("/Users/aocsa/git/iejoin/employees.csv");

  std::vector<Predicate> preds = {{"op1", std::less<int>(), "salary", "salary"},
                                  {"op2", std::greater<int>(), "tax", "tax"}};

//  auto expected = MakePairs(LoopJoin(employees, employees, preds));

//  std::cerr << "expected.sz: " << expected.size() << std::endl;

  std::cerr << "run>>>>\n";
  auto actual = IESelfJoin(employees, preds, 0);

  std::cerr << "actual.sz: " << actual.size() << std::endl;
}

int main() {
  sample_employees();

  return 0;
}