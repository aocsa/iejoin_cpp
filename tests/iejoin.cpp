
#include <gtest/gtest.h>
#include <iostream>

#include <map>
#include <string>
#include <tuple>
#include <vector>
#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <algorithm>

#include "dataframe/dataframe.h"
#include "dataframe/iejoin.h"

void test_west() {
  std::vector<std::map<std::string, int>> west_dict = {{{{"row_index", 0},
                                                         {"t_id", 404},
                                                         {"time", 100},
                                                         {"cost", 6},
                                                         {"cores", 4}},
                                                        {{"row_index", 1},
                                                         {"t_id", 498},
                                                         {"time", 140},
                                                         {"cost", 11},
                                                         {"cores", 2}},
                                                        {{"row_index", 2},
                                                         {"t_id", 676},
                                                         {"time", 80},
                                                         {"cost", 10},
                                                         {"cores", 1}},
                                                        {{"row_index", 3},
                                                         {"t_id", 742},
                                                         {"time", 90},
                                                         {"cost", 5},
                                                         {"cores", 4}}}};

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


void distributed_iejoin_sample() {
  // R data
  std::vector<int> r_x = {5, 6, 7, 1, 2, 3};
  std::vector<int> r_y = {0, 1, 2, 3, 4, 5};

  // S data
  std::vector<int> s_x = {0, 2, 3, 1};
  std::vector<int> s_y = {0, 1, 7, 8};

  DataFrame R = DataFrame::create_empty_dataframe(r_x.size());
  R.create_row_index();
  R.insert("x", r_x);
  R.insert("y", r_y);

  DataFrame S = DataFrame::create_empty_dataframe(s_x.size());
  S.create_row_index();
  S.insert("x", s_x);
  S.insert("y", s_y);

  std::vector<Predicate> preds = {{"op1", kOperator::kLess, "x", "x"},
                                  {"op2", kOperator::kGreater, "y", "y"}};

  //  auto expected = IEJoin(R, S, preds ,1);
  auto expected = ScalableIEJoin(R, S, preds, 1);

  std::cerr << "ScalableIEJoin.sz: " << expected.size() << std::endl;
  for (const auto &[l, r] : expected) {
    printf("({%d}, {%d})\n", l, r);
  }
}

TEST(IEJoinTest, test_west) {
  test_west();
  EXPECT_EQ(2, 1 + 1);
}

TEST(IEJoinTest, distributed_iejoin_sample) {
  distributed_iejoin_sample();
  EXPECT_EQ(2, 1 + 1);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}