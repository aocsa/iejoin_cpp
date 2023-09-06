#pragma once

#include "datasource/csv_datasource.h"
#include "datasource/execution/dataframe_impl.h"
#include "datasource/expression_eval/expressions.h"
#include "datasource/logical_expr.h"
#include "std/core.hpp"

#include <boost/dynamic_bitset.hpp>

namespace datafusionx {

class PhysicalPlan;

class PhysicalPlan {
 public:
  virtual ~PhysicalPlan() = default;

  virtual Schema schema() = 0;

  virtual Sequence execute() = 0;

  virtual std::vector<std::shared_ptr<PhysicalPlan>> children() = 0;

  virtual std::string pretty() { return format_physical_plan(this, 0); }

  virtual std::string toString() = 0;

  // Format a physical plan in human-readable form
  std::string format_physical_plan(PhysicalPlan* plan, int indent) {
    std::string indentation(indent, '\t');
    std::string result = indentation + plan->toString() + "\n";
    for (const auto& child : plan->children()) {
      result += format_physical_plan(child.get(), indent + 1);
    }
    return result;
  }
};


// PrintArray function
void PrintArray(std::string_view name, const Table& L) {
  std::cout << name << ":" << std::endl;
  std::cout << L << std::endl;
}
template <typename Container>
void PrintArray(std::string_view name, const Container& L) {
  std::cout << name << ":" << std::endl;
  for (const auto& row : L) {
    std::cout << row << " ";
  }
  std::cout << std::endl;
}

class ScanExec : public PhysicalPlan {
 public:
  ScanExec(const std::shared_ptr<DataSource>& ds,
           const std::vector<std::string>& projection)
      : ds(ds), projection(projection) {}

  Schema schema() override { return ds->schema().select(projection); }

  std::vector<std::shared_ptr<PhysicalPlan>> children() override {
    return std::vector<std::shared_ptr<PhysicalPlan>>{};
  }

  Sequence execute() override { return ds->scan(projection); }
  std::string toString() override {
    return "ScanExec(" + ds->toString() + ", " + to_string(projection) + ")";
  }

 private:
  std::shared_ptr<DataSource> ds;
  std::vector<std::string> projection;

  // Helper function to convert vector of strings to a single string
  std::string to_string(const std::vector<std::string>& vec) {
    std::string result = "[";
    for (const auto& str : vec) {
      result += str + ", ";
    }
    if (!vec.empty()) {
      result.pop_back();  // remove the last comma
      result.back() = ']';
    } else {
      result += ']';
    }
    return result;
  }
};

class SelectionExec : public PhysicalPlan {
 public:
  SelectionExec(const std::shared_ptr<PhysicalPlan>& input,
                const std::shared_ptr<Expression>& expr)
      : input(input), expr(expr) {}

  Schema schema() override { return input->schema(); }

  std::vector<std::shared_ptr<PhysicalPlan>> children() override { return {input}; }

  Sequence execute() override {
    auto inputExec = input->execute();
    Sequence output;

    for (auto& batch : inputExec) {
      auto result = expr->evaluate(batch);
      auto& column_array =
          std::dynamic_pointer_cast<ArrowFieldVector>(result)->column_array;
      auto bitVecResult = column_array.get_as<bool>();

      auto schema = batch->schema();
      int columnCount = schema.fields.size();
      std::vector<std::shared_ptr<ColumnVector>> filteredFields;
      for (int i = 0; i < columnCount; i++) {
        auto batch_filtered = filter(batch->field(i), bitVecResult);
        filteredFields.push_back(batch_filtered);
      }
      auto record_batch = std::make_shared<RecordBatch>(schema, filteredFields);
      if (record_batch->rowCount() > 0) {
        output.emplace_back(record_batch);
      }
    }

    return output;
  }
  std::string toString() override {
    return "SelectionExec(" + input->toString() + ", " + expr->toString() + ")";
  }

 private:
  std::shared_ptr<ArrowFieldVector> filter(const std::shared_ptr<ColumnVector>& v,
                                           const std::vector<bool>& selection) {
    ColumnArray column_array;
    for (int i = 0; i < selection.size(); i++) {
      if (selection.at(i) == true) {
        column_array.emplace_back(v->getValue(i));
      }
    }
    return std::make_shared<ArrowFieldVector>(column_array);
    ;
  }

 public:
  std::shared_ptr<PhysicalPlan> input;
  std::shared_ptr<Expression> expr;
};

class ProjectionExec : public PhysicalPlan {
 public:
  ProjectionExec(std::shared_ptr<PhysicalPlan> input, Schema schema,
                 std::vector<std::shared_ptr<Expression>> expr)
      : input(std::move(input)), schema_(std::move(schema)), expr(std::move(expr)) {}

  Schema schema() override { return schema_; }

  std::vector<std::shared_ptr<PhysicalPlan>> children() override { return {input}; }

  std::string toString() override {
    return "ProjectionExec(" + input->toString() + ", " + schema_.toString() + ")";
  }

  Sequence execute() override {
    Sequence batches = input->execute();
    Sequence result;

    for (const auto& batch : batches) {
      std::vector<std::shared_ptr<ColumnVector>> columns;
      for (const auto& e : expr) {
        columns.push_back(e->evaluate(batch));
      }
      result.push_back(std::make_shared<RecordBatch>(schema_, columns));
    }

    return result;
  }

 private:
  std::shared_ptr<PhysicalPlan> input;
  Schema schema_;
  std::vector<std::shared_ptr<Expression>> expr;
};


std::shared_ptr<RecordBatch> CreateRecordBatch(const Table & input, const Schema &schema) {
  std::vector<std::shared_ptr<ColumnVector>> columns;
  for (int col_index = 0; col_index < input.num_cols(); col_index++) {
    columns.push_back(
        std::make_shared<ArrowFieldVector>(input.get_column(col_index)));
  }
  return std::make_shared<RecordBatch>(schema, columns);
}

class RepartitionExec : public PhysicalPlan {
 public:
  RepartitionExec(std::shared_ptr<PhysicalPlan> input, Schema schema,
                 int num_partitions)
      : input(std::move(input)), schema_(std::move(schema)), num_partitions(num_partitions) {}

  Schema schema() override { return schema_; }

  std::vector<std::shared_ptr<PhysicalPlan>> children() override { return {input}; }

  std::string toString() override {
    return "RepartitionExec(" + input->toString() + ", " + schema_.toString() + ")";
  }

  Sequence execute() override {
    Sequence batches = input->execute();
    Sequence result;

    // repartition all batches in num_partitions parts 
    Table table = batches[0]->toTable();
    auto n_parts = table.partition(num_partitions);
    for (auto& part : n_parts) {
      result.emplace_back(CreateRecordBatch(part, schema_));
    }
    return result;
  }

 private:
  std::shared_ptr<PhysicalPlan> input;
  Schema schema_;
  int num_partitions;
};


class SortExec : public PhysicalPlan {
 public:
  SortExec(std::shared_ptr<PhysicalPlan> input, Schema schema,
                    std::vector<int> sort_indices, bool local_sort = false)
      : input(std::move(input)), schema_(std::move(schema)), sort_indices(sort_indices), local_sort(local_sort) {}

  Schema schema() override { return schema_; }

  std::vector<std::shared_ptr<PhysicalPlan>> children() override { return {input}; }

  std::string toString() override {
    return "SortExec(" + input->toString() + ", " + schema_.toString() + ")";
  }

  Sequence execute() override {
    Sequence batches = input->execute();
    Sequence result;
    if (local_sort){
      for (const auto& batch : batches) {
        // Sort the batch
        auto sorted_batch = SortBatch(batch, sort_indices);
        result.push_back(sorted_batch);
      }
    } else {
      // global sort
      Table samples = Table::make_empty(batches[0]->schema().columnNames());
      for (const auto& batch : batches) {
        auto table = batch->toTable();
        table = table.sort_by(sort_indices[0]);
        size_t n_samples = (size_t)(std::ceil(table.num_rows() * 0.1));
        samples.merge(table.sample(n_samples));
      }
      samples = samples.sort_by(sort_indices[0]);
      auto partition_plan = samples.sample(batches.size());
      partition_plan = partition_plan.sort_by(sort_indices[0]);
      ColumnArray partition_plan_x_tmp = partition_plan.get_column(sort_indices[0]);
      std::deque<long> partition_plan_x;
      for (auto& val : partition_plan_x_tmp) {
        // TODO: fix get<type>
        partition_plan_x.emplace_back(std::get<long>(val));
      }
      // push front the minimum value for x_name
      partition_plan_x.emplace_front(std::numeric_limits<long>::min());
      // push back the maximum value for x_name
      partition_plan_x.emplace_back(std::numeric_limits<long>::max());

      for (int i = 1; i < partition_plan_x.size(); i++ ) {
        long min_x = partition_plan_x[i-1];
        long max_x =partition_plan_x[i];
        
        Table batch = Table::make_empty(batches[0]->schema().columnNames());
        size_t filtered_sz = 0;
        Table range = Table::make_empty(batches[0]->schema().columnNames());
        for (auto& batch : batches) {
          auto table = batch->toTable();
          auto index_x = sort_indices[0];
          auto filtered = table.filter([=](const RowArray& row) {
            long x_val = std::get<long>(row[index_x]);
            return min_x < x_val && x_val < max_x ;
          });
          range.merge(filtered);
        }
        if(range.num_rows() > 0) {
          range = range.sort_by(sort_indices[0]);
          result.push_back(CreateRecordBatch(range, schema_));
        }
      }
      std::cerr << "result.sz " << result.size() << std::endl;
    }
    return result;
  }

  std::shared_ptr<RecordBatch> SortBatch(std::shared_ptr<RecordBatch> batch,
                                         std::vector<int> sort_indices) {
    std::vector<std::shared_ptr<ColumnVector>> columns;
    for (const auto& index : sort_indices) {
      columns.push_back(batch->field(index));
    }
    auto sorted_indices = SortIndices(columns);
    return ReorderBatch(batch, sorted_indices);
  }

  std::vector<int> SortIndices(std::vector<std::shared_ptr<ColumnVector>> columns) {
    std::vector<int> indices;
    for (int i = 0; i < columns[0]->size(); i++) {
      indices.push_back(i);
    }
    std::sort(indices.begin(), indices.end(), [&](int i, int j) {
      for (const auto& column : columns) {
        if (column->getValue(i) < column->getValue(j)) {
          return true;
        } else if (column->getValue(i) > column->getValue(j)) {
          return false;
        }
      }
      return false;
    });
    return indices;
  }

  std::shared_ptr<RecordBatch> ReorderBatch(std::shared_ptr<RecordBatch> batch,
                                            std::vector<int> indices) {
    std::vector<std::shared_ptr<ColumnVector>> columns;
    for (int i = 0; i < batch->columnCount(); i++) {
      auto column = batch->field(i);
      auto column_array =
          std::dynamic_pointer_cast<ArrowFieldVector>(column)->column_array;
      auto reordered_column_array = ReorderColumn(column_array, indices);
      columns.push_back(std::make_shared<ArrowFieldVector>(reordered_column_array));
    }
    return std::make_shared<RecordBatch>(batch->schema(), columns);
  }

  ColumnArray ReorderColumn(ColumnArray column_array, std::vector<int> indices) {
    ColumnArray reordered_column_array;
    for (const auto& index : indices) {
      reordered_column_array.emplace_back(column_array[index]);
    }
    return reordered_column_array;
  }

 private:
  std::shared_ptr<PhysicalPlan> input;
  Schema schema_;
  std::vector<int> sort_indices;
  bool local_sort;
};

namespace iejoin_exec {
struct Metadata {
  std::string col_name;
  Table::DataType min;
  Table::DataType max;
};

struct Partition {
  int id;
  Metadata metadata_x;
  Metadata metadata_y;
};

enum kOperator {
  kLess,
  kLessEqual,
  kGreater,
  kGreaterEqual,
  kEqual,
  kNotEqual,
};

// input can be <, <=, >, >=, ==, !=
kOperator make_operator(std::string input) {
  if (input == "<") {
    return kLess;
  } else if (input == "<=") {
    return kLessEqual;
  } else if (input == ">") {
    return kGreater;
  } else if (input == ">=") {
    return kGreaterEqual;
  } else if (input == "==") {
    return kEqual;
  } else if (input == "!=") {
    return kNotEqual;
  } else {
    throw std::runtime_error("Unknown operator");
  }
}

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
  int lhs;
  int rhs;

  std::function<bool(DataType, DataType)> condition_fn() const {
    return get_operator_fn(operator_type);
  }
};

std::vector<std::tuple<int, int>> LoopJoin(const Table& left, const Table& right,
                                           const std::vector<Predicate>& preds,
                                           int trace = 0) {
  std::vector<std::tuple<int, int>> result;

  for (size_t i = 0; i < left.num_rows(); i++) {
    for (size_t j = 0; j < right.num_rows(); j++) {
      const RowArray& left_row = left.get_row(i);
      const RowArray& right_row = right.get_row(j);
      bool matching = true;
      for (const Predicate& pred : preds) {
        auto left_row_index = pred.lhs;
        auto right_row_index = pred.rhs;
        if (!pred.condition_fn()(left_row[left_row_index], right_row[right_row_index])) {
          matching = false;
          break;
        }
      }
      if (matching) {
        // TODO: add id in read_csv
        //        assert(left.col_index("id") == 0);
        //        assert(right.col_index("id") == 0);
        result.emplace_back(
            std::get<long>(left_row[0]),
            std::get<long>(right_row[0]));  // get id from column id
      }
    }
  }
  return result;
}

void Mark(Table& L) {
  // Add a marked column that will become P
  // [ ... P]
  std::vector<Table::DataType> p_values;
  p_values.reserve(L.num_rows());
  for (int p = 0; p < L.num_rows(); p++) {
    p_values.push_back(p);
  }
  L.insert("p", p_values);
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


// create a view  and the <iota | view>
Table ArrayOf(const Table &table, const std::vector<std::string> &cols) {
  // Project the predicate columns and a row id as tuples: (rid, X, ...)
  Table result = Table::make_empty(table.num_rows());

  for (auto col_name : cols) {
    auto index = table.col_index(col_name);

    Table::ColumnArray column = table.get_column(index);
    result.insert(col_name, column);
  }
  return result;
}

std::vector<std::pair<int, int>> IEJoin(const Table& T, const Table& Tr,
                                        const std::vector<Predicate>& preds,
                                        int trace = 0) {
  auto op1 = preds[0].condition_fn();
  std::string X = T.column_names().at(preds[0].lhs);
  std::string Xr =  Tr.column_names().at(preds[0].lhs);

  auto op2 = preds[1].condition_fn();
  std::string Y = T.column_names().at(preds[1].lhs);
  std::string Yr = Tr.column_names().at(preds[1].lhs);

  int m = T.num_rows();
  int n = Tr.num_rows();

  if (trace) {
    std::cerr << "n:" << n << "|"
              << "m:" << m << std::endl;
  }

  auto op_name1 = preds[0].operator_type;
  auto op_name2 = preds[1].operator_type;

  /////////////////////////////
  Table L = ArrayOf(T, {X, Y});
  Table Lr = ArrayOf(Tr, {Xr, Yr}); 
  bool descending1 =
      (op_name1 == kOperator::kGreater) || (op_name1 == kOperator::kGreaterEqual);
  L = L.sort_by(X, descending1);
  ColumnArray L1 = L.get_column(1);

  if (trace) PrintArray("L1:", L1);

  Mark(L);
  ////////////////////////////////
  Lr = Lr.sort_by(Xr, descending1);
  ColumnArray Lr1 = Lr.get_column(1);
  Mark(Lr);

  if (trace) PrintArray("Lr1:", Lr1);

  ////////////////////////////////
  bool descending2 = (op_name2 == kOperator::kLess || op_name2 == kOperator::kLessEqual);
  L = L.sort_by(Y, descending2);
  auto L2 = L.get_column(2);
  if (trace) PrintArray("L2:", L2);

  ////////////////////////////////
  assert(L.col_index("id") == 0);
  auto Li = L.get_column(0);
  auto Lk = Lr.get_column(0);

  Lr = Lr.sort_by(Yr, descending2);
  auto L_2 = Lr.get_column(2);
  if (trace) PrintArray("L_2:", L_2);

  std::vector<long> P = L.get_column(3).get_as<long>();
  std::vector<long> Pr = Lr.get_column(3).get_as<long>();
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
      auto t = std::make_pair(std::get<long>(Li[i]), std::get<long>(Lk[k]));
      join_result.emplace_back(t);
      off1 = k + 1;
    }
  }
  return join_result;
}

bool has_intersection_values(DataType min_1, DataType max_1, DataType min_2,
                             DataType max_2) {
  bool ret = max_1 >= min_2 && max_2 >= min_1;
  return ret;
}

bool has_intersection(const Metadata& min_max_1, const Metadata& min_max_2) {
  return has_intersection_values(min_max_1.min, min_max_1.max, min_max_2.min,
                                 min_max_2.max);
}

std::vector<std::pair<int, int>> virtual_cross_join_eq(std::vector<Partition>& lhs,
                                                       std::vector<Partition>& rhs,
                                                       bool trace = 1) {
  std::vector<std::pair<int, int>> result;
  for (auto& lhs_metadata : lhs) {
    for (auto& rhs_metadata : rhs) {
      if (has_intersection(lhs_metadata.metadata_x, rhs_metadata.metadata_x) and
          has_intersection(lhs_metadata.metadata_y, rhs_metadata.metadata_y)) {
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

void PrintMinMaxMetadata(std::vector<Partition>& lhs, std::vector<Partition>& rhs) {
  for (auto& lhs_metadata : lhs) {
    for (auto& rhs_metadata : rhs) {
      std::cout << lhs_metadata.id << "|" << rhs_metadata.id << "|"
                << "(" << to_string(lhs_metadata.metadata_x.min) << " - "
                << to_string(lhs_metadata.metadata_x.max) << ") | ("
                << to_string(lhs_metadata.metadata_x.min) << " - "
                << to_string(lhs_metadata.metadata_x.max) << ")\n";
    }
  }
}

Metadata min_max_metadata_sorted(Table& df, int index) {
  Metadata result;
  auto c = df.get_column(index);
  assert(c.size() > 0);
  auto min = c[0];
  auto max = c[c.size() - 1];
  return Metadata{.col_name = df.column_names()[index], .min = min, .max = max};
}

Metadata min_max_metadata(Table& df, int index) {
  Metadata result;
  auto c = df.get_column(index);
  assert(c.size() > 0);
  auto min = *std::min_element(c.begin(), c.end());
  auto max = *std::max_element(c.begin(), c.end());
  return Metadata{.col_name = df.column_names()[index], .min = min, .max = max};
}

ColumnArray generate_min_max_metadata(Sequence& batches, std::string join_key) {
  Table samples = Table::make_empty({join_key});
  for (auto& batch : batches) {
    auto table = batch->toTable();
    auto join_key_col = table.select({join_key});
    size_t n_samples = (size_t)(std::ceil(join_key_col.num_rows() * 0.1));
    samples.merge(join_key_col.sample(n_samples));
  }
  samples = samples.sort_by(join_key);
  auto partition_plan = samples.sample(batches.size());
  partition_plan = partition_plan.sort_by(join_key);
  std::cerr << "partition_plan: " << partition_plan << std::endl;
  return partition_plan[join_key];
}

Sequence IEJoinMethod2(Sequence left_batches, Sequence right_batches, int r_x, int r_y,int  s_x, int s_y, std::string x_name, std::string y_name, std::vector<Predicate> predicates) {
 
    // r_x op1 s_x AND r_y op2 s_y
    // r_x op1 s_x AND r_y op2 s_y
    Sequence result;
    std::vector<Partition> partitions_lhs;
    int id = 0;
    for (const auto& left_batch : left_batches) {
      auto sorted_left = left_batch->toTable();
      auto metadata_x = min_max_metadata_sorted(sorted_left, r_x);
      auto metadata_y = min_max_metadata(sorted_left, r_y);  // use min-max reduction
      partitions_lhs.emplace_back(
          Partition{.id = id, .metadata_x = metadata_x, .metadata_y = metadata_y});
      id++;
    }

    id = 0;
    std::vector<Partition> partitions_rhs;
    for (const auto& right_batch : right_batches) {
      auto sorted_right = right_batch->toTable();
      auto metadata_x = min_max_metadata_sorted(sorted_right, s_x);
      auto metadata_y = min_max_metadata(sorted_right, s_y);
      partitions_rhs.emplace_back(
          Partition{.id = id, .metadata_x = metadata_x, .metadata_y = metadata_y});
      id++;
    }

    // PrintMinMaxMetadata(partitions_lhs, partitions_rhs);
    std::vector<std::pair<int, int>> result_ids;
    auto cross_join_result = virtual_cross_join_eq(partitions_lhs, partitions_rhs, 1);
    std::cerr << partitions_lhs.size() << "," << partitions_rhs.size()
              << " | cross_join_result.sz: " << cross_join_result.size() << std::endl;

    size_t left_sz = 0;
    size_t right_sz = 0;
    for (int index = 0; index < cross_join_result.size(); index++) {
      auto [lhs_part_index, rhs_part_index] = cross_join_result[index];
      auto expected = IEJoin(left_batches[lhs_part_index]->toTable(),
                               right_batches[rhs_part_index]->toTable(), predicates);
      for (const auto& [x, y] : expected) {
        result_ids.emplace_back(std::make_pair(x, y));
      }
      left_sz += left_batches[lhs_part_index]->rowCount();
      right_sz += right_batches[rhs_part_index]->rowCount();
    }
    std::cerr <<  " IEJOIN( " << left_sz << " x " << right_sz << ")" << std::endl;
    std::cout << " | result_ids.sz: " << result_ids.size() << std::endl;
    return result; 
}


  std::vector<Partition> GeneratePartitionPlan(Sequence& batches, std::string x_name) {
    ColumnArray partition_plan_x_tmp = generate_min_max_metadata(batches, x_name);

    std::deque<long> partition_plan_x;
    for (auto& val : partition_plan_x_tmp) {
      // TODO: fix get<type>
      partition_plan_x.emplace_back(std::get<long>(val));
    }

    // push front the minimum value for x_name
    partition_plan_x.emplace_front(std::numeric_limits<long>::min());
    // push back the maximum value for x_name
    partition_plan_x.emplace_back(std::numeric_limits<long>::max());

    // check ids
    std::vector<Partition> partitions;
    for (int i = 1; i < partition_plan_x.size(); ++i) {
      auto min_x = partition_plan_x[i - 1];
      auto max_x = partition_plan_x[i];
      auto metadata_x = Metadata{.col_name = x_name, .min = min_x, .max = max_x};

      auto metadata_y = Metadata{.col_name = x_name, .min = std::numeric_limits<long>::min(), .max = std::numeric_limits<long>::max()};

      partitions.emplace_back(
          Partition{.id = i - 1, .metadata_x = metadata_x, .metadata_y = metadata_y});
    }
    return partitions;
  }

  Table fetch(Sequence batches, const Partition& partition, std::string x_name, std::string y_name) {
    long min_x =  std::get<long>(partition.metadata_x.min);
    long max_x =  std::get<long>(partition.metadata_x.max);

    long min_y = std::get<long>(partition.metadata_y.min);
    long max_y = std::get<long>(partition.metadata_y.max);
    // // print min and max values
    // std::cout << "min_x: " << to_string(min_x) << "| max_x: " << to_string(max_x) << std::endl;
    // std::cout << "min_y: " << to_string(min_y) << "| max_y: " << to_string(max_y) << std::endl;

    Table result = Table::make_empty(batches[0]->schema().columnNames());
    size_t filtered_sz = 0;
    for (auto& batch : batches) {
      auto table = batch->toTable();
      auto index_x = table.col_index(x_name);
      auto index_y = table.col_index(y_name);

      auto filtered = table.filter([=](const RowArray& row) {
        long x_val = std::get<long>(row[index_x]);
        long y_val = std::get<long>(row[index_y]);
        return min_x <= x_val && x_val <= max_x && min_y <= y_val && y_val <= max_y;
      });
      // std::cout << " input.sz: " << table.num_rows() << "|" << "filtered.sz: " << filtered.num_rows() << std::endl;
      result.merge(filtered);
      filtered_sz += filtered.num_rows();
    }
    // std::cerr << " filtered_data.sz: " << filtered_sz << std::endl;
    return result;
  }


Sequence IEJoinMethod3(Sequence left_batches, Sequence right_batches, int r_x, int r_y,int  s_x, int s_y, std::string x_name, std::string y_name, std::vector<Predicate> predicates) {
    Sequence result;

    // r_x op1 s_x AND r_y op2 s_y
    // r_x op1 s_x AND r_y op2 s_y
    auto partitions_lhs = GeneratePartitionPlan(left_batches, x_name);
    auto partitions_rhs = GeneratePartitionPlan(right_batches, x_name);

    PrintMinMaxMetadata(partitions_lhs, partitions_rhs);
    std::vector<std::pair<long, long>> result_ids;
    auto cross_join_result =
        virtual_cross_join_eq(partitions_lhs, partitions_rhs, 1);
    std::cout << partitions_lhs.size() << "," << partitions_rhs.size() << " | cross_join_result.sz: " << cross_join_result.size()
              << std::endl;
    for (auto k : cross_join_result) {
      std::cout << "[" << k.first << ", " << k.second << "]" << ", ";
    }
    size_t left_sz = 0;
    size_t right_sz = 0;
    for (int index = 0; index < cross_join_result.size(); index++) {
      auto [lhs_part_index, rhs_part_index] = cross_join_result[index];
    
      auto left_table = fetch(left_batches, partitions_lhs[lhs_part_index], x_name, y_name);
      auto right_table = fetch(right_batches, partitions_rhs[rhs_part_index], x_name, y_name);
      std::cerr << "fetch_sz: " << left_table.num_rows() << "x" << right_table.num_rows() << std::endl;
      auto expected = IEJoin(left_table,
                               right_table, predicates);
      for (const auto &[x, y] : expected) {
        result_ids.emplace_back(std::make_pair(x, y));
      }
      left_sz += left_table.num_rows();
      right_sz += right_table.num_rows();
    }
    std::cerr <<  " IEJOIN( " << left_sz << " x " << right_sz << ")" << std::endl;
    std::cout <<  " result_ids.sz: " << result_ids.size() << std::endl;
    return result;
}

enum class IEJoinAlgo {
  kMethod1, // Global Sort
  kMethod2, // Local Sort
  kMethod3, // Sampling based partition
};

class IEJoinExec : public PhysicalPlan {
 public:
  IEJoinExec(std::shared_ptr<PhysicalPlan> left_input,
             std::shared_ptr<PhysicalPlan> right_input, Schema schema,
             std::shared_ptr<LogicalExpr> _join_condition, IEJoinAlgo algo)
      : left_input(std::move(left_input)),
        right_input(std::move(right_input)),
        schema_(std::move(schema)),
        join_condition(std::move(_join_condition)),
        algo(algo)

  {
    // R.X op S.X AND R.Y op' S.Y
    // r_x op1 s_x AND r_y op2 s_y

    // cast to And
    auto and_expr = std::dynamic_pointer_cast<And>(this->join_condition);
    auto left_op = std::dynamic_pointer_cast<BooleanBinaryExpr>(and_expr->l_);
    auto right_op = std::dynamic_pointer_cast<BooleanBinaryExpr>(and_expr->r_);

    r_x = std::dynamic_pointer_cast<ColumnIndex>(left_op->l_)->index;
    s_x = std::dynamic_pointer_cast<ColumnIndex>(left_op->r_)->index;

    r_y = std::dynamic_pointer_cast<ColumnIndex>(right_op->l_)->index;
    s_y = std::dynamic_pointer_cast<ColumnIndex>(right_op->r_)->index;

    s_x -= this->right_input->schema().fields.size();
    s_y -= this->right_input->schema().fields.size();

    x_name = this->left_input->schema().fields[r_x].name;
    y_name = this->right_input->schema().fields[r_y].name;
    std::vector<Predicate> preds = {{"op1", make_operator(left_op->op_), r_x, s_x},
                                    {"op2", make_operator(right_op->op_), r_y, s_y}};

    predicates = preds;
  }

  Schema schema() override { return schema_; }

  std::vector<std::shared_ptr<PhysicalPlan>> children() override {
    return {left_input, right_input};
  }

  std::string toString() override {
    return "JoinExec(" + left_input->toString() + ", " + right_input->toString() + ":" +
           schema_.toString() + ")";
  }

  Sequence execute() override {
    auto left_batches = left_input->execute();
    auto right_batches = right_input->execute();
    if (algo == IEJoinAlgo::kMethod2) {
      return IEJoinMethod2(left_batches, right_batches, r_x, r_y, s_x, s_y, x_name, y_name, predicates);
    }
    return IEJoinMethod3(left_batches, right_batches, r_x, r_y, s_x, s_y, x_name, y_name, predicates);
  }

 private:
  std::shared_ptr<PhysicalPlan> left_input;
  std::shared_ptr<PhysicalPlan> right_input;
  Schema schema_;
  std::shared_ptr<LogicalExpr> join_condition;

 private:
  int r_x, r_y, s_x, s_y;
  std::string x_name, y_name;
  std::vector<Predicate> predicates;
  IEJoinAlgo algo;
};

}  // namespace iejoin_exec
class QueryPlanner {
 public:
  static std::shared_ptr<PhysicalPlan> createPhysicalPlan(
      const std::shared_ptr<LogicalPlan>& plan) {
    if (auto scan = std::dynamic_pointer_cast<Scan>(plan)) {
      return std::make_shared<ScanExec>(scan->dataSource, scan->projection);
    } else if (auto selection = std::dynamic_pointer_cast<Selection>(plan)) {
      auto input = createPhysicalPlan(selection->input);
      auto filterExpr = createPhysicalExpr(selection->expr, selection->input);
      return std::make_shared<SelectionExec>(input, filterExpr);
    } else if (auto projection = std::dynamic_pointer_cast<Projection>(plan)) {
      auto input = createPhysicalPlan(projection->input);
      std::vector<std::shared_ptr<Expression>> projectionExpr;
      std::transform(projection->expr.begin(), projection->expr.end(),
                     std::back_inserter(projectionExpr), [&](const auto& expr) {
                       return createPhysicalExpr(expr, projection->input);
                     });
      std::vector<Field> fields;
      std::transform(
          projection->expr.begin(), projection->expr.end(), std::back_inserter(fields),
          [&projection](const auto& expr) { return expr->toField(projection->input); });
      Schema projectionSchema(fields);
      return std::make_shared<ProjectionExec>(input, projectionSchema, projectionExpr);
    } else if (auto localSort = std::dynamic_pointer_cast<LocalSort>(plan)) {
      auto input = createPhysicalPlan(localSort->input);
      std::vector<int> sort_indices;
      for (const auto& column_index : localSort->sort_indices) {
        sort_indices.push_back(column_index->index);
      }
      return std::make_shared<SortExec>(input, localSort->schema(),
                                                 sort_indices, true);
    } else if (auto global_sort = std::dynamic_pointer_cast<GlobalSort>(plan)) {
      auto input = createPhysicalPlan(global_sort->input);
      std::vector<int> sort_indices;
      for (const auto& column_index : global_sort->sort_indices) {
        sort_indices.push_back(column_index->index);
      }
      return std::make_shared<SortExec>(input, global_sort->schema(),
                                                 sort_indices, false);
    } else if (auto join = std::dynamic_pointer_cast<IEJoinMethod2>(plan)) {
      // handle join
      auto left = createPhysicalPlan(join->left);
      auto right = createPhysicalPlan(join->right);
      return std::make_shared<iejoin_exec::IEJoinExec>(left, right, join->schema(),
                                                       join->join_condition, iejoin_exec::IEJoinAlgo::kMethod2);
    } else if (auto join = std::dynamic_pointer_cast<IEJoinMethod3>(plan)) {
      // handle join
      auto left = createPhysicalPlan(join->left);
      auto right = createPhysicalPlan(join->right);
      return std::make_shared<iejoin_exec::IEJoinExec>(left, right, join->schema(),
                                                       join->join_condition, iejoin_exec::IEJoinAlgo::kMethod3);
    } else if (auto repartition = std::dynamic_pointer_cast<Repartition>(plan)) {
      auto input = createPhysicalPlan(repartition->input);
      return std::make_shared<RepartitionExec>(input, repartition->schema(),
                                                       repartition->n_partitions);
    } 
    else {
      throw std::invalid_argument("Unsupported logical plan");
    }
  }

  static std::shared_ptr<Expression> createPhysicalExpr(
      std::shared_ptr<LogicalExpr> expr, std::shared_ptr<LogicalPlan> input) {
    if (auto le = std::dynamic_pointer_cast<Literal>(expr)) {
      return std::make_shared<LiteralExpression>(le->value);
    } else if (auto column_index = std::dynamic_pointer_cast<ColumnIndex>(expr)) {
      return std::make_shared<ColumnExpression>(column_index->index);
    } else if (auto column = std::dynamic_pointer_cast<Column>(expr)) {
      int index = input->schema().indexOfFirst(column->name);
      if (index == -1) {
        throw std::runtime_error("No column named: " + column->name);
      }
      return std::make_shared<ColumnExpression>(index);
    } else if (auto be = std::dynamic_pointer_cast<BinaryExpr>(expr)) {
      auto l = createPhysicalExpr(be->l_, input);
      auto r = createPhysicalExpr(be->r_, input);

      if (auto eq = std::dynamic_pointer_cast<Eq>(expr)) {
        return std::make_shared<EqExpression>(l, r);
      } else if (auto neq = std::dynamic_pointer_cast<Neq>(expr)) {
        return std::make_shared<NeqExpression>(l, r);
      } else if (auto gt = std::dynamic_pointer_cast<Gt>(expr)) {
        return std::make_shared<GtExpression>(l, r);
      } else if (auto gte = std::dynamic_pointer_cast<GtEq>(expr)) {
        return std::make_shared<GtEqExpression>(l, r);
      } else if (auto lt = std::dynamic_pointer_cast<Lt>(expr)) {
        return std::make_shared<LtExpression>(l, r);
      } else if (auto lte = std::dynamic_pointer_cast<LtEq>(expr)) {
        return std::make_shared<LtEqExpression>(l, r);
      } else if (auto andExpr = std::dynamic_pointer_cast<And>(expr)) {
        return std::make_shared<AndExpression>(l, r);
      } else if (auto orExpr = std::dynamic_pointer_cast<Or>(expr)) {
        return std::make_shared<OrExpression>(l, r);
      }
    }
    throw std::runtime_error("Can't create Physical Expr");
  }
};

}  // namespace datafusionx