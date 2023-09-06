#pragma once

#include "datasource.h"
#include "std/core.hpp"

namespace datafusionx {

struct CsvParserSettings {
  char delimiter;
};

std::vector<Table> read_csv(std::string_view filename, const char& delimiter = ',',
                                   int batch_size = -1);

Schema read_csv_header(std::string_view filename, const char& delimiter = ',');

bool splite_line(const std::string& str_line, std::vector<std::string>& value_str_vector,
                 const char& delimiter);

class CsvDataSource : public DataSource {
 private:
  std::string filename;
  Schema schema_;
  bool hasHeaders;
  int batchSize;
  Schema finalSchema;

  CsvParserSettings defaultSettings() const {
    CsvParserSettings settings;
    settings.delimiter = ',';
    return settings;
  }

  Schema inferSchema() {
    std::ifstream file(filename);
    if (!file) {
      throw std::runtime_error("File not found: " + filename);
    }
    auto settings = defaultSettings();
    return read_csv_header(filename, settings.delimiter);
  }

 public:
  CsvDataSource(std::string filename, std::optional<Schema> schema, bool hasHeaders,
                int batchSize)
      : filename(filename), hasHeaders(hasHeaders), batchSize(batchSize) {
    finalSchema = schema != std::nullopt ? Schema(*schema) : inferSchema();
  }

  Schema schema() const override { return finalSchema; }

  // TODO: use Sequences instead of Sequence
  Sequence scan(
      const std::vector<std::string>& projection) override {
    std::ifstream file(filename);
    if (!file) {
      throw std::runtime_error("File not found: " + filename);
    }

    auto settings = defaultSettings();

    auto batches = read_csv(filename, settings.delimiter, batchSize);
    assert(batches.size() > 0);
    if (projection.size() > 0) {
      for (size_t i = 0; i < batches.size(); i++) {
        batches[i] = batches[i].select(projection);
      }
      finalSchema = finalSchema.select(projection);
    }

    Sequence result;
    for (int i = 0; i < batches.size(); i++) {
      std::vector<std::shared_ptr<ColumnVector>> columns;
      for (int col_index = 0; col_index < batches[i].num_cols(); col_index++) {
        // TODO: fix this copy column
        columns.push_back(
            std::make_shared<ArrowFieldVector>(batches[i].get_column(col_index)));
      }
      result.emplace_back(std::make_shared<RecordBatch>(finalSchema, columns));
    }
    return result;
  }


  std::vector<Table> read_batches(const std::vector<std::string>& projection) override {
    auto batches =  scan(projection);
    std::vector<Table> result;
    result.reserve(batches.size());
    for(auto& batch : batches) {
      auto table = batch->toTable();
      result.emplace_back(table);
    }
    return result;
  }

  std::string toString() override {
      std::stringstream ss;
      ss << "CsvDataSource(" << filename << ")";
      return ss.str();
  }
};

Schema read_csv_header(std::string_view filename, const char& delimiter) {
  std::vector<Table> Tables;
  std::ifstream reader(filename.data());
  if (!reader) {
    throw(std::invalid_argument(filename.data() + std::string(" is invalid!")));
  }
  // read header
  std::string str_line;
  std::vector<std::string> column_names;
  if (std::getline(reader, str_line)) {
    if (splite_line(str_line, column_names, delimiter)) {
    }
  }

  Table df = Table::make_empty(column_names);
  std::vector<std::string> value_str_vector;
  int count = 0;
  while (std::getline(reader, str_line) and count < 2) {
    value_str_vector.clear();
    if (splite_line(str_line, value_str_vector, delimiter)) {
      df.append_from_str(value_str_vector);
      count++;
    }
  }
  RowArray row = df.get_row(0);
  std::vector<Field> fields;
  for (int i = 0; i < row.size(); i++) {
    Field field;
    field.dataType = get_field_type(row[i]);
    field.name = column_names[i];
    fields.push_back(field);
  }
  Schema readSchema(fields);
  return readSchema;
}

// TODO: support more types
// read from csv file
// batch_size: Rows to read; -1 is all
// skip_rows: Rows to skip from the start
std::vector<Table> read_csv(std::string_view filename, const char& delimiter,
                                   int batch_size) {
  std::vector<Table> tables;
  std::ifstream reader(filename.data());
  if (!reader) {
    throw(std::invalid_argument(filename.data() + std::string(" is invalid!")));
  }
  // read header
  std::string str_line;
  std::vector<std::string> column_names;
  if (std::getline(reader, str_line)) {
    if (splite_line(str_line, column_names, delimiter)) {
    }
  }

  Table df = Table::make_empty(column_names);
  std::vector<std::string> value_str_vector;
  int count = 0;
  while (std::getline(reader, str_line) && (batch_size == -1 || count < batch_size)) {
    value_str_vector.clear();
    if (splite_line(str_line, value_str_vector, delimiter)) {
      df.append_from_str(value_str_vector);
      count++;
    }
    if (count == batch_size) {
      tables.emplace_back(df);
      df = Table::make_empty(column_names);
      count = 0;
    }
  }
  if (df.num_rows() > 0) {
    tables.emplace_back(df);
  }
  reader.close();
  return tables;
}

// separate strings by delimiter
bool splite_line(const std::string& str_line, std::vector<std::string>& value_str_vector,
                 const char& delimiter) {
  unsigned long long begin_iter = 0;
  unsigned long long end_iter;
  bool flag = false;
  std::string str_temp;
  while ((end_iter = str_line.find_first_of(delimiter, begin_iter)) !=
         std::string::npos) {
    str_temp = str_line.substr(begin_iter, end_iter - begin_iter);
    value_str_vector.emplace_back(str_temp);
    begin_iter = end_iter + 1;
  }
  if ((begin_iter = str_line.find_last_of(delimiter, str_line.length() - 1)) !=
      std::string::npos) {
    str_temp = str_line.substr(begin_iter + 1, str_line.length() - begin_iter - 1);
    value_str_vector.emplace_back(str_temp);
    flag = true;
  }
  return flag;
}

}  // namespace datafusionx