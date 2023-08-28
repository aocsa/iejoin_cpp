#pragma once

#include "datasource.h"
#include "std/core.hpp"

#include <arrow/array.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>

namespace datafusionx {

class ParquetDataSource : public DataSource {
 private:
  std::string filename;
  Schema finalSchema;

  Schema inferSchema() {
    auto file = arrow::io::ReadableFile::Open(filename).ValueOrDie();
    auto reader = parquet::ParquetFileReader::Open(file);
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();
    std::vector<int> row_groups(metadata->num_row_groups());
    std::vector<Field> fields;
    for (size_t index = 0; index < metadata->schema()->num_columns(); index++) {
      fields.emplace_back(Field(metadata->schema()->Column(index)->name(),
                                metadata->schema()->Column(index)->physical_type()));
    }
    return Schema(fields);
  }

 public:
  ParquetDataSource(std::string filename, std::optional<Schema> schema = std::nullopt)
      : filename(filename) {
    finalSchema = schema != std::nullopt ? Schema(*schema) : inferSchema();
  }

  Schema schema() const override { return finalSchema; }

  std::string toString() override {
    std::stringstream ss;
    ss << "ParquetDataSource(" << filename << ")";
    return ss.str();
  }

  // TODO: use Sequences instead of std::vector<std::shared_ptr<RecordBatch>>
  std::vector<std::shared_ptr<RecordBatch>> scan(
      const std::vector<std::string>& projection) override {
    std::vector<std::shared_ptr<RecordBatch>> batches;
    auto status = ReadParquetFileInBatches(filename, projection, batches);
    assert(status.ok());
    std::cerr << "ReadParquetFileInBatches\n";
    return batches;
  }

  std::vector<Table> read_batches(const std::vector<std::string>& projection) override {
    auto batches = scan(projection);
    std::vector<Table> result;
    result.reserve(batches.size());
    for (auto& batch : batches) {
      auto table = batch->toTable();
      result.emplace_back(table);
    }
    return result;
  }

 private:
  std::shared_ptr<ColumnVector>  ProcessColumn(const std::shared_ptr<arrow::Array>& column) {
    std::vector<Table::DataType> output_array;
    output_array.reserve(column->length());
    Table::DataType result;
    auto type = column->type()->id();
    if (type == arrow::Type::BOOL) {
      auto array = std::static_pointer_cast<arrow::BooleanArray>(column);
      for (int i = 0; i < array->length(); i++) {
        if (!array->IsNull(i)) {
          result = array->Value(i);
        } else {
          result = false;
        }
        output_array.emplace_back(result);
      }
    }
    else if (type == arrow::Type::INT32) {
      auto int_array = std::static_pointer_cast<arrow::Int32Array>(column);
      for (int i = 0; i < int_array->length(); i++) {
        if (!int_array->IsNull(i)) {
          result = int_array->Value(i);
        } else {
          result = std::numeric_limits<int32_t>::min();
        }
        output_array.emplace_back(result);
      }
    } 
    else if (type == arrow::Type::INT64) {
      auto array = std::static_pointer_cast<arrow::Int64Array>(column);
      for (int i = 0; i < array->length(); i++) {
        if (!array->IsNull(i)) {
          result = array->Value(i);
        } else {
          result = std::numeric_limits<int64_t>::min();
        }
        output_array.emplace_back(result);
      }
    } 
    else if (type == arrow::Type::FLOAT) {
      auto float_array = std::static_pointer_cast<arrow::FloatArray>(column);
      for (int i = 0; i < float_array->length(); i++) {
        if (!float_array->IsNull(i)) {
          result = float_array->Value(i);
        } else {
          result = std::numeric_limits<float>::min();
        }
        output_array.emplace_back(result);
      }
    }
    else if (type == arrow::Type::DOUBLE) {
      auto float_array = std::static_pointer_cast<arrow::DoubleArray>(column);
      for (int i = 0; i < float_array->length(); i++) {
        if (!float_array->IsNull(i)) {
          result = float_array->Value(i);
        } else {
          result = std::numeric_limits<double>::min();
        }
        output_array.emplace_back(result);
      }
    } else if (type == arrow::Type::STRING) {
      auto string_array = std::static_pointer_cast<arrow::StringArray>(column);
      for (int i = 0; i < string_array->length(); i++) {
        if (!string_array->IsNull(i)) {
         result = string_array->GetString(i);
        } else {
          result = "";
        }
        output_array.emplace_back(result);
      }
    } else {
      throw std::runtime_error("ProcessColumn: No supported type : " + column->type()->ToString());
    }
    return std::make_shared<ArrowFieldVector>(ColumnArray(output_array));
  }


  arrow::Status ReadParquetFileInBatches(const std::string& file_path, const std::vector<std::string>& projection, std::vector<std::shared_ptr<RecordBatch>> &output) {
    PARQUET_ASSIGN_OR_THROW(auto infile, arrow::io::ReadableFile::Open(file_path));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    PARQUET_THROW_NOT_OK(reader->GetRecordBatchReader({0}, &batch_reader));
    finalSchema = finalSchema.select(projection);

    while (true) {
      std::shared_ptr<arrow::RecordBatch> batch;
      if (!batch_reader->ReadNext(&batch).ok() || batch == nullptr) {
        break;
      }
      std::cerr << "read_batch\n";
      std::vector<std::shared_ptr<ColumnVector>> fields;
      for (int i = 0; i < batch->num_columns(); i++) {
        std::shared_ptr<arrow::Array> column = batch->column(i);
        std::string col_name = finalSchema.fields[i].name;
        // is col_name in projection?
        if (std::ranges::find(projection, col_name) == projection.end()) {
          continue;
        }
        auto column_vector = ProcessColumn(column);

        std::cerr <<  column_vector->size() <<  " | ProcessColumn\n";

        fields.emplace_back(column_vector);
      }

      std::cerr << fields.size() <<  " | new_batch\n";
      auto record_batch = std::make_shared<RecordBatch>(finalSchema, fields);
      output.emplace_back(record_batch);
      std::cerr << "***batch: " << record_batch->toString() << "\n";
    }
    std::cerr << "ReadParquetFileInBatches\n";
    return arrow::Status::OK();
  }
};

}  // namespace datafusionx