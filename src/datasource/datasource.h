#pragma once

#include <iostream>
#include <vector>

#include <iostream>
#include <string>
#include <vector>

#include "record_batch.h"
#include "schema.h"

namespace datacore {

class ColumnVector;

class DataSource {
 public:
  virtual Schema schema() const = 0;
  virtual std::vector<std::shared_ptr<RecordBatch>> scan(
      const std::vector<std::string>& projection) = 0;

  virtual std::string toString() = 0;
};

}  // namespace datacore