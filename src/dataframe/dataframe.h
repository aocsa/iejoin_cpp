
#ifndef Dataframe_H
#define Dataframe_H

#include <algorithm>
#include <cmath>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace frame {

#define max_number_bit 50

using VariantType = std::variant<char, int, long int, float, double, std::string>;

template <class... Ts>
struct overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

enum str_type { string_type = 1, float_type, int_type };

class isNumeric {
 public:
  str_type operator()(const std::string& str) {
    index = 0;
    str_type type = string_type;

    if (str.empty()) return type;

    bool flag = scan_integer(str);
    if (flag) type = int_type;

    if (index < str.size() && str[index] == '.') {
      index++;
      flag = scan_unsigned_integer(str) || flag;
      if (flag) type = float_type;
    }

    if (index < str.size() && (str[index] == 'E' || str[index] == 'e')) {
      index++;
      flag = flag && scan_integer(str);
      if (flag) type = float_type;
    }

    if (!flag || index != str.size()) type = string_type;

    return type;
  }

 private:
  unsigned int index = 0;

  bool scan_integer(const std::string& str) {
    if (index < str.size() && (str[index] == '+' || str[index] == '-')) index++;
    return scan_unsigned_integer(str);
  }

  bool scan_unsigned_integer(const std::string& str) {
    unsigned int start = index;
    while (index < str.size() && str[index] >= '0' && str[index] <= '9') index++;
    return start < index;
  }
};

str_type get_string_type(const std::string& str) {
  static isNumeric isnumeric;
  // directly return string_type while str.size() exceeds max_number_bit
  if (str.size() > max_number_bit) return string_type;
  return isnumeric(str);
}

template <typename T1, typename T2>
class is_same_type {
 public:
  explicit operator bool() { return false; }
};

template <typename T1>
class is_same_type<T1, T1> {
 public:
  explicit operator bool() { return true; }
};

template <typename T>
bool is_numeric_type() {
  if (is_same_type<T, double>() || is_same_type<T, float>() || is_same_type<T, int>() ||
      is_same_type<T, long int>() || is_same_type<T, long long int>())
    return true;
  else
    return false;
}

namespace toolbox {
class user_stringstream {
 public:
  [[maybe_unused]] const std::string& str() { return this->str_; }

  template <typename T>
  void operator>>(T& var) {
    convert.clear();
    convert << str_;
    convert >> var;
  }

  template <typename T>
  void operator<<(const T& var) {
    convert.clear();
    convert << var;
    convert >> str_;
  }

  template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6>
  void operator>>(std::variant<T1, T2, T3, T4, T5, T6>& var) {
    var = this->str_;
  }

  template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6>
  user_stringstream& operator<<(const std::variant<T1, T2, T3, T4, T5, T6>& var) {
    convert.clear();
    std::visit(overloaded{
                   [&](T1 value) { convert << value; },
                   [&](T2 value) { convert << value; },
                   [&](T3 value) { convert << value; },
                   [&](T4 value) { convert << value; },
                   [&](T5 value) { convert << value; },
                   [&](T6 value) { convert << value; },
               },
               var);
    convert >> str_;
    return *this;
  }

 private:
  std::string str_;
  std::stringstream convert;
};
}  // namespace toolbox

template <typename T = frame::VariantType>
class Dataframe {
 public:
  using DataType = T;

  class ColumnArray {
    typedef typename std::vector<T>::const_iterator const_iter;
    typedef typename std::vector<T>::iterator iter;
    std::vector<T>* array = nullptr;

   public:
    explicit ColumnArray(int n = 0) { array = new std::vector<T>(n); }

    ColumnArray(const ColumnArray& _array) { array = new std::vector<T>(*_array.array); }

    ColumnArray(ColumnArray&& _array) noexcept {
      array = new std::vector<T>(std::move(*_array.array));
    }

    explicit ColumnArray(std::vector<T>&& _array) { array = new std::vector<T>(_array); }

    explicit ColumnArray(const std::vector<T>& _array) {
      array = new std::vector<T>(_array);
    }

    ~ColumnArray() { delete array; }

    void insert(const_iter position, const_iter start, const_iter end) {
      array->insert(position, start, end);
    }

    [[nodiscard]] size_t size() const {
      if (array == nullptr) return 0;
      return array->size();
    }

    [[nodiscard]] const_iter begin() const { return array->begin(); }

    [[nodiscard]] const_iter end() const { return array->end(); }

    [[nodiscard]] iter begin() { return array->begin(); }

    [[nodiscard]] iter end() { return array->end(); }

    void erase(const_iter i) { array->erase(i); }

    void emplace_back(const T& item) { array->emplace_back(item); }

    ColumnArray& operator=(const ColumnArray& other) {
      if (this != &other) {
        if (other.size() == array->size()) {
          array->clear();
          array->insert(array->begin(), other.begin(), other.end());
          return *this;
        } else
          throw(std::invalid_argument("The length of the two is not the same"));
      }
      return *this;
    }

    ColumnArray& operator=(const std::vector<T>& _array) {
      if (_array.size() == array->size()) {
        array->clear();
        array->insert(array->begin(), _array.begin(), _array.end());
        return *this;
      }
      throw(std::invalid_argument("The length of the two is not the same"));
    }

    ColumnArray& operator=(std::vector<T>&& _array) {
      if (_array.size() == array->size()) {
        *array = _array;
        return *this;
      }
      throw(std::invalid_argument("The length of the two is not the same"));
    }

    [[maybe_unused]] [[nodiscard]] const std::vector<T>& get_std_vector() const {
      return *array;
    }

    [[maybe_unused]] std::vector<T>& get_std_vector() { return *array; }

    void fill(T value) {
      for (auto& item : *array) {
        item = value;
      }
    }

    template <typename OutputType>
    std::vector<OutputType> get_as() {
      std::vector<OutputType> result;
      result.reserve(this->size());

      for (const auto& item : *array) {
        std::visit(
            overloaded{
                [&result](char value) { result.push_back((OutputType)(value)); },
                [&result](int value) { result.push_back((OutputType)(value)); },
                [&result](long int value) { result.push_back((OutputType)(value)); },
                [&result](float value) { result.push_back((OutputType)(value)); },
                [&result](double value) { result.push_back((OutputType)(value)); },
                [&result](const std::string& value) {
                  result.push_back((OutputType)(value.size()));
                },
            },
            item);
      }
      return result;
    }

    const T& operator[](size_t i) const {
      if (i < array->size())
        return (*array)[i];
      else {
        std::stringstream ssTemp;
        ssTemp << i;
        throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
      }
    }

    T& operator[](size_t i) {
      if (i < array->size())
        return (*array)[i];
      else {
        std::stringstream ssTemp;
        ssTemp << i;
        throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
      }
    }

    // implement a filter function 
    ColumnArray filter(std::vector<bool> selection) {
      ColumnArray result(selection.size());
      for (int i = 0; i < size(); i++) {
        if (selection[i]) {
          result[i] = this->array->at(i);
        }
      }
      return result;
    }

    friend std::ostream& operator<<(std::ostream& cout, ColumnArray& arr) {
      for (const auto& item : *arr.array) {
        cout << item << ' ';
      }
      return cout;
    }
  };

  class RowArray {
    typedef typename std::vector<T*>::const_iterator const_iter;
    typedef typename std::vector<T*>::iterator iter;
    std::vector<T*>* array = nullptr;

   public:
    explicit RowArray(int n = 0) { array = new std::vector<T*>(n); }

    RowArray(const RowArray& _array) { array = new std::vector<T*>(*_array.array); }

    RowArray(RowArray&& _array) noexcept {
      array = new std::vector<T*>(std::move(*_array.array));
    }

    [[maybe_unused]] explicit RowArray(std::vector<T*>&& _array) {
      array = new std::vector<T*>(_array);
    }

    [[maybe_unused]] explicit RowArray(const std::vector<T*>& _array) {
      array = new std::vector<T*>(_array);
    }

    ~RowArray() { delete array; }

    void insert(const_iter position, const_iter start, const_iter end) {
      array->insert(position, start, end);
    }

    [[nodiscard]] size_t size() const {
      if (array == nullptr) return 0;
      return array->size();
    }

    [[nodiscard]] const_iter begin() const { return array->begin(); }

    [[nodiscard]] const_iter end() const { return array->end(); }

    [[nodiscard]] iter begin() { return array->begin(); }

    [[nodiscard]] iter end() { return array->end(); }

    RowArray& operator=(const RowArray& other) {
      if (this != &other) {
        if (other.size() == array->size()) {
          for (int i = 0; i < other.size(); ++i) {
            *(*array)[i] = other[i];
          }
          return *this;
        } else
          throw(std::invalid_argument("The length of the two is not the same"));
      }
      return *this;
    }

    RowArray& operator=(const std::vector<T>& _array) {
      if (_array.size() == array->size()) {
        for (int i = 0; i < _array.size(); ++i) {
          *(*array)[i] = _array[i];
        }
        return *this;
      }
      throw(std::invalid_argument("The length of the two is not the same"));
    }

    RowArray& operator=(std::vector<T>&& _array) {
      if (_array.size() == array->size()) {
        for (int i = 0; i < _array.size(); ++i) {
          *(*array)[i] = _array[i];
        }
        return *this;
      }
      throw(std::invalid_argument("The length of the two is not the same"));
    }

    [[maybe_unused]] const std::vector<T*>& get_point_vector() const { return *array; }

    [[maybe_unused]] std::vector<T*>& get_point_vector() { return *array; }

    [[maybe_unused]] std::vector<T> get_std_vector() const {
      std::vector<T> result;
      if (array == nullptr) throw(std::invalid_argument("This row array is invalid!"));
      for (T* item : *array) {
        result.push_back(*item);
      }
      return result;
    }

    void push_back(T* item) { array->emplace_back(item); }

    const T& operator[](size_t i) const {
      if (i < array->size())
        return *((*array)[i]);
      else {
        std::stringstream ssTemp;
        ssTemp << i;
        throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
      }
    }

    T& operator[](size_t i) {
      if (i < array->size())
        return *((*array)[i]);
      else {
        std::stringstream ssTemp;
        ssTemp << i;
        throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
      }
    }

    friend std::ostream& operator<<(std::ostream& cout, RowArray& arr) {
      for (const auto& item : *arr.array) {
        cout << item << ' ';
      }
      return cout;
    }
  };

 private:
  typedef std::vector<std::string> string_vector;
  typedef typename std::vector<ColumnArray*>::const_iterator Dataframe_const_iter;
  typedef typename std::vector<ColumnArray*>::iterator Dataframe_iter;
  bool is_scaler = false;

 public:
  // constructed by file name
  explicit Dataframe(const std::string& filename, const char& delimiter = ',')
      : dataframe_name(filename), width(0), length(0) {
    read_csv(filename, delimiter);
  }

  // constructed by width and length
  explicit Dataframe(size_t _width = 0, std::string name = "Dataframe")
      : dataframe_name(std::move(name)), width(_width), length(0) {
    string_vector temp;
    for (size_t i = 0; i < _width; i++) temp.emplace_back(std::to_string(i));
    column_paste(temp);
  }

  // constructed by string vector
  explicit Dataframe(const string_vector& columns, std::string name = "Dataframe")
      : dataframe_name(std::move(name)), width(columns.size()), length(0) {
    column_paste(columns);
  }

  // copy constructor
  Dataframe(const Dataframe& dataframe)
      : dataframe_name(dataframe.dataframe_name),
        width(dataframe.width),
        length(dataframe.length),
        column(dataframe.column),
        index(dataframe.index) {
    matrix.clear();
    for (auto i = dataframe.matrix.begin(); i < dataframe.matrix.end(); ++i) {
      matrix.emplace_back(new ColumnArray(**i));
    }
  }

  // move constructor
  Dataframe(Dataframe&& dataframe) noexcept
      : dataframe_name(dataframe.dataframe_name),
        column(std::move(dataframe.column)),
        width(dataframe.width),
        length(dataframe.length),
        index(std::move(dataframe.index)) {
    matrix.clear();
    for (auto i = dataframe.matrix.begin(); i < dataframe.matrix.end(); ++i) {
      matrix.emplace_back(new ColumnArray(std::move(**i)));
    }
  }

  ~Dataframe() {
    for (auto i = 0; i < matrix.size(); ++i) {
      delete matrix[i];
    }
  }

  static Dataframe<T> make_empty(size_t length) {
    Dataframe<T> df;
    df.length = length;
    df.create_row_index();
    return df;
  }


  static Dataframe<T> make_empty(const string_vector& columns) {
    Dataframe<T> df;
    df.column_paste(columns);
    return df;
  }


  Dataframe_iter begin() { return matrix.begin(); }

  Dataframe_const_iter begin() const { return matrix.begin(); }

  Dataframe_iter end() { return matrix.end(); }

  Dataframe_const_iter end() const { return matrix.end(); }

  // determine whether the column is included
  bool contain(const std::string& col) { return index.find(col) != index.end(); }

  // insert one column
  bool insert(const std::string& col) {
    ++width;
    column.emplace_back(col);
    index.emplace(col, index.size());
    matrix.emplace_back(new ColumnArray(length));
    return true;
  }

  // insert one column from std::vector<T>
  bool insert(const std::string& col, ColumnArray&& array) {
    if (array.size() == num_rows()) {
      if (!contain(col)) {
        ++width;
        column.emplace_back(col);
        index.emplace(col, index.size());
        matrix.emplace_back(new ColumnArray(array));
      } else {
        ColumnArray& line = this->operator[](col);
        if (line.size() == array.size()) {
          line = array;
        } else
          throw(std::invalid_argument("The length of the two is not the same"));
      }
      return true;
    } else
      return false;
  }

  // create a row_index column for this dataframe
  void create_row_index() {
    if (!contain("id")) {
      ++width;
      column.emplace_back("id");
      index.emplace("id", index.size());
      std::vector<T> row_index_values;
      row_index_values.reserve(length);
      for (long  i = 0; i < length; ++i) {
        T value = i;
        row_index_values.push_back(value);
      }
      matrix.push_back(new ColumnArray(row_index_values));
    }
  }

  // insert one column from std::vector<T>
  bool insert(const std::string& col, const ColumnArray& array) {
    if (array.size() == num_rows()) {
      if (!contain(col)) {
        ++width;
        column.emplace_back(col);
        index.emplace(col, index.size());
        matrix.emplace_back(new ColumnArray(array));
      } else {
        ColumnArray& line = this->operator[](col);
        if (line.size() == array.size()) {
          line = array;
        } else
          throw(std::invalid_argument("The length of the two is not the same"));
      }
      return true;
    } else
      return false;
  }

  // insert one column from std::vector<T>
  bool insert(const std::string& col, std::vector<T>&& array) {
    if (array.size() == num_rows()) {
      if (!contain(col)) {
        ++width;
        column.emplace_back(col);
        index.emplace(col, index.size());
        matrix.emplace_back(new ColumnArray(array));
      } else {
        ColumnArray& line = this->operator[](col);
        if (line.size() == array.size()) {
          line = array;
        } else
          throw(std::invalid_argument("The length of the two is not the same"));
      }
      return true;
    } else
      return false;
  }

  // insert one column from std::vector<T>
  bool insert(const std::string& col, const std::vector<T>& array) {
    if (array.size() == num_rows()) {
      if (!contain(col)) {
        ++width;
        column.emplace_back(col);
        index.emplace(col, index.size());
        matrix.emplace_back(new ColumnArray(array));
      } else {
        ColumnArray& line = this->operator[](col);
        if (line.size() == array.size()) {
          line = array;
        } else
          throw(std::invalid_argument("The length of the two is not the same"));
      }
      return true;
    } else
      return false;
  }

  // remove one column from str
  bool remove(const std::string& col) {
    auto item = index.find(col);
    if (item != index.end()) {
      --width;
      column.erase(column.begin() + item->second);
      delete *(matrix.begin() + item->second);
      matrix.erase(matrix.begin() + item->second);
      for (auto& index_item : index) {
        if (index_item.second > item->second) {
          index_item.second--;
        }
      }
      index.erase(item);
      return true;
    }
    return false;
  }

  // remove one row from index
  bool remove(size_t i) {
    if (i < length) {
      for (auto& item : matrix) {
        item->erase(item->begin() + i);
      }
      --length;
      return true;
    } else {
      return false;
    }
  }

  // get one row data from index of row
  RowArray operator[](size_t i) {
    if (i < length) {
      RowArray RowArray;
      for (auto& item : matrix) {
        RowArray.push_back(&((*item)[i]));
      }
      return RowArray;
    } else {
      std::stringstream ssTemp;
      ssTemp << i;
      throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
    }
  }

  // get one row data from index of row
  RowArray operator[](size_t i) const {
    if (i < length) {
      RowArray RowArray;
      for (auto& item : matrix) {
        RowArray.push_back(&((*item)[i]));
      }
      return RowArray;
    } else {
      std::stringstream ssTemp;
      ssTemp << i;
      throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
    }
  }

  // get one column data from column str
  ColumnArray& operator[](const std::string& col) {
    auto item = index.find(col);
    if (item != index.end()) {
      return *(matrix[item->second]);
    }
    insert(col);
    return *(matrix.back());
  }

  // get one column data from column str
  const ColumnArray& operator[](const std::string& col) const {
    auto item = index.find(col);
    if (item != index.end()) {
      return *(matrix[item->second]);
    }
    insert(col);
    return *(matrix.back());
  }

  // get one column data from index of column
  const ColumnArray& operator()(size_t i) const {
    if (i < matrix.size()) return *(matrix[i]);
    std::stringstream ssTemp;
    ssTemp << i;
    throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
  }

  // get one column data from index of column
  ColumnArray& operator()(size_t i) {
    if (i < matrix.size()) return *(matrix[i]);
    std::stringstream ssTemp;
    ssTemp << i;
    throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
  }

  // append one row from std::vector<T>
  bool append(const std::vector<T>& array) {
    if (array.size() == width) {
      length++;
      for (size_t i = 0; i < array.size(); ++i) {
        matrix[i]->emplace_back(array[i]);
      }
      return true;
    } else
      return false;
  }

  // append one row from std::vector<T>
  bool append(std::vector<T>&& array) {
    if (array.size() == width) {
      length++;
      for (size_t i = 0; i < array.size(); ++i) {
        matrix[i]->emplace_back(std::move(array[i]));
      }
      return true;
    } else
      return false;
  }

  [[nodiscard]] const size_t& num_cols() const { return width; }

  [[nodiscard]] const size_t& num_rows() const { return length; }

  // concat double Dataframe object vertically
  bool merge(const Dataframe& dataframe) {
    if (dataframe.width == width) {
      length += dataframe.length;
      for (size_t i = 0; i < width; ++i) {
        matrix[i]->insert(matrix[i]->end(), dataframe(i).begin(), dataframe(i).end());
      }
      return true;
    } else
      return false;
  }

  // partition the Dataframe in n parts
  std::vector<Dataframe<DataType>> partition(size_t n)  {
    std::vector<Dataframe<DataType>> dataframes;
    if (n > 0) {
      size_t part = length / n;
      size_t last = length % n;
      size_t start = 0;
      for (size_t i = 0; i < n; ++i) {
        Dataframe<DataType> dataframe;
        size_t dataframe_length;
        if (i == n - 1) {
          dataframe_length = part + last;
        } else {
          dataframe_length = part;
        }
        dataframe.column_paste(this->column_names());
        for (size_t j = start; j < start + dataframe_length; ++j) {
          dataframe.append(this->get_row(j).get_std_vector());
        }
        start += dataframe_length;
        dataframes.emplace_back(std::move(dataframe));
      }
    }
    return dataframes;
  }

  // sample dataframe 
  Dataframe<DataType> sample(size_t n) {
    Dataframe<DataType> dataframe;
    dataframe.column_paste(this->column_names());
    std::vector<size_t> index;
    for (size_t i = 0; i < length; ++i) {
      index.emplace_back(i);
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(index.begin(), index.end(), g);
    for (size_t i = 0; i < n; ++i) {
      dataframe.append(this->get_row(index[i]).get_std_vector());
    }
    return dataframe;
  }

  // sort dataframe by x_name column
  Dataframe sort_by(std::string column_name, bool descending = false) const {
    Dataframe dataframe;
    dataframe.column_paste(this->column_names());
    auto index = this->col_index(column_name);
    auto column = this->get_column(index);
    std::vector<std::pair<T, size_t>> sort_vector;
    for (size_t i = 0; i < column.size(); ++i) {
      sort_vector.emplace_back(column[i], i);
    }
    if (descending) {
      std::sort(sort_vector.begin(), sort_vector.end(), std::greater<>());
    } else {
      std::sort(sort_vector.begin(), sort_vector.end(), std::less<>());
    }
    for (auto& item : sort_vector) {
      dataframe.append(this->get_row(item.second).get_std_vector());
    }
    return dataframe;
  }

  Dataframe sort_by(int sort_indice, bool descending = false) const {
    Dataframe dataframe;
    dataframe.column_paste(this->column_names());
    auto index = sort_indice;
    auto column = this->get_column(index);
    std::vector<std::pair<T, size_t>> sort_vector;
    for (size_t i = 0; i < column.size(); ++i) {
      sort_vector.emplace_back(column[i], i);
    }
    if (descending) {
      std::sort(sort_vector.begin(), sort_vector.end(), std::greater<>());
    } else {
      std::sort(sort_vector.begin(), sort_vector.end(), std::less<>());
    }
    for (auto& item : sort_vector) {
      dataframe.append(this->get_row(item.second).get_std_vector());
    }
    return dataframe;
  }

  // select columns from Dataframe
  Dataframe select(std::vector<std::string> columns) const {
    Dataframe dataframe;
    dataframe.length = length;
    for (auto& column_name : columns) {
      auto index = this->col_index(column_name);
      auto series = this->get_column(index);
      dataframe.insert(column_name, series);
    }
    return dataframe;
  }

  // filter Dataframe by condition
  Dataframe filter(std::function<bool(const RowArray&)> selection_predicate) const {
      Dataframe dataframe;
      dataframe.column_paste(this->column_names());
      for (int i = 0; i < num_rows(); i++) {
        if (selection_predicate(this->get_row(i))) {
          dataframe.append(this->get_row(i).get_std_vector());
        }
      }
      return dataframe;
    }

  // compute cross join for condition x_name == y_name
  Dataframe cross_join(Dataframe& dataframe, std::string x, std::string y) {
    Dataframe result;
    result.column_paste(this->column_names());
    result.column_paste(dataframe.column_names());
    auto index_x = this->col_index(x);
    auto index_y = dataframe.col_index(y);
    auto column_x = this->get_column(index_x);
    auto column_y = dataframe.get_column(index_y);
    for (size_t i = 0; i < column_x.size(); ++i) {
      for (size_t j = 0; j < column_y.size(); ++j) {
        if (column_x[i] == column_y[j]) {
          std::vector<T> row_x = this->get_row(i).get_std_vector();
          std::vector<T> row_y = dataframe.get_row(j).get_std_vector();
          row_x.insert(row_x.end(), row_y.begin(), row_y.end());
          result.append(row_x);
        }
      }
    }
    return result;
  }

  // concat double Dataframe object horizontally
  bool concat_row(const Dataframe& dataframe) {
    if (dataframe.length == length) {
      std::string repeat;
      auto last_width = dataframe.width;
      for (size_t i = 0; i < last_width; ++i) {
        repeat = contain(dataframe.column[i]) ? "_r" : "";
        index.insert({dataframe.column[i] + repeat, index.size()});
        column.emplace_back(dataframe.column[i] + repeat);
        matrix.emplace_back(new ColumnArray(*dataframe.matrix[i]));
      }
      width += dataframe.num_cols();
      return true;
    } else
      return false;
  }

  // concat double Dataframe object horizontally
  bool concat_row(Dataframe&& dataframe) {
    if (dataframe.length == length) {
      std::string repeat;
      auto last_width = dataframe.width;
      for (size_t i = 0; i < last_width; ++i) {
        repeat = contain(dataframe.column[i]) ? "_r" : "";
        index.insert({dataframe.column[i] + repeat, index.size()});
        column.emplace_back(dataframe.column[i] + repeat);
        matrix.emplace_back(new ColumnArray(std::move(*dataframe.matrix[i])));
      }
      width += dataframe.num_cols();
      return true;
    } else
      return false;
  }

  // is empty or not
  bool empty() const { return width == 0 || length == 0; }

  // concat double Dataframe object vertically
  friend Dataframe operator+(const Dataframe& Dataframe1, const Dataframe& Dataframe2) {
    if (!Dataframe1.empty() && !Dataframe2.empty() &&
        Dataframe1.num_cols() == Dataframe2.num_cols()) {
      Dataframe dataframe(Dataframe1.column);
      dataframe.merge(Dataframe1);
      dataframe.merge(Dataframe2);
      dataframe.dataframe_name =
          Dataframe1.dataframe_name + "&" + Dataframe2.dataframe_name;
      return std::move(dataframe);
    } else
      throw(std::invalid_argument("The column length of the two is not the same"));
  }

  // move by equal sign
  Dataframe& operator=(Dataframe&& dataframe) noexcept {
    if (this == &dataframe) return *this;
    clear();
    width = dataframe.width;
    length = dataframe.length;
    dataframe_name = dataframe.dataframe_name;
    column = std::move(dataframe.column);
    index = std::move(dataframe.index);
    matrix.clear();
    for (auto i = dataframe.matrix.begin(); i < dataframe.matrix.end(); ++i) {
      matrix.emplace_back(new ColumnArray(std::move(**i)));
    }
    return *this;
  }

  // copy by equal sign
  Dataframe& operator=(const Dataframe& dataframe) {
    if (this == &dataframe) return *this;
    clear();
    width = dataframe.width;
    length = dataframe.length;
    column = dataframe.column;
    index = dataframe.index;
    dataframe_name = dataframe.dataframe_name;
    matrix.clear();
    for (auto i = dataframe.matrix.begin(); i < dataframe.matrix.end(); ++i) {
      matrix.emplace_back(new ColumnArray(**i));
    }
    return *this;
  }

  // read from csv file
  void read_csv(std::string_view filename, const char& delimiter = ',') {
    clear();
    std::ifstream reader(filename.data());
    if (!reader) {
      throw(std::invalid_argument(filename.data() + std::string(" is invalid!")));
    }

    std::string str_line;
    string_vector value_str_vector;
    if (getline(reader, str_line)) {
      value_str_vector.clear();
      if (splite_line(str_line, value_str_vector, delimiter)) {
        column_paste(value_str_vector);
      }
    }

    while (getline(reader, str_line)) {
      value_str_vector.clear();
      if (splite_line(str_line, value_str_vector, delimiter)) {
        append_from_str(value_str_vector);
      }
    }
    reader.close();
  }
  // write into csv file

  void to_csv(const char& delimiter = ',') const { to_csv(dataframe_name, delimiter); }

  void to_csv(const std::string& filename, const char& delimiter = ',') const {
    std::ofstream cout = std::ofstream(filename, std::ios::out | std::ios::trunc);
    for (auto item = column.begin(); item < column.end() - 1; ++item) {
      cout << *item << delimiter;
    }
    cout << column.back() << '\n';
    for (size_t i = 0; i < num_rows(); ++i) {
      for (auto array = matrix.begin(); array < matrix.end() - 1; ++array) {
        std::visit(overloaded{
                       [&cout](char value) { cout << value; },
                       [&cout](int value) { cout << value; },
                       [&cout](long int value) { cout << value; },
                       [&cout](float value) { cout << value; },
                       [&cout](double value) { cout << value; },
                       [&cout](const std::string& value) { cout << value; },
                   },
                   VariantType((**array)[i]));
        cout << delimiter;
      }
      std::visit(overloaded{
                     [&cout](char value) { cout << value; },
                     [&cout](int value) { cout << value; },
                     [&cout](long int value) { cout << value; },
                     [&cout](float value) { cout << value; },
                     [&cout](double value) { cout << value; },
                     [&cout](const std::string& value) { cout << value; },
                 },
                 VariantType((*matrix.back())[i]));
      cout << '\n';
    }
    cout.close();
  }

  // print Dataframe
  friend std::ostream& operator<<(std::ostream& cout, const Dataframe& dataframe) {
    cout << "name : " << dataframe.dataframe_name << std::endl;
    cout << "width : " << dataframe.width << std::endl;
    cout << "length : " << dataframe.length << std::endl;
    cout.setf(std::ios::fixed, std::ios::floatfield);
    std::string separator = "\t";
    for (size_t j = 0; j < dataframe.width; ++j) {
      cout << dataframe.column[j] << separator;
    }
    cout << std::endl;
    for (size_t i = 0; i < dataframe.length; ++i) {
      for (size_t j = 0; j < dataframe.width; ++j) {
        // check if T is variant type
        if constexpr (std::is_arithmetic<T>::value) {
          cout << (*dataframe.matrix[j])[i] << separator;
        } else {
          std::visit(
              overloaded{
                  [&cout](char value) { cout << '\'' << value << '\''; },
                  [&cout](int value) { cout << std::setprecision(0) << value << 'i'; },
                  [&cout](long int value) {
                    cout << std::setprecision(0) << value << 'i';
                  },
                  [&cout](float value) {
                    if (std::abs(value - static_cast<int>(value)) > 1e-3)
                      cout << std::setprecision(3) << value;
                    else
                      cout << std::setprecision(0) << value;
                    cout << 'f';
                  },
                  [&cout](double value) {
                    if (std::abs(value - static_cast<int>(value)) > 1e-3)
                      cout << std::setprecision(3) << value;
                    else
                      cout << std::setprecision(0) << value;
                    cout << 'f';
                  },
                  [&cout](const std::string& value) { cout << '"' << value << '"'; },
              },
              VariantType((*dataframe.matrix[j])[i]));
          cout << separator;
        }
      }
      cout << std::endl;
    }
    return cout;
  }

  // get string vector of columns

  const std::vector<std::string>& column_names() const { return column; }

  size_t col_index(std::string col) const {
    auto item = index.find(col);
    if (item != index.end()) {
      return item->second;
    }
    throw std::runtime_error("Column not found");
  }

  void set_scaler_flag(bool flag) { is_scaler = flag; }

  [[maybe_unused]] bool get_scaler_flag() { return is_scaler; }

  [[maybe_unused]] const std::string& name() { return dataframe_name; }

 public:
  // clear all data, generate an empty Dataframe
  void clear() {
    length = 0;
    width = 0;
    for (auto i = 0; i < matrix.size(); ++i) {
      delete matrix[i];
    }
    matrix.clear();
    column.clear();
    index.clear();
  }

  // init the column
  bool column_paste(const string_vector& _column) {
    if (!_column.empty()) {
      length = 0;
      width = _column.size();
      column.clear();
      index.clear();
      matrix.clear();
      for (const auto& item : _column) {
        column.emplace_back(item);
        index.emplace(item, index.size());
        matrix.emplace_back(new ColumnArray(0));
      }
      return true;
    } else
      return false;
  }

  [[maybe_unused]] const std::vector<ColumnArray*>& get_matrix() const { return matrix; }

  // print name of column
  [[maybe_unused]] void show_columns() {
    for (const auto& item : index) {
      std::cout << item.first << ',' << item.second << std::endl;
    }
  }

  // get one column data from index of column
  [[maybe_unused]] const ColumnArray& get_column(size_t i) const {
    if (i < matrix.size()) return *(matrix[i]);
    std::stringstream ssTemp;
    ssTemp << i;
    throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
  }

  // get one column data from index of column
  [[maybe_unused]] ColumnArray& get_column(size_t i) {
    if (i < matrix.size()) return *(matrix[i]);
    std::stringstream ssTemp;
    ssTemp << i;
    throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
  }

  // get one row data from index of row
  [[maybe_unused]] RowArray get_row(size_t i) {
    if (i < length) {
      RowArray RowArray;
      for (auto& item : matrix) {
        RowArray.push_back(&((*item)[i]));
      }
      return RowArray;
    } else {
      std::stringstream ssTemp;
      ssTemp << i;
      throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
    }
  }

  // get one row data from index of row
  [[maybe_unused]] const RowArray get_row(size_t i) const {
    if (i < length) {
      RowArray RowArray;
      for (auto& item : matrix) {
        RowArray.push_back(&((*item)[i]));
      }
      return RowArray;
    } else {
      std::stringstream ssTemp;
      ssTemp << i;
      throw(std::out_of_range("the index \'" + ssTemp.str() + "\' is out of range!"));
    }
  }

  // separate strings by delimiter
  bool splite_line(const std::string& str_line, string_vector& value_str_vector,
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

  // append data from string vector
  bool append_from_str(const string_vector& value_str_vector) {
    if (value_str_vector.size() == column.size()) {
      length++;
      std::stringstream stream;
      VariantType item;
      for (size_t i = 0; i < value_str_vector.size(); ++i) {
        stream.clear();
        stream << value_str_vector[i];
        str_type type = get_string_type(value_str_vector[i]);
        if (type == int_type) {
          long int temp;
          stream >> temp;
          item = temp;
        } else if (type == float_type) {
          double temp;
          stream >> temp;
          item = temp;
        } else {
          std::string temp;
          stream >> temp;
          if (std::is_arithmetic_v<T>) {
            item = (int)temp.size();
          } else {
            item = temp;
          }
        }

        std::visit(overloaded{
                       [&](char value) {
                         if (typeid(value) == typeid(T) || is_same_type<T, VariantType>())
                           matrix[i]->emplace_back(T(value));
                       },
                       [&](int value) {
                         if (typeid(value) == typeid(T) || is_same_type<T, VariantType>())
                           matrix[i]->emplace_back(T(value));
                       },
                       [&](long int value) {
                         if (is_numeric_type<T>() || is_same_type<T, VariantType>())
                           matrix[i]->emplace_back(T(value));
                       },
                       [&](float value) {
                         if (typeid(value) == typeid(T) || is_same_type<T, VariantType>())
                           matrix[i]->emplace_back(T(value));
                       },
                       [&](double value) {
                         if (is_numeric_type<T>() || is_same_type<T, VariantType>())
                           matrix[i]->emplace_back(T(value));
                       },
                       [&](const std::string& value) {
                         if (typeid(value) == typeid(T) || is_same_type<T, VariantType>()) {
                           toolbox::user_stringstream uss;
                           uss << value;
                           T temp;
                           uss >> temp;
                           matrix[i]->emplace_back(temp);
                         }
                       },
                   },
                   item);
      }
      return true;
    } else
      return false;
  }

  std::string dataframe_name;
  std::vector<std::string> column;
  std::vector<ColumnArray*> matrix;
  size_t width;
  size_t length;
  std::unordered_map<std::string, size_t> index;
};
};      // namespace datafusionx
#endif  // Dataframe_H