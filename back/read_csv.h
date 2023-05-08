#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <map>

using Row = std::map<std::string, int>;
using Table = std::vector<Row>;

Table read_csv(const std::string &file_name) {
  Table employees;
  std::ifstream file(file_name);

  if (!file.is_open()) {
    std::cerr << "Error opening file: " << file_name << std::endl;
    return employees;
  }

  std::string line;
  std::getline(file, line); // Skip header

  while (std::getline(file, line)) {
    std::istringstream ss(line);
    Row employee;

    std::string token;
    std::getline(ss, token, ',');
    employee["row"] = std::stoi(token);
    employee["id"] = std::stoi(token);

    std::getline(ss, token, ',');
    employee["name"] = token.length();

    std::getline(ss, token, ',');
    employee["dept"] = std::stoi(token);

    std::getline(ss, token, ',');
    employee["salary"] = std::stoi(token);

    std::getline(ss, token, ',');
    employee["tax"] = std::stoi(token);

    employees.push_back(employee);
  }

  file.close();
  return employees;
}