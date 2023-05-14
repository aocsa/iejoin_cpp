//
// Created by galex on 10/21/2022.
//

#pragma once

#include <version>

// utilities
#include <any>
#include <bitset>
#include <chrono>
#include <compare>
// #include <expected>
#include <functional>
#include <initializer_list>
#include <optional>
// #include <source_location>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <variant>

// numeric limits
#include <limits>
#ifdef __cpp_lib_stdfloat
  #include <stdfloat>
#endif

// error
#include <exception>
#include <stdexcept>
#include <system_error>

// string
#include <charconv>
#include <format>
#include <string>

// containers
#include <array>
#include <deque>
#ifdef __cpp_lib_flat_map
  #include <flat_map>
#endif
#ifdef __cpp_lib_flat_set
  #include <flat_set>
#endif
#include <forward_list>
#include <list>
#include <map>
#ifdef __cpp_lib_mdspan
  #include <mdspan>
#endif
#include <queue>
#include <set>
#include <span>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// iterator
#include <iterator>

// ranges
#ifdef __cpp_lib_generator
  #include <generator>
#endif
#include <ranges>

// algorithms
#include <algorithm>
#include <execution>

// numerics
#include <bit>
#include <complex>
#include <numbers>
#include <numeric>
#include <random>
#include <ratio>
#include <valarray>

// input/output
#include <fstream>
#include <iomanip>
#include <iostream>
#ifdef __cpp_lib_print
  #include <print>
#endif
#include <sstream>
#include <streambuf>
