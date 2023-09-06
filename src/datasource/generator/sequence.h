#pragma once

#include <coroutine>
#include <iostream>

namespace datafusionx {
template<typename T>
struct SequenceType {
  struct promise_type {
    T current_value;
    auto get_return_object() { return SequenceType{this}; }
    auto initial_suspend() { return std::suspend_always{}; }
    auto final_suspend() noexcept { return std::suspend_always{}; }
    auto yield_value(T value) {
      current_value = value;
      return std::suspend_always{};
    }
    void return_void() {}
    void unhandled_exception() { std::terminate(); }
  };

  using handle_type = std::coroutine_handle<promise_type>;
  bool move_next() { return coro ? (coro.resume(), !coro.done()) : false; }
  T current_value() { return coro.promise().current_value; }
  SequenceType(SequenceType const&) = delete;
  SequenceType(SequenceType && rhs) : coro(rhs.coro) { rhs.coro = nullptr; }
  ~SequenceType() { if (coro) coro.destroy(); }
 private:
  SequenceType(promise_type* p) : coro(handle_type::from_promise(*p)) {}
  handle_type coro;
};

using Sequence = std::vector<std::shared_ptr<RecordBatch>>;

}