// SCOPE_EXIT is used to execute a series of registered functions when it goes
// out of scope.
// For details, refer to Andrei Alexandrescu's CppCon 2015 talk "Declarative
// Control Flow"
//
// Examples:
//   void Function() {
//     FILE* fp = fopen("my_file.txt", "r");
//     SCOPE_EXIT { fclose(fp); };
//     // Do something.
//   }  // fp will be closed at exit, or pre-emptively if function is early returned due to failure in the middle.

#pragma once

#include <functional>
#include <utility>

#include "utils/meta.hpp"

namespace duckdb {

class ScopeGuard {
private:
    using Func = std::function<void()>;

public:
    ScopeGuard() : func_([]() {}) {}
    explicit ScopeGuard(Func &&func) : func_(std::forward<Func>(func)) {}
    ~ScopeGuard() noexcept {
        func_();
    }

    // Register a new function to be invoked at destruction.
    // Execution will be performed at the reversed order they're registered.
    ScopeGuard &operator+=(Func &&another_func) {
        Func cur_func = std::move(func_);
        func_ = [cur_func = std::move(cur_func), another_func = std::move(another_func)]() {
            // Executed in the reverse order functions are registered.
            another_func();
            cur_func();
        };
        return *this;
    }

private:
    Func func_;
};

namespace internal {

using ScopeGuardFunc = std::function<void()>;

// Constructs a scope guard that calls 'fn' when it exits.
enum class ScopeGuardOnExit {};
inline auto operator+(ScopeGuardOnExit /*unused*/, ScopeGuardFunc fn) {
    return ScopeGuard{std::move(fn)};
}

} // namespace internal

} // namespace duckdb

#define SCOPE_EXIT auto MOONCAKE_UNIQUE_VARIABLE(SCOPE_EXIT_TEMP_EXIT) = duckdb::internal::ScopeGuardOnExit{} + [&]()
