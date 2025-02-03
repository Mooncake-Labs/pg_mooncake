#pragma once

namespace duckdb {

template <typename... Ts> struct VoidTImpl { using type = void; };

template <typename... Ts> using void_t = typename VoidTImpl<Ts...>::type;

} // namespace duckdb
