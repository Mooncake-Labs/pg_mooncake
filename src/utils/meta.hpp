// A few macro utils.

#pragma once

// Need to have two macro invocation to allow [x] and [y] to be replaced.
#define __MOONCAKE_CONCAT(x, y) x##y

#define MOONCAKE_CONCAT(x, y) __MOONCAKE_CONCAT(x, y)

// Macros which gets unique variable name by suffix line number.
#define MOONCAKE_UNIQUE_VARIABLE(base) MOONCAKE_CONCAT(base, __LINE__)
