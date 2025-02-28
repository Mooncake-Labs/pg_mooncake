// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Apapted from abseil `resize_uninitialized` implementation.

#pragma once

#include <cstddef>
#include <string>
#include <type_traits>

#include "utils/type_traits.hpp"

namespace duckdb {

namespace internal {

// In this type trait, we look for a __resize_default_init member function, and
// we use it if available, otherwise, we use resize. We provide HasMember to
// indicate whether __resize_default_init is present.
template <typename string_type, typename = void> struct ResizeUninitializedTraits {
    using HasMember = std::false_type;
    static void Resize(string_type *s, size_t new_size) {
        s->resize(new_size);
    }
};

// __resize_default_init is provided by libc++ >= 8.0
template <typename string_type>
struct ResizeUninitializedTraits<string_type,
                                 void_t<decltype(std::declval<string_type &>().__resize_default_init(237))>> {
    using HasMember = std::true_type;
    static void Resize(string_type *s, size_t new_size) {
        s->__resize_default_init(new_size);
    }
};

} // namespace internal

// Like str->resize(new_size), except any new characters added to "*str" as a
// result of resizing may be left uninitialized, rather than being filled with
// '0' bytes. Typically used when code is then going to overwrite the backing
// store of the std::string with known data.
template <typename string_type, typename = void> void STLStringResizeUninitialized(string_type *s, size_t new_size) {
    internal::ResizeUninitializedTraits<string_type>::Resize(s, new_size);
}

// Create a string with the given size, with all bytes uninitialized. Useful to use as a buffer.
inline std::string CreateResizeUninitializedString(size_t size) {
    std::string content;
    STLStringResizeUninitialized(&content, size);
    return content;
}

} // namespace duckdb
