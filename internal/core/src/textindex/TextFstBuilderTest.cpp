// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "fst_test/burntsushi_fst_cpp/burntsushi_fst_cpp_term_dictionary.h"

namespace milvus::textindex {
namespace {

using fst_test::burntsushi_fst_cpp_impl::BurntSushiFstCppTermDictionary;
using fst_test::burntsushi_fst_cpp_impl::TermView;

TEST(TextFstBuilderTest, SortedStreamBuildMatchesGenericBuild) {
    const std::vector<fst_test::TermEntry> entries{
        {"book", 1}, {"books", 2}, {"fuzzy", 3}, {"你好", 4}};

    BurntSushiFstCppTermDictionary generic;
    generic.Build(entries);

    BurntSushiFstCppTermDictionary streamed;
    std::size_t index = 0;
    streamed.BuildSorted([&]() -> std::optional<TermView> {
        if (index == entries.size()) {
            return std::nullopt;
        }
        const auto& [term, document_frequency] = entries[index++];
        return TermView{term, document_frequency};
    });

    const auto expected = generic.SerializedBytes();
    const auto actual = streamed.SerializedBytes();
    ASSERT_EQ(actual.size(), expected.size());
    EXPECT_TRUE(std::equal(actual.begin(), actual.end(), expected.begin()));
    EXPECT_TRUE(streamed.VerifyChecksum());
}

TEST(TextFstBuilderTest, SortedStreamRejectsInvalidEntries) {
    const auto expect_invalid = [](std::vector<fst_test::TermEntry> entries) {
        BurntSushiFstCppTermDictionary dictionary;
        std::size_t index = 0;
        EXPECT_THROW(dictionary.BuildSorted([&]() -> std::optional<TermView> {
            if (index == entries.size()) {
                return std::nullopt;
            }
            const auto& [term, document_frequency] = entries[index++];
            return TermView{term, document_frequency};
        }),
                     std::invalid_argument);
    };

    expect_invalid({{"books", 1}, {"book", 1}});
    expect_invalid({{"book", 1}, {"book", 1}});
    expect_invalid({{"", 1}});
    expect_invalid({{std::string(1, static_cast<char>(0xff)), 1}});
    expect_invalid({{"book", 0}});
}

}  // namespace
}  // namespace milvus::textindex
