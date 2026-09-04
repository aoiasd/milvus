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

#include "textindex/segment_text_term_dictionary.h"

#include <gtest/gtest.h>

#include <atomic>
#include <stdexcept>
#include <string>
#include <thread>

#include "fst_test/burntsushi_fst_cpp/burntsushi_fst_cpp_term_dictionary.h"
#include "levenshtein_dfa.h"
#include "textindex/text_fst_c.h"

namespace milvus::textindex {
namespace {

using fst_test::TermDictionary;
using fst_test::burntsushi_fst_cpp_impl::BurntSushiFstCppTermDictionary;
using fst_test::burntsushi_fst_cpp_impl::PrepareLevenshteinQuery;

TEST(SegmentTextTermDictionaryTest, ResultCleanupPreservesWorkAccounting) {
    CTextFstFuzzyResult result{};
    result.work_used = 42;
    result.work_limit_exceeded = true;

    FreeTextFstFuzzyResult(&result);

    EXPECT_EQ(result.work_used, 42);
    EXPECT_TRUE(result.work_limit_exceeded);
}

TEST(SegmentTextTermDictionaryTest, MutableTrieDeduplicatesAndTracksMemory) {
    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {});
    EXPECT_EQ(dictionary.TrieStats().term_count, 0);
    EXPECT_EQ(dictionary.TrieStats().memory_bytes, 0);

    dictionary.AddTerms(101, {"fuzzy", "milvus", "fuzzy"});
    const auto first = dictionary.TrieStats();
    EXPECT_EQ(first.term_count, 2);
    EXPECT_GT(first.memory_bytes, 0);

    dictionary.AddTerms(101, {"milvus"});
    const auto duplicate = dictionary.TrieStats();
    EXPECT_EQ(duplicate.term_count, first.term_count);
    EXPECT_EQ(duplicate.memory_bytes, first.memory_bytes);

    dictionary.AddTerms(102, {"search"});
    const auto second_field = dictionary.TrieStats();
    EXPECT_EQ(second_field.term_count, 3);
    EXPECT_GT(second_field.memory_bytes, first.memory_bytes);
}

TEST(SegmentTextTermDictionaryTest, MutableTrieSupportsDamerauAndUtf8) {
    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"book", "你好"});

    auto transposition = dictionary.FuzzySearch(101, {}, "boko", 1, 50, 0);
    ASSERT_EQ(transposition.matches.size(), 1);
    EXPECT_EQ(transposition.matches[0].term, "book");
    EXPECT_EQ(transposition.matches[0].edit_distance, 1);

    auto utf8 = dictionary.FuzzySearch(101, {}, "你号", 1, 50, 0);
    ASSERT_EQ(utf8.matches.size(), 1);
    EXPECT_EQ(utf8.matches[0].term, "你好");
    EXPECT_EQ(utf8.matches[0].edit_distance, 1);
}

TEST(SegmentTextTermDictionaryTest, MutableTrieSupportsConcurrentAddAndSearch) {
    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"term0"});

    std::atomic<bool> start = false;
    std::atomic<bool> done = false;
    std::atomic<std::size_t> searches = 0;
    std::thread reader([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        while (!done.load(std::memory_order_acquire)) {
            const auto result =
                dictionary.FuzzySearch(101, {}, "term0", 1, 10, 0, 1'000'000);
            EXPECT_FALSE(result.work_limit_exceeded);
            searches.fetch_add(1, std::memory_order_release);
        }
    });
    std::thread writer([&] {
        start.store(true, std::memory_order_release);
        while (searches.load(std::memory_order_acquire) == 0) {
            std::this_thread::yield();
        }
        for (int i = 1; i <= 1'000; ++i) {
            dictionary.AddTerms(101, {"term" + std::to_string(i)});
        }
        done.store(true, std::memory_order_release);
    });

    writer.join();
    reader.join();
    EXPECT_GT(searches.load(std::memory_order_acquire), 0);
    EXPECT_EQ(dictionary.TrieStats().term_count, 1'001);
    const auto result =
        dictionary.FuzzySearch(101, {}, "term1000", 0, 10, 0, 1'000'000);
    ASSERT_EQ(result.matches.size(), 1);
    EXPECT_EQ(result.matches[0].term, "term1000");
}

TEST(SegmentTextTermDictionaryTest, PrefixLengthUsesUnicodeCharacters) {
    SegmentTextTermDictionary growing;
    growing.AddTerms(101, {"book", "你好"});

    ASSERT_EQ(growing.FuzzySearch(101, {}, "cook", 1, 50, 0).matches.size(), 1);
    EXPECT_TRUE(growing.FuzzySearch(101, {}, "cook", 1, 50, 1).matches.empty());

    const auto unicode = growing.FuzzySearch(101, {}, "你号", 1, 50, 1);
    ASSERT_EQ(unicode.matches.size(), 1);
    EXPECT_EQ(unicode.matches[0].term, "你好");
    EXPECT_EQ(unicode.matches[0].edit_distance, 1);
    EXPECT_TRUE(growing.FuzzySearch(101, {}, "他好", 1, 50, 1).matches.empty());

    BurntSushiFstCppTermDictionary sealed;
    sealed.Build({{"book", 1}, {"你好", 1}});
    const std::vector<const TermDictionary*> fsts{&sealed};
    ASSERT_EQ(growing.FuzzySearch(102, fsts, "cook", 1, 50, 0).matches.size(),
              1);
    EXPECT_TRUE(
        growing.FuzzySearch(102, fsts, "cook", 1, 50, 1).matches.empty());
    ASSERT_EQ(growing.FuzzySearch(102, fsts, "你号", 1, 50, 1).matches.size(),
              1);
    EXPECT_TRUE(
        growing.FuzzySearch(102, fsts, "他好", 1, 50, 1).matches.empty());
}

TEST(SegmentTextTermDictionaryTest, MutableTrieKeepsBoundedBestMatches) {
    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"boo", "coo", "doo", "zoo"});

    const auto matches = dictionary.FuzzySearch(101, {}, "zoo", 1, 2, 0);
    ASSERT_EQ(matches.matches.size(), 2);
    EXPECT_EQ(matches.matches[0].term, "zoo");
    EXPECT_EQ(matches.matches[0].edit_distance, 0);
    EXPECT_EQ(matches.matches[1].term, "boo");
    EXPECT_EQ(matches.matches[1].edit_distance, 1);
}

TEST(SegmentTextTermDictionaryTest, CombinesFstsAndMutableTrie) {
    BurntSushiFstCppTermDictionary first;
    first.Build({{"boo", 1}, {"coo", 1}});
    BurntSushiFstCppTermDictionary second;
    second.Build({{"doo", 1}});
    const std::vector<const TermDictionary*> fsts{&first, &second};

    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"zoo", "boo"});
    const auto matches = dictionary.FuzzySearch(101, fsts, "zoo", 1, 1, 0);

    // The current contract applies max_expansions to each FST/Trie before
    // merging, so the union may be larger than the configured value.
    ASSERT_EQ(matches.matches.size(), 3);
    EXPECT_EQ(matches.matches[0].term, "zoo");
    EXPECT_EQ(matches.matches[0].edit_distance, 0);
    EXPECT_EQ(matches.matches[1].term, "boo");
    EXPECT_EQ(matches.matches[1].edit_distance, 1);
    EXPECT_EQ(matches.matches[2].term, "doo");
    EXPECT_EQ(matches.matches[2].edit_distance, 1);
}

TEST(SegmentTextTermDictionaryTest, ReusesOnePreparedQueryAcrossSegments) {
    BurntSushiFstCppTermDictionary first_fst;
    first_fst.Build({{"book", 1}});
    BurntSushiFstCppTermDictionary second_fst;
    second_fst.Build({{"books", 1}});
    const std::vector<const TermDictionary*> fsts{&first_fst, &second_fst};

    SegmentTextTermDictionary sealed;
    SegmentTextTermDictionary growing;
    growing.AddTerms(101, {"boo"});
    auto prepared = PrepareLevenshteinQuery("bok", 1, true, 0, 1'000'000);
    ASSERT_FALSE(prepared.work_limit_exceeded);
    ASSERT_TRUE(prepared.query.has_value());

    const auto sealed_matches =
        sealed.FuzzySearchPrepared(101, fsts, *prepared.query, 50, 1'000'000);
    const auto growing_matches =
        growing.FuzzySearchPrepared(101, {}, *prepared.query, 50, 1'000'000);
    ASSERT_EQ(sealed_matches.matches.size(), 1);
    EXPECT_EQ(sealed_matches.matches[0].term, "book");
    ASSERT_EQ(growing_matches.matches.size(), 1);
    EXPECT_EQ(growing_matches.matches[0].term, "boo");

    const auto exact_budget = prepared.work_used + sealed_matches.work_used;
    const auto combined =
        sealed.FuzzySearch(101, fsts, "bok", 1, 50, 0, exact_budget);
    EXPECT_FALSE(combined.work_limit_exceeded);
    EXPECT_EQ(combined.work_used, exact_budget);
}

TEST(SegmentTextTermDictionaryTest,
     BoundsTraversalIndependentlyOfExpansionLimit) {
    BurntSushiFstCppTermDictionary sealed;
    sealed.Build({{"boo", 1}, {"coo", 1}, {"doo", 1}, {"zoo", 1}});
    const std::vector<const TermDictionary*> fsts{&sealed};

    SegmentTextTermDictionary dictionary;
    const auto bounded = dictionary.FuzzySearch(101, fsts, "zoo", 1, 1, 0, 1);
    EXPECT_TRUE(bounded.work_limit_exceeded);
    EXPECT_EQ(bounded.work_used, 1);
    EXPECT_TRUE(bounded.matches.empty());

    const auto complete =
        dictionary.FuzzySearch(101, fsts, "zoo", 1, 1, 0, 1'000'000);
    EXPECT_FALSE(complete.work_limit_exceeded);
    EXPECT_GT(complete.work_used, 1);
    ASSERT_EQ(complete.matches.size(), 1);
    EXPECT_EQ(complete.matches[0].term, "zoo");
}

TEST(SegmentTextTermDictionaryTest, PrefixMissKeepsConsumedWork) {
    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"abcdef"});

    const std::string query = "abcxyf";
    const auto miss =
        dictionary.FuzzySearch(101, {}, query, 1, 1, 5, 1'000'000);
    EXPECT_FALSE(miss.work_limit_exceeded);
    EXPECT_TRUE(miss.matches.empty());
    EXPECT_GT(miss.work_used, query.size() + 1);
}

TEST(SegmentTextTermDictionaryTest, BoundsDfaPreprocessing) {
    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"zoo"});

    const auto bounded = dictionary.FuzzySearch(101, {}, "zoo", 1, 1, 0, 20);
    EXPECT_TRUE(bounded.work_limit_exceeded);
    EXPECT_EQ(bounded.work_used, 20);
    EXPECT_TRUE(bounded.matches.empty());
}

TEST(SegmentTextTermDictionaryTest, ImportsFstsIntoOneMutableTrie) {
    BurntSushiFstCppTermDictionary first;
    first.Build({{"book", 1}, {"fuzzy", 1}});
    BurntSushiFstCppTermDictionary second;
    second.Build({{"books", 1}, {"fuzzy", 1}, {"milvus", 1}});
    const std::vector<const TermDictionary*> fsts{&first, &second};

    SegmentTextTermDictionary dictionary;
    dictionary.AddFstTerms(101, fsts);
    const auto stats = dictionary.TrieStats();
    EXPECT_EQ(stats.term_count, 4);
    EXPECT_GT(stats.memory_bytes, 0);

    // The expansion bound is applied once to the complete imported
    // vocabulary, rather than once per recovery fragment.
    const auto matches = dictionary.FuzzySearch(101, {}, "book", 1, 1, 0);
    ASSERT_EQ(matches.matches.size(), 1);
    EXPECT_EQ(matches.matches[0].term, "book");
    EXPECT_EQ(matches.matches[0].edit_distance, 0);
}

TEST(SegmentTextTermDictionaryTest, RejectsInvalidTermsBeforeMutation) {
    SegmentTextTermDictionary dictionary;
    const std::string invalid_utf8(1, static_cast<char>(0xff));
    EXPECT_THROW(dictionary.AddTerms(101, {"valid", invalid_utf8}),
                 std::invalid_argument);
    EXPECT_THROW(dictionary.AddTerms(101, {""}), std::invalid_argument);
    EXPECT_EQ(dictionary.TrieStats().term_count, 0);
}

}  // namespace
}  // namespace milvus::textindex
