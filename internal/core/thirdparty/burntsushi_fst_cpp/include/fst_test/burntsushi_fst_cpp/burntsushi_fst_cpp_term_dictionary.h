#pragma once

#include "fst_test/term_dictionary.h"

#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

namespace fst_test::burntsushi_fst_cpp_impl {

enum class FuzzyTraversalMode {
    kSpecializedEarlyPrune,
    kRustGenericAligned,
};

enum class EditDistanceMode {
    kDamerauLevenshteinOsa,
    kLevenshtein,
};

struct TermView {
    std::string_view term;
    std::uint32_t document_frequency;
};

// Returns the next strictly byte-sorted term, or nullopt at end of stream.
// The returned view only needs to remain valid until the next reader call.
using SortedTermReader = std::function<std::optional<TermView>()>;

class BurntSushiFstCppTermDictionary final : public TermDictionary {
 public:
    explicit BurntSushiFstCppTermDictionary(
        FuzzyTraversalMode mode = FuzzyTraversalMode::kSpecializedEarlyPrune,
        EditDistanceMode edit_distance_mode =
            EditDistanceMode::kDamerauLevenshteinOsa);
    ~BurntSushiFstCppTermDictionary() override;

    BurntSushiFstCppTermDictionary(const BurntSushiFstCppTermDictionary&) = delete;
    BurntSushiFstCppTermDictionary& operator=(const BurntSushiFstCppTermDictionary&) = delete;
    BurntSushiFstCppTermDictionary(BurntSushiFstCppTermDictionary&&) noexcept;
    BurntSushiFstCppTermDictionary& operator=(BurntSushiFstCppTermDictionary&&) noexcept;

    [[nodiscard]] std::string_view Name() const override;
    void Build(const std::vector<TermEntry>& entries) override;
    void BuildSorted(const SortedTermReader& reader);
    [[nodiscard]] std::optional<std::uint32_t> Lookup(
        std::string_view term) const override;
    [[nodiscard]] FuzzySearchResult FuzzySearch(
        std::string_view query,
        std::uint32_t max_edit_distance,
        std::size_t max_expansions) const override;
    void Save(const std::string& path_prefix) const override;
    void Load(const std::string& path_prefix) override;
    void LoadFile(const std::string& path, bool memory_mapped);
    void LoadBytes(std::span<const std::uint8_t> bytes);
    [[nodiscard]] DictionaryStats Stats() const override;
    [[nodiscard]] DictionaryTraversalResult TraverseTerms() const override;
    void VisitTerms(const TermVisitor& visitor) const override;
    [[nodiscard]] bool IsMemoryMapped() const override;

    [[nodiscard]] std::span<const std::uint8_t> SerializedBytes() const;
    [[nodiscard]] bool VerifyChecksum() const;
    [[nodiscard]] FuzzyTraversalMode TraversalMode() const;
    [[nodiscard]] EditDistanceMode DistanceMode() const;

 private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace fst_test::burntsushi_fst_cpp_impl
