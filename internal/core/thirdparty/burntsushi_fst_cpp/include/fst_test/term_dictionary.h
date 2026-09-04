#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace fst_test {

using TermEntry = std::pair<std::string, std::uint32_t>;

struct FuzzyMatch {
    std::string term;
    std::uint32_t document_frequency;
    std::uint32_t edit_distance;

    bool operator==(const FuzzyMatch&) const = default;
};

struct FuzzySearchResult {
    std::vector<FuzzyMatch> matches;

    // Number of complete dictionary terms examined by the implementation.
    // An automaton-intersection implementation may report zero and expose
    // implementation-specific traversal counters later.
    std::size_t visited_terms = 0;
    std::size_t visited_states = 0;
    std::size_t visited_arcs = 0;
};

struct DictionaryStats {
    std::size_t term_count = 0;
    std::size_t key_storage_bytes = 0;
    std::size_t value_storage_bytes = 0;
};

inline constexpr std::uint64_t kTraversalChecksumOffsetBasis =
    14695981039346656037ULL;
inline constexpr std::uint64_t kTraversalChecksumPrime = 1099511628211ULL;

struct DictionaryTraversalResult {
    std::size_t terms = 0;
    std::size_t term_bytes = 0;
    std::uint64_t checksum = kTraversalChecksumOffsetBasis;
};

inline void
AddTraversalEntry(DictionaryTraversalResult& result,
                  std::string_view term,
                  std::uint32_t document_frequency) {
    const auto mix_byte = [&](std::uint8_t byte) {
        result.checksum =
            (result.checksum ^ byte) * kTraversalChecksumPrime;
    };
    const auto length = static_cast<std::uint64_t>(term.size());
    for (std::size_t shift = 0; shift < sizeof(length); ++shift) {
        mix_byte(static_cast<std::uint8_t>(length >> (shift * 8)));
    }
    for (const auto byte : term) {
        mix_byte(static_cast<std::uint8_t>(byte));
    }
    for (std::size_t shift = 0; shift < sizeof(document_frequency); ++shift) {
        mix_byte(static_cast<std::uint8_t>(document_frequency >> (shift * 8)));
    }
    ++result.terms;
    result.term_bytes += term.size();
}

class TermDictionary {
 public:
    virtual ~TermDictionary() = default;

    [[nodiscard]] virtual std::string_view Name() const = 0;

    virtual void Build(const std::vector<TermEntry>& entries) = 0;

    [[nodiscard]] virtual std::optional<std::uint32_t> Lookup(
        std::string_view term) const = 0;

    [[nodiscard]] virtual FuzzySearchResult FuzzySearch(
        std::string_view query,
        std::uint32_t max_edit_distance,
        std::size_t max_expansions) const = 0;

    virtual void Save(const std::string& path_prefix) const = 0;
    virtual void Load(const std::string& path_prefix) = 0;

    [[nodiscard]] virtual DictionaryStats Stats() const = 0;

    [[nodiscard]] virtual DictionaryTraversalResult TraverseTerms() const = 0;

    // True only when the active query representation is backed directly by
    // read-only file mappings rather than a heap-owned serialized copy.
    [[nodiscard]] virtual bool IsMemoryMapped() const { return false; }
};

}  // namespace fst_test
