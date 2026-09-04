#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace milvus::textindex {

struct LevenshteinDfaBuildResult;

class LevenshteinDfa {
 public:
    [[nodiscard]] std::uint32_t
    InitialState() const;
    [[nodiscard]] std::uint32_t
    Transition(std::uint32_t state, std::uint8_t byte) const;
    [[nodiscard]] bool
    IsMatch(std::uint32_t state) const;
    [[nodiscard]] bool
    CanMatch(std::uint32_t state) const;
    [[nodiscard]] std::uint32_t
    Distance(std::uint32_t state) const;

 private:
    friend LevenshteinDfaBuildResult BuildLevenshteinDfa(std::string_view,
                                                         std::uint32_t,
                                                         std::size_t);

    std::vector<std::array<std::uint32_t, 256>> transitions_;
    std::vector<std::uint8_t> distances_;
    std::uint32_t initial_state_ = 0;
    std::uint8_t max_distance_ = 0;
};

struct LevenshteinDfaBuildResult {
    std::optional<LevenshteinDfa> dfa;
    std::size_t work_used = 0;
    bool work_limit_exceeded = false;
};

struct PreparedLevenshteinQuery {
    std::string query;
    std::string exact_prefix;
    std::optional<LevenshteinDfa> dfa;
    std::uint32_t max_distance = 0;
};

struct PreparedLevenshteinQueryBuildResult {
    std::optional<PreparedLevenshteinQuery> query;
    std::size_t work_used = 0;
    bool work_limit_exceeded = false;
};

[[nodiscard]] LevenshteinDfaBuildResult
BuildLevenshteinDfa(std::string_view query,
                    std::uint32_t max_distance,
                    std::size_t work_budget);

[[nodiscard]] PreparedLevenshteinQueryBuildResult
PrepareLevenshteinQuery(std::string_view query,
                        std::uint32_t max_distance,
                        std::uint32_t prefix_length,
                        std::size_t work_budget);

void
ValidateUtf8(std::string_view text);

// Returns the UTF-8 byte offset after the first char_count Unicode code
// points, clamped to the end of text. The complete input is validated first.
[[nodiscard]] std::size_t
Utf8PrefixByteLength(std::string_view text, std::size_t char_count);

}  // namespace milvus::textindex
