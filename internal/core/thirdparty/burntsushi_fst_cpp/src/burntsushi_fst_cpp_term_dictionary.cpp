#include "fst_test/burntsushi_fst_cpp/burntsushi_fst_cpp_term_dictionary.h"

#include "fst_test/mapped_file.h"
#include "levenshtein_dfa.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace fst_test::burntsushi_fst_cpp_impl {
namespace {

using Address = std::size_t;
using Output = std::uint64_t;

constexpr std::uint64_t kVersion = 3;
constexpr std::uint64_t kFstType = 0;
constexpr Address kEmptyAddress = 0;
constexpr Address kNoneAddress = 1;
constexpr std::size_t kTransitionIndexThreshold = 32;
constexpr std::size_t kTrailerBytes = 20;
constexpr std::size_t kRootAddressOffset = kTrailerBytes + 1;
constexpr std::string_view kArtifactSuffix = ".burntsushi_fst_cpp";
constexpr std::array<std::uint8_t, 63> kCommonInputs = {
    't', 'e', '/', 'o', 'a', 's', 'r', 'i', 'p', 'c', 'n', 'w', '.', 'h',
    'l', 'm', '-', 'd', 'u', '0', '1', '2', 'g', '=', ':', 'b', 'f', '3',
    'y', '5', '&', '_', '4', 'v', '9', '6', '7', '8', 'k', '%', '?', 'x',
    'C', 'D', 'A', 'S', 'F', 'I', 'B', 'E', 'j', 'P', 'T', 'z', 'R', 'N',
    'M', '+', 'L', 'O', 'q', 'H', 'G',
};

constexpr auto kCommonInputIndexes = [] {
    std::array<std::uint8_t, 256> indexes{};
    for (std::size_t index = 0; index < kCommonInputs.size(); ++index) {
        indexes[kCommonInputs[index]] = static_cast<std::uint8_t>(index + 1);
    }
    return indexes;
}();

constexpr auto kCrc32cTable = [] {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t index = 0; index < table.size(); ++index) {
        auto crc = index;
        for (std::uint32_t bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^ (0x82F63B78U & (0U - (crc & 1U)));
        }
        table[index] = crc;
    }
    return table;
}();

constexpr auto kCrc32cTable16 = [] {
    std::array<std::array<std::uint32_t, 256>, 16> tables{};
    tables[0] = kCrc32cTable;
    for (std::size_t index = 0; index < 256; ++index) {
        auto crc = tables[0][index];
        for (std::size_t slice = 1; slice < tables.size(); ++slice) {
            crc = (crc >> 8) ^ tables[0][crc & 0xFFU];
            tables[slice][index] = crc;
        }
    }
    return tables;
}();

void
WriteU32(std::vector<std::uint8_t>& data, std::uint32_t value) {
    for (std::size_t shift = 0; shift < 4; ++shift) {
        data.push_back(static_cast<std::uint8_t>(value >> (shift * 8)));
    }
}

void
WriteU64(std::vector<std::uint8_t>& data, std::uint64_t value) {
    for (std::size_t shift = 0; shift < 8; ++shift) {
        data.push_back(static_cast<std::uint8_t>(value >> (shift * 8)));
    }
}

std::uint32_t
ReadU32(std::span<const std::uint8_t> data, std::size_t offset) {
    if (offset > data.size() || data.size() - offset < 4) {
        throw std::runtime_error("truncated BurntSushi FST u32");
    }
    std::uint32_t value = 0;
    for (std::size_t shift = 0; shift < 4; ++shift) {
        value |= static_cast<std::uint32_t>(data[offset + shift]) << (shift * 8);
    }
    return value;
}

std::uint64_t
ReadU64(std::span<const std::uint8_t> data, std::size_t offset) {
    if (offset > data.size() || data.size() - offset < 8) {
        throw std::runtime_error("truncated BurntSushi FST u64");
    }
    std::uint64_t value = 0;
    for (std::size_t shift = 0; shift < 8; ++shift) {
        value |= static_cast<std::uint64_t>(data[offset + shift]) << (shift * 8);
    }
    return value;
}

std::uint8_t
PackSize(std::uint64_t value) {
    for (std::uint8_t bytes = 1; bytes < 8; ++bytes) {
        if (value < (std::uint64_t{1} << (bytes * 8))) {
            return bytes;
        }
    }
    return 8;
}

void
PackUIntIn(std::vector<std::uint8_t>& data,
           std::uint64_t value,
           std::uint8_t bytes) {
    if (bytes == 0 || bytes > 8 || bytes < PackSize(value)) {
        throw std::logic_error("invalid BurntSushi packed integer width");
    }
    for (std::uint8_t index = 0; index < bytes; ++index) {
        data.push_back(static_cast<std::uint8_t>(value >> (index * 8)));
    }
}

std::uint8_t
PackUInt(std::vector<std::uint8_t>& data, std::uint64_t value) {
    const auto bytes = PackSize(value);
    PackUIntIn(data, value, bytes);
    return bytes;
}

std::uint64_t
UnpackUInt(std::span<const std::uint8_t> data,
           std::size_t offset,
           std::size_t bytes) {
    if (bytes == 0 || bytes > 8 || offset > data.size() ||
        data.size() - offset < bytes) {
        throw std::runtime_error("invalid BurntSushi packed integer");
    }
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < bytes; ++index) {
        value |= static_cast<std::uint64_t>(data[offset + index]) << (index * 8);
    }
    return value;
}

std::uint32_t
Crc32c(const std::uint8_t* bytes, std::size_t length) {
    std::uint32_t crc = 0xFFFFFFFFU;
    while (length >= 16) {
        crc ^= static_cast<std::uint32_t>(bytes[0]) |
               (static_cast<std::uint32_t>(bytes[1]) << 8) |
               (static_cast<std::uint32_t>(bytes[2]) << 16) |
               (static_cast<std::uint32_t>(bytes[3]) << 24);
        crc = kCrc32cTable16[0][bytes[15]] ^
              kCrc32cTable16[1][bytes[14]] ^
              kCrc32cTable16[2][bytes[13]] ^
              kCrc32cTable16[3][bytes[12]] ^
              kCrc32cTable16[4][bytes[11]] ^
              kCrc32cTable16[5][bytes[10]] ^
              kCrc32cTable16[6][bytes[9]] ^
              kCrc32cTable16[7][bytes[8]] ^
              kCrc32cTable16[8][bytes[7]] ^
              kCrc32cTable16[9][bytes[6]] ^
              kCrc32cTable16[10][bytes[5]] ^
              kCrc32cTable16[11][bytes[4]] ^
              kCrc32cTable16[12][(crc >> 24) & 0xFFU] ^
              kCrc32cTable16[13][(crc >> 16) & 0xFFU] ^
              kCrc32cTable16[14][(crc >> 8) & 0xFFU] ^
              kCrc32cTable16[15][crc & 0xFFU];
        bytes += 16;
        length -= 16;
    }
    while (length-- != 0) {
        crc = kCrc32cTable[(crc ^ *bytes++) & 0xFFU] ^ (crc >> 8);
    }
    return ~crc;
}

std::uint32_t
MaskedCrc32c(const std::uint8_t* bytes, std::size_t length) {
    const auto crc = Crc32c(bytes, length);
    return ((crc >> 15) | (crc << 17)) + 0xA282EAD8U;
}

std::uint8_t
CommonIndex(std::uint8_t input) {
    return kCommonInputIndexes[input];
}

std::optional<std::uint8_t>
CommonInput(std::uint8_t index) {
    if (index == 0) {
        return std::nullopt;
    }
    if (index > kCommonInputs.size()) {
        throw std::runtime_error("invalid BurntSushi common-input index");
    }
    return kCommonInputs[index - 1];
}

struct Transition {
    std::uint8_t input = 0;
    Output output = 0;
    Address address = kNoneAddress;

    bool operator==(const Transition&) const = default;
};

struct BuilderNode {
    bool is_final = false;
    Output final_output = 0;
    std::vector<Transition> transitions;

    bool operator==(const BuilderNode&) const = default;
};

struct LastTransition {
    std::uint8_t input = 0;
    Output output = 0;
};

struct UnfinishedNode {
    BuilderNode node;
    std::optional<LastTransition> last;

    void LastCompiled(Address address) {
        if (last.has_value()) {
            node.transitions.push_back(
                Transition{last->input, last->output, address});
            last.reset();
        }
    }

    void AddOutputPrefix(Output prefix) {
        if (node.is_final) {
            node.final_output += prefix;
        }
        for (auto& transition : node.transitions) {
            transition.output += prefix;
        }
        if (last.has_value()) {
            last->output += prefix;
        }
    }
};

class UnfinishedNodes {
 public:
    UnfinishedNodes() {
        stack_.push_back(UnfinishedNode{});
    }

    std::size_t Size() const { return stack_.size(); }

    BuilderNode PopRoot() {
        if (stack_.size() != 1 || stack_.front().last.has_value()) {
            throw std::logic_error("invalid unfinished BurntSushi FST root");
        }
        auto root = std::move(stack_.front().node);
        stack_.clear();
        return root;
    }

    BuilderNode PopFreeze(Address address) {
        auto unfinished = std::move(stack_.back());
        stack_.pop_back();
        unfinished.LastCompiled(address);
        return std::move(unfinished.node);
    }

    BuilderNode PopEmpty() {
        auto unfinished = std::move(stack_.back());
        stack_.pop_back();
        if (unfinished.last.has_value()) {
            throw std::logic_error("expected empty unfinished FST node");
        }
        return std::move(unfinished.node);
    }

    void TopLastFreeze(Address address) {
        if (stack_.empty()) {
            throw std::logic_error("missing unfinished FST node");
        }
        stack_.back().LastCompiled(address);
    }

    void SetRootOutput(Output output) {
        stack_.front().node.is_final = true;
        stack_.front().node.final_output = output;
    }

    std::pair<std::size_t, Output> FindCommonPrefixAndSetOutput(
        std::string_view term,
        Output output) {
        std::size_t index = 0;
        while (index < term.size()) {
            auto& unfinished = stack_.at(index);
            if (!unfinished.last.has_value() ||
                unfinished.last->input != static_cast<std::uint8_t>(term[index])) {
                break;
            }
            ++index;
            auto& transition_output = unfinished.last->output;
            const auto common = std::min(transition_output, output);
            const auto add_prefix = transition_output - common;
            output -= common;
            transition_output = common;
            if (add_prefix != 0) {
                stack_.at(index).AddOutputPrefix(add_prefix);
            }
        }
        return {index, output};
    }

    void AddSuffix(std::string_view suffix, Output output) {
        if (suffix.empty()) {
            return;
        }
        if (stack_.back().last.has_value()) {
            throw std::logic_error("unfinished FST node already has a last transition");
        }
        stack_.back().last = LastTransition{
            static_cast<std::uint8_t>(suffix.front()), output};
        for (std::size_t index = 1; index < suffix.size(); ++index) {
            UnfinishedNode node;
            node.last = LastTransition{
                static_cast<std::uint8_t>(suffix[index]), 0};
            stack_.push_back(std::move(node));
        }
        UnfinishedNode final;
        final.node.is_final = true;
        stack_.push_back(std::move(final));
    }

 private:
    std::vector<UnfinishedNode> stack_;
};

struct RegistryCell {
    Address address = kNoneAddress;
    BuilderNode node;
};

struct RegistryResult {
    std::optional<Address> found;
    RegistryCell* cell = nullptr;
};

class Registry {
 public:
    Registry() : cells_(10'000 * 2) {}

    RegistryResult Entry(const BuilderNode& node) {
        const auto bucket = Hash(node) % 10'000;
        auto& first = cells_[bucket * 2];
        auto& second = cells_[bucket * 2 + 1];
        if (first.address != kNoneAddress && first.node == node) {
            return RegistryResult{first.address, nullptr};
        }
        if (second.address != kNoneAddress && second.node == node) {
            const auto address = second.address;
            std::swap(first, second);
            return RegistryResult{address, nullptr};
        }
        second.node = node;
        std::swap(first, second);
        return RegistryResult{std::nullopt, &first};
    }

 private:
    static std::uint64_t Hash(const BuilderNode& node) {
        constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;
        constexpr std::uint64_t kFnvPrime = 1099511628211ULL;
        std::uint64_t hash = kFnvOffset;
        auto mix = [&](std::uint64_t value) {
            hash = (hash ^ value) * kFnvPrime;
        };
        mix(node.is_final ? 1 : 0);
        mix(node.final_output);
        for (const auto& transition : node.transitions) {
            mix(transition.input);
            mix(transition.output);
            mix(transition.address);
        }
        return hash;
    }

    std::vector<RegistryCell> cells_;
};

std::uint8_t
PackDeltaSize(Address node_address, Address transition_address) {
    const auto delta = transition_address == kEmptyAddress
                           ? kEmptyAddress
                           : node_address - transition_address;
    return PackSize(delta);
}

void
PackDeltaIn(std::vector<std::uint8_t>& data,
            Address node_address,
            Address transition_address,
            std::uint8_t bytes) {
    const auto delta = transition_address == kEmptyAddress
                           ? kEmptyAddress
                           : node_address - transition_address;
    PackUIntIn(data, delta, bytes);
}

void
CompileOneTransitionNext(std::vector<std::uint8_t>& data,
                         std::uint8_t input) {
    const auto common = CommonIndex(input);
    if (common == 0) {
        data.push_back(input);
    }
    data.push_back(static_cast<std::uint8_t>(0xC0U | common));
}

void
CompileOneTransition(std::vector<std::uint8_t>& data,
                     Address node_address,
                     const Transition& transition) {
    const auto output_size = transition.output == 0
                                 ? 0
                                 : PackUInt(data, transition.output);
    const auto transition_size = PackDeltaSize(node_address, transition.address);
    PackDeltaIn(data, node_address, transition.address, transition_size);
    data.push_back(static_cast<std::uint8_t>((transition_size << 4) | output_size));
    const auto common = CommonIndex(transition.input);
    if (common == 0) {
        data.push_back(transition.input);
    }
    data.push_back(static_cast<std::uint8_t>(0x80U | common));
}

void
CompileAnyTransition(std::vector<std::uint8_t>& data,
                     Address node_address,
                     const BuilderNode& node) {
    if (node.transitions.size() > 256) {
        throw std::logic_error("BurntSushi FST node has more than 256 transitions");
    }
    std::uint8_t transition_size = 0;
    std::uint8_t output_size = PackSize(node.final_output);
    bool any_outputs = node.final_output != 0;
    for (const auto& transition : node.transitions) {
        transition_size = std::max(
            transition_size, PackDeltaSize(node_address, transition.address));
        output_size = std::max(output_size, PackSize(transition.output));
        any_outputs = any_outputs || transition.output != 0;
    }
    if (!any_outputs) {
        output_size = 0;
    }
    if (any_outputs) {
        if (node.is_final) {
            PackUIntIn(data, node.final_output, output_size);
        }
        for (auto transition = node.transitions.rbegin();
             transition != node.transitions.rend(); ++transition) {
            PackUIntIn(data, transition->output, output_size);
        }
    }
    for (auto transition = node.transitions.rbegin();
         transition != node.transitions.rend(); ++transition) {
        PackDeltaIn(data, node_address, transition->address, transition_size);
    }
    for (auto transition = node.transitions.rbegin();
         transition != node.transitions.rend(); ++transition) {
        data.push_back(transition->input);
    }
    if (node.transitions.size() > kTransitionIndexThreshold) {
        std::array<std::uint8_t, 256> index{};
        index.fill(255);
        for (std::size_t i = 0; i < node.transitions.size(); ++i) {
            index[node.transitions[i].input] = static_cast<std::uint8_t>(i);
        }
        data.insert(data.end(), index.begin(), index.end());
    }
    data.push_back(static_cast<std::uint8_t>((transition_size << 4) | output_size));
    const bool external_count = node.transitions.empty() || node.transitions.size() > 63;
    if (external_count) {
        data.push_back(node.transitions.size() == 256
                           ? 1
                           : static_cast<std::uint8_t>(node.transitions.size()));
    }
    auto state = static_cast<std::uint8_t>(node.is_final ? 0x40U : 0U);
    if (!external_count) {
        state |= static_cast<std::uint8_t>(node.transitions.size());
    }
    data.push_back(state);
}

class Builder {
 public:
    Builder() {
        WriteU64(data_, kVersion);
        WriteU64(data_, kFstType);
    }

    void Insert(std::string_view term, Output output) {
        if (last_.has_value() && term == *last_) {
            throw std::invalid_argument("duplicate term: " + std::string(term));
        }
        if (last_.has_value() && term < *last_) {
            throw std::invalid_argument("BurntSushi FST input must be sorted");
        }
        last_ = std::string(term);
        if (term.empty()) {
            length_ = 1;
            unfinished_.SetRootOutput(output);
            return;
        }
        auto [prefix_length, remaining_output] =
            unfinished_.FindCommonPrefixAndSetOutput(term, output);
        if (prefix_length == term.size()) {
            throw std::logic_error("duplicate output term passed FST validation");
        }
        ++length_;
        CompileFrom(prefix_length);
        unfinished_.AddSuffix(term.substr(prefix_length), remaining_output);
    }

    std::vector<std::uint8_t> Finish() {
        CompileFrom(0);
        auto root = unfinished_.PopRoot();
        const auto root_address = Compile(root);
        WriteU64(data_, length_);
        WriteU64(data_, root_address);
        const auto checksum = MaskedCrc32c(data_.data(), data_.size());
        WriteU32(data_, checksum);
        return std::move(data_);
    }

 private:
    void CompileFrom(std::size_t state) {
        Address address = kNoneAddress;
        while (state + 1 < unfinished_.Size()) {
            auto node = address == kNoneAddress
                            ? unfinished_.PopEmpty()
                            : unfinished_.PopFreeze(address);
            address = Compile(node);
        }
        unfinished_.TopLastFreeze(address);
    }

    Address Compile(const BuilderNode& node) {
        if (node.is_final && node.transitions.empty() && node.final_output == 0) {
            return kEmptyAddress;
        }
        auto registry = registry_.Entry(node);
        if (registry.found.has_value()) {
            return *registry.found;
        }
        const auto start = data_.size();
        if (node.transitions.size() == 1 && !node.is_final) {
            const auto& transition = node.transitions.front();
            if (transition.address == last_address_ && transition.output == 0) {
                CompileOneTransitionNext(data_, transition.input);
            } else {
                CompileOneTransition(data_, start, transition);
            }
        } else {
            CompileAnyTransition(data_, start, node);
        }
        last_address_ = data_.size() - 1;
        registry.cell->address = last_address_;
        return last_address_;
    }

    std::vector<std::uint8_t> data_;
    UnfinishedNodes unfinished_;
    Registry registry_;
    std::optional<std::string> last_;
    Address last_address_ = kNoneAddress;
    std::size_t length_ = 0;
};

struct Metadata {
    std::size_t term_count = 0;
    Address root_address = kEmptyAddress;
    std::uint32_t checksum = 0;
};

Metadata
ReadMetadata(std::span<const std::uint8_t> data) {
    if (data.size() < 36 || ReadU64(data, 0) != kVersion ||
        ReadU64(data, 8) != kFstType) {
        throw std::runtime_error("invalid BurntSushi FST version or header");
    }
    const auto checksum_offset = data.size() - 4;
    const auto root = ReadU64(data, checksum_offset - 8);
    const auto length = ReadU64(data, checksum_offset - 16);
    if (root > std::numeric_limits<Address>::max() ||
        length > std::numeric_limits<std::size_t>::max()) {
        throw std::runtime_error("BurntSushi FST metadata overflows this platform");
    }
    const auto root_address = static_cast<Address>(root);
    if (root_address != kEmptyAddress &&
        root_address + kRootAddressOffset != data.size()) {
        throw std::runtime_error("invalid BurntSushi FST root address");
    }
    return Metadata{
        .term_count = static_cast<std::size_t>(length),
        .root_address = root_address,
        .checksum = ReadU32(data, checksum_offset),
    };
}

enum class NodeKind { kEmptyFinal, kOneTransitionNext, kOneTransition, kAny };

struct NodeView {
    struct DecodedTransition {
        std::uint8_t input = 0;
        Output output = 0;
        Address address = kNoneAddress;
    };

    std::span<const std::uint8_t> data;
    Address address = kEmptyAddress;
    Address end = kEmptyAddress;
    NodeKind kind = NodeKind::kEmptyFinal;
    bool is_final = true;
    std::size_t transition_count = 0;
    std::size_t transition_size = 0;
    std::size_t output_size = 0;
    std::size_t input_size = 0;
    std::size_t count_size = 0;
    Output final_output = 0;

    static NodeView Read(std::span<const std::uint8_t> bytes, Address address) {
        if (address == kEmptyAddress) {
            return NodeView{.data = bytes};
        }
        if (address >= bytes.size() - kTrailerBytes) {
            throw std::runtime_error("BurntSushi FST node address is out of range");
        }
        NodeView node;
        node.data = bytes;
        node.address = address;
        const auto state = bytes[address];
        const auto kind = (state & 0xC0U) >> 6;
        if (kind == 3) {
            node.kind = NodeKind::kOneTransitionNext;
            node.is_final = false;
            node.transition_count = 1;
            node.input_size = (state & 0x3FU) == 0 ? 1 : 0;
            if (address < node.input_size) {
                throw std::runtime_error("truncated BurntSushi OTN node");
            }
            node.end = address - node.input_size;
            return node;
        }
        if (kind == 2) {
            node.kind = NodeKind::kOneTransition;
            node.is_final = false;
            node.transition_count = 1;
            node.input_size = (state & 0x3FU) == 0 ? 1 : 0;
            if (address < node.input_size + 1) {
                throw std::runtime_error("truncated BurntSushi OT node");
            }
            const auto sizes = bytes[address - node.input_size - 1];
            node.transition_size = sizes >> 4;
            node.output_size = sizes & 0x0FU;
            const auto body = node.input_size + 1 + node.transition_size +
                              node.output_size;
            if (node.transition_size == 0 || node.transition_size > 8 ||
                node.output_size > 8 || address < body) {
                throw std::runtime_error("invalid BurntSushi OT packed sizes");
            }
            node.end = address - body;
            return node;
        }

        node.kind = NodeKind::kAny;
        node.is_final = (state & 0x40U) != 0;
        const auto inline_count = state & 0x3FU;
        node.count_size = inline_count == 0 ? 1 : 0;
        if (address < node.count_size + 1) {
            throw std::runtime_error("truncated BurntSushi AnyTrans node");
        }
        if (inline_count != 0) {
            node.transition_count = inline_count;
        } else {
            const auto encoded = bytes[address - 1];
            node.transition_count = encoded == 1 ? 256 : encoded;
        }
        const auto sizes = bytes[address - node.count_size - 1];
        node.transition_size = sizes >> 4;
        node.output_size = sizes & 0x0FU;
        const auto index_size = node.transition_count > kTransitionIndexThreshold
                                    ? 256
                                    : 0;
        const auto transition_body = index_size + node.transition_count +
                                     node.transition_count * node.transition_size;
        const auto output_body = node.transition_count * node.output_size +
                                 (node.is_final ? node.output_size : 0);
        const auto body = node.count_size + 1 + transition_body + output_body;
        if (node.transition_size > 8 || node.output_size > 8 || address < body) {
            throw std::runtime_error("invalid BurntSushi AnyTrans packed sizes");
        }
        node.end = address - body;
        if (node.is_final && node.output_size != 0) {
            node.final_output = UnpackUInt(bytes, node.end, node.output_size);
        }
        return node;
    }

    std::uint8_t Input(std::size_t index) const {
        if (index >= transition_count || kind == NodeKind::kEmptyFinal) {
            throw std::out_of_range("BurntSushi FST transition index");
        }
        const auto bytes = data;
        if (kind == NodeKind::kOneTransitionNext ||
            kind == NodeKind::kOneTransition) {
            const auto common = CommonInput(bytes[address] & 0x3FU);
            return common.has_value() ? *common : bytes[address - 1];
        }
        const auto index_size = transition_count > kTransitionIndexThreshold ? 256 : 0;
        return bytes[address - count_size - 1 - index_size - index - 1];
    }

    std::optional<std::size_t> FindInput(std::uint8_t input) const {
        if (kind == NodeKind::kEmptyFinal) {
            return std::nullopt;
        }
        if (kind != NodeKind::kAny) {
            return Input(0) == input ? std::optional<std::size_t>(0) : std::nullopt;
        }
        const auto bytes = data;
        if (transition_count > kTransitionIndexThreshold) {
            const auto start = address - count_size - 1 - 256;
            const auto index = static_cast<std::size_t>(bytes[start + input]);
            return index < transition_count ? std::optional<std::size_t>(index)
                                            : std::nullopt;
        }
        const auto start = address - count_size - 1 - transition_count;
        for (std::size_t offset = 0; offset < transition_count; ++offset) {
            if (bytes[start + offset] == input) {
                return transition_count - offset - 1;
            }
        }
        return std::nullopt;
    }

    Address TransitionAddress(std::size_t index) const {
        if (index >= transition_count || kind == NodeKind::kEmptyFinal) {
            throw std::out_of_range("BurntSushi FST transition index");
        }
        if (kind == NodeKind::kOneTransitionNext) {
            if (end == 0) {
                throw std::runtime_error("invalid BurntSushi OTN target");
            }
            return end - 1;
        }
        const auto bytes = data;
        std::size_t offset = 0;
        if (kind == NodeKind::kOneTransition) {
            offset = address - input_size - 1 - transition_size;
        } else {
            const auto index_size = transition_count > kTransitionIndexThreshold ? 256 : 0;
            offset = address - count_size - 1 - index_size - transition_count -
                     index * transition_size - transition_size;
        }
        const auto delta = UnpackUInt(bytes, offset, transition_size);
        if (delta == kEmptyAddress) {
            return kEmptyAddress;
        }
        if (delta > end) {
            throw std::runtime_error("invalid BurntSushi FST transition delta");
        }
        return end - static_cast<Address>(delta);
    }

    Output TransitionOutput(std::size_t index) const {
        if (index >= transition_count || output_size == 0 ||
            kind == NodeKind::kOneTransitionNext) {
            return 0;
        }
        const auto bytes = data;
        std::size_t offset = 0;
        if (kind == NodeKind::kOneTransition) {
            offset = address - input_size - 1 - transition_size - output_size;
        } else {
            const auto index_size = transition_count > kTransitionIndexThreshold ? 256 : 0;
            const auto total_transition_size = index_size + transition_count +
                                               transition_count * transition_size;
            offset = address - count_size - 1 - total_transition_size -
                     index * output_size - output_size;
        }
        return UnpackUInt(bytes, offset, output_size);
    }

    // This is the direct counterpart of upstream Node::transition. In
    // particular, dispatch on the compiled node kind exactly once and decode
    // input/output/address together. Upstream marks this operation
    // #[inline(always)] because it is on the hottest stream traversal path.
    [[gnu::always_inline]] inline DecodedTransition
    FullTransition(std::size_t index) const {
        if (index >= transition_count || kind == NodeKind::kEmptyFinal) {
            throw std::out_of_range("BurntSushi FST transition index");
        }
        const auto bytes = data;
        switch (kind) {
            case NodeKind::kOneTransitionNext: {
                if (end == 0) {
                    throw std::runtime_error("invalid BurntSushi OTN target");
                }
                const auto common = CommonInput(bytes[address] & 0x3FU);
                return DecodedTransition{
                    .input = common.has_value() ? *common : bytes[address - 1],
                    .output = 0,
                    .address = end - 1,
                };
            }
            case NodeKind::kOneTransition: {
                const auto common = CommonInput(bytes[address] & 0x3FU);
                const auto input =
                    common.has_value() ? *common : bytes[address - 1];
                const auto address_offset =
                    address - input_size - 1 - transition_size;
                const auto delta =
                    UnpackUInt(bytes, address_offset, transition_size);
                if (delta != kEmptyAddress && delta > end) {
                    throw std::runtime_error(
                        "invalid BurntSushi FST transition delta");
                }
                const auto output = output_size == 0
                                        ? 0
                                        : UnpackUInt(bytes,
                                                     address_offset - output_size,
                                                     output_size);
                return DecodedTransition{
                    .input = input,
                    .output = output,
                    .address = delta == kEmptyAddress
                                   ? kEmptyAddress
                                   : end - static_cast<Address>(delta),
                };
            }
            case NodeKind::kAny: {
                const auto index_size =
                    transition_count > kTransitionIndexThreshold ? 256 : 0;
                const auto input_offset =
                    address - count_size - 1 - index_size - index - 1;
                const auto address_offset =
                    address - count_size - 1 - index_size - transition_count -
                    index * transition_size - transition_size;
                const auto delta =
                    UnpackUInt(bytes, address_offset, transition_size);
                if (delta != kEmptyAddress && delta > end) {
                    throw std::runtime_error(
                        "invalid BurntSushi FST transition delta");
                }
                Output output = 0;
                if (output_size != 0) {
                    const auto total_transition_size =
                        index_size + transition_count +
                        transition_count * transition_size;
                    const auto output_offset =
                        address - count_size - 1 - total_transition_size -
                        index * output_size - output_size;
                    output = UnpackUInt(bytes, output_offset, output_size);
                }
                return DecodedTransition{
                    .input = bytes[input_offset],
                    .output = output,
                    .address = delta == kEmptyAddress
                                   ? kEmptyAddress
                                   : end - static_cast<Address>(delta),
                };
            }
            case NodeKind::kEmptyFinal:
                break;
        }
        throw std::logic_error("unreachable BurntSushi FST node kind");
    }
};

std::uint32_t
CheckedOutput(Output output) {
    if (output > std::numeric_limits<std::uint32_t>::max()) {
        throw std::overflow_error("BurntSushi FST output exceeds uint32");
    }
    return static_cast<std::uint32_t>(output);
}

FuzzySearchResult
IntersectLevenshteinDfa(std::span<const std::uint8_t> data,
                        Address root_address,
                        const LevenshteinDfa& dfa) {
    FuzzySearchResult result;
    std::string term;
    struct Frame {
        NodeView node;
        Output output = 0;
        std::uint32_t dfa_state = 0;
        std::size_t next_transition = 0;
        bool entered = false;
    };
    std::vector<Frame> stack;
    stack.push_back(Frame{
        .node = NodeView::Read(data, root_address),
        .dfa_state = dfa.InitialState(),
    });
    while (!stack.empty()) {
        auto& frame = stack.back();
        if (!frame.entered) {
            frame.entered = true;
            ++result.visited_states;
            if (frame.node.is_final) {
                ++result.visited_terms;
            }
            if (frame.node.is_final && dfa.IsMatch(frame.dfa_state)) {
                result.matches.push_back(FuzzyMatch{
                    term,
                    CheckedOutput(frame.output + frame.node.final_output),
                    dfa.Distance(frame.dfa_state),
                });
            }
        }
        if (frame.next_transition >= frame.node.transition_count) {
            stack.pop_back();
            if (!stack.empty()) {
                term.pop_back();
            }
            continue;
        }
        const auto index = frame.next_transition++;
        {
            ++result.visited_arcs;
            const auto input = frame.node.Input(index);
            const auto next_dfa_state = dfa.Transition(frame.dfa_state, input);

            // This is a Levenshtein-DFA-specific traversal. Its sink state
            // cannot recover and it has no EOF transition, so reject the arc
            // before decoding the target address, output and node.
            if (!dfa.CanMatch(next_dfa_state)) {
                continue;
            }

            term.push_back(static_cast<char>(input));
            stack.push_back(Frame{
                .node = NodeView::Read(data, frame.node.TransitionAddress(index)),
                .output = frame.output + frame.node.TransitionOutput(index),
                .dfa_state = next_dfa_state,
            });
        }
    }
    return result;
}

class StreamBound {
 public:
    enum class Kind { kIncluded, kExcluded, kUnbounded };

    StreamBound() = default;

    [[nodiscard]] bool ExceededBy(std::string_view input) const {
        switch (kind_) {
            case Kind::kIncluded:
                return input > value_;
            case Kind::kExcluded:
                return input >= value_;
            case Kind::kUnbounded:
                return false;
        }
        throw std::logic_error("unreachable BurntSushi stream bound");
    }

    [[nodiscard]] bool IsEmpty() const {
        return kind_ == Kind::kUnbounded || value_.empty();
    }

    [[nodiscard]] bool IsInclusive() const {
        return kind_ != Kind::kExcluded;
    }

 private:
    Kind kind_ = Kind::kUnbounded;
    std::string value_;
};

struct StreamMatch {
    std::string_view term;
    Output output = 0;
};

class RustGenericAlignedStream {
 public:
    RustGenericAlignedStream(std::span<const std::uint8_t> data,
                             Address root_address,
                             LevenshteinDfa dfa)
        : data_(data), root_address_(root_address), dfa_(std::move(dfa)) {
        input_.reserve(16);
        SeekMin();
    }

    [[nodiscard]] std::optional<StreamMatch> Next() {
        if (empty_output_.has_value()) {
            const auto output = *empty_output_;
            empty_output_.reset();
            if (end_at_.ExceededBy({})) {
                stack_.clear();
                return std::nullopt;
            }
            const auto start = dfa_.InitialState();
            if (dfa_.IsMatch(start)) {
                return StreamMatch{
                    .term = input_,
                    .output = output,
                };
            }
        }
        while (!stack_.empty()) {
            auto state = stack_.back();
            stack_.pop_back();
            if (state.transition >= state.node.transition_count ||
                !dfa_.CanMatch(state.dfa_state)) {
                if (state.node.address != root_address_) {
                    input_.pop_back();
                }
                continue;
            }

            // Match upstream fst::raw::StreamWithState ordering: decode the
            // complete transition and target node before the child state's
            // can_match check happens on its next stack pop.
            const auto index = state.transition;
            const auto transition = state.node.FullTransition(index);
            const auto output = state.output + transition.output;
            const auto next_dfa_state =
                dfa_.Transition(state.dfa_state, transition.input);
            bool is_match = dfa_.IsMatch(next_dfa_state);
            const auto next_node = NodeView::Read(data_, transition.address);
            input_.push_back(static_cast<char>(transition.input));

            // LevenshteinDfa has no EOF transition, but retain the same branch
            // point as the generic Automaton stream.
            if (next_node.is_final) {
                const std::optional<std::uint32_t> eof_state = std::nullopt;
                if (eof_state.has_value()) {
                    is_match = dfa_.IsMatch(*eof_state);
                }
            }

            ++state.transition;
            stack_.push_back(std::move(state));
            stack_.push_back(StreamState{
                .node = next_node,
                .output = output,
                .dfa_state = next_dfa_state,
            });
            if (end_at_.ExceededBy(input_)) {
                stack_.clear();
                return std::nullopt;
            }
            if (next_node.is_final && is_match) {
                return StreamMatch{
                    .term = input_,
                    .output = output + next_node.final_output,
                };
            }
        }
        return std::nullopt;
    }

 private:
    struct StreamState {
        NodeView node;
        std::size_t transition = 0;
        Output output = 0;
        std::uint32_t dfa_state = 0;
    };

    void SeekMin() {
        if (!min_at_.IsEmpty()) {
            throw std::logic_error(
                "bounded BurntSushi stream initialization is not exposed");
        }
        const auto root = NodeView::Read(data_, root_address_);
        if (min_at_.IsInclusive() && root.is_final) {
            empty_output_ = root.final_output;
        }
        stack_.push_back(StreamState{
            .node = root,
            .dfa_state = dfa_.InitialState(),
        });
    }

    std::span<const std::uint8_t> data_;
    Address root_address_ = kEmptyAddress;
    LevenshteinDfa dfa_;
    std::string input_;
    std::optional<Output> empty_output_;
    std::vector<StreamState> stack_;
    StreamBound min_at_;
    StreamBound end_at_;
};

FuzzySearchResult
IntersectRustGenericAligned(std::span<const std::uint8_t> data,
                            Address root_address,
                            LevenshteinDfa dfa,
                            std::string_view query,
                            std::uint32_t max_edit_distance,
                            EditDistanceMode edit_distance_mode) {
    FuzzySearchResult result;
    RustGenericAlignedStream stream(data, root_address, std::move(dfa));
    while (const auto match = stream.Next()) {
        const auto distance =
            edit_distance_mode == EditDistanceMode::kDamerauLevenshteinOsa
                ? DamerauLevenshteinOsa(query, match->term)
                : LevenshteinDistance(query, match->term);
        if (distance > max_edit_distance) {
            throw std::logic_error(
                "Rust-aligned stream returned an out-of-range fuzzy term");
        }
        result.matches.push_back(FuzzyMatch{
            std::string(match->term),
            CheckedOutput(match->output),
            distance,
        });
    }
    return result;
}

template <typename Visitor>
void
VisitTermsIterative(std::span<const std::uint8_t> data,
                    Address root_address,
                    const Visitor& visitor) {
    std::string term;
    struct Frame {
        NodeView node;
        Output output = 0;
        std::size_t next_transition = 0;
        bool entered = false;
    };
    std::vector<Frame> stack;
    stack.push_back(Frame{.node = NodeView::Read(data, root_address)});
    while (!stack.empty()) {
        auto& frame = stack.back();
        if (!frame.entered) {
            frame.entered = true;
            if (frame.node.is_final) {
                visitor(term,
                        CheckedOutput(frame.output + frame.node.final_output));
            }
        }
        if (frame.next_transition >= frame.node.transition_count) {
            stack.pop_back();
            if (!stack.empty()) {
                term.pop_back();
            }
            continue;
        }
        const auto index = frame.next_transition++;
        const auto transition = frame.node.FullTransition(index);
        term.push_back(static_cast<char>(transition.input));
        stack.push_back(Frame{
            .node = NodeView::Read(data, transition.address),
            .output = frame.output + transition.output,
        });
    }
}

DictionaryTraversalResult
TraverseTermsIterative(std::span<const std::uint8_t> data,
                       Address root_address) {
    DictionaryTraversalResult result;
    VisitTermsIterative(
        data, root_address,
        [&](std::string_view term, std::uint32_t document_frequency) {
            AddTraversalEntry(result, term, document_frequency);
        });
    return result;
}

}  // namespace

struct BurntSushiFstCppTermDictionary::Impl {
    FuzzyTraversalMode mode = FuzzyTraversalMode::kSpecializedEarlyPrune;
    EditDistanceMode edit_distance_mode =
        EditDistanceMode::kDamerauLevenshteinOsa;
    std::vector<std::uint8_t> owned_data;
    MappedFile mapped_data;
    std::span<const std::uint8_t> data;
    Metadata metadata;
};

BurntSushiFstCppTermDictionary::BurntSushiFstCppTermDictionary(
    FuzzyTraversalMode mode,
    EditDistanceMode edit_distance_mode)
    : impl_(std::make_unique<Impl>()) {
    impl_->mode = mode;
    impl_->edit_distance_mode = edit_distance_mode;
}

BurntSushiFstCppTermDictionary::~BurntSushiFstCppTermDictionary() = default;

BurntSushiFstCppTermDictionary::BurntSushiFstCppTermDictionary(
    BurntSushiFstCppTermDictionary&&) noexcept = default;

BurntSushiFstCppTermDictionary&
BurntSushiFstCppTermDictionary::operator=(
    BurntSushiFstCppTermDictionary&&) noexcept = default;

std::string_view
BurntSushiFstCppTermDictionary::Name() const {
    if (impl_->mode == FuzzyTraversalMode::kRustGenericAligned) {
        return impl_->edit_distance_mode ==
                       EditDistanceMode::kDamerauLevenshteinOsa
                   ? "burntsushi-fst-cpp-v3-rust-generic-aligned-damerau-osa"
                   : "burntsushi-fst-cpp-v3-rust-generic-aligned-levenshtein";
    }
    return impl_->edit_distance_mode ==
                   EditDistanceMode::kDamerauLevenshteinOsa
               ? "burntsushi-fst-cpp-v3-specialized-damerau-osa"
               : "burntsushi-fst-cpp-v3-specialized-levenshtein";
}

void
BurntSushiFstCppTermDictionary::Build(const std::vector<TermEntry>& entries) {
    std::vector<TermEntry> sorted(entries);
    std::sort(sorted.begin(), sorted.end(),
              [](const TermEntry& left, const TermEntry& right) {
                  return left.first < right.first;
              });
    std::size_t index = 0;
    BuildSorted([&]() -> std::optional<TermView> {
        if (index == sorted.size()) {
            return std::nullopt;
        }
        const auto& [term, document_frequency] = sorted[index++];
        return TermView{
            .term = term,
            .document_frequency = document_frequency,
        };
    });
}

void
BurntSushiFstCppTermDictionary::BuildSorted(
    const SortedTermReader& reader) {
    Builder builder;
    while (const auto entry = reader()) {
        const auto term = entry->term;
        const auto document_frequency = entry->document_frequency;
        if (term.empty()) {
            throw std::invalid_argument("empty terms are not supported");
        }
        ValidateUtf8(term);
        if (document_frequency == 0) {
            throw std::invalid_argument("document frequency must be positive");
        }
        builder.Insert(term, document_frequency);
    }

    auto owned_data = builder.Finish();
    const auto metadata = ReadMetadata(owned_data);
    impl_->mapped_data.Reset();
    impl_->owned_data = std::move(owned_data);
    impl_->data = impl_->owned_data;
    impl_->metadata = metadata;
}

std::optional<std::uint32_t>
BurntSushiFstCppTermDictionary::Lookup(std::string_view term) const {
    if (impl_->data.empty()) {
        return std::nullopt;
    }
    auto node = NodeView::Read(impl_->data, impl_->metadata.root_address);
    Output output = 0;
    for (const unsigned char byte : term) {
        const auto index = node.FindInput(byte);
        if (!index.has_value()) {
            return std::nullopt;
        }
        const auto transition = node.FullTransition(*index);
        output += transition.output;
        node = NodeView::Read(impl_->data, transition.address);
    }
    if (!node.is_final) {
        return std::nullopt;
    }
    return CheckedOutput(output + node.final_output);
}

FuzzySearchResult
BurntSushiFstCppTermDictionary::FuzzySearch(
    std::string_view query,
    std::uint32_t max_edit_distance,
    std::size_t max_expansions) const {
    if (max_edit_distance > 2) {
        throw std::invalid_argument("BurntSushi C++ fuzzy distance must be in [0, 2]");
    }
    FuzzySearchResult result;
    if (max_expansions == 0 || impl_->data.empty()) {
        return result;
    }
    if (max_edit_distance == 0) {
        ValidateUtf8(query);
        if (const auto output = Lookup(query); output.has_value()) {
            result.matches.push_back(
                FuzzyMatch{std::string(query), *output, 0});
        }
        return result;
    }

    const bool transposition_cost_one =
        impl_->edit_distance_mode ==
        EditDistanceMode::kDamerauLevenshteinOsa;
    auto dfa = BuildLevenshteinDfa(
        query, max_edit_distance, transposition_cost_one);
    if (impl_->mode == FuzzyTraversalMode::kRustGenericAligned) {
        result = IntersectRustGenericAligned(impl_->data,
                                             impl_->metadata.root_address,
                                             std::move(dfa),
                                             query,
                                             max_edit_distance,
                                             impl_->edit_distance_mode);
    } else {
        result = IntersectLevenshteinDfa(
            impl_->data, impl_->metadata.root_address, dfa);
    }
    std::sort(result.matches.begin(), result.matches.end(),
              [](const FuzzyMatch& left, const FuzzyMatch& right) {
                  if (left.edit_distance != right.edit_distance) {
                      return left.edit_distance < right.edit_distance;
                  }
                  return left.term < right.term;
              });
    if (result.matches.size() > max_expansions) {
        result.matches.resize(max_expansions);
    }
    return result;
}

void
BurntSushiFstCppTermDictionary::Save(const std::string& path_prefix) const {
    if (impl_->data.empty()) {
        throw std::runtime_error("BurntSushi C++ dictionary has not been built");
    }
    const auto path = path_prefix + std::string(kArtifactSuffix);
    std::ofstream stream(path, std::ios::binary | std::ios::trunc);
    stream.write(reinterpret_cast<const char*>(impl_->data.data()),
                 static_cast<std::streamsize>(impl_->data.size()));
    if (!stream) {
        throw std::runtime_error("failed to write BurntSushi C++ FST: " + path);
    }
}

void
BurntSushiFstCppTermDictionary::Load(const std::string& path_prefix) {
    LoadFile(path_prefix + std::string(kArtifactSuffix), true);
}

void
BurntSushiFstCppTermDictionary::LoadFile(const std::string& path,
                                        bool memory_mapped) {
    impl_->owned_data.clear();
    impl_->owned_data.shrink_to_fit();
    impl_->mapped_data.Reset();
    if (memory_mapped) {
        impl_->mapped_data.Map(path);
        impl_->data = impl_->mapped_data.Bytes();
    } else {
        std::ifstream stream(path, std::ios::binary | std::ios::ate);
        if (!stream) {
            throw std::ios_base::failure(
                "failed to open BurntSushi C++ FST: " + path);
        }
        const auto end = stream.tellg();
        if (end < 0 ||
            static_cast<std::uint64_t>(end) >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::streamsize>::max())) {
            throw std::runtime_error("invalid BurntSushi C++ FST size: " +
                                     path);
        }
        impl_->owned_data.resize(static_cast<std::size_t>(end));
        stream.seekg(0, std::ios::beg);
        if (!impl_->owned_data.empty()) {
            stream.read(reinterpret_cast<char*>(impl_->owned_data.data()),
                        static_cast<std::streamsize>(impl_->owned_data.size()));
        }
        if (!stream) {
            throw std::ios_base::failure(
                "failed to read BurntSushi C++ FST: " + path);
        }
        impl_->data = impl_->owned_data;
    }
    impl_->metadata = ReadMetadata(impl_->data);
    static_cast<void>(NodeView::Read(impl_->data, impl_->metadata.root_address));
    if (!VerifyChecksum()) {
        throw std::runtime_error("BurntSushi C++ FST checksum mismatch: " +
                                 path);
    }
}

void
BurntSushiFstCppTermDictionary::LoadBytes(
    std::span<const std::uint8_t> bytes) {
    impl_->mapped_data.Reset();
    impl_->owned_data.assign(bytes.begin(), bytes.end());
    impl_->data = impl_->owned_data;
    impl_->metadata = ReadMetadata(impl_->data);
    static_cast<void>(NodeView::Read(impl_->data, impl_->metadata.root_address));
    if (!VerifyChecksum()) {
        throw std::runtime_error("BurntSushi C++ FST checksum mismatch");
    }
}

DictionaryStats
BurntSushiFstCppTermDictionary::Stats() const {
    return DictionaryStats{
        .term_count = impl_->metadata.term_count,
        .key_storage_bytes = impl_->data.size(),
        .value_storage_bytes = 0,
    };
}

DictionaryTraversalResult
BurntSushiFstCppTermDictionary::TraverseTerms() const {
    if (impl_->data.empty()) {
        return {};
    }
    return TraverseTermsIterative(impl_->data, impl_->metadata.root_address);
}

void
BurntSushiFstCppTermDictionary::VisitTerms(const TermVisitor& visitor) const {
    if (impl_->data.empty()) {
        return;
    }
    VisitTermsIterative(impl_->data, impl_->metadata.root_address, visitor);
}

std::span<const std::uint8_t>
BurntSushiFstCppTermDictionary::SerializedBytes() const {
    return impl_->data;
}

bool
BurntSushiFstCppTermDictionary::VerifyChecksum() const {
    if (impl_->data.size() < 4) {
        return false;
    }
    return ReadU32(impl_->data, impl_->data.size() - 4) ==
           MaskedCrc32c(impl_->data.data(), impl_->data.size() - 4);
}

bool
BurntSushiFstCppTermDictionary::IsMemoryMapped() const {
    return impl_->mapped_data.IsMapped();
}

FuzzyTraversalMode
BurntSushiFstCppTermDictionary::TraversalMode() const {
    return impl_->mode;
}

EditDistanceMode
BurntSushiFstCppTermDictionary::DistanceMode() const {
    return impl_->edit_distance_mode;
}

}  // namespace fst_test::burntsushi_fst_cpp_impl
