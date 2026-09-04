# BurntSushi/fst-compatible C++ implementation

This package ports the relevant BurntSushi/fst 0.4.7 map path to C++20:

- sorted incremental transducer construction;
- positive integer output factoring;
- bounded two-way LRU suffix-state registry;
- version-3 packed node encoding and masked CRC32C trailer;
- exact lookup and forward lexicographic streaming;
- query-specific Unicode-aware generated edit-distance byte DFA;
- DFA/FST intersection with sink-state subtree pruning.

`EditDistanceMode::kDamerauLevenshteinOsa` is the default. It assigns a cost
of one to an adjacent transposition. Standard Levenshtein behavior remains
available through `EditDistanceMode::kLevenshtein`, where the same
transposition costs two edits. The FST artifact, mmap representation and node
traversal are identical between the two modes; only query-specific DFA
construction and final distance recomputation differ.

The sustained comparison binaries are:

- `burntsushi_fst_cpp_aligned_sustained`: default Damerau-Levenshtein OSA;
- `burntsushi_fst_cpp_aligned_levenshtein_sustained`: standard Levenshtein.

The goal is binary compatibility with upstream `fst 0.4.7` artifacts for the
supported `Map<bytes, u64>` format, followed by performance comparison against
the Rust implementation using the shared benchmark workload.
