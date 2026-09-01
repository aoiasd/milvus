# Local Format L0 Memory Benchmark

`l0_memory.py` is the fixed runner for the `L0-MEM-P0` stage. It compares the
Raw and Vortex scalar local formats while keeping the scalar data in memory.

The service must differ from upstream defaults only in these values:

```text
common.storage.useLoonFFI=true
dataNode.storage.format=vortex
queryNode.mmap.scalarField=false
queryNode.mmap.jsonShredding=false
```

The runner also verifies the retained defaults that define this stage:

```text
queryNode.segcore.tieredStorage.warmup.scalarField=sync
queryNode.segcore.tieredStorage.evictionEnabled=false
queryNode.segcore.tieredStorage.backgroundEvictionEnabled=false
```

Each collection contains exactly three fields:

```text
pk            INT64 primary key
value         one P0 scalar type, local_format=raw|vortex
dummy_vector  BINARY_VECTOR(dim=8), BIN_FLAT/HAMMING
```

The dummy vector contributes one data byte per row and is never queried. No
scalar index is created.

For every type, row count, and local format the runner executes this fixed
sequence:

```text
insert deterministic batches
-> flush every batch
-> manual compact
-> wait for compaction Completed
-> wait for persistent segments and row count to remain stable
-> create the dummy BIN_FLAT index
-> load collection and wait for sync warmup
-> correctness samples
-> TAKE warmup and measurement
-> SCAN warmup and measurement
-> release collection
```

Only one Raw/Vortex collection is loaded at a time. Both formats replay the
same deterministic TAKE templates and use the same approximately 1% SCAN
predicate. The runner fails if their sampled TAKE results or SCAN counts differ.

Run a local smoke test:

```bash
python3 tests/python_client/benchmark/local_format/l0_memory.py \
  --smoke \
  --recreate \
  --run-id local-smoke \
  --output /tmp/local-format-l0-memory-smoke.json
```

Run selected P0 cases and data levels on the 4CU service:

```bash
python3 tests/python_client/benchmark/local_format/l0_memory.py \
  --types int64,double,varchar256,varchar2048,json512,array_int64_32 \
  --row-counts 100000,1000000,10000000 \
  --batch-size 10000 \
  --concurrency 16 \
  --warmup-duration 10 \
  --duration 60 \
  --run-id 4cu-run-001 \
  --output results/l0-memory-4cu-run-001.json
```

Connection settings can be supplied with flags or `MILVUS_URI`,
`MILVUS_TOKEN`, `MILVUS_DB`, `MILVUS_MANAGEMENT_URL`, and
`MILVUS_METRICS_URL` environment variables. Use `--keep-collections` only for
post-run inspection; normal runs drop all benchmark collections.

The JSON result includes runtime configuration, preparation and compaction
details, persistent/loaded segment metadata, sync-warmup/load duration,
correctness digests, QPS, latency p50/p95/p99, and cache metric snapshots before
load, after sync warmup, after each workload, and after release.
