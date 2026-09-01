#!/usr/bin/env python3
"""Reproducible L0-memory benchmark for raw and Vortex scalar local formats."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import statistics
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pymilvus import DataType, MilvusClient

SCENARIO = "L0-MEM-P0"
FORMATS = ("raw", "vortex")
P0_TYPES = ("int64", "double", "varchar256", "varchar2048", "json512", "array_int64_32")
EXPECTED_CONFIG = {
    "common.storage.useLoonFFI": "true",
    "dataNode.storage.format": "vortex",
    "queryNode.mmap.scalarField": "false",
    "queryNode.mmap.jsonShredding": "false",
    "queryNode.segcore.tieredStorage.warmup.scalarField": "sync",
    "queryNode.segcore.tieredStorage.evictionEnabled": "false",
    "queryNode.segcore.tieredStorage.backgroundEvictionEnabled": "false",
}
CACHE_METRIC_PREFIXES = (
    "internal_cache_loaded_bytes",
    "internal_cache_capacity_bytes",
    "internal_cache_eviction_event_total",
    "internal_cache_evicted_bytes_total",
)
METRIC_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>.*)\})?\s+(?P<value>[-+0-9.eE]+|NaN|Inf|-Inf)$"
)
LABEL_RE = re.compile(r'(?P<key>[a-zA-Z_][a-zA-Z0-9_]*)="(?P<value>(?:\\.|[^"\\])*)"')


class BenchmarkError(RuntimeError):
    pass


def log(message: str) -> None:
    print(f"[{datetime.now(tz=UTC).isoformat(timespec='seconds')}] {message}", flush=True)


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_positive_int_csv(value: str) -> list[int]:
    result = [int(item) for item in split_csv(value)]
    if not result or any(item <= 0 for item in result):
        raise argparse.ArgumentTypeError("expected a comma-separated list of positive integers")
    return result


def normalize_config_value(value: str) -> str:
    return value.strip().lower()


def http_get_text(url: str, timeout: float) -> str:
    request = urllib.request.Request(url, headers={"Accept": "application/json,text/plain"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8")


def fetch_runtime_config(management_url: str, timeout: float) -> dict[str, dict[str, str]]:
    configs: dict[str, dict[str, str]] = {}
    base = management_url.rstrip("/")
    for key in EXPECTED_CONFIG:
        query = urllib.parse.urlencode({"keys": key})
        payload = json.loads(http_get_text(f"{base}/management/config/get?{query}", timeout))
        rows = payload.get("configs", [])
        if not rows:
            raise BenchmarkError(f"management API returned no value for config {key!r}")
        row = rows[0]
        if row.get("error"):
            raise BenchmarkError(f"management API failed for config {key!r}: {row['error']}")
        configs[key] = {"expected": EXPECTED_CONFIG[key], "actual": str(row.get("value", ""))}
    return configs


def verify_runtime_config(configs: dict[str, dict[str, str]]) -> None:
    mismatches = []
    for key, values in configs.items():
        if normalize_config_value(values["actual"]) != normalize_config_value(values["expected"]):
            mismatches.append(f"{key}: expected={values['expected']!r}, actual={values['actual']!r}")
    if mismatches:
        raise BenchmarkError("runtime configuration mismatch:\n  " + "\n  ".join(mismatches))


def parse_prometheus_metrics(text: str) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for line in text.splitlines():
        if not line or line.startswith("#") or not line.startswith(CACHE_METRIC_PREFIXES):
            continue
        match = METRIC_RE.match(line)
        if not match:
            continue
        labels = {
            item.group("key"): bytes(item.group("value"), "utf-8").decode("unicode_escape")
            for item in LABEL_RE.finditer(match.group("labels") or "")
        }
        raw_value = match.group("value")
        value = float(raw_value)
        result.setdefault(match.group("name"), []).append({"labels": labels, "value": value})
    for samples in result.values():
        samples.sort(key=lambda sample: sorted(sample["labels"].items()))
    return result


def snapshot_cache_metrics(metrics_url: str, timeout: float) -> dict[str, list[dict[str, Any]]]:
    return parse_prometheus_metrics(http_get_text(metrics_url, timeout))


def new_client(args: argparse.Namespace) -> MilvusClient:
    return MilvusClient(
        uri=args.uri,
        token=args.token,
        db_name=args.db_name,
        timeout=args.rpc_timeout,
    )


def stable_type_salt(type_name: str) -> int:
    return int.from_bytes(hashlib.sha256(type_name.encode()).digest()[:8], "little")


def splitmix64(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    return value ^ (value >> 31)


def row_hash(row_id: int, seed: int, type_name: str) -> int:
    return splitmix64(row_id ^ seed ^ stable_type_salt(type_name))


def random_hex(row_id: int, seed: int, type_name: str, length: int) -> str:
    source = f"{seed}:{type_name}:{row_id}".encode()
    return hashlib.shake_256(source).hexdigest(math.ceil(length / 2))[:length]


def scalar_value(type_name: str, row_id: int, seed: int) -> Any:
    value_hash = row_hash(row_id, seed, type_name)
    bucket = value_hash % 100
    if type_name == "int64":
        return value_hash & 0x7FFFFFFFFFFFFFFF
    if type_name == "double":
        return value_hash / 2**64
    if type_name in {"varchar256", "varchar2048"}:
        length = 256 if type_name == "varchar256" else 2048
        return f"{bucket:02d}" + random_hex(row_id, seed, type_name, length - 2)
    if type_name == "json512":
        # Keep the serialized document close to 512 bytes without relying on compressible filler.
        payload = random_hex(row_id, seed, type_name, 470)
        return {"bucket": bucket, "row": row_id, "payload": payload}
    if type_name == "array_int64_32":
        values = [bucket]
        state = value_hash
        for _ in range(31):
            state = splitmix64(state)
            values.append(state & 0x7FFFFFFFFFFFFFFF)
        return values
    raise BenchmarkError(f"unsupported type: {type_name}")


def scan_filter(type_name: str) -> str:
    if type_name == "int64":
        return "value >= 0 and value < 92233720368547758"
    if type_name == "double":
        return "value >= 0.0 and value < 0.01"
    if type_name in {"varchar256", "varchar2048"}:
        return 'value >= "42" and value < "43"'
    if type_name == "json512":
        return 'value["bucket"] == 42'
    if type_name == "array_int64_32":
        return "value[0] == 42"
    raise BenchmarkError(f"unsupported type: {type_name}")


def add_scalar_field(schema: Any, type_name: str, local_format: str) -> None:
    kwargs: dict[str, Any] = {"local_format": local_format}
    if type_name == "int64":
        data_type = DataType.INT64
    elif type_name == "double":
        data_type = DataType.DOUBLE
    elif type_name in {"varchar256", "varchar2048"}:
        data_type = DataType.VARCHAR
        kwargs["max_length"] = 256 if type_name == "varchar256" else 2048
    elif type_name == "json512":
        data_type = DataType.JSON
    elif type_name == "array_int64_32":
        data_type = DataType.ARRAY
        kwargs.update(element_type=DataType.INT64, max_capacity=32)
    else:
        raise BenchmarkError(f"unsupported type: {type_name}")

    schema.add_field("value", data_type, **kwargs)
    field = schema.fields[-1]
    # Older PyMilvus releases do not yet expose local_format as a common type param.
    # Preserve compatibility with those releases while still sending the server field type param.
    if field.params.get("local_format") != local_format:
        field._type_params["local_format"] = local_format  # noqa: SLF001
    if field.params.get("local_format") != local_format:
        raise BenchmarkError("PyMilvus did not retain the local_format field type parameter")


def create_collection(client: MilvusClient, collection_name: str, type_name: str, local_format: str) -> None:
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("pk", DataType.INT64, is_primary=True, auto_id=False)
    add_scalar_field(schema, type_name, local_format)
    schema.add_field("dummy_vector", DataType.BINARY_VECTOR, dim=8)
    client.create_collection(collection_name=collection_name, schema=schema)


def build_rows(start: int, count: int, type_name: str, seed: int) -> list[dict[str, Any]]:
    return [
        {
            "pk": row_id,
            "value": scalar_value(type_name, row_id, seed),
            "dummy_vector": bytes([row_hash(row_id, seed, "dummy_vector") & 0xFF]),
        }
        for row_id in range(start, start + count)
    ]


def segment_to_dict(segment: Any) -> dict[str, Any]:
    if is_dataclass(segment):
        result = asdict(segment)
    else:
        result = dict(vars(segment))
    if hasattr(segment, "state_name"):
        result["state_name"] = segment.state_name
    if hasattr(segment, "level_name"):
        result["level_name"] = segment.level_name
    return result


def active_segment_snapshot(client: MilvusClient, collection_name: str, timeout: float) -> list[dict[str, Any]]:
    segments = client.list_persistent_segments(collection_name, timeout=timeout)
    active = [segment for segment in segments if segment.state_name not in {"Dropped", "NotExist"}]
    result = [segment_to_dict(segment) for segment in active]
    result.sort(key=lambda item: item["segment_id"])
    return result


def wait_for_stable_segments(
    client: MilvusClient,
    collection_name: str,
    expected_rows: int,
    timeout: float,
    interval: float,
    stable_polls: int,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout
    previous_signature: tuple[Any, ...] | None = None
    unchanged = 0
    last_snapshot: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last_snapshot = active_segment_snapshot(client, collection_name, timeout)
        signature = tuple(
            (item["segment_id"], item["num_rows"], item["state_name"], item["level_name"], item["storage_version"])
            for item in last_snapshot
        )
        row_count = sum(int(item["num_rows"]) for item in last_snapshot)
        if signature and signature == previous_signature and row_count == expected_rows:
            unchanged += 1
            if unchanged >= stable_polls:
                return last_snapshot
        else:
            unchanged = 0
        previous_signature = signature
        time.sleep(interval)
    raise BenchmarkError(
        f"persistent segments for {collection_name} did not stabilize at {expected_rows} rows; "
        f"last snapshot={last_snapshot}"
    )


def wait_for_compaction(client: MilvusClient, job_id: int, timeout: float, interval: float) -> str:
    deadline = time.monotonic() + timeout
    last_state = "Unknown"
    while time.monotonic() < deadline:
        last_state = str(client.get_compaction_state(job_id, timeout=timeout))
        if last_state == "Completed":
            return last_state
        if last_state in {"Failed", "Timeout"}:
            raise BenchmarkError(f"compaction job {job_id} ended in state {last_state}")
        time.sleep(interval)
    raise BenchmarkError(f"compaction job {job_id} timed out after {timeout}s; last state={last_state}")


def prepare_collection(
    client: MilvusClient,
    args: argparse.Namespace,
    collection_name: str,
    type_name: str,
    local_format: str,
    row_count: int,
) -> dict[str, Any]:
    if client.has_collection(collection_name):
        if not args.recreate:
            raise BenchmarkError(f"collection {collection_name} already exists; use --recreate to replace it")
        log(f"dropping existing collection {collection_name}")
        client.drop_collection(collection_name, timeout=args.rpc_timeout)

    log(f"creating {collection_name} ({type_name}, local_format={local_format})")
    create_collection(client, collection_name, type_name, local_format)
    flush_snapshots = []
    inserted = 0
    while inserted < row_count:
        batch_count = min(args.batch_size, row_count - inserted)
        rows = build_rows(inserted, batch_count, type_name, args.seed)
        result = client.insert(collection_name, rows, timeout=args.rpc_timeout)
        if int(result.get("insert_count", 0)) != batch_count:
            raise BenchmarkError(f"short insert into {collection_name}: expected {batch_count}, result={result}")
        client.flush(collection_name, timeout=args.flush_timeout)
        inserted += batch_count
        snapshot = active_segment_snapshot(client, collection_name, args.rpc_timeout)
        flush_snapshots.append({"inserted_rows": inserted, "segments": snapshot})
        log(f"{collection_name}: inserted and flushed {inserted}/{row_count} rows")

    before = wait_for_stable_segments(
        client,
        collection_name,
        row_count,
        args.segment_timeout,
        args.poll_interval,
        args.stable_polls,
    )
    log(f"{collection_name}: starting manual compaction from {len(before)} persistent segments")
    job_id = client.compact(
        collection_name,
        target_size=args.compact_target_size_mb,
        target_size_unit="mb",
        timeout=args.rpc_timeout,
    )
    state = wait_for_compaction(client, job_id, args.compaction_timeout, args.poll_interval)
    plans = str(client.get_compaction_plans(job_id, timeout=args.rpc_timeout))
    after = wait_for_stable_segments(
        client,
        collection_name,
        row_count,
        args.segment_timeout,
        args.poll_interval,
        args.stable_polls,
    )
    log(f"{collection_name}: compaction {job_id} {state}; stable segments {len(before)} -> {len(after)}")

    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name="dummy_vector",
        index_name="dummy_vector_bin_flat",
        index_type="BIN_FLAT",
        metric_type="HAMMING",
        params={},
    )
    client.create_index(collection_name, index_params, timeout=args.index_timeout)
    return {
        "flush_snapshots": flush_snapshots,
        "compaction": {
            "job_id": job_id,
            "state": state,
            "plans": plans,
            "segments_before": before,
            "segments_after": after,
        },
        "dummy_vector": {"dim": 8, "bytes_per_row": 1, "index_type": "BIN_FLAT", "metric_type": "HAMMING"},
    }


def wait_for_load(client: MilvusClient, collection_name: str, timeout: float, interval: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_state: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last_state = client.get_load_state(collection_name, timeout=timeout)
        state = last_state.get("state")
        if getattr(state, "name", str(state)) == "Loaded":
            return {key: getattr(value, "name", str(value)) for key, value in last_state.items()}
        time.sleep(interval)
    raise BenchmarkError(f"collection {collection_name} did not load within {timeout}s; state={last_state}")


def percentile(sorted_values: list[float], quantile: float) -> float:
    if not sorted_values:
        return 0.0
    position = (len(sorted_values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    weight = position - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


class WorkloadExecutor:
    def __init__(
        self,
        args: argparse.Namespace,
        collection_name: str,
        request: Callable[[MilvusClient, int, int], None],
    ) -> None:
        self.args = args
        self.collection_name = collection_name
        self.request = request
        self.clients = [new_client(args) for _ in range(args.concurrency)]

    def close(self) -> None:
        for client in self.clients:
            client.close()

    def run(self, duration: float, record: bool) -> dict[str, Any]:
        barrier = threading.Barrier(self.args.concurrency + 1)
        stop = threading.Event()
        errors: list[str] = []

        def worker(worker_id: int) -> list[float]:
            latencies = []
            sequence_index = worker_id
            barrier.wait()
            while not stop.is_set():
                started = time.perf_counter_ns()
                try:
                    self.request(self.clients[worker_id], sequence_index, worker_id)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"worker={worker_id}: {type(exc).__name__}: {exc}")
                    stop.set()
                    break
                if record:
                    latencies.append((time.perf_counter_ns() - started) / 1_000_000)
                sequence_index += self.args.concurrency
            return latencies

        with ThreadPoolExecutor(max_workers=self.args.concurrency) as executor:
            futures = [executor.submit(worker, worker_id) for worker_id in range(self.args.concurrency)]
            barrier.wait()
            started = time.perf_counter()
            stop.wait(duration)
            stop.set()
            latencies = [latency for future in futures for latency in future.result()]
            elapsed = time.perf_counter() - started

        if errors:
            raise BenchmarkError(f"workload failed for {self.collection_name}: {errors[0]}")
        if not record:
            return {"duration_s": elapsed, "requests": 0}
        if not latencies:
            raise BenchmarkError(f"workload produced no measurements for {self.collection_name}")
        latencies.sort()
        return {
            "duration_s": elapsed,
            "requests": len(latencies),
            "qps": len(latencies) / elapsed,
            "latency_ms": {
                "min": latencies[0],
                "mean": statistics.fmean(latencies),
                "p50": percentile(latencies, 0.50),
                "p95": percentile(latencies, 0.95),
                "p99": percentile(latencies, 0.99),
                "max": latencies[-1],
            },
        }


def make_take_templates(row_count: int, args: argparse.Namespace, type_name: str) -> list[list[int]]:
    rng = random.Random(args.seed ^ stable_type_salt(type_name) ^ row_count)
    return [[rng.randrange(row_count) for _ in range(args.take_size)] for _ in range(args.request_sequence_length)]


def canonical_digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(payload).hexdigest()


def correctness_snapshot(
    client: MilvusClient,
    collection_name: str,
    take_templates: list[list[int]],
    scan_expression: str,
    sample_count: int,
    timeout: float,
) -> dict[str, Any]:
    take_results = []
    for ids in take_templates[:sample_count]:
        rows = client.query(
            collection_name,
            ids=ids,
            output_fields=["pk", "value"],
            timeout=timeout,
        )
        take_results.append(sorted(rows, key=lambda row: row["pk"]))
    scan_result = client.query(
        collection_name,
        filter=scan_expression,
        output_fields=["count(*)"],
        timeout=timeout,
    )
    return {
        "take_digest": canonical_digest(take_results),
        "take_sample_requests": sample_count,
        "scan_digest": canonical_digest(scan_result),
        "scan_result": scan_result,
    }


def benchmark_loaded_collection(
    client: MilvusClient,
    args: argparse.Namespace,
    collection_name: str,
    take_templates: list[list[int]],
    scan_expression: str,
) -> dict[str, Any]:
    metrics: dict[str, Any] = {"before_load": snapshot_cache_metrics(args.metrics_url, args.http_timeout)}
    load_started = time.perf_counter()
    client.load_collection(collection_name, timeout=args.load_timeout)
    load_state = wait_for_load(client, collection_name, args.load_timeout, args.poll_interval)
    load_elapsed = time.perf_counter() - load_started
    loaded_segments = [segment_to_dict(segment) for segment in client.list_loaded_segments(collection_name)]
    metrics["after_sync_warmup"] = snapshot_cache_metrics(args.metrics_url, args.http_timeout)
    correctness = correctness_snapshot(
        client,
        collection_name,
        take_templates,
        scan_expression,
        args.correctness_requests,
        args.rpc_timeout,
    )

    def take_request(worker_client: MilvusClient, sequence_index: int, _worker_id: int) -> None:
        worker_client.query(
            collection_name,
            ids=take_templates[sequence_index % len(take_templates)],
            output_fields=["pk", "value"],
            timeout=args.rpc_timeout,
        )

    def scan_request(worker_client: MilvusClient, _sequence_index: int, _worker_id: int) -> None:
        worker_client.query(
            collection_name,
            filter=scan_expression,
            output_fields=["count(*)"],
            timeout=args.rpc_timeout,
        )

    workloads = {}
    for name, request in (("take", take_request), ("scan", scan_request)):
        log(f"{collection_name}: {name.upper()} warmup for {args.warmup_duration}s")
        executor = WorkloadExecutor(args, collection_name, request)
        try:
            executor.run(args.warmup_duration, record=False)
            log(f"{collection_name}: {name.upper()} measure for {args.duration}s at concurrency {args.concurrency}")
            workloads[name] = executor.run(args.duration, record=True)
        finally:
            executor.close()
        metrics[f"after_{name}"] = snapshot_cache_metrics(args.metrics_url, args.http_timeout)

    client.release_collection(collection_name, timeout=args.rpc_timeout)
    metrics["after_release"] = snapshot_cache_metrics(args.metrics_url, args.http_timeout)
    return {
        "load": {"duration_s": load_elapsed, "state": load_state, "segments": loaded_segments},
        "correctness": correctness,
        "workloads": workloads,
        "cache_metrics": metrics,
    }


def collection_name(run_id: str, type_name: str, row_count: int, local_format: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]", "_", run_id)
    return f"lf_l0_mem_{normalized}_{type_name}_{row_count}_{local_format}"[:255]


def run_case(client: MilvusClient, args: argparse.Namespace, type_name: str, row_count: int) -> dict[str, Any]:
    names = {fmt: collection_name(args.run_id, type_name, row_count, fmt) for fmt in FORMATS}
    take_templates = make_take_templates(row_count, args, type_name)
    expression = scan_filter(type_name)
    case: dict[str, Any] = {
        "type": type_name,
        "row_count": row_count,
        "scan_filter": expression,
        "collections": names,
        "formats": {},
    }
    try:
        for local_format in FORMATS:
            prepared = prepare_collection(
                client,
                args,
                names[local_format],
                type_name,
                local_format,
                row_count,
            )
            measured = benchmark_loaded_collection(
                client,
                args,
                names[local_format],
                take_templates,
                expression,
            )
            case["formats"][local_format] = {"prepare": prepared, **measured}

        raw_correctness = case["formats"]["raw"]["correctness"]
        vortex_correctness = case["formats"]["vortex"]["correctness"]
        case["correctness_match"] = raw_correctness == vortex_correctness
        if not case["correctness_match"]:
            raise BenchmarkError(
                f"Raw/Vortex correctness mismatch for type={type_name}, rows={row_count}: "
                f"raw={raw_correctness}, vortex={vortex_correctness}"
            )
        log(f"case type={type_name}, rows={row_count}: Raw/Vortex correctness matched")
        return case
    finally:
        if not args.keep_collections:
            for name in names.values():
                if client.has_collection(name):
                    try:
                        client.release_collection(name, timeout=args.rpc_timeout)
                    except Exception:  # noqa: BLE001
                        pass
                    client.drop_collection(name, timeout=args.rpc_timeout)


def write_result(path: Path, result: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    temporary.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default=os.getenv("MILVUS_URI", "http://localhost:19530"))
    parser.add_argument("--token", default=os.getenv("MILVUS_TOKEN", "root:Milvus"))
    parser.add_argument("--db-name", default=os.getenv("MILVUS_DB", "default"))
    parser.add_argument("--management-url", default=os.getenv("MILVUS_MANAGEMENT_URL", "http://localhost:9091"))
    parser.add_argument("--metrics-url", default=os.getenv("MILVUS_METRICS_URL", "http://localhost:9091/metrics"))
    parser.add_argument("--types", type=split_csv, default=list(P0_TYPES))
    parser.add_argument("--row-counts", type=parse_positive_int_csv, default=[100_000])
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=20260721)
    parser.add_argument("--take-size", type=int, default=100)
    parser.add_argument("--request-sequence-length", type=int, default=4096)
    parser.add_argument("--correctness-requests", type=int, default=16)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--warmup-duration", type=float, default=10.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--compact-target-size-mb", type=int, default=1024)
    parser.add_argument("--rpc-timeout", type=float, default=120.0)
    parser.add_argument("--flush-timeout", type=float, default=600.0)
    parser.add_argument("--compaction-timeout", type=float, default=1800.0)
    parser.add_argument("--segment-timeout", type=float, default=300.0)
    parser.add_argument("--index-timeout", type=float, default=1800.0)
    parser.add_argument("--load-timeout", type=float, default=1800.0)
    parser.add_argument("--http-timeout", type=float, default=10.0)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--stable-polls", type=int, default=3)
    parser.add_argument("--run-id", default=datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--skip-config-check", action="store_true")
    parser.add_argument("--keep-collections", action="store_true")
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="run INT64 with 10k rows, four flushed batches, 1s warmup, and 2s measurement",
    )
    args = parser.parse_args()

    invalid_types = sorted(set(args.types) - set(P0_TYPES))
    if invalid_types:
        parser.error(f"unsupported --types: {invalid_types}; choices={list(P0_TYPES)}")
    positive_values = {
        "batch-size": args.batch_size,
        "take-size": args.take_size,
        "request-sequence-length": args.request_sequence_length,
        "correctness-requests": args.correctness_requests,
        "concurrency": args.concurrency,
        "stable-polls": args.stable_polls,
    }
    for name, value in positive_values.items():
        if value <= 0:
            parser.error(f"--{name} must be positive")
    if args.correctness_requests > args.request_sequence_length:
        parser.error("--correctness-requests cannot exceed --request-sequence-length")
    if args.warmup_duration <= 0 or args.duration <= 0:
        parser.error("--warmup-duration and --duration must be positive")
    if args.smoke:
        args.types = ["int64"]
        args.row_counts = [10_000]
        args.batch_size = 2_500
        args.concurrency = min(args.concurrency, 4)
        args.warmup_duration = 1.0
        args.duration = 2.0
        args.correctness_requests = min(args.correctness_requests, 4)
    if args.output is None:
        args.output = Path(f"local_format_l0_memory_{args.run_id}.json")
    return args


def main() -> int:
    args = parse_args()
    result: dict[str, Any] = {
        "scenario": SCENARIO,
        "status": "running",
        "started_at": datetime.now(tz=UTC).isoformat(),
        "arguments": {
            key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items() if key != "token"
        },
        "environment": {
            "python": sys.version,
            "pymilvus": __import__("pymilvus").__version__,
        },
        "cases": [],
    }
    client: MilvusClient | None = None
    try:
        log(f"starting {SCENARIO}; output={args.output}")
        configs = fetch_runtime_config(args.management_url, args.http_timeout)
        result["runtime_config"] = configs
        if not args.skip_config_check:
            verify_runtime_config(configs)
        client = new_client(args)
        for type_name in args.types:
            for row_count in args.row_counts:
                case = run_case(client, args, type_name, row_count)
                result["cases"].append(case)
                write_result(args.output, result)
        result["status"] = "passed"
        result["finished_at"] = datetime.now(tz=UTC).isoformat()
        write_result(args.output, result)
        log(f"benchmark passed; result={args.output}")
        return 0
    except Exception as exc:  # noqa: BLE001
        result["status"] = "failed"
        result["finished_at"] = datetime.now(tz=UTC).isoformat()
        result["error"] = {"type": type(exc).__name__, "message": str(exc)}
        write_result(args.output, result)
        log(f"benchmark failed: {type(exc).__name__}: {exc}; partial result={args.output}")
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
