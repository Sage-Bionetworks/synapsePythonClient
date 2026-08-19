"""Query SigNoz for the OTel data emitted by a labelled integration-test run
(`SYNAPSE_TEST_RUN_LABEL`) and turn it into either the suite-level totals or a
per-test load table, per SYNPY-1892.

Not part of the `synapseclient` package - a maintenance script, run manually,
same home as `delete_projects.py` / `empty_trash.py`. Stdlib only.

    SIGNOZ_API_KEY=... python measure_test_load.py totals --label <run.label>
    SIGNOZ_API_KEY=... python measure_test_load.py per-test --label <run.label>

`SIGNOZ_API_KEY` is read from the environment only; it is never printed or logged.
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

SIGNOZ_QUERY_BASE_URL = "https://sagebionetworks.us.signoz.cloud"
QUERY_RANGE_PATH = "/api/v5/query_range"
LOOKBACK_SECONDS = 30 * 24 * 3600  # 30 days is comfortably wider than any run
PAGE_LIMIT = 1000  # SigNoz's own maximum rows per raw-trace page

# Root spans (parent_span_id == "") that are not a test execution: httpx's
# auto-instrumented client spans (named after the HTTP method) and the two
# in-repo spans that can end up rootless when a job/upload happens outside any
# `wrap_with_otel` span (e.g. session-scoped fixture teardown).
_NON_TEST_ROOT_SPAN_NAMES = {
    "GET",
    "POST",
    "PUT",
    "DELETE",
    "PATCH",
    "HEAD",
    "OPTIONS",
    "synapse.async_job",
    "synapse.transfer.upload",
    "Synapse::_waitForAsync",
}


def _require_api_key() -> str:
    api_key = os.environ.get("SIGNOZ_API_KEY")
    if not api_key:
        sys.exit("SIGNOZ_API_KEY is not set in the environment.")
    return api_key


def _query_range(payload: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """The one seam all SigNoz HTTP goes through."""
    request = urllib.request.Request(
        SIGNOZ_QUERY_BASE_URL + QUERY_RANGE_PATH,
        data=json.dumps(payload).encode(),
        headers={"SIGNOZ-API-KEY": api_key, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        sys.exit(f"SigNoz query failed: HTTP {e.code} {e.reason}")


def _time_range_ns() -> Tuple[int, int]:
    end_ns = int(time.time() * 1e9)
    start_ns = end_ns - LOOKBACK_SECONDS * 1_000_000_000
    return start_ns, end_ns


def _metric_group_values(
    metric_name: str, label: str, group_by: str, api_key: str
) -> List[Tuple[str, float]]:
    """Group a cumulative counter metric by one attribute and return
    `[(value, sum), ...]`.

    `timeAggregation: "latest"` is required, not `"sum"`: these are cumulative
    counters, and summing over time inflates the total (measured: 84 becomes
    416 for a single flat run). `"latest"` reads the last reported cumulative
    value per series before summing across series.

    `reduceTo` must be `"max"` for the same reason. SigNoz splits the query
    window into step intervals, and `reduceTo: "sum"` adds up each step's
    already-cumulative value: a run spanning two steps reports exactly twice
    its real total (measured: 487 async-job submissions read as 954). `"max"`
    takes the largest per-step cumulative value, which is the final one.
    """
    start_ns, end_ns = _time_range_ns()
    payload = {
        "schemaVersion": "v1",
        "start": start_ns,
        "end": end_ns,
        "requestType": "scalar",
        "compositeQuery": {
            "queries": [
                {
                    "type": "builder_query",
                    "spec": {
                        "name": "A",
                        "signal": "metrics",
                        "aggregations": [
                            {
                                "metricName": metric_name,
                                "timeAggregation": "latest",
                                "spaceAggregation": "sum",
                                "reduceTo": "max",
                            }
                        ],
                        "filter": {"expression": f"run.label = '{label}'"},
                        "groupBy": [{"name": group_by}],
                    },
                }
            ]
        },
    }
    response = _query_range(payload, api_key)
    rows = response["data"]["data"]["results"][0]["data"]
    return [(value, count) for value, count in rows]


def _raw_trace_rows(
    filter_expression: str,
    select_fields: Sequence[str],
    api_key: str,
    dump_raw: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Fetch every row matching a raw trace query, paging by `offset`.

    `nextCursor` is not usable for this: the v5 raw endpoint returns it empty
    even when the page is full and more rows exist, so trusting it truncates
    silently at one page (measured: 1000 of 1054 root spans, which pushed
    genuinely attributable spans into the unattributed bucket). A full page is
    the only signal that there is more to fetch.
    """
    start_ns, end_ns = _time_range_ns()
    rows: List[Dict[str, Any]] = []
    offset = 0
    while True:
        spec: Dict[str, Any] = {
            "name": "A",
            "signal": "traces",
            "selectFields": [{"name": field} for field in select_fields],
            "filter": {"expression": filter_expression},
            "limit": PAGE_LIMIT,
            "offset": offset,
        }
        payload = {
            "schemaVersion": "v1",
            "start": start_ns,
            "end": end_ns,
            "requestType": "raw",
            "compositeQuery": {"queries": [{"type": "builder_query", "spec": spec}]},
        }
        response = _query_range(payload, api_key)
        if dump_raw is not None:
            dump_raw.append(response)
        result = response["data"]["data"]["results"][0]
        page_rows = result.get("rows") or []
        rows.extend(row["data"] for row in page_rows)
        if len(page_rows) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
    return rows


def cmd_totals(args: argparse.Namespace) -> None:
    api_key = _require_api_key()
    result: Dict[str, Any] = {
        "run.label": args.label,
        "async_job_submissions_by_request_type": dict(
            _metric_group_values(
                "synapse.async_job.submissions", args.label, "request_type", api_key
            )
        ),
        "async_job_submissions_by_outcome": dict(
            _metric_group_values(
                "synapse.async_job.submissions", args.label, "outcome", api_key
            )
        ),
        "uploads_by_external_file_handle": dict(
            _metric_group_values(
                "synapse.file_handle.uploads",
                args.label,
                "external_file_handle",
                api_key,
            )
        ),
        "distinct_service_instance_ids": [
            value
            for value, _ in _metric_group_values(
                "synapse.async_job.submissions",
                args.label,
                "service.instance.id",
                api_key,
            )
        ],
        "git_sha": [
            value
            for value, _ in _metric_group_values(
                "synapse.async_job.submissions", args.label, "git.sha", api_key
            )
        ],
        "xdist_workers": [
            value
            for value, _ in _metric_group_values(
                "synapse.async_job.submissions",
                args.label,
                "xdist.workers",
                api_key,
            )
        ],
    }
    _emit(result, args)


def _join(
    root_rows: Sequence[Dict[str, Any]],
    async_rows: Sequence[Dict[str, Any]],
    upload_rows: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[str]]]:
    """Join async-job and upload spans onto their test root span by `trace_id`.

    Returns `(per_test, unattributed)`. `per_test` maps nodeid -> row with
    `module`, `executions`, `async` (request_type -> per-execution count),
    `uploads` (external -> per-execution count), `cost`, `signature`.
    `unattributed` maps instrument name -> list of trace_ids with no root span
    in this run (§B11 - reported, never used to justify a cut).
    """
    trace_to_nodeid: Dict[str, str] = {
        row["trace_id"]: row["name"]
        for row in root_rows
        if row["name"] not in _NON_TEST_ROOT_SPAN_NAMES
    }
    executions = Counter(trace_to_nodeid.values())

    duration_ns_by_nodeid: Dict[str, float] = defaultdict(float)
    for row in root_rows:
        nodeid = trace_to_nodeid.get(row["trace_id"])
        duration_nano = row.get("duration_nano")
        if nodeid is not None and duration_nano is not None:
            duration_ns_by_nodeid[nodeid] += float(duration_nano)

    raw_async: Dict[str, Counter] = defaultdict(Counter)
    unattributed_async: List[str] = []
    for row in async_rows:
        nodeid = trace_to_nodeid.get(row["trace_id"])
        if nodeid is None:
            unattributed_async.append(row["trace_id"])
        else:
            raw_async[nodeid][row["request_type"]] += 1

    raw_upload: Dict[str, Counter] = defaultdict(Counter)
    unattributed_upload: List[str] = []
    for row in upload_rows:
        if row.get("external") is None:
            # Missing the discriminator attribute entirely - an unmetered
            # `multipart_upload_string_async` span, or data recorded before
            # Slice 2 added it. Excluded, not counted as zero.
            continue
        nodeid = trace_to_nodeid.get(row["trace_id"])
        if nodeid is None:
            unattributed_upload.append(row["trace_id"])
        else:
            raw_upload[nodeid][row["external"]] += 1

    per_test: Dict[str, Dict[str, Any]] = {}
    for nodeid, execution_count in executions.items():
        async_counts = {
            rt: count / execution_count for rt, count in raw_async[nodeid].items()
        }
        upload_counts = {
            ext: count / execution_count for ext, count in raw_upload[nodeid].items()
        }
        signature: Set[Tuple[str, Any]] = {
            ("async_job", rt) for rt, v in async_counts.items() if v > 0
        } | {("upload", ext) for ext, v in upload_counts.items() if v > 0}
        per_test[nodeid] = {
            "module": nodeid.split("::")[0] if "::" in nodeid else nodeid,
            "executions": execution_count,
            "async": async_counts,
            "uploads": upload_counts,
            "cost": sum(async_counts.values()) + sum(upload_counts.values()),
            "signature": signature,
            "duration_sec": round(
                duration_ns_by_nodeid[nodeid] / execution_count / 1e9, 3
            ),
        }

    signature_holders: Dict[Tuple[str, Any], Set[str]] = defaultdict(set)
    for nodeid, row in per_test.items():
        for key in row["signature"]:
            signature_holders[key].add(nodeid)
    for row in per_test.values():
        row["unique"] = {k for k in row["signature"] if len(signature_holders[k]) == 1}

    return per_test, {"async_job": unattributed_async, "upload": unattributed_upload}


def _classify(per_test: Dict[str, Dict[str, Any]]) -> None:
    """Mutate each row with `classification`, `dominator`, `contested_reason`,
    per requirements D2/D3: a candidate is `t` with `cost(t) > 0` and some
    `u != t` whose signature is a superset of `t`'s. `clear` needs a
    same-module dominator at least as expensive; everything else that is a
    candidate is `contested`, for one of three reasons.
    """
    nodeids = list(per_test)
    for t in nodeids:
        row = per_test[t]
        if row["cost"] <= 0:
            row["classification"] = "not-a-candidate"
            row["dominator"] = None
            continue

        dominators = [
            u
            for u in nodeids
            if u != t and row["signature"] <= per_test[u]["signature"]
        ]
        if not dominators:
            row["classification"] = "not-a-candidate"
            row["dominator"] = None
            continue

        same_module_at_least_as_costly = [
            u
            for u in dominators
            if per_test[u]["module"] == row["module"]
            and per_test[u]["cost"] >= row["cost"]
        ]
        if not row["unique"] and same_module_at_least_as_costly:
            dominator = max(
                same_module_at_least_as_costly, key=lambda u: per_test[u]["cost"]
            )
            row["classification"] = "clear"
            row["dominator"] = dominator
            row["contested_reason"] = None
        else:
            dominator = max(dominators, key=lambda u: per_test[u]["cost"])
            row["classification"] = "contested"
            row["dominator"] = dominator
            if row["unique"]:
                row["contested_reason"] = "unique(t) != empty"
            elif per_test[dominator]["module"] != row["module"]:
                row["contested_reason"] = "cross-module dominator only"
            else:
                row["contested_reason"] = "cost(u) < cost(t)"


def cmd_per_test(args: argparse.Namespace) -> None:
    api_key = _require_api_key()
    dump_raw: Optional[List[Dict[str, Any]]] = [] if args.dump_raw else None
    label = args.label

    root_rows = _raw_trace_rows(
        f"run.label = '{label}' AND parent_span_id = ''",
        ["name", "trace_id", "duration_nano"],
        api_key,
        dump_raw,
    )
    async_rows = [
        {"trace_id": r["trace_id"], "request_type": r["synapse.async_job.request_type"]}
        for r in _raw_trace_rows(
            f"run.label = '{label}' AND name = 'synapse.async_job'",
            ["trace_id", "synapse.async_job.request_type"],
            api_key,
            dump_raw,
        )
    ]
    upload_rows = [
        {"trace_id": r["trace_id"], "external": r["synapse.file_handle.external"]}
        for r in _raw_trace_rows(
            f"run.label = '{label}' AND name = 'synapse.transfer.upload' "
            "AND synapse.file_handle.external EXISTS",
            ["trace_id", "synapse.file_handle.external"],
            api_key,
            dump_raw,
        )
    ]

    per_test, unattributed = _join(root_rows, async_rows, upload_rows)
    _classify(per_test)

    if dump_raw is not None:
        with open(args.dump_raw, "w") as f:
            json.dump(dump_raw, f, indent=2)

    result = {
        "run.label": label,
        "root_span_count": len(per_test),
        "async_job_total": len(async_rows),
        "async_job_unattributed": len(unattributed["async_job"]),
        "upload_total": len(upload_rows),
        "upload_unattributed": len(unattributed["upload"]),
        "unattributed_trace_ids": unattributed,
        "per_test": {
            nodeid: {
                **row,
                "signature": sorted(f"{k}:{v}" for k, v in row["signature"]),
                "unique": sorted(f"{k}:{v}" for k, v in row["unique"]),
            }
            for nodeid, row in per_test.items()
        },
    }
    _emit(result, args)


def _emit(result: Dict[str, Any], args: argparse.Namespace) -> None:
    if getattr(args, "csv", False):
        per_test = result.get("per_test")
        if not per_test:
            sys.exit("--csv only applies to per-test output.")
        writer = csv.writer(sys.stdout)
        writer.writerow(
            [
                "nodeid",
                "module",
                "executions",
                "cost",
                "duration_sec",
                "classification",
                "dominator",
            ]
        )
        for nodeid, row in per_test.items():
            writer.writerow(
                [
                    nodeid,
                    row["module"],
                    row["executions"],
                    row["cost"],
                    row["duration_sec"],
                    row["classification"],
                    row["dominator"],
                ]
            )
    else:
        print(json.dumps(result, indent=2, default=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    totals_parser = subparsers.add_parser(
        "totals", help="Suite-level totals for a labelled run."
    )
    totals_parser.add_argument("--label", required=True)
    totals_parser.add_argument("--json", action="store_true", default=True)
    totals_parser.set_defaults(func=cmd_totals)

    per_test_parser = subparsers.add_parser(
        "per-test", help="Per-test load table for a labelled run."
    )
    per_test_parser.add_argument("--label", required=True)
    per_test_parser.add_argument("--json", action="store_true", default=True)
    per_test_parser.add_argument("--csv", action="store_true")
    per_test_parser.add_argument(
        "--dump-raw", metavar="FILE", help="Write unparsed SigNoz responses to FILE."
    )
    per_test_parser.set_defaults(func=cmd_per_test)

    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
