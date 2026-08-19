"""Unit tests for .github/scripts/measure_test_load.py.

The script lives outside the `synapseclient` package (a maintenance script,
not client instrumentation), so it is loaded via `importlib.util` rather than
imported as a module.
"""

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT_PATH = (
    Path(__file__).resolve().parents[3] / ".github" / "scripts" / "measure_test_load.py"
)
_spec = importlib.util.spec_from_file_location("measure_test_load", _SCRIPT_PATH)
measure_test_load = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(measure_test_load)

_join = measure_test_load._join
_classify = measure_test_load._classify


def _root(trace_id: str, name: str, duration_nano=None) -> dict:
    row = {"trace_id": trace_id, "name": name}
    if duration_nano is not None:
        row["duration_nano"] = duration_nano
    return row


def _async(trace_id: str, request_type: str) -> dict:
    return {"trace_id": trace_id, "request_type": request_type}


def _upload(trace_id: str, external) -> dict:
    return {"trace_id": trace_id, "external": external}


class TestJoin:
    def test_executions_from_repeated_root_span_names(self) -> None:
        roots = [_root("t1", "test_a"), _root("t2", "test_a"), _root("t3", "test_b")]

        per_test, _ = _join(roots, [], [])

        assert per_test["test_a"]["executions"] == 2
        assert per_test["test_b"]["executions"] == 1

    def test_async_and_upload_counts_divided_by_executions(self) -> None:
        roots = [_root("t1", "test_a"), _root("t2", "test_a")]
        async_rows = [_async("t1", "rt1"), _async("t1", "rt1"), _async("t2", "rt1")]
        upload_rows = [_upload("t1", True)]

        per_test, _ = _join(roots, async_rows, upload_rows)

        # 3 async spans over 2 executions -> 1.5 per execution.
        assert per_test["test_a"]["async"]["rt1"] == 1.5
        # 1 upload span over 2 executions -> 0.5 per execution.
        assert per_test["test_a"]["uploads"][True] == 0.5
        assert per_test["test_a"]["cost"] == 2.0

    def test_upload_rows_missing_external_attribute_are_excluded(self) -> None:
        roots = [_root("t1", "test_a")]
        upload_rows = [_upload("t1", None), _upload("t1", True)]

        per_test, unattributed = _join(roots, [], upload_rows)

        assert per_test["test_a"]["uploads"] == {True: 1.0}
        assert unattributed["upload"] == []

    def test_spans_with_no_root_span_are_unattributed(self) -> None:
        roots = [_root("t1", "test_a")]
        async_rows = [_async("t1", "rt1"), _async("no-such-trace", "rt1")]
        upload_rows = [_upload("no-such-trace", True)]

        per_test, unattributed = _join(roots, async_rows, upload_rows)

        assert unattributed["async_job"] == ["no-such-trace"]
        assert unattributed["upload"] == ["no-such-trace"]
        assert "no-such-trace" not in per_test

    def test_non_test_root_span_names_are_not_test_executions(self) -> None:
        roots = [
            _root("t1", "test_a"),
            _root("t2", "DELETE"),
            _root("t3", "synapse.async_job"),
        ]

        per_test, _ = _join(roots, [], [])

        assert list(per_test) == ["test_a"]

    def test_signature_only_includes_nonzero_keys(self) -> None:
        roots = [_root("t1", "test_a")]
        async_rows = [_async("t1", "rt1")]

        per_test, _ = _join(roots, async_rows, [])

        assert per_test["test_a"]["signature"] == {("async_job", "rt1")}

    def test_unique_is_empty_when_another_test_shares_the_key(self) -> None:
        roots = [_root("t1", "test_a"), _root("t2", "test_b")]
        async_rows = [_async("t1", "rt1"), _async("t2", "rt1")]

        per_test, _ = _join(roots, async_rows, [])

        assert per_test["test_a"]["unique"] == set()
        assert per_test["test_b"]["unique"] == set()

    def test_duration_sec_parsed_from_root_span_duration_nano(self) -> None:
        roots = [_root("t1", "test_a", duration_nano=2_500_000_000)]

        per_test, _ = _join(roots, [], [])

        assert per_test["test_a"]["duration_sec"] == 2.5

    def test_duration_sec_averaged_over_executions(self) -> None:
        roots = [
            _root("t1", "test_a", duration_nano=1_000_000_000),
            _root("t2", "test_a", duration_nano=3_000_000_000),
        ]

        per_test, _ = _join(roots, [], [])

        assert per_test["test_a"]["duration_sec"] == 2.0

    def test_unique_holds_the_key_held_by_no_other_test(self) -> None:
        roots = [_root("t1", "test_a"), _root("t2", "test_b")]
        async_rows = [_async("t1", "rt1"), _async("t2", "rt2")]

        per_test, _ = _join(roots, async_rows, [])

        assert per_test["test_a"]["unique"] == {("async_job", "rt1")}
        assert per_test["test_b"]["unique"] == {("async_job", "rt2")}


class TestClassify:
    def _rows(self, **tests) -> dict:
        """Build per_test rows directly, skipping `_join`, for scoring-only tests."""
        rows = {}
        for nodeid, (module, cost, signature) in tests.items():
            rows[nodeid] = {
                "module": module,
                "cost": cost,
                "signature": set(signature),
                "unique": set(),
            }
        return rows

    def test_cost_zero_is_never_a_candidate(self) -> None:
        rows = self._rows(t=("mod", 0, []))

        _classify(rows)

        assert rows["t"]["classification"] == "not-a-candidate"
        assert rows["t"]["dominator"] is None

    def test_clear_when_same_module_dominator_covers_it_at_no_less_cost(self) -> None:
        rows = self._rows(
            t=("mod", 1, [("async_job", "rt1")]),
            u=("mod", 2, [("async_job", "rt1"), ("async_job", "rt2")]),
        )

        _classify(rows)

        assert rows["t"]["classification"] == "clear"
        assert rows["t"]["dominator"] == "u"

    def test_contested_cross_module_dominator_only(self) -> None:
        rows = self._rows(
            t=("mod_a", 1, [("async_job", "rt1")]),
            u=("mod_b", 2, [("async_job", "rt1"), ("async_job", "rt2")]),
        )

        _classify(rows)

        assert rows["t"]["classification"] == "contested"
        assert rows["t"]["contested_reason"] == "cross-module dominator only"

    def test_contested_dominator_cheaper_than_candidate(self) -> None:
        rows = self._rows(
            t=("mod", 2, [("async_job", "rt1")]),
            u=("mod", 1, [("async_job", "rt1"), ("async_job", "rt2")]),
        )

        _classify(rows)

        assert rows["t"]["classification"] == "contested"
        assert rows["t"]["contested_reason"] == "cost(u) < cost(t)"

    def test_contested_when_candidate_has_unique_coverage(self) -> None:
        rows = self._rows(
            t=("mod", 1, [("async_job", "rt1")]),
            u=("mod", 2, [("async_job", "rt1"), ("async_job", "rt2")]),
        )
        rows["t"]["unique"] = {("async_job", "rt1")}

        _classify(rows)

        assert rows["t"]["classification"] == "contested"
        assert rows["t"]["contested_reason"] == "unique(t) != empty"

    def test_no_dominator_is_not_a_candidate(self) -> None:
        rows = self._rows(
            t=("mod", 1, [("async_job", "rt1"), ("upload", True)]),
            u=("mod", 2, [("async_job", "rt1")]),
        )

        _classify(rows)

        assert rows["t"]["classification"] == "not-a-candidate"
        assert rows["t"]["dominator"] is None


def _raw_response(rows: list, next_cursor: str = "") -> dict:
    return {
        "data": {
            "data": {
                "results": [
                    {"rows": [{"data": row} for row in rows], "nextCursor": next_cursor}
                ]
            }
        }
    }


class TestPaging:
    def test_full_page_is_followed_even_when_next_cursor_is_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(measure_test_load, "PAGE_LIMIT", 2)
        pages = [
            _raw_response([{"trace_id": "t1"}, {"trace_id": "t2"}]),
            _raw_response([{"trace_id": "t3"}]),
        ]
        offsets = []

        def fake_query_range(payload, api_key):
            spec = payload["compositeQuery"]["queries"][0]["spec"]
            offsets.append(spec["offset"])
            return pages[len(offsets) - 1]

        monkeypatch.setattr(measure_test_load, "_query_range", fake_query_range)

        rows = measure_test_load._raw_trace_rows("run.label = 'x'", ["trace_id"], "key")

        assert [row["trace_id"] for row in rows] == ["t1", "t2", "t3"]
        assert offsets == [0, 2]

    def test_short_page_ends_the_walk(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(measure_test_load, "PAGE_LIMIT", 2)
        calls = []

        def fake_query_range(payload, api_key):
            calls.append(payload)
            return _raw_response([{"trace_id": "t1"}])

        monkeypatch.setattr(measure_test_load, "_query_range", fake_query_range)

        rows = measure_test_load._raw_trace_rows("run.label = 'x'", ["trace_id"], "key")

        assert len(rows) == 1
        assert len(calls) == 1


class TestMetricAggregation:
    def test_cumulative_counter_is_reduced_by_max_not_sum(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = {}

        def fake_query_range(payload, api_key):
            captured["payload"] = payload
            return {"data": {"data": {"results": [{"data": [["a", 3]]}]}}}

        monkeypatch.setattr(measure_test_load, "_query_range", fake_query_range)

        values = measure_test_load._metric_group_values(
            "synapse.async_job.submissions", "some-label", "request_type", "key"
        )

        aggregation = captured["payload"]["compositeQuery"]["queries"][0]["spec"][
            "aggregations"
        ][0]
        assert aggregation["reduceTo"] == "max"
        assert aggregation["timeAggregation"] == "latest"
        assert values == [("a", 3)]


class TestCli:
    def test_help_exits_zero_without_signoz_api_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("SIGNOZ_API_KEY", raising=False)

        result = subprocess.run(
            [sys.executable, str(_SCRIPT_PATH), "--help"],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

    def test_totals_without_key_exits_nonzero_and_never_prints_a_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("SIGNOZ_API_KEY", raising=False)

        result = subprocess.run(
            [sys.executable, str(_SCRIPT_PATH), "totals", "--label", "some-label"],
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "SIGNOZ_API_KEY" in result.stdout + result.stderr
