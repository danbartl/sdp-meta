"""A bounded business model, never a substitute for AUTO CDC execution."""

import json
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake
from polars.testing import assert_frame_equal

from .cases import HISTORY, PAYLOAD, SCHEMA, expected
from .delivery import make_delivery


def split_quality(frame, maximum=None):
    valid = (
        pl.col("event_id").is_not_null()
        & pl.col("account_id").is_not_null()
        & (pl.col("account_id") != "")
        & (pl.col("sequence") >= 0)
        & pl.col("operation").is_in(["UPSERT", "DELETE"])
        & ((pl.col("operation") == "DELETE") | (pl.col("amount") >= 0))
    )
    if maximum is not None:
        valid &= (pl.col("operation") == "DELETE") | (pl.col("amount") <= maximum)
    valid = valid.fill_null(False)
    return frame.filter(valid), frame.filter(~valid)


def simulate(events):
    """Exact event repeats are idempotent; conflicting identities/order are rejected."""
    unique = events.unique()
    for key in (["event_id"], ["account_id", "sequence"]):
        if unique.group_by(key).len().filter(pl.col("len") > 1).height:
            raise ValueError(f"conflicting events for {key}")
    history = []
    current = {}
    for row in unique.sort(["account_id", "sequence"]).iter_rows(named=True):
        key, amount, seq = row["account_id"], row["amount"], row["sequence"]
        previous = current.get(key)
        if row["operation"] != "DELETE" and previous and previous[1] == amount:
            continue
        if previous:
            previous[3] = seq
            del current[key]
        if row["operation"] != "DELETE":
            version = [key, amount, seq, None]
            history.append(version)
            current[key] = version
    return {
        "current": pl.DataFrame(
            [r[:2] for r in current.values()], schema={k: SCHEMA[k] for k in PAYLOAD}, orient="row"
        ),
        "history": pl.DataFrame(
            history,
            schema={
                "account_id": pl.String,
                "amount": pl.Int64,
                "__START_AT": pl.Int64,
                "__END_AT": pl.Int64,
            },
            orient="row",
        ),
    }


def assert_result(actual, wanted):
    if set(actual.columns) != set(wanted.columns):
        raise AssertionError(f"columns differ: {actual.columns} != {wanted.columns}")
    keys = HISTORY if "__START_AT" in wanted.columns else PAYLOAD
    assert_frame_equal(actual.select(keys).sort(keys), wanted.select(keys).sort(keys))


def run_local(root: Path):
    root.mkdir(parents=True, exist_ok=False)
    source = root / "events"
    clean = pl.DataFrame(schema=SCHEMA)
    checks = []
    versions = []
    rejected = []
    stages = [
        ("initial", "initial", "initial", None),
        ("incremental", "incremental", "incremental", None),
        ("late", "late", "late", None),
        ("replay", "incremental", "late", None),
        ("recovery", "recovery", "recovery", None),
        ("rule_change", "rule_change", "recovery", 600),
    ]
    for stage, delivery, golden, maximum in stages:
        incoming, manifest = make_delivery(delivery, stage, root)
        write_deltalake(source, incoming.to_arrow(), mode="append")
        versions.append(DeltaTable(source).version())
        valid, invalid = split_quality(incoming, maximum)
        rejected.extend(invalid["event_id"].to_list())
        clean = pl.concat([clean, valid])
        for kind, result in simulate(clean).items():
            assert_result(result, expected(golden, kind))
        checks.append(
            {
                "stage": stage,
                "status": "passed",
                "delta_version": versions[-1],
                "delivery": manifest,
            }
        )
    assert DeltaTable(source, version=0).to_pyarrow_table().num_rows == 4
    assert rejected == [
        "bad_negative",
        "bad_key",
        "bad_operation",
        "bad_null",
        "bad_operation",
        "g70",
    ]
    report = {
        "scope": "local_business_model_and_delta_io",
        "native_status": "not_run",
        "stages": checks,
        "quarantined_event_ids": rejected,
        "limitations": [
            "No AUTO CDC, checkpoints, Unity Catalog or SDP-META execution",
            "recovery stage models the expected result, not a process failure",
        ],
    }
    (root / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report
