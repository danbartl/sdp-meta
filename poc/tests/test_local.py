import polars as pl
import pytest

from sdp_meta_poc.cases import SCHEMA, batch, expected
from sdp_meta_poc.local import assert_result, run_local, simulate, split_quality


def test_all_goldens_and_delta_versions(tmp_path):
    report = run_local(tmp_path / "run")
    assert report["native_status"] == "not_run"
    assert [s["delta_version"] for s in report["stages"]] == list(range(6))


def test_reusing_output_cannot_overwrite_evidence(tmp_path):
    with pytest.raises(FileExistsError):
        run_local(tmp_path)


def test_late_arrival_and_input_order_do_not_change_business_history():
    events = pl.concat([batch(n) for n in ("initial", "incremental", "late")])
    valid, _ = split_quality(events)
    for seed in (1, 2, 3):
        result = simulate(valid.sample(fraction=1, shuffle=True, seed=seed))
        assert_result(result["history"], expected("late", "history"))


@pytest.mark.parametrize(
    "rows",
    [
        [("a", "A", 10, 1, "UPSERT"), ("a", "A", 20, 2, "UPSERT")],
        [("a", "A", 10, 1, "UPSERT"), ("b", "A", 20, 1, "UPSERT")],
    ],
)
def test_conflicting_event_ids_or_sequences_rejected(rows):
    with pytest.raises(ValueError, match="conflicting"):
        simulate(pl.DataFrame(rows, schema=SCHEMA, orient="row"))


def test_null_delete_is_valid_but_null_upsert_is_not():
    frame = pl.DataFrame(
        [("d", "A", None, 2, "DELETE"), ("u", "B", None, 2, "UPSERT")], schema=SCHEMA, orient="row"
    )
    valid, invalid = split_quality(frame)
    assert valid["event_id"].to_list() == ["d"]
    assert invalid["event_id"].to_list() == ["u"]


def test_checker_rejects_wrong_values_extra_duplicates_and_dtypes():
    golden = expected("initial", "current")
    wrong = [
        golden.with_columns(pl.lit(0, dtype=pl.Int64).alias("amount")),
        pl.concat([golden, golden.head(1)]),
        golden.with_columns(pl.col("amount").cast(pl.Float64)),
    ]
    for frame in wrong:
        with pytest.raises(AssertionError):
            assert_result(frame, golden)


def test_rule_revision_only_applies_to_new_events():
    accepted, _ = split_quality(batch("recovery"))
    rejected, quarantine = split_quality(batch("rule_change"), maximum=600)
    assert accepted.height == 1 and rejected.height == 0 and quarantine.height == 1
