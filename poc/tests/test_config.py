import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from sdp_meta_poc.cli import capabilities
from sdp_meta_poc.config import Settings, bundle_resources, configuration_files, digest
from sdp_meta_poc.remote import expected_failure, run_update


@pytest.mark.parametrize(
    "catalog,namespace", [("x;DROP", "poc_abcdef"), ("main", "prod"), ("main", "poc_../../etc")]
)
def test_unsafe_namespace_rejected(catalog, namespace):
    with pytest.raises(ValueError):
        Settings(catalog, namespace)


def test_one_bronze_owner_and_two_silver_targets():
    files = configuration_files(Settings("main", "poc_example"))
    flows = files["onboarding.json"]
    assert len([r for r in flows if "source_format" in r]) == 1
    assert {r["silver_table"] for r in flows} == {"current", "history"}
    assert digest(files) != digest(configuration_files(Settings("main", "poc_example"), 2))


def test_framework_wheel_is_optional_and_never_copied():
    settings = Settings("main", "poc_example")
    assert set(bundle_resources(settings)["resources"]["pipelines"]) == {"sdp", "native"}
    config = bundle_resources(settings, "/Volumes/private/build/wheels/framework.whl")
    assert len(config["resources"]["pipelines"]) == 3
    assert "framework.whl" not in json.dumps(config["resources"]["pipelines"]["sdp"])


def test_bitemporal_gap_is_visible_not_silently_supported():
    repo = Path(__file__).resolve().parents[2]
    report = capabilities(repo)
    assert report["cdc_call_arguments"]
    assert report["bitemporal_system_sequence_forwarded"] is False


def test_only_injected_error_counts_as_expected_failure():
    assert expected_failure({"state": "FAILED", "events": ["POC_INJECTED_FAILURE"]})
    assert not expected_failure({"state": "FAILED", "events": ["Permission denied"]})
    assert not expected_failure({"state": "CANCELED", "events": ["POC_INJECTED_FAILURE"]})


def test_timeout_stops_pipeline_and_cannot_pass():
    stopped = []
    api = SimpleNamespace(
        start_update=lambda **kw: SimpleNamespace(update_id="update-1"),
        get_update=lambda *args: SimpleNamespace(update=SimpleNamespace(state="RUNNING")),
        stop=lambda pipeline: stopped.append(pipeline),
        list_pipeline_events=lambda *args, **kw: [],
    )
    result = run_update(SimpleNamespace(pipelines=api), "pipeline-1", timeout=-1, poll_seconds=0)
    assert result["state"] == "TIMED_OUT" and stopped == ["pipeline-1"]
    assert "event_error" in result
