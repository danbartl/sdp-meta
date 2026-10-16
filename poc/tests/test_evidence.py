import hashlib
import json
from types import SimpleNamespace

import pytest

from sdp_meta_poc import remote
from sdp_meta_poc.config import Settings


def manifest(tmp_path):
    wheels = {}
    for name in ("sdp.whl", "poc.whl"):
        payload = name.encode()
        (tmp_path / name).write_bytes(payload)
        wheels[name] = hashlib.sha256(payload).hexdigest()
    path = tmp_path / "build.json"
    path.write_text(json.dumps({"wheels": wheels}))
    return path


def test_build_tampering_fails_before_any_remote_work(tmp_path):
    path = manifest(tmp_path)
    remote.verify_build(path)
    (tmp_path / "sdp.whl").write_bytes(b"different")
    with pytest.raises(ValueError, match="changed"):
        remote.verify_build(path)


@pytest.mark.parametrize("break_stage", [None, "initial", "injected_failure"])
def test_orchestrator_never_marks_partial_or_wrong_failure_success(
    monkeypatch, tmp_path, break_stage
):
    from databricks import sdk

    path = manifest(tmp_path)
    root = tmp_path / "volume"
    root.mkdir()
    monkeypatch.setattr(Settings, "volume", property(lambda self: str(root)))
    monkeypatch.setattr(remote, "version", lambda name: "test")
    monkeypatch.setattr(remote, "onboard", lambda *args: None)
    monkeypatch.setattr(remote, "spec_snapshot", lambda *args: {"version": 0})
    monkeypatch.setattr(remote, "check_tables", lambda *args: {"test_double": True})
    pipeline = SimpleNamespace(as_dict=lambda: {"spec": {}})
    monkeypatch.setattr(
        sdk,
        "WorkspaceClient",
        lambda: SimpleNamespace(pipelines=SimpleNamespace(get=lambda *args: pipeline)),
    )

    def run(*args):
        control = json.loads((root / "conf/control.json").read_text())
        if break_stage == "initial":
            return {"state": "FAILED", "events": ["Access denied"]}
        if control["fault"]:
            return {
                "state": "FAILED",
                "events": [
                    "Access denied" if break_stage == "injected_failure" else "POC_INJECTED_FAILURE"
                ],
            }
        return {"state": "COMPLETED", "events": []}

    monkeypatch.setattr(remote, "run_update", run)
    report = remote.compare(
        SimpleNamespace(sql=lambda sql: None),
        Settings("sandbox", "poc_evidence"),
        {"native": "n", "sdp": "s"},
        str(path),
    )
    assert report["status"] == ("failed" if break_stage else "passed")
    assert len(report["stages"]) == ({"initial": 1, "injected_failure": 6}.get(break_stage, 8))
    assert json.loads((root / "evidence/report.json").read_text())["status"] == report["status"]
    assert report["decision"].startswith("pending")
