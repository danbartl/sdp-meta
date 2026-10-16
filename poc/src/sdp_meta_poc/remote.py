"""Workspace-only orchestration and evidence. No remote calls during local tests."""

import hashlib
import json
import time
from collections import Counter
from importlib.metadata import version
from pathlib import Path

import polars as pl

from .cases import HISTORY, PAYLOAD, expected
from .config import configuration_files, digest, write_config
from .delivery import make_delivery
from .local import assert_result


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


def verify_build(path):
    manifest = json.loads(path.read_text(encoding="utf-8"))
    for name, sha in manifest["wheels"].items():
        if Path(name).name != name or not name.endswith(".whl"):
            raise ValueError("build manifest contains an invalid wheel filename")
        if hashlib.sha256((path.parent / name).read_bytes()).hexdigest() != sha:
            raise ValueError(f"wheel changed since build: {name}")
    if len(manifest["wheels"]) != 2:
        raise ValueError("build must contain the SDP-META and PoC wheels")
    return manifest


def file_hashes(root, pattern):
    return {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(root.glob(pattern))}


def run_update(client, pipeline_id, *, timeout=1800, poll_seconds=5):
    started = time.monotonic()
    update_id = client.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False).update_id
    result = {"pipeline_id": pipeline_id, "update_id": update_id}
    while True:
        update = client.pipelines.get_update(pipeline_id, update_id).update
        state = getattr(update.state, "value", update.state)
        if state in {"COMPLETED", "FAILED", "CANCELED"}:
            result.update(state=state, seconds=time.monotonic() - started, update=update.as_dict())
            break
        if time.monotonic() - started > timeout:
            client.pipelines.stop(pipeline_id)
            result.update(state="TIMED_OUT", seconds=time.monotonic() - started)
            break
        time.sleep(poll_seconds)
    try:
        for attempt in range(3):
            result["events"] = [
                event.as_dict()
                for event in client.pipelines.list_pipeline_events(
                    pipeline_id,
                    filter=f"origin.update_id = '{update_id}'",
                )
            ]
            if result["events"]:
                break
            if attempt < 2:
                time.sleep(poll_seconds)
        if not result["events"]:
            result["event_error"] = "No events returned for the completed update"
    except Exception as error:
        result["event_error"] = str(error)
    return result


def expected_failure(result):
    return result["state"] == "FAILED" and "POC_INJECTED_FAILURE" in json.dumps(result)


def onboard(spark, settings, revision):
    from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec

    OnboardDataflowspec(
        spark,
        {
            "onboarding_file_path": f"{settings.volume}/conf/onboarding.json",
            "database": settings.schema("control"),
            "env": "dev",
            "bronze_dataflowspec_table": "bronze_dataflowspec",
            "silver_dataflowspec_table": "silver_dataflowspec",
            "import_author": "synthetic-poc",
            "version": str(revision),
            "overwrite": "True",
        },
        uc_enabled=True,
    ).onboard_dataflow_specs()


def spec_snapshot(spark, settings):
    result = {}
    for layer in ("bronze", "silver"):
        table = f"{settings.schema('control')}.{layer}_dataflowspec"
        result[layer] = {
            "delta_version": spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"],
            "rows": sorted(
                [r.asDict(recursive=True) for r in spark.table(table).collect()],
                key=lambda r: json.dumps(r, sort_keys=True, default=str),
            ),
        }
    return result


def check_tables(spark, settings, arm, golden, event_ids, rejected, dqx):
    schema = settings.schema(arm)
    result = {}
    for kind, columns in (("current", PAYLOAD), ("history", HISTORY)):
        frame = spark.table(f"{schema}.{kind}").select(*columns)
        required_types = {c: "string" if c == "account_id" else "bigint" for c in columns}
        if dict(frame.dtypes) != required_types:
            raise AssertionError(f"{arm}.{kind} schema differs: {frame.dtypes}")
        records = [r.asDict() for r in frame.limit(101).collect()]
        actual = pl.DataFrame(records, schema=expected(golden, kind).schema)
        assert_result(actual, expected(golden, kind))
        result[kind] = records
    for name, wanted in (("events", event_ids), ("quarantine", rejected)):
        frame = spark.table(f"{schema}.{name}")
        rows = [r.asDict() for r in frame.select("event_id", "source_file").limit(101).collect()]
        if Counter(r["event_id"] for r in rows) != Counter(wanted):
            raise AssertionError(f"{arm}.{name}: wrong event multiset: {rows}")
        if any(not r["source_file"] or "/landing/" not in r["source_file"] for r in rows):
            raise AssertionError(f"{arm}.{name}: missing source-file provenance")
        result[name] = rows
        if dqx:
            flagged = [
                r["event_id"] for r in frame.filter("poc_dqx_invalid").select("event_id").collect()
            ]
            if Counter(flagged) != Counter(e for e in wanted if e == "bad_operation"):
                raise AssertionError(f"{arm}.{name}: DQX hook classified unexpected events")
    return result


def compare(spark, settings, pipelines, build_manifest, *, dqx=False):
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    build = verify_build(Path(build_manifest))
    if set(pipelines) not in ({"native", "sdp"}, {"native", "sdp", "framework"}):
        raise ValueError("expected native/sdp and optionally framework pipeline ids")
    # A new namespace is required so previous results cannot masquerade as this run.
    for arm in ["control", *pipelines]:
        spark.sql(f"CREATE SCHEMA {settings.schema(arm)}")
    spark.sql(f"CREATE VOLUME {settings.schema('control')}.fixtures")
    root = Path(settings.volume)
    (root / "landing").mkdir()
    evidence = root / "evidence"
    report = {
        "scope": "native_workspace",
        "status": "running",
        "evidence_path": str(evidence),
        "build": build,
        "decision": "pending_review_of_native_results_and_open_gates",
        "packages": {
            p: version(p) for p in ("sdp-meta-comparison-poc", "databricks-labs-sdp-meta")
        },
        "dqx": dqx,
        "arms": list(pipelines),
        "stages": [],
        "open_gates": [
            "bitemporal",
            "production ZIP/Excel adapters",
            "hard process cancellation",
            "authorization and production rollout",
            "cost and scale",
        ],
    }
    if "framework" not in pipelines:
        report["open_gates"].append("existing framework comparison: no wheel supplied")
    if not dqx:
        report["open_gates"].append("DQX extension hook: disabled")
    save(evidence / "report.json", report)
    rejected, accepted = [], []
    quarantine_ids = {"bad_negative", "bad_key", "bad_operation", "bad_null", "g70"}
    stages = [
        ("initial", "initial", "initial", 1, False),
        ("incremental", "incremental", "incremental", 1, False),
        ("late", "late", "late", 1, False),
        ("replay", "incremental", "late", 1, False),
        ("no_new_files", None, "late", 1, False),
        ("injected_failure", "recovery", None, 1, True),
        ("recovery", None, "recovery", 1, False),
        ("rule_change", "rule_change", "recovery", 2, False),
    ]
    try:
        last_revision = None
        for stage, delivery, golden, revision, fault in stages:
            entry = {"name": stage, "arms": {}, "status": "running"}
            report["stages"].append(entry)
            config_hash = write_config(settings, revision, fault, dqx)
            entry["configuration"] = configuration_files(settings, revision, fault, dqx)
            if revision != last_revision:
                onboard(spark, settings, revision)
                last_revision = revision
            if delivery:
                incoming, entry["delivery"] = make_delivery(delivery, stage, root)
                for event in incoming["event_id"]:
                    (rejected if event in quarantine_ids else accepted).append(event)
            entry["config_hash"] = config_hash
            entry["configuration_file_hashes"] = file_hashes(root / "conf", "*")
            entry["spec_before"] = spec_snapshot(spark, settings)
            entry["input_files"] = file_hashes(root / "landing", "*.csv")
            for arm, pipeline_id in pipelines.items():
                arm_result = entry["arms"][arm] = {}
                try:
                    arm_result["pipeline"] = client.pipelines.get(pipeline_id).as_dict()
                    if arm == "framework":
                        dependencies = arm_result["pipeline"]["spec"]["environment"]["dependencies"]
                        arm_result["framework_wheels"] = {
                            p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
                            for p in dependencies
                            if p.startswith("/Volumes/") and p.endswith(".whl")
                        }
                        if not arm_result["framework_wheels"]:
                            raise ValueError(
                                "no private framework wheel found in pipeline dependencies"
                            )
                    update = run_update(client, pipeline_id)
                    arm_result["run"] = update
                    if fault:
                        if not expected_failure(update):
                            raise AssertionError("expected failure marker was not observed")
                    else:
                        if update["state"] != "COMPLETED":
                            raise AssertionError(f"pipeline ended as {update['state']}")
                        arm_result["tables"] = check_tables(
                            spark,
                            settings,
                            arm,
                            golden,
                            accepted,
                            rejected,
                            dqx,
                        )
                    if "event_error" in update:
                        raise AssertionError("pipeline event evidence unavailable")
                    arm_result["status"] = "passed"
                except Exception as error:
                    arm_result.update(status="failed", error=repr(error))
                save(evidence / stage / f"{arm}.json", arm_result)
            entry["spec_after"] = spec_snapshot(spark, settings)
            if digest(entry["spec_before"]) != digest(entry["spec_after"]):
                raise AssertionError("DataflowSpecs changed during the comparison stage")
            if entry["input_files"] != file_hashes(root / "landing", "*.csv"):
                raise AssertionError("input files changed during the comparison stage")
            if entry["configuration_file_hashes"] != file_hashes(root / "conf", "*"):
                raise AssertionError("configuration files changed during the comparison stage")
            entry["status"] = (
                "passed"
                if all(a["status"] == "passed" for a in entry["arms"].values())
                else "failed"
            )
            save(evidence / "report.json", report)
            if entry["status"] == "failed":
                raise AssertionError(f"stage {stage} failed; later stages not executed")
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=repr(error))
    finally:
        save(evidence / "report.json", report)
    return report
