import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from .cases import DDL, VALID_SQL

UPSTREAM_COMMIT = "17f5a954c87d460acfed3d815ef9046bd2ea5b72"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


@dataclass(frozen=True)
class Settings:
    catalog: str
    namespace: str

    def __post_init__(self):
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", self.catalog):
            raise ValueError("catalog must be a simple UC identifier")
        if not re.fullmatch(r"poc_[a-z0-9_]{6,40}", self.namespace):
            raise ValueError("namespace must match poc_[a-z0-9_]{6,40}")

    def schema(self, arm):
        if arm not in {"control", "sdp", "native", "framework"}:
            raise ValueError(f"unknown arm {arm}")
        return f"{self.catalog}.{self.namespace}_{arm}"

    @property
    def volume(self):
        return f"/Volumes/{self.catalog}/{self.namespace}_control/fixtures"


def onboarding(settings, revision=1):
    root = settings.volume
    bronze = {
        "data_flow_id": "1",
        "data_flow_group": "comparison",
        "source_format": "cloudFiles",
        "source_details": {
            "source_path_dev": f"{root}/landing",
            "source_schema_path": f"{root}/conf/events.ddl",
            "source_metadata": {
                "include_autoloader_metadata_column": "true",
                "autoloader_metadata_col_name": "source_metadata",
            },
        },
        "bronze_catalog_dev": settings.catalog,
        "bronze_database_dev": f"{settings.namespace}_sdp",
        "bronze_table": "events",
        "bronze_reader_options": {
            "cloudFiles.format": "csv",
            "header": "true",
            "cloudFiles.schemaEvolutionMode": "none",
        },
        "bronze_data_quality_expectations_json_dev": f"{root}/conf/quality.json",
        "bronze_catalog_quarantine_dev": settings.catalog,
        "bronze_database_quarantine_dev": f"{settings.namespace}_sdp",
        "bronze_quarantine_table": "quarantine",
        "bronze_table_properties": {"poc.revision": str(revision)},
    }
    rows = []
    for index, (target, scd) in enumerate((("history", "2"), ("current", "1"))):
        row = (
            dict(bronze)
            if index == 0
            else {
                "data_flow_id": "2",
                "data_flow_group": "comparison",
                "bronze_catalog_dev": settings.catalog,
                "bronze_database_dev": f"{settings.namespace}_sdp",
                "bronze_table": "events",
            }
        )
        row.update(
            {
                "silver_catalog_dev": settings.catalog,
                "silver_database_dev": f"{settings.namespace}_sdp",
                "silver_table": target,
                "silver_transformation_json_dev": f"{root}/conf/silver.json",
                "silver_cdc_apply_changes": {
                    "keys": ["account_id"],
                    "sequence_by": "sequence",
                    "scd_type": scd,
                    "apply_as_deletes": "operation = 'DELETE'",
                    "except_column_list": ["sequence", "operation"],
                },
            }
        )
        rows.append(row)
    return rows


def configuration_files(settings, revision=1, fault=False, dqx=False):
    maximum = None if revision == 1 else 600
    expression = VALID_SQL + (" AND (operation = 'DELETE' OR amount <= 600)" if maximum else "")
    return {
        "events.ddl": DDL,
        "onboarding.json": onboarding(settings, revision),
        "quality.json": {"expect_or_quarantine": {"valid_event": expression}},
        "silver.json": [
            {
                "target_table": target,
                "select_exp": ["account_id", "amount", "sequence", "operation"],
            }
            for target in ("current", "history")
        ],
        "control.json": {"revision": revision, "maximum": maximum, "fault": fault, "dqx": dqx},
    }


def bundle_resources(settings, framework_wheel=None, dqx=False):
    deps = [
        "${workspace.file_path}/vendor/sdp_meta_comparison_poc-0.1.0-py3-none-any.whl",
        "${workspace.file_path}/vendor/databricks_labs_sdp_meta-0.1.0-py3-none-any.whl",
    ]
    if dqx:
        deps.append("databricks-labs-dqx==0.16.0")
    arms = ["native", "sdp"]
    if framework_wheel:
        if not framework_wheel.startswith("/Volumes/") or not framework_wheel.endswith(".whl"):
            raise ValueError("framework wheel must be an explicit /Volumes/.../*.whl path")
        arms.append("framework")
    pipelines = {}
    for arm in arms:
        configuration = {
            "poc.catalog": settings.catalog,
            "poc.namespace": settings.namespace,
            "poc.arm": arm,
            "spark.sql.session.timeZone": "UTC",
        }
        if arm == "sdp":
            for layer in ("bronze", "silver"):
                configuration[f"{layer}.dataflowspecTable"] = (
                    f"{settings.schema('control')}.{layer}_dataflowspec"
                )
                configuration[f"{layer}.group"] = "comparison"
        pipelines[arm] = {
            "name": f"${{bundle.name}}-{arm}",
            "catalog": settings.catalog,
            "schema": f"{settings.namespace}_{arm}",
            "serverless": True,
            "channel": "CURRENT",
            "continuous": False,
            "development": True,
            "configuration": configuration,
            "environment": {
                "environment_version": "4",
                "dependencies": deps + ([framework_wheel] if arm == "framework" else []),
            },
            "libraries": [{"notebook": {"path": "${workspace.file_path}/notebooks/pipeline"}}],
        }
    return {
        "resources": {
            "pipelines": pipelines,
            "jobs": {
                "compare": {
                    "name": "${bundle.name}-compare",
                    "max_concurrent_runs": 1,
                    "tasks": [
                        {
                            "task_key": "compare",
                            "timeout_seconds": 14400,
                            "notebook_task": {
                                "notebook_path": "${workspace.file_path}/notebooks/compare",
                                "base_parameters": {
                                    "catalog": settings.catalog,
                                    "namespace": settings.namespace,
                                    "pipelines": json.dumps(
                                        {a: f"${{resources.pipelines.{a}.id}}" for a in arms}
                                    ),
                                    "build_manifest": (
                                        "${workspace.file_path}/vendor/build-manifest.json"
                                    ),
                                    "dqx": str(dqx).lower(),
                                },
                            },
                            "environment_key": "poc",
                        }
                    ],
                    "environments": [
                        {
                            "environment_key": "poc",
                            "spec": {
                                "environment_version": "4",
                                "dependencies": deps,
                            },
                        }
                    ],
                }
            },
        }
    }


def write_config(settings, revision=1, fault=False, dqx=False):
    files = configuration_files(settings, revision, fault, dqx)
    root = Path(settings.volume) / "conf"
    root.mkdir(parents=True, exist_ok=True)
    for name, value in files.items():
        text = value if isinstance(value, str) else json.dumps(value, indent=2)
        (root / name).write_text(text, encoding="utf-8")
    return digest(files)
