# Databricks notebook source
import json

from pyspark.sql import SparkSession

from sdp_meta_poc.config import Settings
from sdp_meta_poc.remote import compare

report = compare(
    SparkSession.active(),
    Settings(dbutils.widgets.get("catalog"), dbutils.widgets.get("namespace")),  # noqa: F821
    json.loads(dbutils.widgets.get("pipelines")),  # noqa: F821
    dbutils.widgets.get("build_manifest"),  # noqa: F821
    dqx=dbutils.widgets.get("dqx") == "true",  # noqa: F821
)
print(json.dumps(report, indent=2))
if report["status"] != "passed":
    raise RuntimeError(f"Comparison failed; evidence: {report['evidence_path']}")
