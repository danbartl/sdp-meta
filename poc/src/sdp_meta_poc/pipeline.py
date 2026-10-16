"""Real Lakeflow entry points. Imported only inside the Databricks pipeline."""

import json
import runpy
import tempfile
from pathlib import Path

from .cases import DDL, VALID_SQL
from .config import Settings


def initialize(spark):
    from pyspark import pipelines as dp
    from pyspark.sql import functions as F

    settings = Settings(spark.conf.get("poc.catalog"), spark.conf.get("poc.namespace"))
    control = json.loads((Path(settings.volume) / "conf/control.json").read_text())
    arm = spark.conf.get("poc.arm")

    def enrich(frame, _spec=None):
        if control["fault"]:
            frame = frame.withColumn(
                "amount",
                F.when(F.col("event_id") == "f60", F.raise_error("POC_INJECTED_FAILURE")).otherwise(
                    F.col("amount")
                ),
            )
        frame = frame.withColumn("source_file", F.col("source_metadata.file_path"))
        if control["dqx"]:
            from databricks.labs.dqx.engine import DQEngine
            from databricks.sdk import WorkspaceClient

            engine = DQEngine(WorkspaceClient())
            frame = engine.apply_checks_by_metadata(
                frame,
                [
                    {
                        "name": "operation_known",
                        "criticality": "error",
                        "check": {
                            "function": "is_in_list",
                            "arguments": {
                                "column": "operation",
                                "allowed": ["UPSERT", "DELETE"],
                            },
                        },
                    }
                ],
            ).withColumn("poc_dqx_invalid", F.col("_errors").isNotNull())
        return frame

    if arm == "sdp":
        from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline

        DataflowPipeline.invoke_dlt_pipeline(
            spark,
            "bronze_silver",
            bronze_custom_transform_func=enrich,
        )
        return

    expression = VALID_SQL
    if control["maximum"] is not None:
        expression += f" AND (operation = 'DELETE' OR amount <= {int(control['maximum'])})"

    @dp.temporary_view(name="raw_events")
    def raw_events():
        frame = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaEvolutionMode", "none")
            .schema(DDL)
            .load(f"{settings.volume}/landing")
            .withColumn("source_metadata", F.col("_metadata"))
        )
        return enrich(frame)

    @dp.table(name="events")
    @dp.expect_all_or_drop({"valid_event": expression})
    def events():
        return spark.readStream.table("raw_events")

    @dp.table(name="quarantine")
    def quarantine():
        return spark.readStream.table("raw_events").filter(
            ~F.coalesce(F.expr(expression), F.lit(False))
        )

    @dp.temporary_view(name="changes")
    def changes():
        return spark.readStream.table("events").select(
            "account_id", "amount", "sequence", "operation"
        )

    if arm == "framework":
        # Exercise the installed compiler/runtime; no private implementation is vendored here.
        from .framework import compile_framework

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = compile_framework(root, settings)
            runpy.run_path(str(result.pipeline_path))
        return

    for target, scd in (("current", 1), ("history", 2)):
        dp.create_streaming_table(target)
        dp.create_auto_cdc_flow(
            target=target,
            source="changes",
            keys=["account_id"],
            sequence_by="sequence",
            stored_as_scd_type=scd,
            apply_as_deletes=F.expr("operation = 'DELETE'"),
            except_column_list=["sequence", "operation"],
        )
