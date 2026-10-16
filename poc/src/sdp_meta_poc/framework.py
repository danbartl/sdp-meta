"""Adapter contract for the separately installed governance compiler."""

import json


def compile_framework(root, settings):
    from lakeflow_governance.compiler.compiler import compile_repository

    for name, scd in (("current", 1), ("history", 2)):
        folder = root / "contracts" / name
        folder.mkdir(parents=True)
        contract = {
            "api_version": "governance.bank/v1alpha2",
            "kind": "Dataset",
            "metadata": {
                "name": name,
                "domain": "poc",
                "owner": "poc@example.invalid",
                "criticality": "high",
                "description": "Synthetic comparison",
            },
            "spec": {
                "pattern": f"auto_cdc_scd{scd}",
                "source": {"type": "change_feed", "table": "changes"},
                "target": {
                    "catalog": "${catalog}",
                    "schema": "${schema}",
                    "table": name,
                },
                "columns": [
                    {
                        "name": column,
                        "data_type": dtype,
                        "nullable": True,
                        "comment": f"Synthetic {column} for the comparison",
                    }
                    for column, dtype in [
                        ("account_id", "STRING"),
                        ("amount", "BIGINT"),
                        ("sequence", "BIGINT"),
                        ("operation", "STRING"),
                    ]
                ],
                "keys": ["account_id"],
                "changes": {
                    "business_sequence_column": "sequence",
                    "operation_column": "operation",
                    "delete_expression": "operation = 'DELETE'",
                },
            },
        }
        (folder / "dataset.yaml").write_text(json.dumps(contract), encoding="utf-8")
    return compile_repository(root / "contracts", root / "generated")
