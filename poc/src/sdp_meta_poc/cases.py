"""Small explicit inputs and hand-authored expectations, not engine-derived goldens."""

import polars as pl

SCHEMA = {
    "event_id": pl.String,
    "account_id": pl.String,
    "amount": pl.Int64,
    "sequence": pl.Int64,
    "operation": pl.String,
}
DDL = "event_id STRING, account_id STRING, amount BIGINT, sequence BIGINT, operation STRING"
PAYLOAD = ["account_id", "amount"]
HISTORY = [*PAYLOAD, "__START_AT", "__END_AT"]
VALID_SQL = (
    "event_id IS NOT NULL AND account_id IS NOT NULL AND account_id <> '' "
    "AND sequence IS NOT NULL AND sequence >= 0 "
    "AND operation IN ('UPSERT', 'DELETE') "
    "AND (operation = 'DELETE' OR (amount IS NOT NULL AND amount >= 0))"
)

# Amounts are integer minor units. Decimal and timezone coverage is a separate qualification.
BATCHES = {
    "initial": [
        ("a10", "A", 100, 10, "UPSERT"),
        ("b10", "B", 200, 10, "UPSERT"),
        ("bad_negative", "X", -1, 10, "UPSERT"),
        ("bad_key", None, 100, 10, "UPSERT"),
    ],
    "incremental": [
        ("a30", "A", 120, 30, "UPSERT"),
        ("b40", "B", None, 40, "DELETE"),
        ("c20", "C", 300, 20, "UPSERT"),
        ("a30", "A", 120, 30, "UPSERT"),
        ("bad_operation", "X", 1, 20, "OTHER"),
    ],
    "late": [
        ("a20", "A", 110, 20, "UPSERT"),
        ("b30", "B", 250, 30, "UPSERT"),
        ("d50", "D", 400, 50, "UPSERT"),
        ("bad_null", "E", None, 20, "UPSERT"),
    ],
    "recovery": [("f60", "F", 500, 60, "UPSERT")],
    "rule_change": [("g70", "G", 700, 70, "UPSERT")],
}

EXPECTED = {
    "initial": {
        "current": [("A", 100), ("B", 200)],
        "history": [("A", 100, 10, None), ("B", 200, 10, None)],
    },
    "incremental": {
        "current": [("A", 120), ("C", 300)],
        "history": [
            ("A", 100, 10, 30),
            ("A", 120, 30, None),
            ("B", 200, 10, 40),
            ("C", 300, 20, None),
        ],
    },
    "late": {
        "current": [("A", 120), ("C", 300), ("D", 400)],
        "history": [
            ("A", 100, 10, 20),
            ("A", 110, 20, 30),
            ("A", 120, 30, None),
            ("B", 200, 10, 30),
            ("B", 250, 30, 40),
            ("C", 300, 20, None),
            ("D", 400, 50, None),
        ],
    },
}
EXPECTED["recovery"] = {
    "current": [*EXPECTED["late"]["current"], ("F", 500)],
    "history": [*EXPECTED["late"]["history"], ("F", 500, 60, None)],
}


def batch(name):
    return pl.DataFrame(BATCHES[name], schema=SCHEMA, orient="row")


def expected(name, kind):
    schema = {key: SCHEMA[key] for key in PAYLOAD}
    if kind == "history":
        schema.update({"__START_AT": pl.Int64, "__END_AT": pl.Int64})
    return pl.DataFrame(EXPECTED[name][kind], schema=schema, orient="row")
