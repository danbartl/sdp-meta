import argparse
import ast
import hashlib
import json
import subprocess
import sys
import uuid
from pathlib import Path

import yaml

from .config import UPSTREAM_COMMIT, Settings, bundle_resources
from .local import run_local


def capabilities(repo):
    source = repo / "src/databricks/labs/sdp_meta/dataflow_pipeline.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    calls = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "create_auto_cdc_flow"
    ]
    arguments = [sorted(k.arg for k in call.keywords if k.arg) for call in calls]
    return {
        "scope": "static_source_inspection",
        "native_status": "not_run",
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "cdc_call_arguments": arguments,
        "bitemporal_system_sequence_forwarded": bool(calls)
        and all("system_sequence_by" in names for names in arguments),
        "note": "Absence is an integration gap. Presence alone does not prove native support.",
    }


def build(poc):
    repo = poc.parent
    vendor = poc / "vendor"
    vendor.mkdir(exist_ok=True)
    for wheel in vendor.glob("*.whl"):
        raise ValueError(
            f"vendor already contains {wheel.name}; use a fresh checkout for a new build"
        )
    for project in (repo, poc):
        subprocess.run(
            [sys.executable, "-m", "build", "--wheel", "--outdir", str(vendor)],
            cwd=project,
            check=True,
        )
    status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=repo, check=True, capture_output=True, text=True
    ).stdout
    manifest = {
        "upstream_commit": UPSTREAM_COMMIT,
        "checkout_commit": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo, check=True, capture_output=True, text=True
        ).stdout.strip(),
        "dirty": bool(status),
        "wheels": {
            w.name: hashlib.sha256(w.read_bytes()).hexdigest() for w in vendor.glob("*.whl")
        },
        "capabilities": capabilities(repo),
    }
    (vendor / "build-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    local = commands.add_parser("local")
    local.add_argument("--output", type=Path)
    commands.add_parser("inspect")
    commands.add_parser("build")
    commands.add_parser("validate-offline")
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--catalog", required=True)
    prepare.add_argument("--namespace", default=f"poc_{uuid.uuid4().hex[:12]}")
    prepare.add_argument("--framework-wheel")
    prepare.add_argument("--dqx", action="store_true")
    args = parser.parse_args()
    poc = Path.cwd()
    if not (poc / "databricks.yml").is_file() or not (poc / "pyproject.toml").is_file():
        parser.error("run this command from the poc directory")
    if args.command == "local":
        path = args.output or poc / ".state" / uuid.uuid4().hex
        result = run_local(path)
        print(json.dumps({"report": str(path / "report.json"), **result}, indent=2))
    elif args.command == "inspect":
        print(json.dumps(capabilities(poc.parent), indent=2))
    elif args.command == "build":
        print(json.dumps(build(poc), indent=2))
    elif args.command == "validate-offline":
        import jsonschema
        import regex

        schema = json.loads(
            subprocess.run(
                ["databricks", "bundle", "schema"],
                capture_output=True,
                encoding="utf-8",
                check=True,
            ).stdout
        )
        configuration = yaml.safe_load((poc / "databricks.yml").read_text())
        configuration.update(yaml.safe_load((poc / "resources.generated.yml").read_text()))

        # The CLI schema uses Unicode properties (Go/ECMA syntax), absent from Python re.
        def unicode_pattern(validator, pattern, instance, subschema):
            if isinstance(instance, str) and not regex.search(pattern, instance):
                yield jsonschema.ValidationError(f"{instance!r} does not match {pattern!r}")

        validator = jsonschema.validators.extend(
            jsonschema.Draft202012Validator,
            {"pattern": unicode_pattern},
        )
        validator(schema).validate(configuration)
        print("Bundle JSON schema passed. Workspace validation and native execution: NOT RUN.")
    else:
        settings = Settings(args.catalog, args.namespace)
        resources = bundle_resources(settings, args.framework_wheel, args.dqx)
        (poc / "resources.generated.yml").write_text(yaml.safe_dump(resources, sort_keys=False))
        print(f"Prepared {settings.namespace}; run bundle validate, deploy, then run compare.")


if __name__ == "__main__":
    main()
