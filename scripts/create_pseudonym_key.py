"""Create the pseudonymisation key in a Databricks secret scope, once.

patient_id = HMAC-SHA256(key, NHS number). The key is generated here and stored only in the
secret scope: it never appears in code, logs or git. It is NOT overwritten if it exists:
a new key would change every patient_id and break linkage across runs; key rotation is a
planned migration, not a side effect of re-running setup.

Usage: python scripts/create_pseudonym_key.py [--scope gbdemo] [--key pseudonym_key]
Needs the Databricks CLI on PATH (or DATABRICKS_CLI=<path to databricks.exe>).
"""
import argparse
import json
import os
import secrets
import subprocess

CLI = os.environ.get("DATABRICKS_CLI", "databricks")


def cli(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run([CLI, *args], capture_output=True, text=True, check=check)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", default="gbdemo")
    ap.add_argument("--key", default="pseudonym_key")
    a = ap.parse_args()

    scopes = json.loads(cli("secrets", "list-scopes", "-o", "json").stdout or "[]")
    if a.scope not in {s["name"] for s in scopes}:
        cli("secrets", "create-scope", a.scope)
        print(f"created secret scope {a.scope}")
    keys = json.loads(cli("secrets", "list-secrets", a.scope, "-o", "json").stdout or "[]")
    if a.key in {k["key"] for k in keys}:
        print(f"{a.scope}/{a.key} already exists: left unchanged (rotating it would change every patient_id)")
        return
    cli("secrets", "put-secret", a.scope, a.key, "--string-value", secrets.token_hex(32))
    print(f"created {a.scope}/{a.key} (value not shown)")


if __name__ == "__main__":
    main()
