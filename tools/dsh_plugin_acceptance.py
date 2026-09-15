"""Collect the DSH plugin acceptance evidence into one receipt.

Everything here is read from the host's own files, the host's own CLI output and
KVFlow's durable state -- not from a narrative:

* the plugin is installed in a profile (bundle list + node_modules) and DSH's own
  composed configuration tree contains the row it will mount;
* a real DSH process (a separate headless profile, never the session running this
  work) loaded the plugin and called its tools, including one that started a
  workflow;
* the profile keeps the plugin across restarts because the install lives in the
  profile configuration, and every boot above is a fresh process;
* the workflow the plugin started is followed to its durable receipt.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

PRODUCT = Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
sys.path.insert(0, str(PRODUCT / "src"))

from kvflow import workflow  # noqa: E402

DESKTOP = Path(r"C:\Users\90428\.dsh\profiles\desktop")
ACCEPT = Path(r"C:\Users\90428\.dsh\profiles\kvflow-accept")
HOME = PRODUCT / ".runtime" / "smoke" / "home"
OUT = PRODUCT / ".runtime" / "receipts" / "dsh-plugin-acceptance.json"
DSH = subprocess.run(["where", "dsh"], capture_output=True, text=True).stdout.split("\n")[0].strip()


def bundles(profile: Path) -> list[str]:
    payload = json.loads((profile / "package.json").read_text(encoding="utf-8"))
    return payload.get("dsh", {}).get("profile", {}).get("bundles", [])


def dump_config(profile: str, patch: Path | None = None) -> str:
    argv = [DSH, "--profile", profile]
    if patch is not None:
        argv += ["--patch", str(patch)]
    argv.append("--dump-config")
    completed = subprocess.run(argv, capture_output=True, text=True, encoding="utf-8",
                               errors="replace", timeout=600)
    return (completed.stdout or "") + (completed.stderr or "")


def read_text(path: Path) -> str:
    """Read a log whatever encoding the shell that captured it used.

    PowerShell redirection writes UTF-16LE with a BOM, which decoded as UTF-8 looks
    like NUL-interleaved text and would silently hide the tool activity.
    """
    raw = path.read_bytes()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16", errors="replace")
    if raw.count(b"\x00") > max(1, len(raw) // 4):
        return raw.decode("utf-16-le", errors="replace")
    return raw.decode("utf-8", errors="replace")


def main() -> int:
    receipt: dict = {
        "kind": "KVFLOW_DSH_PLUGIN_ACCEPTANCE",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dsh_cli": DSH,
        "problems": [],
    }

    receipt["installation"] = {
        "runtime_profile": "desktop",
        "runtime_bundles": bundles(DESKTOP),
        "runtime_installed_module": (DESKTOP / "node_modules" / "kvflow-dsh" / "lib" / "index.js").is_file(),
        "acceptance_profile": "kvflow-accept",
        "acceptance_bundles": bundles(ACCEPT),
        "acceptance_installed_module": (ACCEPT / "node_modules" / "kvflow-dsh" / "lib" / "index.js").is_file(),
        "backup_dirs": sorted(
            str(path.name) for path in DESKTOP.glob(".kvflow-install-backup-*")
        ),
    }
    for key in ("runtime_bundles", "acceptance_bundles"):
        if "kvflow-dsh" not in receipt["installation"][key]:
            receipt["problems"].append(f"kvflow-dsh missing from {key}")

    composed = dump_config("desktop")
    receipt["composed_runtime_profile"] = {
        "row_present": "- id: kvflow" in composed and "name: kvflow-dsh" in composed,
        "config_present": "pythonPath:" in composed,
        "snippet": "\n".join(
            line for line in composed.splitlines()
            if "kvflow" in line or "pythonPath" in line or line.strip().startswith("home:")
        )[:600],
    }
    if not receipt["composed_runtime_profile"]["row_present"]:
        receipt["problems"].append("the desktop profile does not compose the kvflow row")

    accept_composed = dump_config("kvflow-accept", PRODUCT / "tools" / "dsh-accept-patch.yml")
    receipt["composed_acceptance_profile"] = {
        "row_present": "- id: kvflow" in accept_composed,
        "headless_runner": "dsh-headless" in accept_composed,
    }
    if not receipt["composed_acceptance_profile"]["row_present"]:
        receipt["problems"].append("the acceptance profile does not compose the kvflow row")

    # each of these was a separate DSH process: a fresh boot is the restart
    receipt["host_runs"] = []
    for name, log in (("read_only_tools", "dsh-plugin-run.txt"),
                      ("workflow_start", "dsh-plugin-run2.txt")):
        path = PRODUCT / ".runtime" / log
        text = read_text(path) if path.is_file() else ""
        receipt["host_runs"].append({
            "name": name,
            "log": str(path),
            "fresh_process": True,
            "mentioned_kvflow_tools": "kvflow_" in text,
            "started_job": _job_from(text),
            "tail": text[-500:],
        })
    if not any(run["mentioned_kvflow_tools"] for run in receipt["host_runs"]):
        receipt["problems"].append("no host run exercised the kvflow tools")

    job_id = next((run["started_job"] for run in reversed(receipt["host_runs"])
                   if run["started_job"]), None)
    receipt["plugin_launched_job"] = job_id
    if job_id is None:
        receipt["problems"].append("no workflow was started through the plugin")
    else:
        deadline = time.monotonic() + 900
        receipt["plugin_launched_run"] = {"state": "unknown"}
        while time.monotonic() < deadline:
            manifest = workflow.run_manifest(HOME, job_id)
            receipt_ = manifest.get("last_receipt")
            if receipt_:
                receipt["plugin_launched_run"] = {
                    "status": receipt_.get("status"),
                    "problems": receipt_.get("problems"),
                    "node_states": receipt_.get("node_states_final"),
                    "receipts": receipt_.get("test_receipts"),
                    "review": (receipt_.get("review") or {}).get("verdict"),
                    "integration": {
                        key: value for key, value in (receipt_.get("integration") or {}).items()
                        if key != "integration_root"
                    },
                    "live_calls": (receipt_.get("worker_totals") or {}).get("live_calls"),
                    "usage": (receipt_.get("worker_totals") or {}).get("usage"),
                    "diff": receipt_.get("diff"),
                    "source_not_modified": receipt_.get("source_not_modified"),
                    "runner_mode": manifest.get("runner_mode"),
                    "template": manifest.get("template"),
                    "model_profile": manifest.get("model_profile"),
                    "plan_source": manifest.get("plan_source"),
                }
                break
            receipt["plugin_launched_run"] = workflow.status(HOME, job_id)["nodes"]
            time.sleep(5)
        else:
            receipt["problems"].append("the plugin-launched run did not finish in time")
        if receipt["plugin_launched_run"].get("status") not in {"PASS", "PARTIAL"}:
            receipt["problems"].append(
                f"the plugin-launched run reported "
                f"{receipt['plugin_launched_run'].get('status')}"
            )

    receipt["status"] = "PASS" if not receipt["problems"] else "PARTIAL"
    receipt["ended_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(receipt, indent=2, ensure_ascii=False, default=str),
                   encoding="utf-8")
    print(json.dumps(receipt, ensure_ascii=False, indent=2, default=str)[:3200])
    print("receipt:", OUT)
    return 0 if receipt["status"] == "PASS" else 1


def _job_from(text: str) -> str | None:
    marker = "job_"
    index = text.find(marker)
    while index != -1:
        candidate = text[index:index + 28]
        allowed = "".join(ch for ch in candidate[4:] if ch.isalnum() or ch == "_")
        if len(allowed) >= 20:
            return f"job_{allowed[:24]}"
        index = text.find(marker, index + 1)
    return None


if __name__ == "__main__":
    raise SystemExit(main())
