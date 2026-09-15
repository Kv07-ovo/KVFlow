PRODUCT=KVFlow
VERSION=0.1.0
RELEASE_STATUS=PARTIAL

FINAL_RELEASE_COMMIT=(this commit)

SOAK_PROCESS_ISOLATION=PASS
- continuation soak pid 41892: root .agent_os_runtime/soak, db soak/agent_os.sqlite3, receipt soak-20260915T054018Z.jsonl, 1h run, finished on its own
- continuous soak pid 60404: root .agent_os_runtime/soak-4h-release, db soak-4h-release/agent_os.sqlite3, receipt soak-20260915T063207Z.jsonl
- different runtime home, different SQLite file, different workspace root, different receipt; neither touched the other's state

DURABILITY_4H=PARTIAL
LONGEST_CONTINUOUS_SOAK=4.0000h (14400.1s in one process: started 2026-09-15 14:32:07, ended 18:32:12 local; 3960 cycles)
TOTAL_OBSERVED_SOAK=4.86h = 3.03h + 1.00h continuation + 4.00h continuous (three separate processes; only the last is one continuous run)
SOAK_ERRORS=25 harness-counted, all in the [backup_restore] phase and none a product fault: 8 x ConflictError "the restore was refused" (the product correctly refusing a restore into a non-empty destination) + 17 x Windows OSError WinError 145 temp-directory races inside the harness's own rmtree. Product-level counters: lock_errors=0, contention_events=0, mcp_errors=0, web_errors=0.
- the runner's own verdict is PARTIAL because of that counter, and the release rule SOAK_ERRORS=0 is therefore not literally met
SQLITE_INTEGRITY=ok (final invariant and every sample)
FOREIGN_KEY_ERRORS=0 (foreign_keys ON in every connection; no violation surfaced)
ORPHAN_RUNS=0
MCP_SESSION_LEAK=0 (660 sessions opened, 660 closed, mcp_errors 0)
UNEXPECTED_OPEN_LEASES=0 (active_leases=6, all live at the cut; no stale lease after settle)
PENDING_BUDGET_RESERVATIONS=0 (reservations row count 0, reservation_states {})
MCP_SESSION_LEAK=0
RESOURCE_GROWTH=BOUNDED (RSS 74.3 -> 84.4 MB, peak 88.7 MB over 4h; DB 0.3 -> 46.4 MB; workspace root 0.3 -> 180.7 MB, both explained by 3300 jobs / 15453 runs / 72183 events)
- load mix: 14133 claims, 13284 completions, 189 failures, 660 cancels, 660 child restarts, 643 backups, 635 restores, 15740 web requests

FINAL_BACKUP_RESTORE=PASS
- taken from the real runtime homes after the soak and restored into empty directories: semantic fingerprint identical (3 contracts, 1 decision, 3 operations, 4 requirements, 1 invariant, 1 submission), task state identical (5 jobs, 7 receipts, 8 knowledge records)
- restore made 0 model calls, replayed 0 side effects, resumed 0 tasks

DSH_DAILY_PROFILE_RESTART=WAITING_FOR_SAFE_POINT
- 9 DSH processes are running and this very session is the active coding task with an armed goal; a restart would kill in-flight work, so nothing was force-killed
DSH_RESTART_PERSISTENCE=NOT_RUN_AFTER_DAILY_RESTART
- the daily profile is intact and composes (dependency + bundle + patch row present, `dsh --dump-config` composes the kvflow row; plugin bundle lib/index.js sha256 CC9078D0B9FE9A20B2C949A7A25DA0143F6C13A4B1E87E618EF99ACF9AB9C5BE, version 0.1.0)
DSH_READ_ONLY_TASK=NOT_RUN_AFTER_DAILY_RESTART
DSH_MANAGED_WRITE_TASK=NOT_RUN_AFTER_DAILY_RESTART (the earlier fresh-process acceptance did drive a managed write to PASS: job_e874cd7a2df642408df8)

FINAL_CROSS_PROJECT_E2E=PASS (single round, problems=[], on commit c9a1f249; no product code changed since)
FINAL_RELIABILITY_SMOKE=PASS (7/7)

MCP_E2E=PASS   PLUGIN_E2E=PASS   PROJECT_ISOLATION=PASS   PERMISSION_MODEL=PASS   BUDGET_ENFORCEMENT=PASS

PRODUCT_TEST_SUITE=351 passed (kvflow/tests, pytest)
RELEASE_GATE_CHECKS=release_gate_check.py (backup/restore of two homes), semantic_e2e.py (13/13, 7/7 smoke), dsh_plugin_acceptance.py, cross_project_e2e.py, refusal_recovery_e2e.py, kvstock_protection_proof.py - counted separately from pytest, never added into the test count

KVSTOCK_PROTECTED=PASS   GOLDEN_PROTECTED=PASS
FORWARD_PROTECTION_SCOPE=KNOWN_PATHS_ONLY (no Forward tree exists in this checkout; no disk scan performed)

KNOWN_LIMITATIONS
- DURABILITY_4H: the continuous 4-hour run completed, but the release rule SOAK_ERRORS=0 is not literally met (25 harness-counted errors; 8 product refusals that are correct behaviour, 17 Windows temp-dir races in the soak harness). The runner's own verdict is PARTIAL.
- The daily DSH profile was not restarted (no safe point while this task runs), so persistence, the read-only task and the managed-write task through the daily profile are unproven.
- FORWARD_PROTECTION_SCOPE stays KNOWN_PATHS_ONLY.
- deepseek_only puts manager and reviewer on one model configuration: INDEPENDENT_EXECUTION_PATH, not an independent third-party audit.

SUPPORTED_SCOPE=single machine, the measured Windows environment, trusted registered projects, the measured worker concurrency (3), the current provider configuration, the current DSH compatibility version

RECOMMENDATION=PARTIAL
Remaining blockers only:
1. Decide whether the 25 harness-counted soak errors (none a product fault) satisfy the gate; if a literal zero is required, the soak harness's backup/restore phase needs its temp-dir handling and its refusal accounting corrected, then one more continuous 4h run.
2. Restart the daily DSH profile at a safe point (no active coding task) and run the read-only and minimal managed-write tasks through it.