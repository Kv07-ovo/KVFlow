PRODUCT=KVFlow
VERSION=0.1.0
RELEASE_STATUS=PARTIAL

FINAL_RELEASE_COMMIT=(this commit; see below)

DURABILITY_4H=PARTIAL
LONGEST_CONTINUOUS_SOAK=3.03h (one process, cycles to 3260, integrity ok, orphan 0)
TOTAL_OBSERVED_SOAK=3.89h = 3.03h + 0.86h continuation (second process, still running as pid 41892)
SOAK_ERRORS=0 error events in both persisted receipts; 3 internal retries counted by the continuation runner
SQLITE_INTEGRITY=ok on every sample of both runs
ORPHAN_RUNS=0
RESOURCE_GROWTH=BOUNDED (rss 79-86 MB flat, db 8-11 MB, root ~25 MB; reservations and pending operations settle to 0)
NOTE=a fresh continuous 4h soak was restarted at 14:32 local (pid 60404, root .agent_os_runtime/soak-4h-release; the first attempt failed because a relative --root produced a non-absolute project root and was restarted with an absolute path). It needs 4:00:00 of real time, so the gate item is not met in this session.

DSH_DAILY_PROFILE_RESTART=WAITING_FOR_SAFE_POINT
- 8 DSH processes are running and this very session is an active DSH coding task with an armed goal; restarting the daily instance would kill in-flight work, so it was not forced.
DSH_RESTART_PERSISTENCE=NOT_PROVEN_FOR_DAILY_PROFILE
- the installed profile is intact and composes: host status reports dependency+bundle+patch row present, `dsh --dump-config` composes the kvflow row (evidence: host-install.json, dsh-plugin-acceptance.json)
- restart-level proof exists only for a fresh headless profile process, not for the restarted daily desktop profile
DSH_READ_ONLY_TASK=NOT_RUN_AFTER_DAILY_RESTART (blocked by the item above)
DSH_MANAGED_WRITE_TASK=NOT_RUN_AFTER_DAILY_RESTART (the earlier headless acceptance did drive a managed write to PASS: job_e874cd7a2df642408df8, receipt exit 0, APPROVE, integration applied)

FINAL_CROSS_PROJECT_E2E=PASS (single round, same release candidate, problems=[])
PYTHON_E2E=PASS (impl_test WORKER_COMPLETE, APPROVE, 6 live calls)
WEB_E2E=PASS (real `node --test` exit 0 via the pinned bundled Node, APPROVE, 6 live calls)
DOCS_DATA_E2E=PASS (artifact profile exit 0, APPROVE, 17 live calls)
THREE_WORKER_E2E=PASS (alpha/beta/gamma real overlapping workers + dependent merge)
PROJECT_ISOLATION=PASS (no file, knowledge, artifact or budget bleed)

FINAL_RELIABILITY_SMOKE=PASS (7/7: CAS conflict, duplicate idempotency, stale result, semantic mismatch, failed requirement blocks DONE, unknown outcome without blind retry, single-writer protection)

BACKUP_RESTORE_FINAL=PASS (two homes round-tripped; restored semantic fingerprint identical: 3 contracts, 1 decision, 3 operations, 4 requirements, 1 invariant, 1 submission; 5 jobs and 10 receipts identical; no model call, no side effect replayed, no task resumed)
MCP_E2E=PASS (receipt from this candidate)
PLUGIN_E2E=PASS (fresh headless boot, verified install)
INSTALL_UPGRADE_UNINSTALL=PASS (install/upgrade with backup+rollback, uninstall restores the profile)

README_STATUS_SYNC=PASS (no "in progress"/"not started" rows remain)
DOC_COMMAND_VALIDATION=PASS (doctor, runs, profile list, template list, project list, mcp status, host status, coordinate show/gate, backup exercised against the real runtime)
NOTE=one mistyped PowerShell variable made a command use C:\Users\90428 as its runtime home, creating a stray kvflow.sqlite3 and workspace directory there; both were removed immediately and no product code was involved.

KVSTOCK_PROTECTED=PASS (6 trees, 20,401 files unchanged; writes refused)
GOLDEN_PROTECTED=PASS (golden_reference is one of the fingerprinted trees)
FORWARD_PROTECTION_SCOPE=KNOWN_PATHS_ONLY (no Forward tree exists in this restored checkout; no private-disk scan was performed)

TESTS=351 passed (kvflow/tests) at this code identity; the release-gate tooling added afterwards is not part of that count

KNOWN_LIMITATIONS
- DURABILITY_4H: no single continuous 4-hour run yet; the restarted soak needs real time.
- DSH daily-instance restart, and therefore the read-only and managed-write tasks through the restarted daily profile, are not proven for the daily instance (safe point not reached; nothing was force-killed).
- FORWARD_PROTECTION_SCOPE stays KNOWN_PATHS_ONLY.
- deepseek_only puts manager and reviewer on one model configuration: INDEPENDENT_EXECUTION_PATH, never an independent third-party audit.
- Live-model variance is real and kept in the ledger: earlier rounds had legs PARTIAL/BLOCKED; the final round above is a clean single-round PASS.

SUPPORTED_SCOPE=single machine, the measured Windows environment, trusted registered projects, the measured worker concurrency (3), the current provider configuration, the current DSH compatibility version

RECOMMENDATION=PARTIAL
Remaining real blockers, and nothing else:
1. Let the restarted continuous soak finish 4:00:00 and re-check errors/integrity/orphans/growth.
2. Restart the daily DSH profile at a safe point and re-run the read-only and managed-write tasks through it.