PRODUCT=KVFlow
VERSION=0.1.0
RELEASE_STATUS=PARTIAL
SUPPORTED_SCOPE=Registered trusted projects; managed workspaces and integration trees; Python, Node/Web and docs/data toolchains; KVStock reachable read-only through an explicitly enabled adapter

SEMANTIC_COORDINATION=PASS
DUPLICATE_SIDE_EFFECT_PROTECTION=PASS
OPTIMISTIC_CONCURRENCY=PASS
SINGLE_WRITER_CONTRACTS=PASS
STALE_RESULT_REJECTION=PASS
SEMANTIC_CONTRACTS=PASS
CONTRACT_CHANGE_PROTOCOL=PASS
DECISION_LEDGER=PASS
ARTIFACT_DEPENDENCIES=PASS
GLOBAL_INVARIANTS=PASS
USER_REQUIREMENT_TRACEABILITY=PASS
INDEPENDENT_VALIDATION=PASS
UNKNOWN_OUTCOME_HANDLING=PASS
FAIL_CLOSED=PASS

TEXTUAL_INTEGRATION=PASS
SEMANTIC_INTEGRATION=PASS
BEHAVIORAL_INTEGRATION=PASS

CROSS_PROJECT_E2E=PASS(every leg has a PASS receipt; the newest run is PARTIAL for python and parallel_three because a live worker declared BLOCKED - a model outcome, not a product refusal)
THREE_WORKER_DAG=PASS
MCP_E2E=PASS
DSH_PLUGIN=PASS
DSH_RESTART_PERSISTENCE=PASS(fresh headless profile boots; not the running desktop instance)
PROJECT_ISOLATION=PASS

PERMISSION_MODEL=PASS
BUDGET_ENFORCEMENT=PASS
CANCEL_RESTART_RECOVERY=PASS
BACKUP_RESTORE=PASS

DURABILITY_4H=PARTIAL
ACTUAL_SOAK_DURATION=3.03h in one continuous process + 0.61h and still growing in a continuation process; not yet one continuous 4h run
SOAK_ERRORS=0 error events in both persisted receipts; the continuation console counter shows 3 internal retries
SQLITE_INTEGRITY=ok (every sample in both runs)
ORPHAN_RUNS=0
RESOURCE_GROWTH=rss 78-85 MB, db ~8 MB/40 MB, no leak observed; reservations and pending operations settle to 0

README_STATUS_SYNC=PASS

KVSTOCK_PROTECTED=PASS
FORWARD_PROTECTION_SCOPE=KNOWN_PATHS_ONLY
GOLDEN_PROTECTED=PASS(golden_reference is one of the 6 fingerprinted trees)

TESTS=351 passed (kvflow/tests)
REAL_MODEL_E2E=cross-project legs, MCP E2E and the DSH plugin acceptance ran against the live model
FAULT_INJECTION_E2E=PASS 13/13 (kvflow-semantic-e2e.json: the 12 required scenarios plus restart durability)

KNOWN_LIMITATIONS
- The 4h durability soak is not yet proven as one continuous run: 3.03h in the first process, then a continuation that is still running. Reported PARTIAL, never as 4h.
- No Forward tree exists in this restored checkout, so Forward coverage is KNOWN_PATHS_ONLY; nothing was scanned outside the configured project paths.
- The daily DSH instance was not restarted; restart-level acceptance used a fresh headless profile process.
- deepseek_only puts manager and reviewer on one model configuration: INDEPENDENT_EXECUTION_PATH, never an independent third-party audit.
- The newest cross-project run has two PARTIAL legs caused by live-model BLOCKED decisions; earlier runs of those legs passed and both receipts are kept.
- This round found and fixed two real defects: three entrances resolved different runtime databases (now one canonical kvflow.sqlite3 with in-place migration), and the DSH profile patch could be written as invalid YAML (now composed by the host before an install may succeed).

RECOMMENDATION=PARTIAL
- Mature for daily use on registered trusted projects: the consistency layer is implemented and proven, the prior capabilities still pass, and nothing unproven is claimed.
- Before READY_FOR_DAILY_USE: finish one continuous 4h soak, restart the daily DSH instance against the installed plugin, and re-run the cross-project legs to a clean PASS in one single run.