PRODUCT: KVFlow - Universal Agent Development Workflow
VERSION: 0.1.0
STATUS: PASS

SCOPE
- One authoritative durable task state: registry -> templates -> plan -> dispatch -> worker
  execution -> manager review -> integration -> knowledge -> report.
- The core starts and finishes a task with no KVStock source, database, environment or data
  present, and carries no product/quant/path/port/model-name hardcoding.

DELIVERED
- Core (src/kvflow/core/*): contracts, state machine, store, GLOBAL->PROJECT->JOB budget
  chain, capability tickets, path policy, snapshots/worktrees, scheduler, knowledge, tools,
  MCP transport, web UI, backup/restore.
- Product layer: declarative registry (profiles, scopes, protection), 4 validated templates
  (feature, bugfix, refactor, docs_or_data), model profiles deepseek_only + hybrid, plan
  compiler with validation, workflow runner with parallel waves, bounded manager-directed
  rework plus settlement of nodes left without an outcome.
- Entrances over that one state: DSH native plugin (10 tools + /flow + guidance), MCP stdio
  server, standalone CLI.
- Lifecycle: install/upgrade/uninstall on a real DSH profile (bundle patch + cordis row,
  backup, rollback, host-compose verification), project onboard/show/doctor/rebind/remove,
  backup/restore/verify.
- Adapters: deny-by-default opt-in registry; enabling grants reads only.

EVIDENCE (kvflow/.runtime/receipts/)
- tests: 322 passed (kvflow/tests).
- cross-project E2E PASS on every leg: python_project (live plan, APPROVE, receipt exit 0),
  docs_project (artifact profile exit 0, APPROVE, integrated), web_project (live plan; real
  `node --test` receipt exit 0 through the pinned bundled Node; APPROVE; src/app.mjs and
  test/dekebab.test.mjs integrated), isolation (no file or knowledge bleed between two
  projects), parallel_three (3 real overlapping workers plus a dependent node).
- dsh-plugin-acceptance: PASS - installed by `kvflow install` (verified, host composed the
  patch); a fresh headless DSH boot drove job_e874cd7a2df642408df8 to PASS (exit 0, APPROVE).
- refusal-recovery: PASS - protected root and traversal scope PATH_DENIED while the approved
  root stays writable; BUDGET_DENIED when a job cap cannot cover one call; recovery parks an
  interrupted node as FIX with 0 open runs and 0 jobs left active.
- protection: PASS - 6 protected trees of the original KVStock product, 20,401 files,
  unchanged after an adapter read; writes into protected roots refused by the product's own
  config validation and path policy.
- mcp-e2e: PASS - MCP stdio session with a live manager plan, APPROVE, integration applied.

KNOWN LIMITATIONS
- deepseek_only puts manager and reviewer on one model configuration: not an independent
  third-party audit.
- parallel_three uses a scripted plan with real workers (scripted_plan_real_workers); the
  other legs use live manager plans.
- The web leg passes because the approved profile pins this machine's bundled Node runtime
  (not on PATH) and uses `node --test` auto-discovery; on Node 22 the older
  `node --test test/` form fails with MODULE_NOT_FOUND even when the suite itself passes.
- Earlier web rounds in the ledger ended PARTIAL/BLOCKED with no test receipt claimed; the
  PASS run is a separate, later job.
- v1 KVStock Agent OS stays frozen and separate; its 4h soak stopped at cycle 3260 / 3.03h
  with sqlite_integrity ok and orphan_open_runs 0, and a continuation soak now runs
  (receipt soak-20260915T054018Z.jsonl).
- Cumulative model spend for this work stays far below the 100 CNY cap.
