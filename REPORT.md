PRODUCT: KVFlow - Universal Agent Development Workflow
VERSION: 0.1.0
STATUS: PARTIAL (product complete and verified; web leg proven by a source-level check, not by a web test runner)

WHAT IT IS
- One product with one durable task state: registry -> templates -> plan -> dispatch ->
  worker execution -> manager review -> integration -> knowledge -> report.
- Core is free of KVStock/ETF/Forward/Golden/Tushare/quant/user-name/desktop-path/
  port/model-name hardcoding; it starts and finishes a task with no KVStock present.

DELIVERED
- Core (src/kvflow/core/*): contracts, state machine, store, budget chain GLOBAL->PROJECT->
  JOB, capability tickets, path policy, snapshots/worktrees, scheduler, knowledge, tools,
  MCP transport, web UI, backup/restore.
- Product layer: registry + declarative profiles, 4 templates (feature, bugfix, refactor,
  docs_or_data), model profiles deepseek_only + hybrid, planner with validation, workflow
  runner with parallel waves, bounded manager-directed rework, result/status/control.
- Entrances: DSH plugin (10 tools + /flow + guidance), MCP stdio server, standalone CLI.
- Lifecycle: kvflow install/upgrade/uninstall (profile bundle patch + cordis row, backup,
  rollback, host-compose verification), project onboard/show/doctor/rebind/remove,
  backup/restore/verify.
- Optional adapters: deny-by-default, read-only KVStock adapter with explicit opt-in.

EVIDENCE (receipts in kvflow/.runtime/receipts/)
- tests: 316 passed (kvflow/tests).
- kvflow-cross-project-e2e.json: python_project PASS, docs_project PASS, isolation PASS,
  parallel_three PASS (3 real overlapping workers), web_project PARTIAL.
- dsh-plugin-acceptance.json: PASS - installed by `kvflow install` (verified, host composed
  the patch); fresh headless DSH boot called the plugin tools; job_e874cd7a2df642408df8
  PASS, node impl_verify WORKER_COMPLETE, profile receipt exit 0, review APPROVE, 7 live calls.
- host-install.json / host-upgrade.json: install, upgrade and uninstall round trips against a
  real DSH profile, each with a backup; a profile the host cannot compose is rolled back.
- kvflow-protection.json: PASS - 6 protected trees of the original KVStock product,
  20,401 files, unchanged after an adapter read; writes into protected roots refused by the
  product's own config validation and path policy.
- kvflow-mcp-e2e.json: MCP stdio PASS (live manager plan, APPROVE, integration applied).

KNOWN LIMITATIONS (not hidden)
- web_project is PARTIAL: no node/deno/bun exists on this machine, so the registered node
  test runner cannot execute. The leg uses a shipped source-level web checker
  (kvflow/checks/webcheck.py) and the receipt says so; the live worker refused to claim a
  passing test it could not run (status BLOCKED, no fabricated receipt).
- Manager and reviewer share one model configuration in deepseek_only; that is not an
  independent third-party audit.
- parallel_three uses a scripted plan with real workers (recorded as
  scripted_plan_real_workers).
- The v1 KVStock Agent OS release stays frozen and separate; KVFlow only reads it through
  the optional adapter.
- Cumulative model spend for this work is a few yuan, far below the 100 CNY cap.
