# KVFlow — universal agent development workflow

KVFlow turns an ordinary project directory into a project an agent team can work
on: it plans, dispatches, implements, tests, reviews and integrates, with the
durable state, budget, permissions, recovery and provenance of the Agent OS core
it was extracted from.

Install once, choose a project, describe the requirement, watch the work land.
The product is project-independent: the core runs in a clean environment with no
other product's source, database, environment variables or data.

## Status (honest)

| Piece | State |
|---|---|
| Generic core (state machine, SQLite store, leases, budget ledger, path and capability security, scheduler, knowledge, executor profiles, backup/restore) | **done** — extracted from the Agent OS v1.0 release |
| Project registry, declarative project config, onboarding and diagnostics | **done** — `kvflow project onboard/show/doctor/rebind/remove` |
| Four workflow templates (`feature`, `bugfix`, `refactor`, `docs_or_data`) | **done** — schema-validated data, no eval |
| Model profiles (`deepseek_only`, `hybrid`) and budget profiles | **done** — `kvflow profile list/show` |
| Fixed-argv execution profiles (web `node --test`, artifact checker) | **done** — allowlisted executables (a profile may pin an absolute path), list execution, no shell string |
| Workflow runner (`kvflow run`) | **done** — parallel waves, bounded manager-directed rework, integration + review; `kvflow runs/result/status` |
| Semantic coordination (contracts, ownership, decisions, artifacts, idempotency, staleness, requirement traceability, invariants, final gate) | **done** — `kvflow coordinate show/gate`, receipts in `.runtime/receipts/kvflow-semantic-e2e.json` |
| MCP server surface (`kvflow-mcp`) | **done** — stdio server over the shared tool table |
| DSH native plugin (10 tools + `/flow`, installed through the profile bundle patch) | **done** — `kvflow install/upgrade/uninstall`, verified against a fresh headless boot |
| Cross-project acceptance E2E (Python, Web/TS, docs/data) | **done** — every leg PASS, `kvflow-cross-project-e2e.json` |
| Install / upgrade / uninstall with backup / restore | **done** — host profile patch with rollback, plus `kvflow backup/restore/verify` |
| KVStock as an optional, explicitly enabled read-only adapter | **done** — deny-by-default; protection proof over 6 frozen trees |
| Test suite | **done** — see `TESTS` in `REPORT.md` for the current count |


## Quick start

```bash
python -m venv .venv && . .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -e .[dev]

kvflow --home <runtime dir> doctor                        # environment check
kvflow --home <runtime dir> template list                 # four workflow templates
kvflow --home <runtime dir> profile list                  # model + budget profiles
kvflow --home <runtime dir> project onboard --path <dir>  # draft a scope, read it
kvflow --home <runtime dir> project onboard --path <dir> --write   # approve it
kvflow --home <runtime dir> project doctor                # registry health
kvflow --home <runtime dir> plan "add rate limiting" --project <project_id>
kvflow --home <runtime dir> run  "add rate limiting" --project <project_id>
kvflow --home <runtime dir> runs --limit 10                    # recent runs
kvflow --home <runtime dir> result <job_id>                    # every piece of evidence
kvflow --home <runtime dir> coordinate show --project <id>      # semantic state
kvflow --home <runtime dir> coordinate gate --project <id> --job <job_id>
kvflow host status --profile desktop                            # DSH host integration
kvflow install --profile desktop --python <py> --python-path <src>
```

`--home` owns the durable store (one database, `kvflow.sqlite3`, shared by the CLI,
the MCP server, the host plugin and the background runner), the managed workspaces,
the registry index and the budgets. A project keeps only its small
`.kvflow/project.json`.

The user flow stays this small: open DSH → pick a project → `/flow <requirement>` (or
`kvflow run`) → read the result. Contract versions, artifact revisions, idempotency
keys, the operation ledger and the decision graph are managed inside KVFlow; the user
is only interrupted for a genuine product conflict, an external permission, a budget
decision or an unknown side effect.


## Layout

```
src/kvflow/core/     the extracted, project-independent orchestration core
src/kvflow/          registry, templates, model/budget profiles, workflow, CLI, MCP
src/kvflow/coordination.py  semantic contracts, canonical ownership, decisions,
                     artifacts, the operation ledger, staleness and the final gate
src/kvflow/adapters/ optional, explicitly enabled domain adapters (KVStock is one)
src/kvflow/checks/   deterministic evidence producers that ship with the product
tests/core/          the Agent OS v1.0 suite, unchanged in substance
tools/               the extraction scripts and the acceptance harnesses
```

## Fail closed

A critical result that cannot be proven safe is reported, never assumed: `BLOCKED`,
`CONFLICT`, `STALE_RESULT`, `OUTCOME_UNKNOWN`, `VALIDATION_FAILED` and `PARTIAL` are
real outcomes, and a manager's approval cannot turn a failed program gate into DONE.
The gate itself lives in `kvflow.coordination.SemanticCoordinator.final_gate`.


## Trust boundary

* A workspace copy is change isolation, not an OS sandbox. Untrusted repositories
  are refused unattended execution; the product states `TRUSTED_PROJECTS_ONLY`.
* Only the worker role may change files; the manager plans and reviews.
* Every capability ticket is bound to one project, job, node, run, attempt, fence
  and lease; a model cannot obtain a role by asking for it.
* Budgets are integer micro-CNY with hard ceilings and atomic reservations.
* Detected files (`package.json`, `pyproject.toml`, …) are evidence about a
  project, never an authorization.
