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
| Generic core (state machine, SQLite store, leases, budget ledger, path and capability security, scheduler, knowledge, executor profiles, backup/restore) | **done** — extracted from the Agent OS v1.0 release, `288` tests pass |
| Project registry, declarative project config, onboarding and diagnostics | **done** — `kvflow project onboard/show/doctor/rebind/remove` |
| Four workflow templates (`feature`, `bugfix`, `refactor`, `docs_or_data`) | **done** — schema-validated data, no eval |
| Model profiles (`deepseek_only`, `hybrid`) and budget profiles | **done** — `kvflow profile list/show` |
| Fixed-argv execution profiles (web `npm test`, artifact checker) | **done** — allowlisted executables, list execution, no shell string |
| Workflow runner (`kvflow run`) | in progress |
| MCP server surface (`kvflow-mcp`) | in progress |
| DSH native plugin | in progress |
| Cross-project acceptance E2E (Python, Web/TS, docs/data) | not started |

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
```

`--home` owns the durable store, the managed workspaces, the registry index and
the budgets. A project keeps only its small `.kvflow/project.json`.

## Layout

```
src/kvflow/core/     the extracted, project-independent orchestration core
src/kvflow/          registry, templates, model/budget profiles, workflow, CLI, MCP
src/kvflow/adapters/ optional, explicitly enabled domain adapters (KVStock is one)
src/kvflow/checks/   deterministic evidence producers that ship with the product
tests/core/          the Agent OS v1.0 suite, unchanged in substance
tools/               the extraction scripts, so the baseline can be reproduced
```

## Trust boundary

* A workspace copy is change isolation, not an OS sandbox. Untrusted repositories
  are refused unattended execution; the product states `TRUSTED_PROJECTS_ONLY`.
* Only the worker role may change files; the manager plans and reviews.
* Every capability ticket is bound to one project, job, node, run, attempt, fence
  and lease; a model cannot obtain a role by asking for it.
* Budgets are integer micro-CNY with hard ceilings and atomic reservations.
* Detected files (`package.json`, `pyproject.toml`, …) are evidence about a
  project, never an authorization.
