"""Workflow templates: declarative, schema-validated, never executable text.

A template is data. It names stages, the role that performs each one, the tools
that stage may use, the registered profile that proves it and what artifact it
must produce. Nothing in a template is evaluated as code and a template cannot
grant a permission: the tool list is intersected with the project's authorized
actions before any capability is issued.

Four templates ship with the product:

``feature``      understand -> inspect -> design -> implement -> test -> review
``bugfix``       reproduce -> locate -> minimal fix -> regression -> review
``refactor``     behaviour baseline -> bounded change -> equivalence -> review
``docs_or_data`` understand input -> produce -> check facts/artifacts

The product's planner turns the stages into a DAG; the core scheduler runs it.
A lightweight task does not have to activate three workers or write an
architecture document: the template's ``scale`` decides the shape, and the
manager may pick a smaller pre-approved template for a small request.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Literal

from .core.contracts import Action, Contract, Id, StrictBool, StrictInt, Text
from .core.errors import ConfigError, NotFoundError

TEMPLATE_DIR = "templates"


class Stage(Contract):
    """One worker-executable node of a template."""

    id: Id
    title: Text
    objective: Text
    tools: list[Action]
    write: StrictBool
    test_profile: Id | None = None
    produces: Text
    depends_on: list[Id] = []


class WorkflowTemplate(Contract):
    """A pre-approved workflow shape, validated before it can be used."""

    id: Id
    title: Text
    intent: Text
    #: who turns the request into a DAG: the manager, or the deterministic planner
    plan: Literal["manager", "deterministic"]
    #: who judges the evidence at the end
    review: Literal["manager", "none"]
    stages: list[Stage]
    acceptance: list[Text]
    scale: Literal["small", "standard", "large"]
    max_parallel_workers: StrictInt
    notes: Text

    def model_post_init(self, __context: object) -> None:  # noqa: D105 - pydantic hook
        ids = [stage.id for stage in self.stages]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate stage id")
        if not 1 <= int(self.max_parallel_workers) <= 3:
            raise ValueError("max_parallel_workers must be between 1 and 3")
        known = set(ids)
        for stage in self.stages:
            unknown = set(stage.depends_on) - known
            if unknown:
                raise ValueError(f"stage {stage.id} depends on unknown stages {sorted(unknown)}")
        remaining = {stage.id: set(stage.depends_on) for stage in self.stages}
        while remaining:
            ready = {key for key, deps in remaining.items() if not deps}
            if not ready:
                raise ValueError("cyclic stage dependencies")
            remaining = {
                key: deps - ready for key, deps in remaining.items() if key not in ready
            }


def _templates() -> dict[str, WorkflowTemplate]:
    feature = WorkflowTemplate(
        id="feature",
        title="New feature",
        intent="Understand the request, inspect the current code, implement, prove it, review it.",
        plan="manager",
        review="manager",
        scale="standard",
        max_parallel_workers=3,
        notes=(
            "The manager may split the request into independent bounded sub-tasks when the"
            " code really allows it; a single-node plan is a valid answer for a small change."
        ),
        acceptance=[
            "the requested behaviour exists in the changed files",
            "an approved execution profile exits zero after the change",
            "the manager approves the changed content digest",
        ],
        stages=[
            Stage(
                id="survey",
                title="Understand and inspect",
                objective=(
                    "Read the registered project's relevant files and report the current"
                    " behaviour, the constraints that apply and the exact files a change"
                    " would touch. Change nothing.\n\nRequest: {requirement}"
                ),
                tools=["repo.read", "repo.list", "repo.search", "repo.status"],
                write=False,
                produces="a sourced description of the current behaviour and the files at stake",
            ),
            Stage(
                id="implement",
                title="Implement the change",
                objective=(
                    "Implement the requested behaviour with the smallest coherent change."
                    " Run the registered profile and report its real exit code.\n\n"
                    "Request: {requirement}"
                ),
                tools=["repo.read", "repo.list", "repo.search", "repo.status", "repo.diff",
                       "repo.patch", "test.run_profile", "operation.status"],
                write=True,
                test_profile="test",
                depends_on=["survey"],
                produces="a patch inside the declared write scopes and an executor receipt",
            ),
            Stage(
                id="verify",
                title="Independent verification",
                objective=(
                    "Re-run the registered profile against the current workspace state and"
                    " report every failure you can find, with the real receipt."
                ),
                tools=["repo.read", "repo.list", "repo.status", "repo.diff",
                       "test.run_profile", "operation.status"],
                write=False,
                test_profile="test",
                depends_on=["implement"],
                produces="a verification receipt that names its profile and exit code",
            ),
        ],
    )

    bugfix = WorkflowTemplate(
        id="bugfix",
        title="Bug fix",
        intent="Reproduce the defect, locate it, fix it minimally, prove no regression.",
        plan="manager",
        review="manager",
        scale="small",
        max_parallel_workers=2,
        notes="Reproduction comes before any edit; a fix without a reproduction is not accepted.",
        acceptance=[
            "the defect is reproduced by an executor profile before the change",
            "the minimal change makes that profile exit zero",
            "the same profile still exits zero afterwards",
        ],
        stages=[
            Stage(
                id="reproduce",
                title="Reproduce",
                objective=(
                    "Reproduce the reported defect with the registered profile and report the"
                    " exact failure output and the files involved.\n\nDefect: {requirement}"
                ),
                tools=["repo.read", "repo.list", "repo.search", "repo.status",
                       "test.run_profile", "operation.status"],
                write=False,
                test_profile="test",
                produces="a failing executor receipt that reproduces the defect",
            ),
            Stage(
                id="fix",
                title="Minimal fix",
                objective=(
                    "Make the smallest change that removes the reproduced failure. Do not"
                    " refactor unrelated code. Run the profile and report the real result."
                ),
                tools=["repo.read", "repo.list", "repo.search", "repo.status", "repo.diff",
                       "repo.patch", "test.run_profile", "operation.status"],
                write=True,
                test_profile="test",
                depends_on=["reproduce"],
                produces="a minimal patch and a passing executor receipt",
            ),
            Stage(
                id="regression",
                title="Regression check",
                objective=(
                    "Run the registered profile again on the resulting workspace and report"
                    " whether anything else changed behaviour."
                ),
                tools=["repo.read", "repo.list", "repo.status", "repo.diff",
                       "test.run_profile", "operation.status"],
                write=False,
                test_profile="test",
                depends_on=["fix"],
                produces="a regression receipt",
            ),
        ],
    )

    refactor = WorkflowTemplate(
        id="refactor",
        title="Refactor",
        intent="Freeze the current behaviour, change the structure inside a bound, prove equivalence.",
        plan="deterministic",
        review="manager",
        scale="standard",
        max_parallel_workers=2,
        notes=(
            "Behaviour is pinned by a baseline run first; the change may not widen the"
            " declared write scopes."
        ),
        acceptance=[
            "a baseline profile run passes before the change",
            "the structural change stays inside the declared scopes",
            "the same profile passes afterwards with the same reported tests",
        ],
        stages=[
            Stage(
                id="baseline",
                title="Behaviour baseline",
                objective=(
                    "Run the registered profile unchanged and record its exact result as the"
                    " behaviour baseline.\n\nRefactor request: {requirement}"
                ),
                tools=["repo.read", "repo.list", "repo.status", "test.run_profile",
                       "operation.status"],
                write=False,
                test_profile="test",
                produces="a baseline receipt with its reported test count",
            ),
            Stage(
                id="restructure",
                title="Bounded structural change",
                objective=(
                    "Perform the requested structural change without changing observable"
                    " behaviour. Stay inside the declared write scopes."
                ),
                tools=["repo.read", "repo.list", "repo.search", "repo.status", "repo.diff",
                       "repo.patch", "test.run_profile", "operation.status"],
                write=True,
                test_profile="test",
                depends_on=["baseline"],
                produces="a structural patch and its executor receipt",
            ),
            Stage(
                id="equivalence",
                title="Equivalence check",
                objective=(
                    "Run the profile again and compare the reported tests with the baseline"
                    " receipt; report any difference."
                ),
                tools=["repo.read", "repo.list", "repo.status", "repo.diff",
                       "test.run_profile", "operation.status"],
                write=False,
                test_profile="test",
                depends_on=["restructure"],
                produces="an equivalence receipt compared against the baseline",
            ),
        ],
    )

    docs_or_data = WorkflowTemplate(
        id="docs_or_data",
        title="Documentation or data work",
        intent="Understand the input, produce the artifact, check the facts or data in it.",
        plan="deterministic",
        review="manager",
        scale="small",
        max_parallel_workers=2,
        notes=(
            "No code build is forced on a documentation or data project: the artifact"
            " checker profile verifies that the declared outputs exist, are non-empty and"
            " are recorded by digest."
        ),
        acceptance=[
            "the declared output artifact exists with its digest recorded",
            "the artifact checker profile exits zero",
            "every factual claim names its source",
        ],
        stages=[
            Stage(
                id="understand_input",
                title="Understand the input",
                objective=(
                    "Read the registered inputs and list what the deliverable must contain"
                    " and where each fact comes from. Change nothing.\n\nTask: {requirement}"
                ),
                tools=["repo.read", "repo.list", "repo.search", "repo.status"],
                write=False,
                produces="an input inventory with sources",
            ),
            Stage(
                id="produce",
                title="Produce the artifact",
                objective=(
                    "Produce the requested document or data artifact inside the declared"
                    " output scope. Keep every source reference explicit.\n\nTask: {requirement}"
                ),
                tools=["repo.read", "repo.list", "repo.search", "repo.status", "repo.diff",
                       "repo.patch"],
                write=True,
                depends_on=["understand_input"],
                produces="the declared output artifact",
            ),
            Stage(
                id="check",
                title="Check facts and artifacts",
                objective=(
                    "Run the artifact checker profile over the produced output and report its"
                    " real exit code, then list any claim that still lacks a source."
                ),
                tools=["repo.read", "repo.list", "repo.status", "repo.diff",
                       "test.run_profile", "operation.status"],
                write=False,
                test_profile="artifact",
                depends_on=["produce"],
                produces="an artifact-check receipt and a list of unsourced claims",
            ),
        ],
    )

    return {template.id: template for template in (feature, bugfix, refactor, docs_or_data)}


BUILTIN: dict[str, WorkflowTemplate] = _templates()
DEFAULT_TEMPLATE = "feature"


def _template_dir(home: str | os.PathLike[str] | None) -> Path | None:
    if home is None:
        return None
    return Path(home).expanduser() / TEMPLATE_DIR


def catalogue(*, home: str | os.PathLike[str] | None = None) -> list[dict]:
    """Every usable template: the built-ins plus any validated user template."""
    table = load_all(home=home)
    return [
        {
            "id": template.id,
            "title": template.title,
            "intent": template.intent,
            "plan": template.plan,
            "review": template.review,
            "scale": template.scale,
            "max_parallel_workers": template.max_parallel_workers,
            "stages": [stage.id for stage in template.stages],
            "acceptance": list(template.acceptance),
            "source": "builtin" if template.id in BUILTIN else "user",
        }
        for template in (table[key] for key in sorted(table))
    ]


def load_all(*, home: str | os.PathLike[str] | None = None) -> dict[str, WorkflowTemplate]:
    """Built-ins, overridden or extended by validated user templates."""
    table = dict(BUILTIN)
    directory = _template_dir(home)
    if directory is not None and directory.is_dir():
        for path in sorted(directory.glob("*.json")):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                template = WorkflowTemplate.model_validate(raw)
            except Exception as exc:  # noqa: BLE001 - a bad template is a typed refusal
                raise ConfigError(
                    "a user workflow template failed validation",
                    path=str(path), problem=str(exc)[:400],
                ) from exc
            table[template.id] = template
    return table


def get(template_id: str, *, home: str | os.PathLike[str] | None = None) -> WorkflowTemplate:
    table = load_all(home=home)
    try:
        return table[template_id]
    except KeyError as exc:
        raise NotFoundError(
            "unknown workflow template", template_id=template_id, known=sorted(table)
        ) from exc


def describe(template: WorkflowTemplate) -> dict:
    return {
        "id": template.id,
        "title": template.title,
        "intent": template.intent,
        "plan": template.plan,
        "review": template.review,
        "scale": template.scale,
        "max_parallel_workers": template.max_parallel_workers,
        "notes": template.notes,
        "acceptance": list(template.acceptance),
        "stages": [
            {
                "id": stage.id,
                "title": stage.title,
                "role": "worker",
                "write": stage.write,
                "tools": [action.value for action in stage.tools],
                "test_profile": stage.test_profile,
                "depends_on": list(stage.depends_on),
                "produces": stage.produces,
            }
            for stage in template.stages
        ],
    }


def choose_for(requirement: str) -> str:
    """A conservative first guess; the manager may always pick another template.

    This is a suggestion for onboarding and for the ``--template auto`` path, not
    a planning decision: it never narrows what a user may ask for.
    """
    text = requirement.casefold()
    markers = (
        ("bugfix", ("bug", "fix", "broken", "regression", "crash", "报错", "修复", "缺陷")),
        ("refactor", ("refactor", "cleanup", "restructure", "rename", "重构", "整理结构")),
        ("docs_or_data", ("doc", "readme", "report", "data", "csv", "文档", "报告", "数据")),
    )
    for template_id, words in markers:
        if any(word in text for word in words):
            return template_id
    return DEFAULT_TEMPLATE
