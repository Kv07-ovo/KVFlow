"""Strict, serializable v1 boundaries.

Valid data is *not* an authorization token. Registration records, observed model
identity, test receipts, reviews and user decisions are produced by trusted
runtime services and bound to persisted authorization plus executed evidence.
Model JSON may only ever propose facts; nothing here grants permission. The
capability ticket that actually authorizes a tool call is issued by
:mod:`kvflow.core.security` and is never part of a model-visible payload.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime
from enum import Enum
from pathlib import PurePath
from typing import Annotated, Any, Literal, Sequence

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

Id = Annotated[StrictStr, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")]
Digest = Annotated[StrictStr, Field(pattern=r"^[a-f0-9]{64}$")]
Text = Annotated[StrictStr, Field(min_length=1, max_length=32768)]
Counter = Annotated[StrictInt, Field(ge=0)]
Positive = Annotated[StrictInt, Field(gt=0)]

#: sentinels that must never be accepted where a real digest is required
PLACEHOLDER_DIGESTS = frozenset({"NOT_EXPOSED", "UNKNOWN", "NONE", "TODO", "0" * 64})
#: sentinel used for every provider field the provider did not actually return
NOT_EXPOSED = "NOT_EXPOSED"

WINDOWS_RESERVED = frozenset(
    {"con", "prn", "aux", "nul", "conin$", "conout$", "clock$"}
    | {f"{p}{i}" for p in ("com", "lpt") for i in "123456789"}
)


class Contract(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, validate_default=True, allow_inf_nan=False
    )
    #: pydantic models are not pytest test classes, even when named Test*
    __test__ = False


class Role(str, Enum):
    WORKER = "worker"
    MANAGER = "manager"
    ORCHESTRATOR = "orchestrator"


class Action(str, Enum):
    """Named operations only. There is deliberately no arbitrary-SQL/URL action."""

    REPO_READ = "repo.read"
    REPO_SEARCH = "repo.search"
    REPO_LIST = "repo.list"
    REPO_STATUS = "repo.status"
    REPO_DIFF = "repo.diff"
    REPO_PATCH = "repo.patch"
    REPO_COMMIT = "repo.commit"
    TEST_RUN = "test.run_profile"
    OPERATION_STATUS = "operation.status"
    OPERATION_RESULT = "operation.result"
    OPERATION_CANCEL = "operation.cancel"
    RESEARCH_READ = "research.read_named"
    RESEARCH_QUALIFICATION = "research.read_qualification"
    RESEARCH_SPEC = "research.propose_spec"
    RESEARCH_RUN = "research.run_authorized"
    KNOWLEDGE_SEARCH = "knowledge.search"
    KNOWLEDGE_READ = "knowledge.read"
    KNOWLEDGE_PROPOSE = "knowledge.propose"
    KNOWLEDGE_APPEND = "knowledge.append_authorized"


class State(str, Enum):
    """Mirror of :class:`kvflow.core.state.TaskState` (single source: state.py)."""

    NEW = "NEW"
    PLANNING = "PLANNING"
    ASSIGNED = "ASSIGNED"
    WAITING_DEPENDENCIES = "WAITING_DEPENDENCIES"
    WAITING_CAPACITY = "WAITING_CAPACITY"
    WORKING = "WORKING"
    SELF_REVIEW = "SELF_REVIEW"
    WORKER_COMPLETE = "WORKER_COMPLETE"
    MANAGER_REVIEW = "MANAGER_REVIEW"
    MANAGER_APPROVED = "MANAGER_APPROVED"
    INTEGRATED = "INTEGRATED"
    READY_TO_APPLY = "READY_TO_APPLY"
    APPLIED = "APPLIED"
    DONE = "DONE"
    FIX = "FIX"
    BLOCKED = "BLOCKED"
    PAUSED = "PAUSED"
    PAUSED_BUDGET = "PAUSED_BUDGET"
    CANCELLED = "CANCELLED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"


def relative_scope(value: str) -> str:
    """Validate a *lexical* project-relative scope.

    This is a shape check only. Containment, reparse points and ancestry are
    decided against the real filesystem by :class:`kvflow.core.security.PathPolicy`.
    """
    if not isinstance(value, str) or isinstance(value, bool) or not value or len(value) > 2048:
        raise ValueError("invalid relative scope")
    clean = value.replace("\\", "/")
    if clean == ".":
        return clean
    if clean.startswith("/") or clean.startswith("//"):
        raise ValueError("scope must be project relative")
    if len(clean) > 1 and clean[1] == ":":
        raise ValueError("scope must not carry a drive letter")
    if clean.endswith("/"):
        raise ValueError("scope must not end with a separator")
    for part in clean.split("/"):
        if part in {"", ".", ".."}:
            raise ValueError("unsafe scope component")
        if part[-1] in " .":
            raise ValueError("scope component must not end with a space or dot")
        if part.startswith(":"):
            raise ValueError("scope component must not start with a colon")
        if any(ord(c) < 32 or c in ':<>"|?*' for c in part):
            raise ValueError("unsafe scope character")
        if part.split(".")[0].casefold() in WINDOWS_RESERVED:
            raise ValueError("reserved device name")
    return clean


class Identity(Contract):
    """Configured versus provider-observed model identity.

    ``provider_*`` stays ``NOT_EXPOSED`` unless the provider actually returned
    the value; a command-line argument is never promoted into provider evidence.
    """

    display_name: Text
    requested_model: Text
    requested_effort: Text
    provider_returned_model: Text = NOT_EXPOSED
    provider_returned_effort: Text = NOT_EXPOSED
    verification_level: Literal[
        "CONFIGURED_ONLY", "PROVIDER_MODEL", "PROVIDER_MODEL_AND_EFFORT", "FIXTURE"
    ]
    execution_kind: Literal["LIVE", "FIXTURE"]

    @model_validator(mode="after")
    def consistent_identity(self) -> "Identity":
        model_known = self.provider_returned_model not in {"UNKNOWN", NOT_EXPOSED}
        effort_known = self.provider_returned_effort not in {"UNKNOWN", NOT_EXPOSED}
        if (self.verification_level == "FIXTURE") != (self.execution_kind == "FIXTURE"):
            raise ValueError("fixture identity must not claim a live model")
        if self.verification_level == "CONFIGURED_ONLY" and (model_known or effort_known):
            raise ValueError("provider observations require their verification level")
        if self.verification_level in {"PROVIDER_MODEL", "PROVIDER_MODEL_AND_EFFORT"} and not model_known:
            raise ValueError("provider model is unknown")
        if self.verification_level == "PROVIDER_MODEL_AND_EFFORT" and not effort_known:
            raise ValueError("provider effort is unknown")
        if self.verification_level == "PROVIDER_MODEL" and effort_known:
            raise ValueError("observed effort requires PROVIDER_MODEL_AND_EFFORT")
        return self


class BudgetLimits(Contract):
    """Plan-level execution budget. Money lives in the durable budget ledger."""

    calls: Counter
    input_tokens: Counter
    output_tokens: Counter
    tool_calls: Counter
    storage_bytes: Counter
    wall_seconds: Positive
    concurrency: Annotated[StrictInt, Field(ge=1, le=3)]
    deadline: datetime

    _aware_deadline = field_validator("deadline")(lambda v: _aware(v))


class BudgetScope(Contract):
    """Durable budget scope with an integer micro-CNY cap and a frozen deadline."""

    scope_id: Id
    scope_kind: Literal["GLOBAL", "PROJECT", "JOB"]
    parent_scope_id: Id | None = None
    project_id: Id | None = None
    job_id: Id | None = None
    calls: Counter
    input_tokens: Counter
    output_tokens: Counter
    tool_calls: Counter
    storage_bytes: Counter
    wall_seconds: Counter
    micro_cny: Counter
    concurrency: Annotated[StrictInt, Field(ge=1, le=8)]
    deadline: datetime
    authorization_digest: Digest

    _aware = field_validator("deadline")(lambda v: _aware(v))

    @model_validator(mode="after")
    def scope_shape(self) -> "BudgetScope":
        if self.scope_kind == "GLOBAL" and (self.project_id or self.job_id or self.parent_scope_id):
            raise ValueError("global scope carries no project/job/parent")
        if self.scope_kind == "PROJECT" and (not self.project_id or self.job_id):
            raise ValueError("project scope requires exactly a project")
        if self.scope_kind == "JOB" and (not self.project_id or not self.job_id):
            raise ValueError("job scope requires project and job")
        return self


#: interpreters and package managers a fixed-argv profile may invoke, by file name.
#: A profile is user-approved configuration, but the executable still has to be
#: one the product is willing to run, and the argv is always executed as a list.
ARGV_EXECUTABLE_ALLOWLIST = frozenset(
    {"python", "python3", "python.exe", "node", "node.exe", "npm", "npm.cmd", "npx",
     "npx.cmd", "pnpm", "pnpm.cmd", "yarn", "yarn.cmd", "tsc", "deno", "bun", "git",
     "git.exe", "pytest"}
)

#: characters that only mean something to a shell; KVFlow never uses one
ARGV_METACHARACTERS = re.compile(r"[&|;<>`$\r\n]")


def assert_safe_argv(argv: Sequence[str], *, limit: int = 32) -> list[str]:
    """Validate one pre-registered argv; returns it unchanged when it is safe."""
    if not argv:
        raise ValueError("an argv profile needs a non-empty argv")
    if len(argv) > limit:
        raise ValueError(f"an argv profile is limited to {limit} elements")
    for index, part in enumerate(argv):
        if not isinstance(part, str) or not part or len(part) > 512:
            raise ValueError("argv elements must be 1..512 characters")
        if ARGV_METACHARACTERS.search(part):
            raise ValueError(
                f"argv element {index} contains a shell metacharacter; KVFlow always"
                " executes a list, never a shell string"
            )
    executable = PurePath(argv[0]).name.casefold()
    if executable not in ARGV_EXECUTABLE_ALLOWLIST:
        raise ValueError(f"argv executable {executable!r} is not in the allowlist")
    return list(argv)


class TestProfile(Contract):
    """A pre-registered execution profile; never an arbitrary command string.

    Two shapes exist. A ``runner`` profile (pytest/unittest/readonly_inventory)
    declares targets and KVFlow builds the interpreter argv itself. An ``argv``
    profile declares the whole fixed argv for a project whose proof is ``npm test``
    or an artifact checker; it is validated against an executable allowlist and is
    still executed as a list with ``shell=False``.
    """

    id: Id
    project_id: Id
    description: Text
    runner: Literal["pytest", "unittest", "readonly_inventory", "argv"]
    targets: Annotated[list[StrictStr], Field(max_length=128)] = Field(default_factory=list)
    cwd: StrictStr = "."
    pythonpath: Annotated[list[StrictStr], Field(max_length=8)] = Field(default_factory=list)
    timeout_seconds: Annotated[StrictInt, Field(ge=1, le=3600)] = 180
    output_limit_bytes: Annotated[StrictInt, Field(ge=1024, le=1048576)] = 65536
    code_digest: Digest
    authorization_digest: Digest
    heavy: StrictBool = False
    #: the fixed argv of an ``argv`` profile, verbatim and validated
    argv: Annotated[list[StrictStr], Field(max_length=32)] = Field(default_factory=list)
    #: a profile the project has expressly registered for isolated research runs.
    #: ``research.run_authorized`` refuses any profile that is not marked here, so
    #: an ordinary test profile can never be reused as a research entry point.
    research_authorized: StrictBool = False

    @field_validator("targets")
    @classmethod
    def safe_targets(cls, values: list[str]) -> list[str]:
        return [relative_scope(v) for v in values]

    @field_validator("pythonpath")
    @classmethod
    def safe_pythonpath(cls, values: list[str]) -> list[str]:
        return [relative_scope(v) for v in values]

    _cwd = field_validator("cwd")(relative_scope)

    @model_validator(mode="after")
    def profile_shape(self) -> "TestProfile":
        if self.runner == "argv":
            assert_safe_argv(list(self.argv))
            if self.targets:
                raise ValueError("an argv profile declares argv, not targets")
        else:
            if not self.targets:
                raise ValueError("a runner profile needs at least one target")
            if self.argv:
                raise ValueError("argv belongs to the argv runner only")
        return self


class Project(Contract):
    """Explicitly registered project.

    ``source_root`` is the user's registered project (never written by the
    product); ``managed_root`` is the owned worktree/snapshot root that task
    writes resolve inside. They are distinct identities, not one guessed root.
    """

    id: Id
    display_name: Text
    source_root: Text
    managed_root: Text
    allowed_read_roots: Annotated[list[StrictStr], Field(min_length=1, max_length=128)]
    allowed_write_roots: Annotated[list[StrictStr], Field(max_length=128)]
    protected_roots: Annotated[list[StrictStr], Field(max_length=128)]
    test_profiles: Annotated[list[TestProfile], Field(max_length=128)]
    allowed_tools: Annotated[list[Action], Field(min_length=1)]
    trusted: StrictBool
    authorization_digest: Digest
    integration_policy: Literal["MANAGED_BRANCH", "REVIEW_ONLY"] = "MANAGED_BRANCH"
    registered_at: datetime

    _registered = field_validator("registered_at")(lambda v: _aware(v))
    _digest = field_validator("authorization_digest")(lambda v: _real_digest(v))

    @field_validator("allowed_read_roots", "allowed_write_roots", "protected_roots")
    @classmethod
    def scopes(cls, values: list[str]) -> list[str]:
        normalized = [relative_scope(v) for v in values]
        if len({v.casefold() for v in normalized}) != len(normalized):
            raise ValueError("duplicate scope")
        return normalized

    @model_validator(mode="after")
    def roots_and_profiles(self) -> "Project":
        _absolute(self.source_root, "source_root")
        _absolute(self.managed_root, "managed_root")
        if _fold(self.source_root) == _fold(self.managed_root):
            raise ValueError("source and managed roots must be distinct")
        if not self.trusted:
            raise ValueError("untrusted project cannot be registered for execution")
        for root in self.allowed_write_roots:
            for protected in self.protected_roots:
                if _fold(root) == _fold(protected) or _fold(root).startswith(_fold(protected) + "/"):
                    raise ValueError("write root overlaps a protected root")
                if protected == ".":
                    raise ValueError("the whole project cannot be a protected root while writes exist")
        if len({p.id for p in self.test_profiles}) != len(self.test_profiles):
            raise ValueError("duplicate test profile")
        for profile in self.test_profiles:
            if profile.project_id != self.id:
                raise ValueError("test profile belongs to another project")
        if len(set(self.allowed_tools)) != len(self.allowed_tools):
            raise ValueError("duplicate action")
        return self


class NodeSpec(Contract):
    """One DAG node. ``lineage_key`` is what makes retry counts un-resettable."""

    id: Id
    lineage_key: Id
    dependencies: Annotated[list[Id], Field(max_length=128)] = Field(default_factory=list)
    objective: Text
    write_scopes: Annotated[list[StrictStr], Field(max_length=128)] = Field(default_factory=list)
    test_profile: Id
    allowed_tools: Annotated[list[Action], Field(min_length=1)]
    role: Role = Role.WORKER

    _scopes = field_validator("write_scopes")(Project.scopes.__func__)

    @model_validator(mode="after")
    def node_constraints(self) -> "NodeSpec":
        if self.role == Role.ORCHESTRATOR:
            raise ValueError("models cannot assign Orchestrator authority")
        if self.id in self.dependencies or len(set(self.dependencies)) != len(self.dependencies):
            raise ValueError("duplicate/self dependency")
        if len(set(self.allowed_tools)) != len(self.allowed_tools):
            raise ValueError("duplicate action")
        return self


class Plan(Contract):
    """Immutable, versioned plan. Full content is persisted, not just nodes."""

    id: Id
    job_id: Id
    version: Positive
    revision: Positive
    objective: Text
    deliverables: Annotated[list[Text], Field(min_length=1, max_length=128)]
    constraints: Annotated[list[Text], Field(min_length=1, max_length=128)]
    acceptance: Annotated[list[Text], Field(min_length=1, max_length=128)]
    nodes: Annotated[list[NodeSpec], Field(min_length=1, max_length=128)]
    roles: Annotated[list[Role], Field(min_length=1)]
    allowed_roots: Annotated[list[StrictStr], Field(min_length=1, max_length=128)]
    tool_profiles: Annotated[list[Id], Field(min_length=1, max_length=128)]
    resource_budget: BudgetLimits
    approval_boundaries: Annotated[list[Text], Field(min_length=1, max_length=128)]
    authorization_digest: Digest
    created_at: datetime

    _created = field_validator("created_at")(lambda v: _aware(v))
    _digest = field_validator("authorization_digest")(lambda v: _real_digest(v))
    _roots = field_validator("allowed_roots")(Project.scopes.__func__)

    @model_validator(mode="after")
    def topology(self) -> "Plan":
        ids = {n.id for n in self.nodes}
        if len(ids) != len(self.nodes):
            raise ValueError("duplicate node")
        if Role.ORCHESTRATOR in self.roles or len(set(self.roles)) != len(self.roles):
            raise ValueError("invalid model roles")
        if Role.WORKER not in self.roles:
            raise ValueError("plan must include the worker role")
        if len(set(self.tool_profiles)) != len(self.tool_profiles):
            raise ValueError("duplicate tool profile")
        plan_roots = {_fold(r) for r in self.allowed_roots}
        remaining = {n.id: set(n.dependencies) for n in self.nodes}
        for node in self.nodes:
            if set(node.dependencies) - ids:
                raise ValueError("unknown dependency")
            if node.role not in self.roles:
                raise ValueError("node exceeds plan roles")
            if node.test_profile not in self.tool_profiles:
                raise ValueError("node exceeds plan test profiles")
            if node.allowed_tools and not set(node.allowed_tools) <= set(node.allowed_tools):
                raise ValueError("duplicate action")  # pragma: no cover - defensive
            for scope in node.write_scopes:
                if not any(_within(scope, root) for root in plan_roots):
                    raise ValueError(f"write scope {scope!r} exceeds plan allowed roots")
        while remaining:
            ready = {key for key, deps in remaining.items() if not deps}
            if not ready:
                raise ValueError("cyclic dependencies")
            remaining = {
                key: deps - ready for key, deps in remaining.items() if key not in ready
            }
        return self


class Binding(Contract):
    """Durable identity of one worker attempt. Not a credential."""

    project_id: Id
    job_id: Id
    node_id: Id
    run_id: Id
    attempt: Positive
    fence: Positive


class ArtifactRef(Contract):
    id: Id
    digest: Digest
    relative_path: StrictStr
    size_bytes: Counter

    _path = field_validator("relative_path")(relative_scope)
    _digest = field_validator("digest")(lambda v: _real_digest(v))


class WorkerResult(Binding):
    status: Literal["WORKER_COMPLETE", "FIX_REQUIRED", "BLOCKED"]
    summary: Text
    identity: Identity
    base_digest: Digest
    content_digest: Digest
    changed_files: list[StrictStr]
    artifacts: list[ArtifactRef]
    test_receipt_ids: list[Id]
    self_review: Text
    self_review_receipt_ids: list[Id]

    _base = field_validator("base_digest")(lambda v: _real_digest(v))
    _content = field_validator("content_digest")(lambda v: _real_digest(v))

    @field_validator("changed_files")
    @classmethod
    def paths(cls, values: list[str]) -> list[str]:
        return [relative_scope(v) for v in values]

    @model_validator(mode="after")
    def completed_evidence(self) -> "WorkerResult":
        if self.status == "WORKER_COMPLETE":
            if not self.test_receipt_ids:
                raise ValueError("completion requires an executor test receipt")
            if set(self.self_review_receipt_ids) != set(self.test_receipt_ids):
                raise ValueError("completion requires test-bound post-test self-review")
            if self.identity.execution_kind == "FIXTURE":
                raise ValueError("a fixture identity cannot claim WORKER_COMPLETE")
        return self


class TestReceipt(Contract):
    """Executor-produced receipt. The Worker cannot author this object."""

    id: Id
    binding: Binding
    profile_id: Id
    code_digest: Digest
    argv: Annotated[list[StrictStr], Field(min_length=1, max_length=64)]
    cwd: StrictStr
    exit_code: StrictInt
    duration_ms: Counter
    timeout_seconds: Positive
    reported_tests: Counter | None = None
    stdout_digest: Digest
    stderr_digest: Digest
    stdout_bytes: Counter
    stderr_bytes: Counter
    stdout_artifact: StrictStr | None = None
    stderr_artifact: StrictStr | None = None
    truncated: StrictBool = False
    started_at: datetime
    finished_at: datetime

    _code = field_validator("code_digest")(lambda v: _real_digest(v))
    _stdout = field_validator("stdout_digest", "stderr_digest")(lambda v: _real_digest(v))
    _started = field_validator("started_at", "finished_at")(lambda v: _aware(v))


class Review(Binding):
    id: Id
    verdict: Literal["APPROVE", "FIX", "BLOCKED"]
    reviewer: Identity
    reviewer_role: Literal["manager"]
    content_digest: Digest
    result_digest: Digest
    test_receipt_ids: Annotated[list[Id], Field(min_length=1)]
    findings: list[Text]
    rationale: Text
    created_at: datetime

    _content = field_validator("content_digest", "result_digest")(lambda v: _real_digest(v))
    _created = field_validator("created_at")(lambda v: _aware(v))

    @model_validator(mode="after")
    def review_evidence(self) -> "Review":
        if self.reviewer.execution_kind == "FIXTURE":
            raise ValueError("a fixture identity cannot issue a manager review")
        return self


class KnowledgeRecord(Contract):
    """Append-only knowledge with provenance and explicit authority boundaries."""

    id: Id
    scope: Literal["PROJECT", "GLOBAL_PREFERENCE"]
    privacy: Literal["SHARED", "PRIVATE"] = "SHARED"
    project_id: Id | None = None
    job_id: Id | None = None
    run_id: Id | None = None
    topic: Id
    kind: Literal[
        "USER_DECISION",
        "CONSTRAINT",
        "REPORTED_FACT",
        "VERIFIED_FACT_WITH_SCOPE",
        "HISTORICAL_STATE",
        "OPEN_QUESTION",
        "WORKER_PROPOSAL",
        "MANAGER_DECISION",
        "PREFERENCE",
    ]
    author: Literal["worker", "manager", "user", "executor", "authorized_import"]
    verification: Literal["REPORTED", "EXECUTOR_VERIFIED", "USER_CONFIRMED", "IMPORTED_WITH_SOURCE"]
    content: Text
    source_ref: Text
    source_digest: Digest
    authorization_digest: Digest
    observed_at: datetime
    effective_at: datetime
    supersedes: Annotated[list[Id], Field(max_length=128)] = Field(default_factory=list)
    contradicts: Annotated[list[Id], Field(max_length=128)] = Field(default_factory=list)

    _observed = field_validator("observed_at", "effective_at")(lambda v: _aware(v))
    _digests = field_validator("source_digest", "authorization_digest")(
        lambda v: _real_digest(v)
    )

    @model_validator(mode="after")
    def authority_and_scope(self) -> "KnowledgeRecord":
        if self.scope == "PROJECT" and not self.project_id:
            raise ValueError("project record must be scoped to a project")
        if self.scope == "GLOBAL_PREFERENCE":
            if self.project_id or self.job_id or self.run_id:
                raise ValueError("global scope carries no project/job/run")
            if self.kind not in {"PREFERENCE", "USER_DECISION"}:
                raise ValueError("global scope only carries expressly authorized preferences")
        if self.author == "worker" and (
            self.kind not in {"WORKER_PROPOSAL", "REPORTED_FACT", "OPEN_QUESTION"}
            or self.verification != "REPORTED"
        ):
            raise ValueError("worker can only propose reported knowledge")
        if self.kind == "USER_DECISION" and (
            self.author not in {"user", "authorized_import"}
            or self.verification not in {"USER_CONFIRMED", "IMPORTED_WITH_SOURCE"}
        ):
            raise ValueError("user decisions require user evidence")
        if self.kind == "VERIFIED_FACT_WITH_SCOPE" and self.verification != "EXECUTOR_VERIFIED":
            raise ValueError("a verified fact requires an executor receipt")
        if self.verification == "EXECUTOR_VERIFIED" and self.author != "executor":
            raise ValueError("executor verification requires the actual executor")
        if self.verification == "USER_CONFIRMED" and self.author != "user":
            raise ValueError("model/import cannot claim direct user confirmation")
        if self.author == "authorized_import" and self.verification != "IMPORTED_WITH_SOURCE":
            raise ValueError("imported verification must retain its source")
        if self.id in self.supersedes or self.id in self.contradicts:
            raise ValueError("self-referential knowledge")
        if set(self.supersedes) & set(self.contradicts):
            raise ValueError("a record cannot both supersede and contradict one source")
        return self


class NumericBounds(Contract):
    minimum: float | StrictInt
    maximum: float | StrictInt

    @field_validator("minimum", "maximum", mode="before")
    @classmethod
    def actual_number(cls, value: Any) -> Any:
        if isinstance(value, bool) or type(value) not in {int, float}:
            raise ValueError("finite numeric bound required")
        if not math.isfinite(value):
            raise ValueError("finite numeric bound required")
        return value

    @model_validator(mode="after")
    def ordered(self) -> "NumericBounds":
        if self.minimum > self.maximum:
            raise ValueError("inverted bounds")
        return self


#: research attempts are frozen by spec and never expanded automatically
MAX_EXPERIMENT_ATTEMPTS = 3


class ExperimentSpec(Contract):
    """Isolated, frozen research attempt. Never authorizes Alpha/Forward work."""

    id: Id
    project_id: Id
    job_id: Id
    node_id: Id
    hypothesis_lineage_id: Id
    authorization_digest: Digest
    hypothesis: Text
    profile_id: Id
    code_digest: Digest
    data_digest: Digest
    data_effective_time: datetime
    parameters: Annotated[dict[Id, NumericBounds], Field(max_length=64)]
    metrics: Annotated[dict[Id, NumericBounds], Field(min_length=1, max_length=64)]
    maximum_candidates: Annotated[StrictInt, Field(ge=1, le=100)]
    maximum_attempts: Annotated[StrictInt, Field(ge=1, le=MAX_EXPERIMENT_ATTEMPTS)]
    computational_budget_seconds: Positive
    resource_budget: BudgetLimits
    input_scope: StrictStr = "."
    output_scope: StrictStr
    exposure_status: Literal["ALREADY_EXPOSED_EXPLORATORY", "ENGINEERING_FIXTURE"]
    conclusion_boundary: Literal["NO_ALPHA_OR_FORWARD_VALIDATION"] = (
        "NO_ALPHA_OR_FORWARD_VALIDATION"
    )

    _output = field_validator("output_scope", "input_scope")(relative_scope)
    _digests = field_validator(
        "authorization_digest", "code_digest", "data_digest"
    )(lambda v: _real_digest(v))
    _effective = field_validator("data_effective_time")(lambda v: _aware(v))


class ExperimentRun(Binding):
    """One authorized research execution inside the frozen spec's bounds.

    It exists so an isolated research attempt is *traceable*: which spec, which
    attempt, which executor receipt, and which output digest was produced. It
    never carries an Alpha or Forward conclusion, and the literal below cannot be
    widened by a caller.
    """

    id: Id
    experiment_id: Id
    outcome: Literal[
        "COMPLETED", "FAILED_EXIT_CODE", "FAILED_TIME", "REFUSED", "OUTCOME_UNKNOWN"
    ]
    conclusion_boundary: Literal["NO_ALPHA_OR_FORWARD_VALIDATION"] = (
        "NO_ALPHA_OR_FORWARD_VALIDATION"
    )
    receipt_id: Id | None = None
    candidate_count: Counter = 0
    output_scope: StrictStr
    output_digest: Digest
    observed_at: datetime
    notes: list[Text] = Field(default_factory=list, max_length=16)

    _output = field_validator("output_scope")(relative_scope)
    _digest = field_validator("output_digest")(lambda v: _real_digest(v))
    _observed = field_validator("observed_at")(lambda v: _aware(v))


class Operation(Contract):
    """Durable handle for a long tool invocation."""

    id: Id
    binding: Binding
    action: Action
    status: Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "OUTCOME_UNKNOWN"]
    request_digest: Digest
    created_at: datetime
    updated_at: datetime
    result_digest: Digest | None = None
    result_artifact: StrictStr | None = None
    error_code: StrictStr | None = None

    _digests = field_validator("request_digest")(lambda v: _real_digest(v))
    _times = field_validator("created_at", "updated_at")(lambda v: _aware(v))


class MailboxMessage(Contract):
    """Structured Manager<->Worker exchange. Never a free-form group chat."""

    id: Id
    binding: Binding
    sender_role: Literal["manager", "worker"]
    kind: Literal["FIX_REQUEST", "CLARIFICATION", "BLOCKER", "INFO"]
    body: Text
    created_at: datetime
    read_at: datetime | None = None

    _created = field_validator("created_at")(lambda v: _aware(v))


class ContextManifestEntry(Contract):
    reference: Text
    source_digest: Digest | None = None
    summary: Text
    truncated: StrictBool = False
    original_bytes: Counter
    included_bytes: Counter


class ContextManifest(Contract):
    binding: Binding
    created_at: datetime
    entries: Annotated[list[ContextManifestEntry], Field(min_length=1, max_length=256)]
    total_bytes: Counter
    budget_bytes: Positive
    plan_version: Positive
    plan_revision: Positive

    _created = field_validator("created_at")(lambda v: _aware(v))

    @model_validator(mode="after")
    def bounded(self) -> "ContextManifest":
        if self.total_bytes != sum(e.included_bytes for e in self.entries):
            raise ValueError("manifest total must equal the sum of included bytes")
        if self.total_bytes > self.budget_bytes:
            raise ValueError("manifest exceeds its declared budget")
        for entry in self.entries:
            if entry.included_bytes > entry.original_bytes:
                raise ValueError("included bytes exceed the original size")
        return self


class Publication(Contract):
    """Idempotent record of an integration hand-off. Never applies to a source root."""

    id: Id
    project_id: Id
    job_id: Id
    node_id: Id
    state: Literal["INTEGRATED", "READY_TO_APPLY", "APPLIED"]
    integration_branch: StrictStr
    content_digest: Digest
    review_id: Id
    idempotency_key: Text
    target: Literal["MANAGED_WORKSPACE"]
    created_at: datetime

    _content = field_validator("content_digest")(lambda v: _real_digest(v))
    _created = field_validator("created_at")(lambda v: _aware(v))


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("timestamp required")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a UTC offset")
    return value


def _real_digest(value: str) -> str:
    if not isinstance(value, str) or value in PLACEHOLDER_DIGESTS:
        raise ValueError("a real content digest is required")
    if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("digest must be lowercase sha256 hex")
    return value


def _absolute(value: str, field: str) -> str:
    from pathlib import PurePosixPath, PureWindowsPath

    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} is required")
    if not (PureWindowsPath(value).is_absolute() or PurePosixPath(value).is_absolute()):
        raise ValueError(f"{field} must be absolute")
    return value


def _fold(value: str) -> str:
    return value.replace("\\", "/").rstrip("/").casefold() or "."


def _within(scope: str, root: str) -> bool:
    scope_f, root_f = _fold(scope), _fold(root)
    if root_f in {".", ""}:
        return True
    return scope_f == root_f or scope_f.startswith(root_f + "/")


def canonical_json(value: Contract | dict[str, Any]) -> str:
    data = value.model_dump(mode="json") if isinstance(value, Contract) else value
    return json.dumps(
        data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def content_digest(value: Contract | dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def json_schemas() -> dict[str, dict[str, Any]]:
    """Export the real validation schemas instead of hand-written approximations."""
    return {
        cls.__name__: cls.model_json_schema()
        for cls in (
            Identity,
            BudgetLimits,
            BudgetScope,
            TestProfile,
            Project,
            NodeSpec,
            Plan,
            WorkerResult,
            TestReceipt,
            Review,
            KnowledgeRecord,
            ExperimentSpec,
            Operation,
            MailboxMessage,
            ContextManifest,
            Publication,
        )
    }
