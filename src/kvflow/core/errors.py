"""Typed failure taxonomy for the v1 foundation.

Every rejection carries a machine-readable ``code`` so tests and audit records
can assert the *reason* for a denial instead of accepting any exception. A
security test that only asserts ``pytest.raises(Exception)`` cannot tell a real
containment decision from an unrelated crash; the codes below make the
distinction checkable.
"""

from __future__ import annotations


class V1Error(Exception):
    """Base class for all v1 foundation failures."""

    code = "V1_ERROR"

    def __init__(self, message: str, **details: object) -> None:
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, object]:
        return {"code": self.code, "message": self.message, **self.details}


class ContractError(V1Error):
    code = "CONTRACT_INVALID"


class StateTransitionError(V1Error):
    code = "ILLEGAL_TRANSITION"

    def __init__(self, message: str, *, src: str | None = None, dst: str | None = None, **rest: object) -> None:
        super().__init__(message, src=src, dst=dst, **rest)


class ConcurrencyError(V1Error):
    """Compare-and-swap or lease-fence rejection."""

    code = "CAS_CONFLICT"


class NotFoundError(V1Error):
    code = "NOT_FOUND"


class PathDenied(V1Error):
    code = "PATH_DENIED"


class CapabilityDenied(V1Error):
    code = "CAPABILITY_DENIED"


class LeaseDenied(CapabilityDenied):
    code = "LEASE_DENIED"


class BudgetDenied(V1Error):
    code = "BUDGET_DENIED"


class CapacityUnavailable(BudgetDenied):
    """Temporary slot shortage: wait, never silently pause the task forever."""

    code = "WAITING_CAPACITY"


class AttemptExhausted(V1Error):
    code = "ATTEMPT_EXHAUSTED"


class ReservationConflict(V1Error):
    code = "RESERVATION_CONFLICT"


class AuthorizationError(V1Error):
    code = "AUTHORIZATION_INVALID"


class ConfigError(V1Error):
    code = "CONFIG_INVALID"


class ConflictError(V1Error):
    """An integration or content conflict that must be resolved, not overwritten."""

    code = "CONFLICT"


class SizeError(V1Error):
    """A payload crossed a declared size bound."""

    code = "SIZE_LIMIT"


class ProviderError(V1Error):
    """A model provider call failed; ``outcome`` says whether it may be retried."""

    code = "PROVIDER_ERROR"

    def __init__(
        self,
        message: str,
        *,
        outcome: str = "OUTCOME_UNKNOWN",
        kind: str = "provider",
        **details: object,
    ) -> None:
        super().__init__(message, outcome=outcome, kind=kind, **details)
        self.outcome = outcome
        self.kind = kind


class AuthorityDenied(V1Error):
    """The Orchestrator refused an action for lack of authorization."""

    code = "AUTHORITY_DENIED"
