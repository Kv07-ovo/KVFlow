"""An approved empty changeset is not an integration failure.

The daily DSH read-only task produced exactly this shape: the manager approved a run
whose deliverable is a report, so nothing was applied - and the gate failed
TEXTUAL_INTEGRATION, which marks a correct read-only outcome as broken. A run that
*did* change files and applied none is still a hard failure.
"""

from __future__ import annotations

from kvflow import coordination as coord
from kvflow.core.store import Store


def _coordinator(tmp_path):
    store = Store(tmp_path / "coordination.sqlite3")
    store.initialize()
    return coord.SemanticCoordinator(store, project_id="proj", job_id="job-1")


def test_an_approved_empty_changeset_is_not_applicable(tmp_path):
    coordinator = _coordinator(tmp_path)
    coordinator.publish_contract(contract_id="API", scope="api", created_by="manager",
                                 document={"v": 1})
    layers = coord.integration_layers(coordinator, applied=[], changed=[],
                                      receipts_exit_zero=True,
                                      node_states={"inspect_verify": "WORKER_COMPLETE"})
    assert layers["textual"] == "NOT_APPLICABLE"
    gate = coordinator.final_gate(node_states={"inspect_verify": "WORKER_COMPLETE"},
                                  receipts_exit_zero=True, integration=layers)
    assert "TEXTUAL_INTEGRATION" not in gate["failed"], gate["failed"]
    assert gate["passed"] is True, gate["failed"]


def test_changed_files_that_were_not_applied_still_fail(tmp_path):
    coordinator = _coordinator(tmp_path)
    coordinator.publish_contract(contract_id="API", scope="api", created_by="manager",
                                 document={"v": 1})
    layers = coord.integration_layers(coordinator, applied=[],
                                      changed=["src/app.py"],
                                      receipts_exit_zero=True,
                                      node_states={"impl": "WORKER_COMPLETE"})
    assert layers["textual"] == "FAIL"
    gate = coordinator.final_gate(node_states={"impl": "WORKER_COMPLETE"},
                                  receipts_exit_zero=True, integration=layers)
    assert "TEXTUAL_INTEGRATION" in gate["failed"]


def test_applied_files_still_pass(tmp_path):
    coordinator = _coordinator(tmp_path)
    coordinator.publish_contract(contract_id="API", scope="api", created_by="manager",
                                 document={"v": 1})
    layers = coord.integration_layers(coordinator, applied=["src/app.py"],
                                      changed=["src/app.py"], receipts_exit_zero=True,
                                      node_states={"impl": "WORKER_COMPLETE"})
    assert layers["textual"] == "PASS"


def test_without_a_change_list_the_old_strict_rule_applies(tmp_path):
    """A caller that does not say what changed gets the strict verdict, not a pass."""
    coordinator = _coordinator(tmp_path)
    coordinator.publish_contract(contract_id="API", scope="api", created_by="manager",
                                 document={"v": 1})
    layers = coord.integration_layers(coordinator, applied=[], receipts_exit_zero=True,
                                      node_states={"impl": "WORKER_COMPLETE"})
    assert layers["textual"] == "FAIL"
