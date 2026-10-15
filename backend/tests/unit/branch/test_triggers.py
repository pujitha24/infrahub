from datetime import timedelta
from uuid import uuid4

from infrahub.branch.triggers import TRIGGER_BRANCH_DELETED_PURGE_TASKS
from infrahub.trigger.models import ExecuteWorkflow


def test_purge_of_a_deleted_branch_starts_after_its_runs_have_been_read() -> None:
    """The purge starts a minute after the deletion, so callers waiting on the branch's runs read them settled first."""
    [action] = TRIGGER_BRANCH_DELETED_PURGE_TASKS.actions
    assert isinstance(action, ExecuteWorkflow)

    run_deployment = action.get(id=uuid4())

    assert run_deployment.schedule_after == timedelta(seconds=60)
