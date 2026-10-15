from datetime import timedelta

from infrahub.events.branch_action import BranchDeletedEvent, BranchMergedEvent
from infrahub.trigger.models import BuiltinTriggerDefinition, EventTrigger, ExecuteWorkflow, jinja_parameter
from infrahub.workflows.catalogue import BRANCH_MERGED, BRANCH_PURGE_TASKS

TRIGGER_BRANCH_MERGED = BuiltinTriggerDefinition(
    name="branch-merged-trigger",
    trigger=EventTrigger(
        events={BranchMergedEvent.event_name},
    ),
    actions=[
        ExecuteWorkflow(
            workflow=BRANCH_MERGED,
            parameters={
                "source_branch": jinja_parameter("{{ event.payload['data']['branch_name'] }}"),
                "context": {
                    "__prefect_kind": "json",
                    "value": {"__prefect_kind": "jinja", "template": "{{ event.payload['context'] | tojson }}"},
                },
            },
        ),
    ],
)

# Callers wait on a branch's flow runs by reading them until they settle, and read a purged run as an error.
BRANCH_PURGE_TASKS_DELAY = timedelta(seconds=60)

TRIGGER_BRANCH_DELETED_PURGE_TASKS = BuiltinTriggerDefinition(
    name="branch-deleted-purge-tasks-trigger",
    trigger=EventTrigger(
        events={BranchDeletedEvent.event_name},
    ),
    actions=[
        ExecuteWorkflow(
            workflow=BRANCH_PURGE_TASKS,
            parameters={
                "branch_name": jinja_parameter("{{ event.payload['data']['branch_name'] }}"),
            },
            schedule_after=BRANCH_PURGE_TASKS_DELAY,
        ),
    ],
)
