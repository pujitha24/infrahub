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
                # Prefect relates an event to the flow run that emitted it, here the run that deleted the branch.
                "deleting_flow_run_id": jinja_parameter(
                    "{% if 'flow-run' in event.resource_in_role %}"
                    "{{ event.resource_in_role['flow-run'].id | replace('prefect.flow-run.', '') }}"
                    "{% endif %}"
                ),
            },
        ),
    ],
)
