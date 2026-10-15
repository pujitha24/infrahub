import inspect
from uuid import uuid4

from prefect import flow
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
from prefect.client.schemas.objects import State, StateType
from prefect.events.schemas.events import Event, RelatedResource, Resource
from prefect.utilities.schema_tools.hydration import HydrationContext, hydrate

from infrahub.branch.tasks import purge_deleted_branch_tasks
from infrahub.branch.triggers import TRIGGER_BRANCH_DELETED_PURGE_TASKS
from infrahub.core.branch import Branch
from infrahub.events.branch_action import BranchDeletedEvent
from infrahub.task_manager.flow_run.prefect_client import PrefectClientAdapter
from infrahub.trigger.models import ExecuteWorkflow
from infrahub.workflows.constants import TAG_NAMESPACE, WorkflowTag
from tests.helpers.events import dummy_event_meta


@flow
def _noop_flow() -> None:
    """A trivial flow used only to create branch-tagged flow runs standing in for tasks."""


async def test_purge_removes_settled_tasks_of_a_deleted_branch_but_leaves_running_ones(
    prefect_client: PrefectClient,
) -> None:
    """A deleted branch's settled tasks are removed so completed tasks no longer surface.

    In-flight work keeps the same branch tag while it runs, so only settled runs are purged; a
    running run is left untouched. Cleanup is best-effort and follows the committed deletion.
    """
    branch_name = f"task-leak-branch-{uuid4()}"
    branch_tag = WorkflowTag.BRANCH.render(identifier=branch_name)

    settled_run = await prefect_client.create_flow_run(
        flow=_noop_flow, tags=[TAG_NAMESPACE, branch_tag], state=State(type=StateType.COMPLETED)
    )
    running_run = await prefect_client.create_flow_run(
        flow=_noop_flow, tags=[TAG_NAMESPACE, branch_tag], state=State(type=StateType.RUNNING)
    )

    await purge_deleted_branch_tasks(branch_name=branch_name)

    adapter = PrefectClientAdapter(client=prefect_client)
    settled_after = await adapter.read_flow_runs(
        flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=[settled_run.id]))
    )
    running_after = await adapter.read_flow_runs(
        flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=[running_run.id]))
    )

    assert settled_after == []
    assert [run.id for run in running_after] == [running_run.id]


async def test_purge_triggered_by_a_branch_deletion_keeps_the_run_that_deleted_the_branch(
    prefect_client: PrefectClient,
) -> None:
    """The run that deleted the branch survives the purge its own event triggers.

    A caller that waited on that run still reads it after it settles, and reads a missing run as an error.
    """
    branch_name = f"task-leak-branch-{uuid4()}"
    branch_tag = WorkflowTag.BRANCH.render(identifier=branch_name)

    deleting_run = await prefect_client.create_flow_run(
        flow=_noop_flow, tags=[TAG_NAMESPACE, branch_tag], state=State(type=StateType.COMPLETED)
    )
    earlier_run = await prefect_client.create_flow_run(
        flow=_noop_flow, tags=[TAG_NAMESPACE, branch_tag], state=State(type=StateType.COMPLETED)
    )

    branch = Branch(name=branch_name, uuid=uuid4())
    deleted_event = BranchDeletedEvent(
        meta=dummy_event_meta(branch=branch), branch_name=branch_name, branch_id=str(branch.uuid), sync_with_git=False
    )
    # Prefect relates an event emitted inside a flow run to that run, under the "flow-run" role.
    prefect_event = Event(
        event=deleted_event.event_name,
        resource=Resource(deleted_event.get_resource()),
        related=[RelatedResource(item) for item in deleted_event.get_related()]
        + [
            RelatedResource(
                {
                    "prefect.resource.id": f"prefect.flow-run.{deleting_run.id}",
                    "prefect.resource.role": "flow-run",
                    "prefect.resource.name": deleting_run.name,
                }
            )
        ],
        payload=deleted_event.get_event_payload(),
    )
    action = TRIGGER_BRANCH_DELETED_PURGE_TASKS.actions[0]
    assert isinstance(action, ExecuteWorkflow)
    parameters = hydrate(
        action.parameters,
        HydrationContext(render_jinja=True, jinja_context={"event": prefect_event}, raise_on_error=True),
    )

    # Unpacked keyword arguments could include `return_state`, so the checker cannot pick the awaitable overload.
    purge_run = purge_deleted_branch_tasks(**parameters)
    assert inspect.isawaitable(purge_run)
    await purge_run

    adapter = PrefectClientAdapter(client=prefect_client)
    remaining = await adapter.read_flow_runs(
        flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=[deleting_run.id, earlier_run.id]))
    )

    assert [run.id for run in remaining] == [deleting_run.id]
