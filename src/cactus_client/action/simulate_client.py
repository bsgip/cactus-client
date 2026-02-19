from datetime import timedelta
from typing import Any

from cactus_test_definitions.csipaus import (
    CSIPAusReadingLocation,
    CSIPAusReadingType,
    CSIPAusResource,
)

from cactus_client.action.der_controls import action_respond_der_controls
from cactus_client.action.discovery import DISCOVERY_LIST_PAGE_SIZE, discover_resource
from cactus_client.action.mup import action_insert_readings, action_upsert_mup
from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.time import utc_now

SIMULATE_MUP_ID = "simulate_mup_id"


def fake_reading_value(repeat_number: int, cycle_length: int, min_value: float, max_value: float) -> float:
    """Generates a fake reading value in a predictable loop of steps between min and max value.

    cycle_length must be at least 2 (to include min/max value)"""
    step_amount = (max_value - min_value) / (cycle_length - 1)
    step = repeat_number % cycle_length
    return min_value + step * step_amount


async def action_simulate_client(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    frequency_seconds: int = resolved_parameters["frequency_seconds"]  # Mandatory param
    total_simulations: int = resolved_parameters["total_simulations"]  # Mandatory param

    now = utc_now()

    #
    # Do discovery
    #
    for resource in context.resource_tree.discover_resource_plan(
        [CSIPAusResource.EndDevice, CSIPAusResource.MirrorUsagePoint, CSIPAusResource.DERControl]
    ):
        await discover_resource(resource, step, context, DISCOVERY_LIST_PAGE_SIZE)

    #
    # Check for DERControl responses
    #
    await action_respond_der_controls(step, context)

    #
    # If this is the first run - lets create MirrorUsagePoints for later use
    # For other repeats - start submitting readings
    #
    if step.repeat_number == 0:
        await action_upsert_mup(
            {
                "mup_id": SIMULATE_MUP_ID,
                "location": CSIPAusReadingLocation.Site,
                "reading_types": [CSIPAusReadingType.ActivePowerAverage, CSIPAusReadingType.VoltageSinglePhaseAverage],
            },
            step,
            context,
        )
    else:
        #
        # Submit readings for the previous timestep
        #
        await action_insert_readings(
            {
                "mup_id": SIMULATE_MUP_ID,
                "values": {
                    CSIPAusReadingType.ActivePowerAverage: fake_reading_value(step.repeat_number, 5, 2500, 3500),
                    CSIPAusReadingType.VoltageSinglePhaseAverage: fake_reading_value(step.repeat_number, 5, 235, 240),
                },
            },
            step,
            context,
        )

    # When we are done - either queue up another repeat or report success
    if step.repeat_number >= total_simulations:
        return ActionResult.done()
    else:
        return ActionResult(completed=True, repeat=True, not_before=now + timedelta(seconds=frequency_seconds))
