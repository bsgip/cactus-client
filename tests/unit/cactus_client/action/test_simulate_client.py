import unittest.mock as mock

import pytest
from cactus_test_definitions.csipaus import CSIPAusResource

from cactus_client.action.simulate_client import SIMULATE_MUP_ID, action_simulate_client
from cactus_client.model.context import ExecutionContext


@pytest.mark.parametrize(
    "repeat_number, total_simulations, expect_mup_discovery, expect_repeat",
    [
        (0, 3, True, True),  # First run - discovers MUPs and creates the simulated MUP
        (1, 3, False, True),  # Later runs - MUPs are not rediscovered (would drop our MUP off the limited page)
        (3, 3, False, False),  # Final run
    ],
)
@mock.patch("cactus_client.action.simulate_client.action_insert_readings")
@mock.patch("cactus_client.action.simulate_client.action_upsert_mup")
@mock.patch("cactus_client.action.simulate_client.action_respond_der_controls")
@mock.patch("cactus_client.action.simulate_client.discover_resource")
@pytest.mark.asyncio
async def test_action_simulate_client(
    mock_discover_resource: mock.MagicMock,
    mock_respond_der_controls: mock.MagicMock,
    mock_upsert_mup: mock.MagicMock,
    mock_insert_readings: mock.MagicMock,
    repeat_number: int,
    total_simulations: int,
    expect_mup_discovery: bool,
    expect_repeat: bool,
    testing_contexts_factory,
):
    context: ExecutionContext
    context, step = testing_contexts_factory(mock.Mock())
    step.repeat_number = repeat_number

    result = await action_simulate_client(
        {"frequency_seconds": 60, "total_simulations": total_simulations}, step, context
    )

    discovered = [c.args[0] for c in mock_discover_resource.call_args_list]
    assert CSIPAusResource.EndDevice in discovered
    assert CSIPAusResource.DERControl in discovered
    assert (CSIPAusResource.MirrorUsagePoint in discovered) is expect_mup_discovery
    mock_respond_der_controls.assert_called_once()

    if repeat_number == 0:
        mock_upsert_mup.assert_called_once()
        assert mock_upsert_mup.call_args.args[0]["mup_id"] == SIMULATE_MUP_ID
        mock_insert_readings.assert_not_called()
    else:
        mock_upsert_mup.assert_not_called()
        mock_insert_readings.assert_called_once()
        assert mock_insert_readings.call_args.args[0]["mup_id"] == SIMULATE_MUP_ID

    assert result.repeat is expect_repeat
