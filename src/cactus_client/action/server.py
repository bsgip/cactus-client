from http import HTTPMethod

from cactus_client.model.context import ExecutionContext
from cactus_client.model.execution import StepExecution
from cactus_client.model.http import ServerResponse
from cactus_client.time import utc_now


async def request_for_step(
    step: StepExecution, context: ExecutionContext, url: str, method: HTTPMethod
) -> ServerResponse:
    session = context.session(step)

    context.progress.log_step_progress(step, f"Requesting {method} {url}")

    requested_at = utc_now()
    async with session.request(method=method, url=url) as raw_response:

        response = await ServerResponse.from_response(raw_response, requested_at=requested_at, received_at=utc_now())
        context.responses.log_response_body(response)
        return response
