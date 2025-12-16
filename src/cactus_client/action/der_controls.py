import logging
import re
from datetime import datetime
from http import HTTPMethod
from typing import Any, Optional, cast

from cactus_test_definitions.csipaus import CSIPAusResource
from envoy_schema.server.schema.sep2.der import DERControlResponse as DERControl
from envoy_schema.server.schema.sep2.end_device import EndDeviceResponse
from envoy_schema.server.schema.sep2.event import EventStatusType
from envoy_schema.server.schema.sep2.response import ResponseType, DERControlResponse

from cactus_client.action.server import (
    client_error_request_for_step,
    resource_to_sep2_xml,
    submit_and_refetch_resource_for_step,
)
from cactus_client.error import CactusClientException
from cactus_client.model.context import AnnotationNamespace, ExecutionContext, StoredResourceAnnotations
from cactus_client.model.execution import ActionResult, StepExecution
from cactus_client.model.resource import StoredResource
from cactus_client.schema.validator import to_hex_binary
from cactus_client.time import utc_now

logger = logging.getLogger(__name__)


def get_edev_lfdi_for_der_control(
    step: StepExecution, context: ExecutionContext, der_ctl: StoredResource, der_control_href: Optional[str]
) -> Optional[str]:
    """Helper function to reduce duplicate code. Checks for a non None parent end device lfdi given a DER control"""
    resource_store = context.discovered_resources(step)
    edev = resource_store.get_ancestor_of(CSIPAusResource.EndDevice, der_ctl.id)
    if edev is None:
        context.warnings.log_step_warning(
            step,
            message=f"Could not find EndDevice parent for DERControl with href {der_control_href}",
        )
        return None

    edev_lfdi = cast(EndDeviceResponse, edev.resource).lFDI
    if edev_lfdi is None:
        context.warnings.log_step_warning(
            step,
            message=f"Could not find EndDevice lfdi parent for DERControl {der_control_href}",
        )
        return None

    return edev_lfdi


def determine_response_status(
    event_status: EventStatusType,
    annotations: StoredResourceAnnotations,
    der_control: DERControl,
    current_time: datetime,
) -> ResponseType | None:
    """
    Determines what response status to send based on server EventStatus and what we've already sent.

    Returns None if no response should be sent for the current state.
    """

    # If we have previously sent a cancelled or superseded response, no further response is necessary
    sent_cancelled = annotations.has_tag(AnnotationNamespace.RESPONSES, ResponseType.EVENT_CANCELLED)
    sent_superseded = annotations.has_tag(AnnotationNamespace.RESPONSES, ResponseType.EVENT_SUPERSEDED)
    if sent_cancelled or sent_superseded:
        return None

    # Cancelled
    if event_status == EventStatusType.Cancelled:
        return ResponseType.EVENT_CANCELLED

    # Superseded
    if event_status == EventStatusType.Superseded:
        return ResponseType.EVENT_SUPERSEDED

    # For Scheduled - send received
    sent_received = annotations.has_tag(AnnotationNamespace.RESPONSES, ResponseType.EVENT_RECEIVED)
    if event_status == EventStatusType.Scheduled and not sent_received:
        return ResponseType.EVENT_RECEIVED

    # For active - figure out where we are up to
    if event_status == EventStatusType.Active:
        if not sent_received:
            return ResponseType.EVENT_RECEIVED

        # See if the control is currently in progress
        event_start: int = der_control.interval.start
        event_duration = der_control.interval.duration
        event_end = event_start + event_duration
        current_timestamp = int(current_time.timestamp())

        if current_timestamp >= event_start:
            if not annotations.has_tag(AnnotationNamespace.RESPONSES, ResponseType.EVENT_STARTED):
                return ResponseType.EVENT_STARTED

        # Check if control should have completed
        # NOTE: Currently the discovery process will remove old controls, so this branch will not ever be accessed
        # A fix is in progress
        if current_timestamp >= event_end:
            if not annotations.has_tag(AnnotationNamespace.RESPONSES, ResponseType.EVENT_COMPLETED):
                return ResponseType.EVENT_COMPLETED

        return None  # Not yet started, or still ongoing

    return None


async def action_respond_der_controls(step: StepExecution, context: ExecutionContext) -> ActionResult:
    """Enumerates all known DERControls and sends a Response for any that require it."""

    resource_store = context.discovered_resources(step)

    stored_der_controls = [sr for sr in resource_store.get_for_type(CSIPAusResource.DERControl)]

    # Go through all DER controls to see if a response is required
    for der_ctl in stored_der_controls:
        der_control = cast(DERControl, der_ctl.resource)

        # Filter for DERControls that require a response
        # Both reply to and response required must be set. If neither, pass silently, if only one, add warning)
        reply_to = der_control.replyTo
        response_req = der_control.responseRequired

        if reply_to is None and response_req is None:
            continue

        if reply_to is None or response_req is None:
            context.warnings.log_step_warning(
                step,
                message=f"""Both reply to and response required should be set or both empty.
                Found reply to: {reply_to}, response required: {response_req}""",
            )
            continue

        # Figure out what response to send using event status, and check if we have already sent a response
        status = EventStatusType(der_control.EventStatus_.currentStatus)

        der_ctl_annotations = context.resource_annotations(step, der_ctl.id)
        current_time = utc_now()
        response_status = determine_response_status(status, der_ctl_annotations, der_control, current_time)

        # If None, we've already sent all applicable responses
        if response_status is None:
            continue

        # Find the matching device lfdi
        edev_lfdi = get_edev_lfdi_for_der_control(step, context, der_ctl, der_control.href)
        if edev_lfdi is None:
            continue  # Already a warning set in the function, just dont sent a response

        # Send the response
        response = DERControlResponse(
            endDeviceLFDI=edev_lfdi,
            status=response_status,
            createdDateTime=int(current_time.timestamp()),
            subject=der_control.mRID,
        )

        await submit_and_refetch_resource_for_step(
            DERControlResponse, step, context, HTTPMethod.POST, reply_to, response
        )

        # Update tags to track this response was sent
        der_ctl_annotations.add_tag(AnnotationNamespace.RESPONSES, response_status)

    return ActionResult.done()


async def action_send_malformed_response(
    resolved_parameters: dict[str, Any], step: StepExecution, context: ExecutionContext
) -> ActionResult:
    """
    Sends a malformed Response (using the most recent DERControl replyTo) - expects a failure response.

    Parameters:
        mrid_unknown: include mRID for DERControl that does not exist
        endDeviceLFDI_unknown: include LFDI for an EndDevice that does not exist
        response_invalid: post back control response = 15 (reserved)
    """

    resource_store = context.discovered_resources(step)

    # Extract resolved params
    mrid_unknown: bool = resolved_parameters["mrid_unknown"]
    endDeviceLFDI_unknown: bool = resolved_parameters["endDeviceLFDI_unknown"]
    response_invalid: bool = resolved_parameters["response_invalid"]

    # At least one parameter should be true
    if not mrid_unknown and not endDeviceLFDI_unknown and not response_invalid:
        raise CactusClientException(
            "Expected at least one of mrid_unknown, endDeviceLFDI_unknown, or response_invalid to be true."
        )

    # Find for DERControls that have replyTo set
    stored_der_controls = [sr for sr in resource_store.get_for_type(CSIPAusResource.DERControl)]
    der_controls_with_reply = [sr for sr in stored_der_controls if cast(DERControl, sr.resource).replyTo is not None]

    if not der_controls_with_reply:
        raise CactusClientException("No DERControls found with replyTo set. Cannot send malformed response.")

    # Get the most recent one
    most_recent_der_ctl = der_controls_with_reply[-1]
    der_control = cast(DERControl, most_recent_der_ctl.resource)
    reply_to = der_control.replyTo

    # Determine the endDeviceLFDI_unknown (either go find it, or set to fake one)
    edev_lfdi = (
        to_hex_binary(999999)
        if endDeviceLFDI_unknown
        else get_edev_lfdi_for_der_control(step, context, most_recent_der_ctl, der_control.href)
    )
    if edev_lfdi is None:
        raise CactusClientException(f"Could not find EndDevice lfdi parent for DERControl {der_control.href}")

    # Determine the mRID (generate fake if mrd_unknown true)
    subject_mrid = "0xFFFFFFFF" if mrid_unknown else der_control.mRID

    # Create the malformed response
    current_time = utc_now()
    response = DERControlResponse(
        endDeviceLFDI=edev_lfdi,
        status=ResponseType.EVENT_RECEIVED,  # Will edit XML if needed
        createdDateTime=int(current_time.timestamp()),
        subject=subject_mrid,
    )
    response_xml = resource_to_sep2_xml(response)

    # Malform the XML if needed to set status to 15
    if response_invalid:
        response_xml = re.sub(r"<status>.*?</status>", r"<status>15</status>", response_xml)

    # Send the malformed response and expect a client error
    await client_error_request_for_step(step, context, cast(str, reply_to), HTTPMethod.POST, response_xml)

    return ActionResult.done()
