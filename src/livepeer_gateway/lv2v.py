from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

from . import lp_rpc_pb2
from .capabilities import CapabilityId, build_capabilities
from .control import Control
from .errors import (
    LivepeerGatewayError,
    NoOrchestratorAvailableError,
    OrchestratorRejection,
    SkipPaymentCycle,
)
from .events import Events
from .media_output import LagPolicy, MediaOutput
from .media_publish import MediaPublish, MediaPublishConfig
from .orchestrator import _http_origin, post_json
from .selection import orchestrator_selector
from .remote_signer import PaymentSession
from .trickle_subscriber import TrickleSubscriber

_LOG = logging.getLogger(__name__)


def _is_str_dict(v: object) -> bool:
    return isinstance(v, dict) and all(isinstance(k, str) and isinstance(val, str) for k, val in v.items())


def _parse_token(token: str) -> dict[str, Any]:
    try:
        decoded = base64.b64decode(token, validate=True)
    except (binascii.Error, ValueError) as e:
        raise LivepeerGatewayError("Invalid token: expected base64-encoded JSON") from e

    try:
        payload = json.loads(decoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise LivepeerGatewayError("Invalid token: expected UTF-8 JSON payload") from e

    if not isinstance(payload, dict):
        raise LivepeerGatewayError("Invalid token: payload must be a JSON object")

    signer = payload.get("signer")
    discovery = payload.get("discovery")
    if signer is not None and not isinstance(signer, str):
        raise LivepeerGatewayError("Invalid token: signer must be a string")
    if discovery is not None and not isinstance(discovery, str):
        raise LivepeerGatewayError("Invalid token: discovery must be a string")

    signer_headers = payload.get("signer_headers")
    discovery_headers = payload.get("discovery_headers")
    if signer_headers is not None and not _is_str_dict(signer_headers):
        raise LivepeerGatewayError("Invalid token: signer_headers must be a {string: string} object")
    if discovery_headers is not None and not _is_str_dict(discovery_headers):
        raise LivepeerGatewayError("Invalid token: discovery_headers must be a {string: string} object")

    return {
        "signer": signer,
        "discovery": discovery,
        "signer_headers": signer_headers,
        "discovery_headers": discovery_headers,
    }


@dataclass(frozen=True)
class StartJobRequest:
    # The ID of the Gateway request (for logging purposes).
    request_id: Optional[str] = None
    # ModelId Name of the pipeline to run in the live video to video job.
    model_id: Optional[str] = None
    # Params Initial parameters for the pipeline.
    params: Optional[dict[str, Any]] = None
    # StreamId The Stream ID (for logging purposes).
    stream_id: Optional[str] = None

    def to_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.request_id is not None:
            payload["gateway_request_id"] = self.request_id
        if self.model_id is not None:
            payload["model_id"] = self.model_id
        if self.params is not None:
            payload["params"] = self.params
        if self.stream_id is not None:
            payload["stream_id"] = self.stream_id
        return payload


@dataclass(frozen=True)
class LiveVideoToVideo:
    raw: dict[str, Any]
    manifest_id: Optional[str] = None
    publish_url: Optional[str] = None
    subscribe_url: Optional[str] = None
    control_url: Optional[str] = None
    events_url: Optional[str] = None
    orchestrator_info: Optional[lp_rpc_pb2.OrchestratorInfo] = None
    control: Optional[Control] = None
    events: Optional[Events] = None
    _media: Optional[MediaPublish] = field(default=None, repr=False, compare=False)
    _payment_session: Optional["PaymentSession"] = field(default=None, repr=False, compare=False)
    _payment_task: Optional[asyncio.Task] = field(default=None, repr=False, compare=False)

    @staticmethod
    def from_json(
        data: dict[str, Any],
        *,
        orchestrator_info: Optional[lp_rpc_pb2.OrchestratorInfo] = None,
        payment_session: Optional["PaymentSession"] = None,
    ) -> "LiveVideoToVideo":
        control_url = data.get("control_url") if isinstance(data.get("control_url"), str) else None
        control = Control(control_url) if control_url else None
        publish_url = data.get("publish_url") if isinstance(data.get("publish_url"), str) else None
        events_url = data.get("events_url") if isinstance(data.get("events_url"), str) else None
        events = Events(events_url) if events_url else None
        return LiveVideoToVideo(
            raw=data,
            control_url=control_url,
            events_url=events_url,
            manifest_id=data.get("manifest_id") if isinstance(data.get("manifest_id"), str) else None,
            publish_url=publish_url,
            subscribe_url=data.get("subscribe_url") if isinstance(data.get("subscribe_url"), str) else None,
            orchestrator_info=orchestrator_info,
            control=control,
            events=events,
            _payment_session=payment_session,
        )

    def start_media(self, config: MediaPublishConfig) -> MediaPublish:
        """
        Instantiate and return a MediaPublish helper for this job.
        """
        if not self.publish_url:
            raise LivepeerGatewayError("No publish_url present on this LiveVideoToVideo job")
        if self._media is None:
            media = MediaPublish(
                self.publish_url,
                mime_type=config.mime_type,
                keyframe_interval_s=config.keyframe_interval_s,
                fps=config.fps,
            )
            object.__setattr__(self, "_media", media)
        return self._media

    def media_output(
        self,
        *,
        start_seq: int = -2,
        max_retries: int = 5,
        max_segment_bytes: Optional[int] = None,
        connection_close: bool = False,
        chunk_size: int = 64 * 1024,
        max_segments: int = 5,
        on_lag: LagPolicy = LagPolicy.LATEST,
    ) -> MediaOutput:
        """
        Convenience helper to create a `MediaOutput` for this job.

        This uses `subscribe_url` from the job response and raises if missing.
        Subscription tuning is configured once here (and stored on the returned `MediaOutput`).
        """
        if not self.subscribe_url:
            raise LivepeerGatewayError("No subscribe_url present on this LiveVideoToVideo job")
        return MediaOutput(
            self.subscribe_url,
            start_seq=start_seq,
            max_retries=max_retries,
            max_segment_bytes=max_segment_bytes,
            connection_close=connection_close,
            chunk_size=chunk_size,
            max_segments=max_segments,
            on_lag=on_lag,
        )

    @property
    def payment_session(self) -> Optional["PaymentSession"]:
        """
        Access the PaymentSession for this job, if available.
        """
        return self._payment_session

    def start_payment_sender(self) -> Optional[asyncio.Task]:
        """
        Start the background payment sender if a payment session and
        subscribe_url are available and the task isn't already running.

        Requires a running asyncio event loop.  If called from sync code
        (no running loop), logs a warning and returns ``None``.  The
        method is idempotent -- safe to call multiple times.

        Returns the ``asyncio.Task``, or ``None`` if payments could not
        be started.
        """
        if getattr(self, "_payment_task", None) is not None:
            return self._payment_task
        if not self._payment_session or not self.subscribe_url:
            return None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            _LOG.warning(
                "No running event loop; per-segment payments not started. "
                "Call job.start_payment_sender() from async code to enable."
            )
            return None
        task = loop.create_task(
            _payment_sender(self.subscribe_url, self._payment_session)
        )
        object.__setattr__(self, "_payment_task", task)
        return task

    async def close(self) -> None:
        """
        Close any nested helpers (control, media, payment sender, etc)
        best-effort.
        """
        tasks = []
        payment_task = getattr(self, "_payment_task", None)
        if payment_task is not None and not payment_task.done():
            payment_task.cancel()
            tasks.append(payment_task)
        if self.control is not None:
            tasks.append(self.control.close_control())
        if self._media is not None:
            tasks.append(self._media.close())
        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError
            ):
                raise result


async def _payment_sender(
    subscribe_url: str,
    session: PaymentSession,
) -> None:
    """
    Send one payment per trickle output segment.

    Uses a long-lived :class:`TrickleSubscriber` with
    ``connection_close=True``.  Reads only headers (zero body bytes) per
    segment, sends the payment, then closes the segment.  The built-in
    preconnect prefetches the next segment while the payment is in
    flight.

    Runs until the trickle stream ends or the task is cancelled.
    Payment errors are logged but do not stop the loop.
    """
    last_payment_at = 0.0
    async with TrickleSubscriber(
        subscribe_url,
        connection_close=True,
    ) as subscriber:
        while True:
            segment = await subscriber.next()
            if segment is None:
                _LOG.debug("Payment sender: stream ended for %s", subscribe_url)
                return
            seq = segment.seq()
            try:
                now = time.monotonic()
                if now - last_payment_at >= 5.0:
                    _LOG.debug("Payment sender: sending payment for seq=%s", seq)
                    # TODO make async-native
                    await asyncio.to_thread(session.send_payment)
                    last_payment_at = now
            except SkipPaymentCycle as e:
                _LOG.debug(
                    "Payment sender: skipping payment for seq=%s (%s)",
                    seq,
                    e,
                )
            except Exception:
                _LOG.exception("Payment sender: failed for seq=%s", seq)
            finally:
                await segment.close()


def start_lv2v(
    orch_url: Optional[Sequence[str] | str],
    req: StartJobRequest,
    *,
    token: Optional[str] = None,
    signer_url: Optional[str] = None,
    signer_headers: Optional[dict[str, str]] = None,
    discovery_url: Optional[str] = None,
    discovery_headers: Optional[dict[str, str]] = None,
) -> LiveVideoToVideo:
    """
    Start a live video-to-video job.

    Selects an orchestrator with LV2V capability and calls
    POST {info.transcoder}/live-video-to-video with JSON body.

    If called from within a running asyncio event loop, a background
    task is automatically started to send per-segment payments.
    Otherwise a warning is logged and payments can be started later
    via ``job.start_payment_sender()``.

    Optional ``token`` can be provided as a base64-encoded JSON object.
    Explicit keyword arguments taken precedence over token values.

    Discovery precedence (highest -> lowest):
    1) explicit ``orch_url`` list
    2) explicit ``discovery_url`` argument
    3) token ``discovery`` value
    4) remote signer discovery endpoint derived from the resolved signer URL
    """
    if not req.model_id:
        raise LivepeerGatewayError("start_lv2v requires model_id")

    resolved_signer_url = signer_url
    resolved_signer_headers = signer_headers
    resolved_discovery_url = discovery_url
    resolved_discovery_headers = discovery_headers
    if token is not None:
        token_data = _parse_token(token)
        if resolved_signer_url is None:
            resolved_signer_url = token_data.get("signer")
        if resolved_signer_headers is None:
            resolved_signer_headers = token_data.get("signer_headers")
        if resolved_discovery_url is None:
            resolved_discovery_url = token_data.get("discovery")
        if resolved_discovery_headers is None:
            resolved_discovery_headers = token_data.get("discovery_headers")

    capabilities = build_capabilities(CapabilityId.LIVE_VIDEO_TO_VIDEO, req.model_id)
    # Orchestrator discovery precedence:
    # orch_url -> discovery_url -> signer_url
    cursor = orchestrator_selector(
        orch_url,
        signer_url=resolved_signer_url,
        signer_headers=resolved_signer_headers,
        discovery_url=resolved_discovery_url,
        discovery_headers=resolved_discovery_headers,
        capabilities=capabilities,
    )

    start_rejections: list[OrchestratorRejection] = []
    while True:
        try:
            selected_url, info = cursor.next()
        except NoOrchestratorAvailableError as e:
            # No more successful OrchestratorInfo responses remain.
            # Surface a single aggregated "all orchestrators failed" error.
            all_rejections = list(e.rejections) + start_rejections
            if all_rejections:
                raise NoOrchestratorAvailableError(
                    f"All orchestrators failed ({len(all_rejections)} tried)",
                    rejections=all_rejections,
                ) from None
            raise

        try:
            session = PaymentSession(
                resolved_signer_url,
                info,
                signer_headers=resolved_signer_headers,
                type="lv2v",
                capabilities=capabilities,
            )
            p = session.get_payment()
            headers: dict[str, str] = {
                "Livepeer-Payment": p.payment,
                "Livepeer-Segment": p.seg_creds,
            }

            base = _http_origin(info.transcoder)
            url = f"{base}/live-video-to-video"
            data = post_json(url, req.to_json(), headers=headers)
            job = LiveVideoToVideo.from_json(
                data,
                orchestrator_info=info,
                payment_session=session,
            )
            if not job.manifest_id:
                raise LivepeerGatewayError("LiveVideoToVideo response missing manifest_id")
            session.set_manifest_id(job.manifest_id)
            job.start_payment_sender()
            return job
        except LivepeerGatewayError as e:
            _LOG.debug(
                "start_lv2v candidate failed, trying fallback if available: %s (%s)",
                selected_url,
                str(e),
            )
            start_rejections.append(OrchestratorRejection(url=selected_url, reason=str(e)))
