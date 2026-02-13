from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

from . import lp_rpc_pb2
from .capabilities import CapabilityId, build_capabilities
from .control import Control
from .errors import LivepeerGatewayError, SkipPaymentCycle
from .events import Events
from .media_output import LagPolicy, MediaOutput
from .media_publish import MediaPublish, MediaPublishConfig
from .orchestrator import SelectOrchestrator, _http_origin, post_json
from .remote_signer import PaymentSession

_LOG = logging.getLogger(__name__)


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
    _last_payment_at: float = field(default=0.0, repr=False, compare=False)

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

    async def send_payment_if_due(self, interval_s: float = 5.0) -> None:
        """
        Send payment inline if enough time has elapsed.
        """
        if not self._payment_session:
            return
        interval_s = max(0.0, float(interval_s))
        now = time.monotonic()
        if now - self._last_payment_at < interval_s:
            return
        try:
            await asyncio.to_thread(self._payment_session.send_payment)
            object.__setattr__(self, "_last_payment_at", now)
        except SkipPaymentCycle as e:
            _LOG.debug(
                "Payment sender: skipping payment for manifest_id=%s (%s)",
                self.manifest_id,
                e,
            )
        except Exception:
            _LOG.exception(
                "Payment sender: failed for manifest_id=%s",
                self.manifest_id,
            )

    async def close(self) -> None:
        """
        Close any nested helpers (control, media, payment sender, etc)
        best-effort.
        """
        tasks = []
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


def start_lv2v(
    orch_url: Optional[Sequence[str] | str],
    req: StartJobRequest,
    *,
    signer_url: Optional[str] = None,
    discovery_url: Optional[str] = None,
) -> LiveVideoToVideo:
    """
    Start a live video-to-video job.

    Selects an orchestrator with LV2V capability and calls
    POST {info.transcoder}/live-video-to-video with JSON body.
    """
    if not req.model_id:
        raise LivepeerGatewayError("start_lv2v requires model_id")

    capabilities = build_capabilities(CapabilityId.LIVE_VIDEO_TO_VIDEO, req.model_id)
    _, info = SelectOrchestrator(
        orch_url,
        signer_url=signer_url,
        discovery_url=discovery_url,
        capabilities=capabilities,
    )

    session = PaymentSession(
        signer_url,
        info,
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
    return job
