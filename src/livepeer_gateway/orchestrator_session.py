"""
Cohesive session wrapper that caches OrchestratorInfo, coordinates
payments/jobs, and optionally runs a background payment sender.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, Sequence

from . import lp_rpc_pb2
from .capabilities import CapabilityId, build_capabilities
from .errors import LivepeerGatewayError
from .live_payment import LivePaymentConfig, LivePaymentSender
from .lv2v import LiveVideoToVideo, StartJobRequest
from .orchestrator import SelectOrchestrator, _http_origin, post_json
from .remote_signer import GetPaymentResponse, PaymentSession

_LOG = logging.getLogger(__name__)


class OrchestratorSession:
    """
    Cohesive session wrapper that caches OrchestratorInfo and coordinates
    payments/jobs with optional refresh.

    Supports orchestrator discovery and selection: pass a list of orchestrator
    URLs (or a comma-delimited string), a ``discovery_url``, or rely on
    ``{signer_url}/discover-orchestrators``.  On each refresh (cache miss,
    expiry, or forced), ``SelectOrchestrator`` re-runs discovery and parallel
    health probing so the session can fail over to a different orchestrator.

    If ``live_payment_config`` is provided and a ``signer_url`` is set, the
    session will automatically send periodic payments to the orchestrator
    to keep the session funded during streaming.
    """

    def __init__(
        self,
        orchestrators: Optional[Sequence[str] | str] = None,
        *,
        signer_url: Optional[str] = None,
        discovery_url: Optional[str] = None,
        max_age_s: Optional[float] = None,
        live_payment_config: Optional[LivePaymentConfig] = None,
    ) -> None:
        self._orchestrators = orchestrators
        self._signer_url = signer_url
        self._discovery_url = discovery_url
        self._max_age_s = max_age_s
        self._live_payment_config = live_payment_config

        self._orch_url: Optional[str] = None
        self._info: Optional[lp_rpc_pb2.OrchestratorInfo] = None
        self._info_fetched_at: Optional[float] = None
        self._jobs: list[LiveVideoToVideo] = []
        self._payment_senders: list[LivePaymentSender] = []

    def _is_info_valid(self) -> bool:
        if self._info is None:
            return False
        if self._max_age_s is None or self._info_fetched_at is None:
            return True
        return (time.monotonic() - self._info_fetched_at) <= self._max_age_s

    def invalidate(self) -> None:
        """
        Drop cached orchestrator info so the next call refreshes it.
        """
        self._info = None
        self._info_fetched_at = None

    def ensure_info(
        self,
        *,
        force: bool = False,
        capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
    ) -> lp_rpc_pb2.OrchestratorInfo:
        """
        Return cached OrchestratorInfo, refreshing if missing/expired or forced.

        Runs ``SelectOrchestrator`` which discovers candidates and probes them
        in parallel, selecting the first healthy one.  The selected orchestrator
        URL is remembered for subsequent operations (e.g. starting jobs).
        """
        if not force and capabilities is None and self._is_info_valid():
            assert self._info is not None
            return self._info

        orch_url, info = SelectOrchestrator(
            self._orchestrators,
            signer_url=self._signer_url,
            discovery_url=self._discovery_url,
            capabilities=capabilities,
        )
        self._orch_url = orch_url
        self._info = info
        self._info_fetched_at = time.monotonic()
        return info

    def refresh(
        self,
        *,
        capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
    ) -> lp_rpc_pb2.OrchestratorInfo:
        """
        Force refresh of orchestrator info regardless of cache age.
        """
        return self.ensure_info(force=True, capabilities=capabilities)

    def get_payment(
        self,
        *,
        typ: str = "lv2v",
        model_id: Optional[str] = None,
    ) -> GetPaymentResponse:
        """
        Fetch payment credentials, refreshing orchestrator info on 480.

        Matches go-livepeer behavior: max 3 consecutive session refreshes when
        the remote signer returns HTTP 480 (HTTPStatusRefreshSession), indicating
        that ticket params or auth token have expired.
        """
        caps = (
            build_capabilities(CapabilityId.LIVE_VIDEO_TO_VIDEO, model_id)
            if typ == "lv2v" and model_id
            else None
        )

        # On first call, force refresh if caps provided (ensures capability-specific pricing)
        force = caps is not None
        info = self.ensure_info(force=force, capabilities=caps)

        session = PaymentSession(
            self._signer_url,
            info,
            type=typ,
            model_id=model_id,
            capabilities=caps,
        )
        return session.get_payment()

    def start_job(
        self,
        req: StartJobRequest,
        *,
        typ: str = "lv2v",
    ) -> LiveVideoToVideo:
        """
        Start a job using cached/refreshable OrchestratorInfo and payment.

        If ``live_payment_config`` is set and a ``signer_url`` is provided, a
        background payment sender will be started to keep the session funded
        during streaming.
        """
        caps = (
            build_capabilities(CapabilityId.LIVE_VIDEO_TO_VIDEO, req.model_id)
            if req.model_id
            else None
        )
        info = self.ensure_info(force=(caps is not None), capabilities=caps)

        # Create a PaymentSession for this job
        session = PaymentSession(
            self._signer_url,
            info,
            type=typ,
            model_id=req.model_id,
            capabilities=caps,
        )
        p = session.get_payment()
        headers: dict[str, Optional[str]] = {
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
        self._jobs.append(job)

        # Set manifest_id on the session for subsequent payments
        if job.manifest_id:
            session.set_manifest_id(job.manifest_id)

        # Start live payment sender if configured and signer is available
        if self._live_payment_config is not None and self._signer_url and req.model_id:
            payment_sender = LivePaymentSender(
                session,
                config=self._live_payment_config,
            )
            payment_sender.start()
            self._payment_senders.append(payment_sender)

        return job

    async def aclose(self) -> None:
        """
        Async close for any created LiveVideoToVideo helpers and payment senders.
        """
        # Stop payment senders first
        if self._payment_senders:
            stop_tasks = [sender.stop() for sender in self._payment_senders]
            await asyncio.gather(*stop_tasks, return_exceptions=True)
            self._payment_senders.clear()

        # Close jobs
        if self._jobs:
            tasks = [job.close() for job in self._jobs]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            self._jobs.clear()

    def close(self) -> None:
        """
        Synchronous close convenience wrapper around aclose().
        """
        if not self._jobs and not self._payment_senders:
            return
        try:
            asyncio.run(self.aclose())
        except RuntimeError as e:
            if "asyncio.run()" in str(e):
                # Running inside an existing loop; caller should await aclose().
                raise LivepeerGatewayError(
                    "OrchestratorSession.close() cannot run inside an active event loop; "
                    "await OrchestratorSession.aclose() instead."
                ) from e
            raise
