from __future__ import annotations

import asyncio
import time
from typing import Optional

from . import lp_rpc_pb2
from .errors import LivepeerGatewayError, SessionRefreshRequired
from .live_payment import LivePaymentSender, LivePaymentConfig
from .orchestrator import (
    GetPaymentResponse,
    GetPayment,
    LiveVideoToVideo,
    OrchestratorClient,
    StartJobRequest,
    _start_job_with_headers,
    build_capabilities,
    CAPABILITY_LIVE_VIDEO_TO_VIDEO,
)


class OrchestratorSession:
    """
    Cohesive session wrapper that reuses an OrchestratorClient, caches
    OrchestratorInfo, and coordinates payments/jobs with optional refresh.

    If live_payment_config is provided and a signer_url is set, the session
    will automatically send periodic payments to the orchestrator to keep
    the session funded during streaming.
    """

    def __init__(
        self,
        orch_url: str,
        *,
        signer_url: Optional[str] = None,
        max_age_s: Optional[float] = None,
        live_payment_config: Optional[LivePaymentConfig] = None,
    ) -> None:
        self._orch_url = orch_url
        self._signer_url = signer_url
        self._max_age_s = max_age_s
        self._live_payment_config = live_payment_config

        self._client = OrchestratorClient(orch_url, signer_url=signer_url)
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
        caps: Optional[lp_rpc_pb2.Capabilities] = None,
    ) -> lp_rpc_pb2.OrchestratorInfo:
        """
        Return cached OrchestratorInfo, refreshing if missing/expired or forced.
        """
        if not force and caps is None and self._is_info_valid():
            assert self._info is not None
            return self._info

        info = self._client.GetOrchestratorInfo(caps=caps)
        self._info = info
        self._info_fetched_at = time.monotonic()
        return info

    def refresh(
        self,
        *,
        caps: Optional[lp_rpc_pb2.Capabilities] = None,
    ) -> lp_rpc_pb2.OrchestratorInfo:
        """
        Force refresh of orchestrator info regardless of cache age.
        """
        return self.ensure_info(force=True, caps=caps)

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
        max_refresh_attempts = 3
        caps = build_capabilities(CAPABILITY_LIVE_VIDEO_TO_VIDEO, model_id) if typ == "lv2v" and model_id else None

        for attempt in range(max_refresh_attempts + 1):
            # On first attempt, force refresh if caps provided (ensures capability-specific pricing)
            # On retry attempts after 480, always force refresh
            force = (attempt > 0) or (caps is not None)
            info = self.ensure_info(force=force, caps=caps)
            try:
                return GetPayment(self._signer_url, info, typ=typ, model_id=model_id)
            except SessionRefreshRequired:
                if attempt >= max_refresh_attempts:
                    raise LivepeerGatewayError(
                        f"Too many consecutive session refreshes ({max_refresh_attempts})"
                    )
                self.invalidate()
                # Loop continues with refreshed info
            except LivepeerGatewayError:
                # Non-480 errors: try once with fresh info (existing behavior)
                if attempt == 0:
                    self.invalidate()
                    continue
                raise

        raise LivepeerGatewayError("Unexpected: exhausted retry loop")

    def start_job(
        self,
        req: StartJobRequest,
        *,
        typ: str = "lv2v",
    ) -> LiveVideoToVideo:
        """
        Start a job using cached/refreshable OrchestratorInfo and payment.

        If live_payment_config is set and a signer_url is provided, a background
        payment sender will be started to keep the session funded during streaming.

        IMPORTANT: The initial payment's state is passed to the LivePaymentSender
        so it can continue with the correct ticket nonces. Without this, subsequent
        payments would reuse nonces and be rejected by the orchestrator.
        """
        caps = build_capabilities(CAPABILITY_LIVE_VIDEO_TO_VIDEO, req.model_id) if req.model_id else None
        info = self.ensure_info(force=(caps is not None), caps=caps)
        payment = self.get_payment(typ=typ, model_id=req.model_id)
        headers: dict[str, Optional[str]] = {
            "Livepeer-Payment": payment.payment,
            "Livepeer-Segment": payment.seg_creds,
        }

        job = _start_job_with_headers(info, req, headers)
        self._jobs.append(job)

        # Start live payment sender if configured and signer is available
        if self._live_payment_config is not None and self._signer_url and req.model_id:
            manifest_id = job.manifest_id or req.request_id or "unknown"

            # Create a refresh callback that captures the model_id for capability-specific refresh
            model_id_for_refresh = req.model_id

            def refresh_callback() -> lp_rpc_pb2.OrchestratorInfo:
                caps = build_capabilities(CAPABILITY_LIVE_VIDEO_TO_VIDEO, model_id_for_refresh)
                return self.refresh(caps=caps)

            payment_sender = LivePaymentSender(
                self._signer_url,
                info,
                manifest_id,
                req.model_id,
                config=self._live_payment_config,
                # Pass the initial payment state so the sender continues with correct nonces
                initial_state=payment.state,
                # Pass refresh callback so sender can refresh on 480
                refresh_info_callback=refresh_callback,
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
