"""
Live payment processor for continuous payment during video streaming.

Periodically calls the remote signer's /generate-live-payment endpoint
and forwards payment credentials to the orchestrator's /payment endpoint.
"""
from __future__ import annotations

import asyncio
import logging
import ssl
import time
from dataclasses import dataclass
from typing import Callable, Optional
from urllib.request import Request, urlopen

from . import lp_rpc_pb2
from .errors import LivepeerGatewayError, SessionRefreshRequired
from .orchestrator import (
    GetPayment,
    PaymentState,
    _http_origin,
)

# Type alias for the refresh callback
RefreshInfoCallback = Callable[[], lp_rpc_pb2.OrchestratorInfo]

# Maximum consecutive session refresh attempts (matches go-livepeer)
MAX_REFRESH_ATTEMPTS = 3

# Default video parameters for pixel calculation (matches go-livepeer)
DEFAULT_WIDTH = 1280
DEFAULT_HEIGHT = 720
DEFAULT_FPS = 30.0

# Default payment interval in seconds
DEFAULT_PAYMENT_INTERVAL = 5.0

# Timeout for payment requests
PAYMENT_REQUEST_TIMEOUT = 60.0


@dataclass
class LivePaymentConfig:
    """Configuration for live payment processing."""
    interval_s: float = DEFAULT_PAYMENT_INTERVAL
    width: int = DEFAULT_WIDTH
    height: int = DEFAULT_HEIGHT
    fps: float = DEFAULT_FPS


class LivePaymentSender:
    """
    Periodically sends payments to keep a live video session funded.

    This mirrors go-livepeer's LivePaymentProcessor behavior:
    1. Every `interval_s` seconds, calculate pixels processed since last payment
    2. Call remote signer's /generate-live-payment to get fresh payment credentials
    3. Forward credentials to orchestrator's /payment endpoint

    IMPORTANT: The remote signer returns an opaque 'state' blob that contains
    ticket nonce information. This state MUST be sent with subsequent payment
    requests to ensure unique ticket nonces. Without proper state tracking,
    the orchestrator will reject payments with "already seen nonce" errors.

    When the remote signer returns HTTP 480 (SessionRefreshRequired), the
    orchestrator info must be refreshed to get new ticket params before retrying.
    """

    def __init__(
        self,
        signer_url: str,
        orchestrator_info: lp_rpc_pb2.OrchestratorInfo,
        manifest_id: str,
        model_id: str,
        *,
        config: Optional[LivePaymentConfig] = None,
        initial_state: Optional[PaymentState] = None,
        refresh_info_callback: Optional[RefreshInfoCallback] = None,
    ) -> None:
        self._signer_url = signer_url
        self._orch_info = orchestrator_info
        self._manifest_id = manifest_id
        self._model_id = model_id
        self._config = config or LivePaymentConfig()
        self._refresh_info_callback = refresh_info_callback

        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._last_payment_time: Optional[float] = None
        self._running = False
        self._error: Optional[BaseException] = None

        # State tracking for ticket nonces - CRITICAL for payment acceptance
        self._payment_state: Optional[PaymentState] = initial_state

    @property
    def pixels_per_second(self) -> float:
        """Calculate pixels per second based on config."""
        return self._config.width * self._config.height * self._config.fps

    def start(self) -> None:
        """Start the background payment task."""
        if self._running:
            return

        self._running = True
        self._stop_event = asyncio.Event()
        self._last_payment_time = time.monotonic()
        self._task = asyncio.create_task(self._payment_loop())

    async def stop(self) -> None:
        """Stop the background payment task."""
        if not self._running:
            return

        self._running = False
        if self._stop_event:
            self._stop_event.set()

        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

    async def _payment_loop(self) -> None:
        """Main payment loop that runs in the background."""
        logging.info(
            "LivePaymentSender started: interval=%.1fs, manifest_id=%s",
            self._config.interval_s,
            self._manifest_id,
        )

        while self._running and self._stop_event and not self._stop_event.is_set():
            try:
                # Wait for the interval
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._config.interval_s,
                    )
                    # If we get here, stop was requested
                    break
                except asyncio.TimeoutError:
                    # Normal timeout - time to send payment
                    pass

                await self._send_payment()

            except Exception as e:
                logging.error("LivePaymentSender error: %s", e, exc_info=True)
                self._error = e
                # Continue trying - don't stop on payment errors

        logging.info("LivePaymentSender stopped: manifest_id=%s", self._manifest_id)

    async def _send_payment(self) -> None:
        """Send a single payment to the orchestrator."""
        now = time.monotonic()
        if self._last_payment_time is None:
            self._last_payment_time = now

        seconds_since_last = now - self._last_payment_time
        pixels_since_last = int(self.pixels_per_second * seconds_since_last)

        logging.debug(
            "Processing live payment: secs=%.2f, pixels=%d, manifest_id=%s",
            seconds_since_last,
            pixels_since_last,
            self._manifest_id,
        )

        # Get fresh payment credentials from remote signer
        # Handle 480 (SessionRefreshRequired) by refreshing orchestrator info and retrying
        payment_resp = await self._get_payment_with_refresh()

        # Update state from response for next payment
        # This is CRITICAL - without updating state, subsequent payments will reuse nonces
        if payment_resp.state is not None:
            self._payment_state = payment_resp.state
            logging.debug(
                "Updated payment state: manifest_id=%s",
                self._manifest_id,
            )

        # Forward payment to orchestrator's /payment endpoint
        await self._forward_payment_to_orchestrator(
            payment_resp.payment,
            payment_resp.seg_creds,
        )

        self._last_payment_time = now

    async def _get_payment_with_refresh(self):
        """
        Get payment from signer, handling 480 (SessionRefreshRequired) by
        refreshing orchestrator info and retrying.

        Matches go-livepeer behavior: max 3 consecutive session refreshes.
        """
        for attempt in range(MAX_REFRESH_ATTEMPTS + 1):
            try:
                return GetPayment(
                    self._signer_url,
                    self._orch_info,
                    typ="lv2v",
                    model_id=self._model_id,
                    state=self._payment_state,
                    manifest_id=self._manifest_id,
                )
            except SessionRefreshRequired:
                if attempt >= MAX_REFRESH_ATTEMPTS:
                    raise LivepeerGatewayError(
                        f"Too many consecutive session refreshes ({MAX_REFRESH_ATTEMPTS}) "
                        f"for manifest_id={self._manifest_id}"
                    )

                logging.info(
                    "Session refresh required (480), refreshing orchestrator info: "
                    "manifest_id=%s, attempt=%d",
                    self._manifest_id,
                    attempt + 1,
                )

                # Refresh orchestrator info using the callback
                if self._refresh_info_callback is None:
                    raise LivepeerGatewayError(
                        "Session refresh required but no refresh callback provided"
                    )

                try:
                    # Run refresh in thread pool since it's synchronous
                    self._orch_info = await asyncio.to_thread(self._refresh_info_callback)
                    logging.info(
                        "Orchestrator info refreshed: manifest_id=%s",
                        self._manifest_id,
                    )
                except Exception as e:
                    logging.error(
                        "Failed to refresh orchestrator info: %s, manifest_id=%s",
                        e,
                        self._manifest_id,
                    )
                    raise

                # Continue to retry with refreshed info

        raise LivepeerGatewayError("Unexpected: exhausted refresh retry loop")

    async def _forward_payment_to_orchestrator(
        self,
        payment: str,
        seg_creds: Optional[str],
    ) -> None:
        """Forward payment credentials to the orchestrator."""
        base_url = _http_origin(self._orch_info.transcoder)
        url = f"{base_url}/payment"

        headers = {
            "Livepeer-Payment": payment,
        }
        if seg_creds:
            headers["Livepeer-Segment"] = seg_creds

        # Run blocking HTTP request in thread pool
        def do_request() -> bytes:
            req = Request(url, data=b"", headers=headers, method="POST")
            ssl_ctx = ssl._create_unverified_context()

            with urlopen(req, timeout=PAYMENT_REQUEST_TIMEOUT, context=ssl_ctx) as resp:
                if resp.status != 200:
                    raise LivepeerGatewayError(
                        f"Orchestrator rejected payment: HTTP {resp.status}"
                    )
                return resp.read()

        try:
            response_data = await asyncio.to_thread(do_request)
            logging.debug(
                "Payment accepted by orchestrator: manifest_id=%s, response_len=%d",
                self._manifest_id,
                len(response_data),
            )
        except Exception as e:
            logging.error(
                "Failed to forward payment to orchestrator: %s, url=%s",
                e,
                url,
            )
            raise
