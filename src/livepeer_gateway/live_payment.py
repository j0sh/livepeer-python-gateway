"""
Live payment processor for continuous payment during video streaming.

Periodically calls PaymentSession.send_payment() to generate and forward
payment credentials to the orchestrator's /payment endpoint.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

from .errors import SkipPaymentCycle
from .remote_signer import PaymentSession

_LOG = logging.getLogger(__name__)

# Default video parameters for pixel calculation (matches go-livepeer)
DEFAULT_WIDTH = 1280
DEFAULT_HEIGHT = 720
DEFAULT_FPS = 30.0

# Default payment interval in seconds
DEFAULT_PAYMENT_INTERVAL = 5.0


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
    1. Every ``interval_s`` seconds, send a payment via the PaymentSession
    2. The PaymentSession internally handles state round-tripping (ticket nonces),
       session refresh on HTTP 480, and forwarding to the orchestrator

    The PaymentSession manages the opaque signer state that ensures unique
    ticket nonces across consecutive payments.
    """

    def __init__(
        self,
        payment_session: PaymentSession,
        *,
        config: Optional[LivePaymentConfig] = None,
    ) -> None:
        self._session = payment_session
        self._config = config or LivePaymentConfig()
        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._running = False
        self._error: Optional[BaseException] = None

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
        _LOG.info(
            "LivePaymentSender started: interval=%.1fs",
            self._config.interval_s,
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
                _LOG.error("LivePaymentSender error: %s", e, exc_info=True)
                self._error = e
                # Continue trying - don't stop on payment errors

        _LOG.info("LivePaymentSender stopped")

    async def _send_payment(self) -> None:
        """Send a single payment to the orchestrator via PaymentSession."""
        try:
            # Run blocking send_payment in thread pool
            await asyncio.to_thread(self._session.send_payment)
            _LOG.debug("Payment sent successfully")
        except SkipPaymentCycle as e:
            _LOG.debug("Payment cycle skipped: %s", e)
        except Exception as e:
            _LOG.error("Failed to send payment: %s", e)
            raise
