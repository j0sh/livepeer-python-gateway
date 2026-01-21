from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from typing import Any, AsyncIterator, Optional

from .errors import LivepeerGatewayError
from .trickle_subscriber import TrickleSubscriber


class Events:
    def __init__(self, events_url: str) -> None:
        self.events_url = events_url

    def __call__(
        self,
        *,
        start_seq: int = -2,
        max_retries: int = 5,
        max_buffered_events: int = 256,
        max_event_bytes: int = 1_048_576,
        overflow: str = "drop_oldest",
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Subscribe to the trickle events channel.

        Each yielded item is a decoded JSON object (dict). The underlying network
        subscription starts lazily on first iteration.

        max_event_bytes applies per segment (per JSON message), not across the entire stream.
        """
        url = self.events_url

        async def _read_all(segment: "SegmentReader", *, chunk_size: int = 33 * 1024) -> bytes:
            parts = []
            try:
                while True:
                    chunk = await segment.read(chunk_size=chunk_size)
                    if not chunk:
                        break
                    parts.append(chunk)
            finally:
                await segment.close()
            return b"".join(parts)

        async def _iter() -> AsyncIterator[dict[str, Any]]:
            if max_buffered_events < 1:
                raise ValueError("max_buffered_events must be >= 1")
            if max_event_bytes < 1:
                raise ValueError("max_event_bytes must be >= 1")
            if overflow not in ("error", "drop_oldest", "drop_newest"):
                raise ValueError("overflow must be one of: error, drop_oldest, drop_newest")

            queue: asyncio.Queue[object] = asyncio.Queue(maxsize=max_buffered_events)
            error: Optional[BaseException] = None
            end = object()

            def enqueue_end() -> None:
                while True:
                    try:
                        queue.put_nowait(end)
                        return
                    except asyncio.QueueFull:
                        try:
                            _ = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            continue

            def enqueue_event(data: dict[str, Any]) -> None:
                try:
                    queue.put_nowait(data)
                    return
                except asyncio.QueueFull:
                    if overflow == "drop_newest":
                        logging.warning("Trickle events queue full; dropping newest event")
                        return
                    if overflow == "drop_oldest":
                        try:
                            _ = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                        try:
                            queue.put_nowait(data)
                        except asyncio.QueueFull:
                            logging.warning(
                                "Trickle events queue full; dropping newest event after drop_oldest"
                            )
                        return
                    raise LivepeerGatewayError(
                        "Trickle events queue is full; increase max_buffered_events or consume faster"
                    )

            async def producer() -> None:
                nonlocal error
                try:
                    async with TrickleSubscriber(
                        url,
                        start_seq=start_seq,
                        max_retries=max_retries,
                        max_bytes=max_event_bytes,
                    ) as subscriber:
                        while True:
                            segment = await subscriber.next()
                            if segment is None:
                                break
                            payload = await _read_all(segment)
                            if not payload:
                                raise LivepeerGatewayError("Trickle event segment was empty")

                            try:
                                data = json.loads(payload.decode("utf-8"))
                            except Exception as e:
                                snippet = payload[:256].decode("utf-8", errors="replace")
                                raise LivepeerGatewayError(
                                    f"Trickle event JSON decode failed: {e} (payload={snippet!r})"
                                ) from e

                            if not isinstance(data, dict):
                                raise LivepeerGatewayError(
                                    f"Trickle event must be JSON, got {type(data).__name__}"
                                )

                            enqueue_event(data)
                except Exception as e:
                    error = e if isinstance(e, LivepeerGatewayError) else LivepeerGatewayError(
                        f"Trickle events subscription error: {e.__class__.__name__}: {e}"
                    )
                finally:
                    enqueue_end()

            task = asyncio.create_task(producer())
            try:
                while True:
                    item = await queue.get()
                    if item is end:
                        if error:
                            raise error
                        break
                    yield item  # type: ignore[misc]
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

        return _iter()

