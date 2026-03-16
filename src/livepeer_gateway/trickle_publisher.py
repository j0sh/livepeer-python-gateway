from __future__ import annotations

import asyncio
import logging
from typing import Optional, AsyncIterator, Callable

import aiohttp

from .errors import LivepeerGatewayError


_LOG = logging.getLogger(__name__)


class TricklePublishError(LivepeerGatewayError):
    """Base error for trickle publisher failures."""


class TrickleSegmentWriteError(TricklePublishError):
    """A single segment write failed, but the publisher may still be usable."""

    def __init__(
        self,
        message: str,
        *,
        seq: int,
        url: Optional[str] = None,
        status: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.seq = seq
        self.url = url
        self.status = status


class TricklePublisherTerminalError(TricklePublishError):
    """The trickle publisher entered a terminal failure state."""

    def __init__(
        self,
        message: str,
        *,
        consecutive_failures: int,
        url: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.consecutive_failures = consecutive_failures
        self.url = url


class TricklePublisher:
    """
    Trickle publisher that streams bytes to a sequence of HTTP POST endpoints:
      - Create stream: POST {base_url}
      - Write segment: POST {base_url}/{seq} (streaming body)
      - Close stream: DELETE {base_url}

    The API matches the usage pattern:
        async with TricklePublisher(url, "application/json") as pub:
            async with await pub.next() as seg:
                await seg.write(b"...")
    """

    def __init__(
        self,
        url: str,
        mime_type: str,
        *,
        connection_close: bool = False,
        max_consecutive_failures: int = 3,
    ):
        self.url = url.rstrip("/")
        self.mime_type = mime_type
        self.connection_close = connection_close
        self.seq = 0
        self._max_consecutive_failures = max(1, int(max_consecutive_failures))

        # Lazily initialized async runtime bits (safe to construct in sync code).
        self._lock: Optional[asyncio.Lock] = None
        self._session: Optional[aiohttp.ClientSession] = None

        # Preconnected writer state for the next segment.
        self._next_state: Optional[_SegmentPostState] = None

        # Terminal failure for the whole publisher. Once set, no new segments
        # should be opened or written.
        self._terminal_error: Optional[TricklePublisherTerminalError] = None
        self._consecutive_failures: int = 0
        self._stats: dict[str, int] = {
            "segments_started": 0,
            "segments_completed": 0,
            "segments_failed": 0,
            "post_attempts": 0,
            "post_retries_no_body_consumed": 0,
            "post_success": 0,
            "post_http_failures": 0,
            "post_exceptions": 0,
            "post_404": 0,
            "segment_writer_put_timeouts": 0,
            "bytes_submitted_to_transport": 0,
            "terminal_failures": 0,
        }

    async def __aenter__(self) -> "TricklePublisher":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    async def _ensure_runtime(self) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        if self._session is None:
            # Ignore TLS validation (matches the rest of this repo).
            connector = aiohttp.TCPConnector(ssl=False)
            self._session = aiohttp.ClientSession(connector=connector)

    def _stream_url(self, seq: int) -> str:
        return f"{self.url}/{seq}"

    async def preconnect(self, seq: int) -> _SegmentPostState:
        """
        Start the POST for `seq` in the background and return mutable segment state.
        """
        await self._ensure_runtime()
        assert self._session is not None

        url = self._stream_url(seq)
        _LOG.debug("Trickle preconnect: %s", url)

        state = _SegmentPostState(seq)
        asyncio.create_task(self._run_post(url, state))
        return state

    async def _run_post(self, url: str, seg_state: _SegmentPostState) -> None:
        await self._ensure_runtime()
        assert self._session is not None

        seq = seg_state.seq
        final_exc: Optional[TrickleSegmentWriteError] = None
        final_status: Optional[int] = None
        final_body: Optional[str] = None

        for attempt in range(2):
            self._stats["post_attempts"] += 1
            seg_state.data_consumed = False
            headers = {"Content-Type": self.mime_type}
            if self.connection_close:
                headers["Connection"] = "close"
            try:
                # Intentionally do not set an overall aiohttp request timeout here.
                # A trickle segment may stay open for an arbitrary amount of time while
                # the producer is still streaming bytes, so a wall-clock POST timeout
                # would incorrectly fail healthy long-lived uploads. The bounded failure
                # path for stalled delivery is the per-chunk queue.put timeout in
                # SegmentWriter.write(), which detects when the HTTP client stops
                # consuming request-body data fast enough.
                resp = await self._session.post(
                    url,
                    headers=headers,
                    data=self._stream_data(seg_state),
                )
                final_status = resp.status
                final_body = await resp.text() if resp.status != 200 else None
                resp.release()
                if resp.status == 200:
                    self._consecutive_failures = 0
                    self._stats["post_success"] += 1
                    self._stats["segments_completed"] += 1
                    return
                self._stats["post_http_failures"] += 1
                final_exc = TrickleSegmentWriteError(
                    f"Trickle POST failed url={url} status={resp.status} body={final_body!r}",
                    seq=seq,
                    url=url,
                    status=resp.status,
                )
            except Exception as e:
                self._stats["post_exceptions"] += 1
                err = TrickleSegmentWriteError(
                    f"Trickle POST exception url={url}",
                    seq=seq,
                    url=url,
                )
                err.__cause__ = e
                final_exc = err
                final_status = None
                final_body = None

            if final_status == 404:
                # Stream doesn't exist on the server; fail fast and do not retry.
                self._stats["post_404"] += 1
                break

            if not seg_state.data_consumed and attempt == 0:
                self._stats["post_retries_no_body_consumed"] += 1
                _LOG.warning(
                    "Trickle POST retrying same segment url=%s (no request body consumed)",
                    url,
                )
                continue
            break

        if final_status is not None:
            _LOG.error("Trickle POST failed url=%s status=%s body=%r", url, final_status, final_body)
        else:
            _LOG.error("Trickle POST exception url=%s error=%s", url, final_exc)
        assert final_exc is not None
        self._record_segment_failure(final_exc, seg_state)
        if final_status == 404 and self._terminal_error is None:
            _LOG.error("Trickle publisher channel does not exist url=%s", self.url)
            terminal_exc = TricklePublisherTerminalError(
                "Trickle publisher channel does not exist",
                consecutive_failures=self._consecutive_failures,
                url=self.url,
            )
            terminal_exc.__cause__ = final_exc
            self._terminal_error = terminal_exc
            self._stats["terminal_failures"] += 1

    def _record_segment_failure(
        self,
        exc: TrickleSegmentWriteError,
        seg_state: _SegmentPostState,
    ) -> None:
        seg_state.error = exc
        self._stats["segments_failed"] += 1
        self._consecutive_failures += 1
        # check whether failure limit has been hit
        if self._terminal_error is None and self._consecutive_failures >= self._max_consecutive_failures:
            _LOG.error(
                "Trickle publisher reached terminal failure state after %s consecutive failures",
                self._consecutive_failures,
            )
            terminal_exc = TricklePublisherTerminalError(
                "Trickle publisher reached terminal failure state",
                consecutive_failures=self._consecutive_failures,
                url=self.url,
            )
            terminal_exc.__cause__ = exc
            self._terminal_error = terminal_exc
            self._stats["terminal_failures"] += 1

    async def _run_delete(self) -> None:
        await self._ensure_runtime()
        assert self._session is not None

        try:
            resp = await self._session.delete(self.url)
            resp.release()
        # Suppress any shutdown-time exceptions, including cancellation.
        except BaseException:
            _LOG.error("Trickle DELETE exception url=%s", self.url, exc_info=True)

    async def _stream_data(self, seg_state: _SegmentPostState) -> AsyncIterator[bytes]:
        while True:
            chunk = await seg_state.queue.get()
            if chunk is None:
                break
            seg_state.data_consumed = True
            yield chunk

    async def create(self) -> None:
        await self._ensure_runtime()
        assert self._session is not None

        resp = await self._session.post(
            self.url,
            headers={"Expect-Content": self.mime_type},
            data={},
        )
        if resp.status != 200:
            body = await resp.text()
            resp.release()
            raise ValueError(f"Trickle create failed: status={resp.status} body={body!r}")
        resp.release()

    async def next(self) -> "SegmentWriter":
        await self._ensure_runtime()
        assert self._lock is not None

        async with self._lock:
            if self._terminal_error is not None:
                raise self._terminal_error
            if self._next_state is None or self._next_state.seq != self.seq:
                # don't have queue, or a queue for the wrong seq
                self._next_state = await self.preconnect(self.seq)

            seg_state = self._next_state
            assert seg_state is not None
            self._next_state = None
            self._stats["segments_started"] += 1

            # Preconnect the next segment in the background.
            self.seq += 1
            asyncio.create_task(self._preconnect_task(self.seq))

        return SegmentWriter(
            seg_state,
            error_getter=lambda: self._terminal_error,
            on_write_bytes=self._record_write_bytes,
            on_write_timeout=self._record_write_timeout,
        )

    async def _preconnect_task(self, seq: int) -> None:
        await self._ensure_runtime()
        assert self._lock is not None

        # Hold the lock across preconnect so only one task can reserve and
        # publish the next-state slot for this sequence at a time.
        async with self._lock:
            if self._terminal_error is not None:
                return
            if self._next_state is not None:
                return
            if self.seq != seq:
                # seq is stale
                return
            self._next_state = await self.preconnect(seq)

    async def close(self) -> None:
        # If the publisher was never used, avoid creating a session just to close it.
        if self._session is None and self._lock is None and self._next_state is None:
            return

        try:
            await self._ensure_runtime()
        # Close is best-effort; suppress cancellation/runtime-init failures.
        except BaseException:
            _LOG.warning("Trickle close suppressed runtime init failure url=%s", self.url, exc_info=True)
            return

        assert self._lock is not None

        _LOG.debug("Trickle close: %s", self.url)
        try:
            async with self._lock:
                if self._next_state is not None:
                    await SegmentWriter(self._next_state).close()
                    self._next_state = None

                if self._session is not None:
                    try:
                        await self._run_delete()
                    # Best-effort shutdown: do not abort close on delete failures,
                    # including cancellation.
                    except BaseException:
                        _LOG.warning("Trickle close suppressed delete failure url=%s", self.url, exc_info=True)
                    try:
                        await self._session.close()
                    # Session close should not block the rest of teardown.
                    except BaseException:
                        _LOG.warning("Trickle close suppressed session close failure url=%s", self.url, exc_info=True)
                    self._session = None
        # Close should not raise; preserve best-effort semantics even on cancellation.
        except BaseException:
            _LOG.warning("Trickle close suppressed failure url=%s", self.url, exc_info=True)
            if self._session is not None:
                try:
                    await self._session.close()
                # Final fallback close must remain non-throwing.
                except BaseException:
                    _LOG.warning("Trickle close suppressed fallback session close failure url=%s", self.url, exc_info=True)
                self._session = None

    def _record_write_bytes(self, byte_count: int) -> None:
        self._stats["bytes_submitted_to_transport"] += max(0, byte_count)

    def _record_write_timeout(self) -> None:
        self._stats["segment_writer_put_timeouts"] += 1

    def get_stats(self) -> dict:
        return {
            **self._stats,
            "seq": self.seq,
            "consecutive_failures": self._consecutive_failures,
            "terminal_error": self._terminal_error is not None,
        }


class _SegmentPostState:
    __slots__ = ("seq", "queue", "error", "data_consumed")

    def __init__(self, seq: int) -> None:
        self.seq = seq
        self.queue: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=1)
        # Failure for this one segment only, not necessarily terminal
        self.error: Optional[TrickleSegmentWriteError] = None
        self.data_consumed: bool = False

_SEGMENT_QUEUE_PUT_TIMEOUT_S = 5.0

class SegmentWriter:
    def __init__(
        self,
        seg_state: _SegmentPostState,
        *,
        error_getter: Optional[Callable[[], Optional[TricklePublisherTerminalError]]] = None,
        on_write_bytes: Optional[Callable[[int], None]] = None,
        on_write_timeout: Optional[Callable[[], None]] = None,
    ):
        self._seg_state = seg_state
        self.queue = seg_state.queue
        self._seq = seg_state.seq
        self._error_getter = error_getter
        self._on_write_bytes = on_write_bytes
        self._on_write_timeout = on_write_timeout

    async def write(self, data: bytes) -> None:
        if self._error_getter is not None:
            err = self._error_getter()
            if err is not None:
                raise err
        if self._seg_state.error is not None:
            raise self._seg_state.error

        try:
            # This bounds local backpressure while feeding the request body; it does
            # not bound the total lifetime of the HTTP POST once the body is drained.
            await asyncio.wait_for(self.queue.put(data), timeout=_SEGMENT_QUEUE_PUT_TIMEOUT_S)
            if self._on_write_bytes is not None:
                self._on_write_bytes(len(data))
        except asyncio.TimeoutError as e:
            if self._on_write_timeout is not None:
                self._on_write_timeout()
            if self._error_getter is not None:
                err = self._error_getter()
                if err is not None:
                    raise err
            raise TrickleSegmentWriteError(
                f"Trickle segment writer timed out after {_SEGMENT_QUEUE_PUT_TIMEOUT_S:.1f}s",
                seq=self._seq,
            ) from e

    async def close(self) -> None:
        # Close is best-effort; capture any errors, log them and move on.
        if self._seg_state.error is not None:
            return
        try:
            await asyncio.wait_for(self.queue.put(None), timeout=_SEGMENT_QUEUE_PUT_TIMEOUT_S)
        # BaseException to also capture cancellation errors, timeout errors, etc
        except BaseException:
            _LOG.warning("Trickle segment close suppressed seq=%s", self._seq, exc_info=True)

    async def __aenter__(self) -> "SegmentWriter":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    def seq(self) -> int:
        return self._seq


