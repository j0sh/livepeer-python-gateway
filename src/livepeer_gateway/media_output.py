from __future__ import annotations

"""
Helpers for consuming trickle media outputs as segments, bytes, or frames.
"""

import asyncio
import logging
import time
from enum import Enum
from contextlib import suppress
from typing import AsyncIterator, Optional

from .errors import LivepeerGatewayError
from .media_decode import (
    AudioDecodedMediaFrame,
    DecodedMediaFrame,
    MpegTsDecoder,
    VideoDecodedMediaFrame,
    decoder_error,
    is_decoder_end,
)

from .segment_reader import SegmentReader
from .trickle_subscriber import TrickleSubscriber

_LOG = logging.getLogger(__name__)
_MEDIA_OUTPUT_SUMMARY_INTERVAL_S = 10.0

class LagPolicy(Enum):
    """
    Policy for handling consumers that fall behind the segment window.
    """
    FAIL = "fail"
    LATEST = "latest"
    EARLIEST = "earliest"


class MediaOutput:
    """
    Access a trickle media output

    Exposes:
      - per-segment iteration (SegmentReader objects)
      - continuous byte stream (bytes chunks)
      - individual audio and video frames

    Segments are sourced from a single shared subscriber so that multiple
    iterators can consume the same output concurrently without duplicate
    network requests.

    Attributes:
        subscribe_url: Trickle subscribe URL for this output.
        start_seq: Initial server sequence when subscribing.
        max_retries: Max retries for segment fetches.
        max_segment_bytes: Safety bound for a single segment size.
        connection_close: Whether to close connections after each segment.
        chunk_size: Byte chunk size yielded by bytes()/frames().
        max_segments: Max number of segments retained in memory.
        on_lag: Behavior when a consumer falls behind the segment window.
            - LagPolicy.FAIL: raise LivepeerGatewayError.
            - LagPolicy.LATEST: skip to the newest available segment.
            - LagPolicy.EARLIEST: retry from the oldest available segment.
        _sub: Shared trickle subscriber.
        _segments: In-memory window of SegmentReader objects.
        _lock: Coroutine-level lock for segment fetching/eviction.
        _eos: End-of-stream indicator.
        _next_local_seq: Local sequence counter for fetched segments.
        _base_seq: Local sequence of _segments[0].
    """

    def __init__(
        self,
        subscribe_url: str,
        *,
        start_seq: int = -2,
        max_retries: int = 5,
        max_segment_bytes: Optional[int] = None,
        connection_close: bool = False,
        chunk_size: int = 64 * 1024,
        max_segments: int = 5,
        on_lag: LagPolicy = LagPolicy.LATEST,
    ) -> None:
        if max_segments < 1:
            raise ValueError("max_segments must be >= 1")
        self.subscribe_url = subscribe_url
        self.start_seq = start_seq
        self.max_retries = max_retries
        self.max_segment_bytes = max_segment_bytes
        self.connection_close = connection_close
        self.chunk_size = chunk_size
        self.max_segments = max_segments
        self.on_lag = on_lag

        self._sub: Optional[TrickleSubscriber] = None
        self._segments: list[SegmentReader] = []
        self._lock = asyncio.Lock()
        self._eos = False
        self._next_local_seq = 0
        self._base_seq = 0
        self._started_at = time.time()
        self._last_summary_at = self._started_at
        self._stats: dict[str, int] = {
            "segments_consumed": 0,
            "bytes_read": 0,
            "chunks_read": 0,
            "content_type_errors": 0,
            "segment_read_errors": 0,
            "segment_max_bytes_exceeded": 0,
            "consumer_lag_skip_latest": 0,
            "consumer_lag_retry_earliest": 0,
            "consumer_lag_fail": 0,
            "video_frames_decoded": 0,
            "audio_frames_decoded": 0,
            "decode_errors": 0,
        }

    def segments(
        self,
    ) -> AsyncIterator[SegmentReader]:
        """
        Read the trickle media channel and yield SegmentReader objects.

        Segments are shared across iterators.
        """
        async def _iter() -> AsyncIterator[SegmentReader]:
            seq = 0
            segment = await self._next_segment(seq)
            while segment is not None:
                yield segment
                # Use the returned segment's local seq in case we skipped ahead.
                seq = segment._local_seq + 1
                segment = await self._next_segment(seq)

        return _iter()

    def bytes(
        self,
    ) -> AsyncIterator[bytes]:
        """
        Read the trickle media channel and yield a continuous byte stream.
        """

        async def _iter() -> AsyncIterator[bytes]:
            async for chunk in self._iter_bytes():
                yield chunk

        return _iter()

    def frames(
        self,
    ) -> AsyncIterator[AudioDecodedMediaFrame | VideoDecodedMediaFrame]:
        """
        Read the trickle media channel, decode MPEG-TS, and yield raw frames.
        """

        async def _iter() -> AsyncIterator[AudioDecodedMediaFrame | VideoDecodedMediaFrame]:
            decoder = MpegTsDecoder()
            output = decoder.output_queue()
            decoder.start()

            async def _feed() -> None:
                async for chunk in self._iter_bytes():
                    decoder.feed(chunk)
                decoder.close()

            producer_task = asyncio.create_task(_feed())
            try:
                while True:
                    item = await asyncio.to_thread(output.get)
                    err = decoder_error(item)
                    if err is not None:
                        self._stats["decode_errors"] += 1
                        raise LivepeerGatewayError(
                            f"Media decode error: {err.__class__.__name__}: {err}"
                        ) from err
                    if is_decoder_end(item):
                        if producer_task.done():
                            exc = producer_task.exception()
                            if exc:
                                raise exc
                        break
                    if isinstance(item, DecodedMediaFrame):
                        if item.kind == "video":
                            self._stats["video_frames_decoded"] += 1
                        elif item.kind == "audio":
                            self._stats["audio_frames_decoded"] += 1
                        self._maybe_log_summary()
                        yield item
            finally:
                decoder.stop()
                if not producer_task.done():
                    producer_task.cancel()
                with suppress(asyncio.CancelledError):
                    await producer_task
                await asyncio.to_thread(decoder.join)

        return _iter()

    async def _iter_bytes(
        self,
    ) -> AsyncIterator[bytes]:
        checked_content_type = False
        seq = 0
        segment = await self._next_segment(seq)
        while segment is not None:
            if not checked_content_type:
                try:
                    _require_mpegts_content_type(segment.headers().get("Content-Type"))
                except Exception:
                    self._stats["content_type_errors"] += 1
                    raise
                checked_content_type = True
            reader = segment.make_reader()
            self._stats["segments_consumed"] += 1
            while True:
                chunk = await reader.read(chunk_size=self.chunk_size)
                if not chunk:
                    break
                self._stats["chunks_read"] += 1
                self._stats["bytes_read"] += len(chunk)
                yield chunk
            segment_stats = segment.get_stats()
            self._stats["segment_read_errors"] += segment_stats.get("read_errors", 0)
            self._stats["segment_max_bytes_exceeded"] += segment_stats.get(
                "max_bytes_exceeded", 0
            )
            self._maybe_log_summary()
            # Use the returned segment's local seq in case we skipped ahead.
            seq = segment._local_seq + 1
            segment = await self._next_segment(seq)

    async def _next_segment(
        self,
        seq: int,
    ) -> Optional[SegmentReader]:
        """
        Return the segment at seq, lazily advancing the subscriber if needed.
        """
        # Safe lock-free read: asyncio only context-switches on awaits, and this
        # block has no awaits. That means _segments/_base_seq cannot change
        # until we return or enter the locked slow path below.
        relative = seq - self._base_seq
        if 0 <= relative < len(self._segments):
            return self._segments[relative]

        async with self._lock:
            relative = seq - self._base_seq
            if relative < 0:
                if self.on_lag is LagPolicy.FAIL:
                    self._stats["consumer_lag_fail"] += 1
                    raise LivepeerGatewayError(
                        "consumer fell behind segment window"
                    )
                if self._segments:
                    if self.on_lag is LagPolicy.EARLIEST:
                        self._stats["consumer_lag_retry_earliest"] += 1
                        _LOG.warning(
                            "MediaOutput consumer fell behind segment window; "
                            "retrying from earliest"
                        )
                        return self._segments[0]
                    self._stats["consumer_lag_skip_latest"] += 1
                    _LOG.warning(
                        "MediaOutput consumer fell behind segment window; "
                        "skipping to latest"
                    )
                    return self._segments[-1]
            elif relative < len(self._segments):
                return self._segments[relative]

            while (seq - self._base_seq) >= len(self._segments):
                if self._eos:
                    return None
                if self._sub is None:
                    self._sub = TrickleSubscriber(
                        self.subscribe_url,
                        start_seq=self.start_seq,
                        max_retries=self.max_retries,
                        max_bytes=self.max_segment_bytes,
                        connection_close=self.connection_close,
                    )
                segment = await self._sub.next()
                if segment is None:
                    self._eos = True
                    return None
                segment._local_seq = self._next_local_seq
                self._next_local_seq += 1
                self._segments.append(segment)

                prev = len(self._segments) - 2
                if prev >= 0:
                    await self._segments[prev].close()

                while len(self._segments) > self.max_segments:
                    self._segments.pop(0)
                    self._base_seq += 1

            relative = seq - self._base_seq
            if 0 <= relative < len(self._segments):
                return self._segments[relative]
            return None

    async def close(self) -> None:
        self._log_summary(prefix="close")
        for segment in self._segments:
            await segment.close()
        if self._sub is not None:
            await self._sub.close()

    async def __aenter__(self) -> "MediaOutput":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    def _maybe_log_summary(self) -> None:
        now = time.time()
        if now - self._last_summary_at >= _MEDIA_OUTPUT_SUMMARY_INTERVAL_S:
            self._last_summary_at = now
            self._log_summary(prefix="periodic")

    def _log_summary(self, *, prefix: str) -> None:
        sub_stats = self._sub.get_stats() if self._sub is not None else {}
        _LOG.info(
            "MediaOutput summary (%s): elapsed=%.1fs segments_consumed=%d "
            "bytes_read=%d chunks_read=%d content_type_errors=%d "
            "segment_read_errors=%d segment_max_bytes_exceeded=%d "
            "consumer_lag_skip_latest=%d consumer_lag_retry_earliest=%d "
            "consumer_lag_fail=%d video_frames_decoded=%d audio_frames_decoded=%d "
            "decode_errors=%d sub_get_attempts=%d sub_get_retries=%d sub_get_failures=%d "
            "sub_get_404_eos=%d sub_get_470_reset=%d sub_seq_gap_events=%d",
            prefix,
            max(0.0, time.time() - self._started_at),
            self._stats["segments_consumed"],
            self._stats["bytes_read"],
            self._stats["chunks_read"],
            self._stats["content_type_errors"],
            self._stats["segment_read_errors"],
            self._stats["segment_max_bytes_exceeded"],
            self._stats["consumer_lag_skip_latest"],
            self._stats["consumer_lag_retry_earliest"],
            self._stats["consumer_lag_fail"],
            self._stats["video_frames_decoded"],
            self._stats["audio_frames_decoded"],
            self._stats["decode_errors"],
            sub_stats.get("get_attempts", 0),
            sub_stats.get("get_retries", 0),
            sub_stats.get("get_failures", 0),
            sub_stats.get("get_404_eos", 0),
            sub_stats.get("get_470_reset", 0),
            sub_stats.get("seq_gap_events", 0),
        )


def _normalize_content_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return value.split(";", 1)[0].strip().lower()


def _require_mpegts_content_type(value: Optional[str]) -> None:
    normalized = _normalize_content_type(value)
    if normalized != "video/mp2t":
        raise LivepeerGatewayError(
            f"Expected MPEG-TS Content-Type 'video/mp2t', got {value!r}"
        )

