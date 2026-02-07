from __future__ import annotations

"""
Helpers for consuming trickle media outputs as segments, bytes, or frames.
"""

import asyncio
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
    ) -> None:
        self.subscribe_url = subscribe_url
        self.start_seq = start_seq
        self.max_retries = max_retries
        self.max_segment_bytes = max_segment_bytes
        self.connection_close = connection_close
        self.chunk_size = chunk_size

        self._sub: Optional[TrickleSubscriber] = None
        self._segments: list[SegmentReader] = []
        self._lock = asyncio.Lock()
        self._eos = False

    def segments(
        self,
    ) -> AsyncIterator[SegmentReader]:
        """
        Read the trickle media channel and yield SegmentReader objects.

        Segments are shared across iterators.
        """
        async def _iter() -> AsyncIterator[SegmentReader]:
            index = 0
            while True:
                segment = await self._next_segment(index)
                if segment is None:
                    break
                yield segment
                index += 1

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
        index = 0
        while True:
            segment = await self._next_segment(index)
            if segment is None:
                break
            if not checked_content_type:
                _require_mpegts_content_type(segment.headers().get("Content-Type"))
                checked_content_type = True
            reader = segment.make_reader()
            while True:
                chunk = await reader.read(chunk_size=self.chunk_size)
                if not chunk:
                    break
                yield chunk
            index += 1

    async def _next_segment(self, index: int) -> Optional[SegmentReader]:
        """
        Return the segment at index, lazily advancing the subscriber if needed.
        """
        if index < len(self._segments):
            return self._segments[index]

        async with self._lock:
            while len(self._segments) <= index:
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
                    await self._sub.__aenter__()
                segment = await self._sub.next()
                if segment is None:
                    self._eos = True
                    return None
                self._segments.append(segment)

                prev = len(self._segments) - 2
                if prev >= 0:
                    await self._segments[prev].close()

            return self._segments[index]

    async def close(self) -> None:
        for segment in self._segments:
            await segment.close()
        if self._sub is not None:
            await self._sub.close()

    async def __aenter__(self) -> "MediaOutput":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()


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

