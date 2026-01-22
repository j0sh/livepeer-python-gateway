from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import AsyncIterator, Optional, Tuple

from .errors import LivepeerGatewayError
from .media_decode import DecodedMediaFrame, MpegTsDecoder, decoder_error, is_decoder_end

from .trickle_subscriber import SegmentReader, TrickleSubscriber


class MediaOutput:
    """
    Access a trickle media output

    Exposes both:
      - per-segment iteration (SegmentReader objects)
      - continuous byte stream (bytes chunks)
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

    def segments(
        self,
    ) -> AsyncIterator[SegmentReader]:
        """
        Read the trickle media channel and yield SegmentReader objects.

        The caller is responsible for closing each segment.
        """
        url = self.subscribe_url

        async def _iter() -> AsyncIterator[SegmentReader]:
            async with TrickleSubscriber(
                url,
                start_seq=self.start_seq,
                max_retries=self.max_retries,
                max_bytes=self.max_segment_bytes,
                connection_close=self.connection_close,
            ) as subscriber:
                while True:
                    segment = await subscriber.next()
                    if segment is None:
                        break
                    yield segment

        return _iter()

    def bytes(
        self,
    ) -> AsyncIterator[bytes]:
        """
        Read the trickle media channel and yield a continuous byte stream.
        """

        async def _iter() -> AsyncIterator[bytes]:
            async for chunk, _, _ in self._iter_bytes_with_meta(
            ):
                yield chunk

        return _iter()

    def frames(
        self,
    ) -> AsyncIterator[DecodedMediaFrame]:
        """
        Read the trickle media channel, decode MPEG-TS, and yield raw frames.
        """

        async def _iter() -> AsyncIterator[DecodedMediaFrame]:
            decoder = MpegTsDecoder()
            output = decoder.output_queue()
            decoder.start()

            async def _feed() -> None:
                async for chunk, seq, is_first in self._iter_bytes_with_meta(
                ):
                    if is_first:
                        decoder.mark_seq(seq)
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

    async def _iter_bytes_with_meta(
        self,
    ) -> AsyncIterator[Tuple[bytes, Optional[int], bool]]:
        checked_content_type = False
        async for segment in self.segments(
        ):
            if not checked_content_type:
                _require_mpegts_content_type(segment.headers().get("Content-Type"))
                checked_content_type = True
            seq = segment.seq()
            first_chunk = True
            try:
                while True:
                    chunk = await segment.read(chunk_size=self.chunk_size)
                    if not chunk:
                        break
                    yield chunk, seq, first_chunk
                    first_chunk = False
            finally:
                await segment.close()


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

