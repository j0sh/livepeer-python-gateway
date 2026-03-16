from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional, Protocol

import aiohttp


class AsyncByteSource(Protocol):
    """
    aiohttp-compatible shape:
      await source.read(n) -> bytes
    Returns b"" on EOF.
    """
    async def read(self, n: int) -> bytes: ...


@dataclass
class _SegmentBuffer:
    """
    Shared replayable byte buffer backed by an async source (aiohttp StreamReader-like).

    Properties:
      - Multiple readers (independent cursors).
      - Buffer grows monotonically (no eviction/compaction).
      - Only one coroutine pulls from the source at a time (producer lock).
      - Readers return buffered bytes immediately; ONLY when at the write-head do we pull more.
      - When empty at head, pull a chunk.

    This is the "on_demand" approach (no watermark coordination between readers).
    """
    source: AsyncByteSource
    producer_read_size: int = 32 * 1024   # size of each source.read request
    max_bytes: Optional[int] = None       # safety bound on TOTAL buffered bytes

    def __post_init__(self) -> None:
        self._buf = bytearray()
        self._eof = False
        self._error: Optional[BaseException] = None
        self._stats: dict[str, int] = {
            "chunks_read": 0,
            "bytes_read": 0,
            "read_errors": 0,
            "max_bytes_exceeded": 0,
        }

        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition(self._lock)
        self._producer_lock = asyncio.Lock()

    async def close(self) -> None:
        async with self._cond:
            self._eof = True
            self._cond.notify_all()

    async def _produce_chunk(self) -> None:
        """
        Read ONE chunk from the source into the buffer and return immediately.
        """
        async with self._producer_lock:
            async with self._cond:
                if self._error is not None or self._eof:
                    return

            try:
                chunk = await self.source.read(self.producer_read_size)
            except Exception as exc:
                async with self._cond:
                    self._stats["read_errors"] += 1
                    self._error = exc
                    self._eof = True
                    self._cond.notify_all()
                return

            async with self._cond:
                if not chunk:
                    self._eof = True
                    self._cond.notify_all()
                    return

                self._buf.extend(chunk)
                self._stats["chunks_read"] += 1
                self._stats["bytes_read"] += len(chunk)

                if self.max_bytes is not None and len(self._buf) > self.max_bytes:
                    self._stats["max_bytes_exceeded"] += 1
                    self._error = ValueError(
                        f"Trickle segment exceeds max size ({len(self._buf)} > {self.max_bytes})"
                    )
                    self._eof = True

                self._cond.notify_all()

    async def _read_for(self, cursor: int, n: int) -> Optional[bytes]:
        """
        Return up to n bytes starting at cursor.
        Returns None on EOF/closed.
        Raises stored error.
        """
        if n <= 0:
            raise ValueError("read size must be > 0")

        # Fast path: buffered bytes available.
        async with self._cond:
            if self._error is not None:
                raise self._error

            available = len(self._buf) - cursor
            if available > 0:
                k = min(n, available)
                return bytes(self._buf[cursor : cursor + k])

            if self._eof:
                return None

        # Slow path: at write-head -> produce a chunk and return it
        await self._produce_chunk()

        # Re-check; if still empty (contention/small chunks), wait until notified.
        while True:
            async with self._cond:
                if self._error is not None:
                    raise self._error

                available = len(self._buf) - cursor
                if available > 0:
                    k = min(n, available)
                    return bytes(self._buf[cursor : cursor + k])

                if self._eof:
                    return None

                await self._cond.wait()


class _SegmentCursor:
    """
    A cursor into a _SegmentBuffer.
    Returned by SegmentReader.make_reader().
    """
    def __init__(self, writer: _SegmentBuffer):
        self._w = writer
        self._pos = 0

    async def read(self, chunk_size: int = 32 * 1024) -> Optional[bytes]:
        out = await self._w._read_for(self._pos, chunk_size)
        if out is None:
            return None
        self._pos += len(out)
        return out

class SegmentReader:
    """
    Your existing SegmentReader interface, upgraded:
      - SegmentReader.read() reads via a shared replayable buffer.
      - SegmentReader.make_reader() returns an independent cursor reader with .read().

    The underlying aiohttp response is only consumed once (by the shared writer).
    """
    def __init__(
        self,
        response: aiohttp.ClientResponse,
        *,
        max_bytes: Optional[int] = 10 * 1024 * 1024,
        producer_read_size: int = 32 * 1024,
    ):
        self.response = response
        self._max_bytes = max_bytes
        self._local_seq = -1

        self._writer = _SegmentBuffer(
            source=response.content,
            producer_read_size=producer_read_size,
            max_bytes=max_bytes,
        )

    def make_reader(self) -> _SegmentCursor:
        return _SegmentCursor(self._writer)

    def seq(self) -> int:
        seq_str = self.response.headers.get("Lp-Trickle-Seq")
        try:
            return int(seq_str)
        except (TypeError, ValueError):
            return -1

    def eos(self) -> bool:
        # Header-based end-of-segment marker (your existing behavior).
        return self.response.headers.get("Lp-Trickle-Closed") is not None

    def headers(self) -> "aiohttp.typedefs.LooseHeaders":
        return self.response.headers

    async def close(self) -> None:
        # Stop readers and wake any waiters.
        await self._writer.close()

        # Close the aiohttp response (your existing behavior).
        if self.response is None:
            return
        if not self.response.closed:
            self.response.release()
            self.response.close()

    def get_stats(self) -> dict:
        stats = dict(self._writer._stats)
        stats["segment_seq"] = self.seq()
        return stats
