from __future__ import annotations

import asyncio
import logging
import math
import os
import queue
import threading
import time
from dataclasses import dataclass
from fractions import Fraction
from typing import Optional, Set, Awaitable, Any, BinaryIO

import av
from av.video.frame import PictureType

from .errors import LivepeerGatewayError
from .trickle_publisher import (
    TricklePublisher,
    TricklePublisherTerminalError,
    TrickleSegmentWriteError,
)

_LOG = logging.getLogger(__name__)

_OUT_TIME_BASE = Fraction(1, 90_000)
_READ_CHUNK = 64 * 1024
_DRAIN_TIMEOUT_S = 5.0
_PUBLISH_SUMMARY_INTERVAL_S = 10.0
_STOP = object()


def _fraction_from_time_base(time_base: object) -> Fraction:
    numerator = getattr(time_base, "numerator", None)
    denominator = getattr(time_base, "denominator", None)
    if numerator is not None and denominator is not None:
        return Fraction(int(numerator), int(denominator))
    return Fraction(time_base)


def _rescale_pts(pts: int, src_tb: Fraction, dst_tb: Fraction) -> int:
    if src_tb == dst_tb:
        return int(pts)
    return int(round(float((Fraction(pts) * src_tb) / dst_tb)))


def _normalize_fps(fps: Optional[float]) -> int:
    if fps is None or not math.isfinite(fps) or fps <= 0:
        fps = 30.0
    return max(1, int(round(fps)))


@dataclass(frozen=True)
class MediaPublishConfig:
    fps: Optional[float] = None
    mime_type: str = "video/mp2t"
    keyframe_interval_s: float = 2.0


class MediaPublish:
    def __init__(
        self,
        publish_url: str,
        *,
        config: MediaPublishConfig = MediaPublishConfig(),
    ) -> None:
        self.publish_url = publish_url
        self._publisher = TricklePublisher(
            publish_url,
            config.mime_type,
        )
        self._keyframe_interval_s = float(config.keyframe_interval_s)
        self._fps_hint = config.fps

        self._queue: queue.Queue[object] = queue.Queue(maxsize=1)
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._segment_tasks: Set[asyncio.Task[None]] = set()
        self._start_lock = threading.Lock()

        self._closed = False
        self._error: Optional[BaseException] = None
        self._started_at = time.time()
        self._last_summary_at = self._started_at
        self._stats: dict[str, int] = {
            "frames_in": 0,
            "frames_dropped_before_encode": 0,
            "segments_started": 0,
            "segments_completed": 0,
            "segments_failed": 0,
            "terminal_failures": 0,
            "bytes_streamed_to_trickle": 0,
            "segment_writer_put_timeouts": 0,
            "encoder_errors": 0,
        }

        # Encoder state (owned by the encoder thread).
        self._container: Optional[av.container.OutputContainer] = None
        self._video_stream: Optional[av.video.stream.VideoStream] = None
        self._wallclock_start: Optional[float] = None
        self._last_keyframe_time: Optional[float] = None
        self._last_out_pts: Optional[int] = None

    async def write_frame(self, frame: av.VideoFrame) -> None:
        if self._closed:
            raise LivepeerGatewayError("MediaPublish is closed")
        if not isinstance(frame, av.VideoFrame):
            raise TypeError(f"write_frame expects av.VideoFrame, got {type(frame).__name__}")
        if self._error:
            raise LivepeerGatewayError(f"MediaPublish failed: {self._error}") from self._error

        if self._loop is None:
            self._loop = asyncio.get_running_loop()

        self._ensure_thread()
        self._stats["frames_in"] += 1
        self._put_queue(frame)

    def _put_queue(self, item: object) -> None:
        """Enqueue *item* with prefer-latest semantics.

        Bounded queue (maxsize=1): if full, drop the oldest frame so the
        encoder always receives the most recent input. The _STOP sentinel is
        never discarded -- if encountered while draining, re-enqueue _STOP and
        drop the incoming item.
        """
        if self._closed and item is not _STOP:
            return

        max_retries = 10
        for _ in range(max_retries):
            try:
                self._queue.put_nowait(item)
                return
            except queue.Full:
                if self._closed and item is not _STOP:
                    return
                try:
                    old = self._queue.get_nowait()
                except queue.Empty:
                    continue
                if old is _STOP:
                    # We dequeued shutdown while making room. Prioritize
                    # re-enqueueing _STOP and drop whatever item we were
                    # attempting to queue (prefer shutdown over new input).
                    item = _STOP
                    continue
                if item is not _STOP:
                    self._stats["frames_dropped_before_encode"] += 1
        _LOG.error("MediaPublish _put_queue exceeded retry limit (%d); dropping item", max_retries)
        if item is not _STOP:
            self._stats["frames_dropped_before_encode"] += 1

    async def _suppress_close_step(self, step_name: str, awaitable: Awaitable[Any]) -> None:
        try:
            await awaitable
        except Exception:
            _LOG.warning("MediaPublish close suppressed %s failure", step_name, exc_info=True)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        # Close is best-effort; capture any errors, log them and move on.
        # Intentionally step-wise: each shutdown action has its own
        # suppression so one failure does not prevent later cleanup.
        if self._thread is not None:
            await self._suppress_close_step("sentinel enqueue", asyncio.to_thread(self._put_queue, _STOP))
            await self._suppress_close_step("encoder join", asyncio.to_thread(self._thread.join, 2.0))
            if self._thread.is_alive():
                _LOG.warning("MediaPublish encoder thread still alive after join timeout")

        # Segment tasks may be blocked writing into trickle when the network
        # path is unhealthy; cancel them first so the close stays bounded.
        for task in list(self._segment_tasks):
            task.cancel()
        if self._segment_tasks:
            await self._suppress_close_step(
                "segment task gather",
                asyncio.gather(*list(self._segment_tasks), return_exceptions=True),
            )

        await self._suppress_close_step("publisher close", self._publisher.close())

        if self._error:
            _LOG.warning(
                "MediaPublish close suppressed prior publish failure: %s",
                self._error,
                exc_info=(type(self._error), self._error, self._error.__traceback__),
            )
        self._log_publish_summary(prefix="close")

    def _ensure_thread(self) -> None:
        with self._start_lock:
            if self._thread is not None:
                return
            self._thread = threading.Thread(
                target=self._run_encoder,
                name="MediaPublishEncoder",
                daemon=True,
            )
            self._thread.start()

    def _run_encoder(self) -> None:
        try:
            while True:
                item = self._queue.get()
                if item is _STOP or self._error is not None:
                    break
                frame = item
                if self._container is None:
                    self._open_container(frame)
                self._encode_frame(frame)

            self._flush_encoder()
        except Exception as e:
            self._error = e
            self._stats["encoder_errors"] += 1
            _LOG.error("MediaPublish encoder error", exc_info=True)
        finally:
            if self._container is not None:
                try:
                    self._container.close()
                except Exception:
                    _LOG.exception("MediaPublish failed to close container")
            self._container = None
            self._video_stream = None

    def _open_container(self, first_frame: av.VideoFrame) -> None:
        if self._loop is None:
            raise RuntimeError("MediaPublish loop is not set")

        def custom_io_open(url: str, flags: int, options: dict) -> object:
            read_fd, write_fd = os.pipe()
            read_file = os.fdopen(read_fd, "rb", buffering=0)
            write_file = os.fdopen(write_fd, "wb", buffering=0)
            self._schedule_pipe_reader(read_file)
            return write_file

        segment_options = {
            "segment_time": str(self._keyframe_interval_s),
            "segment_format": "mpegts",
        }

        self._container = av.open(
            "%d.ts",
            format="segment",
            mode="w",
            io_open=custom_io_open,
            options=segment_options,
        )

        video_opts = {
            "bf": "0",
            "preset": "superfast",
            "tune": "zerolatency",
            "forced-idr": "1",
        }
        video_kwargs = {
            "time_base": _OUT_TIME_BASE,
            "width": first_frame.width,
            "height": first_frame.height,
            "pix_fmt": "yuv420p",
        }

        rounded_fps = _normalize_fps(self._fps_hint)
        self._video_stream = self._container.add_stream("libx264", rate=rounded_fps, options=video_opts, **video_kwargs)

    def _encode_frame(self, frame: av.VideoFrame) -> None:
        if self._video_stream is None or self._container is None:
            raise RuntimeError("MediaPublish encoder is not initialized")

        source_pts = frame.pts
        source_tb = frame.time_base

        if frame.format.name != "yuv420p":
            frame = frame.reformat(format="yuv420p")

        current_time_s, out_pts = self._compute_pts(source_pts, source_tb)
        if self._last_out_pts is not None and out_pts <= self._last_out_pts:
            # timestamp would overlap with previous frame, so drop
            # happens if frames come in faster than the encode rate
            return
        self._last_out_pts = out_pts
        frame.pts = out_pts
        frame.time_base = _OUT_TIME_BASE

        if (
            self._last_keyframe_time is None
            or current_time_s - self._last_keyframe_time >= self._keyframe_interval_s
        ):
            frame.pict_type = PictureType.I
            self._last_keyframe_time = current_time_s
        else:
            frame.pict_type = PictureType.NONE

        packets = self._video_stream.encode(frame)
        for packet in packets:
            self._container.mux(packet)

    def _flush_encoder(self) -> None:
        if self._video_stream is None or self._container is None:
            return
        packets = self._video_stream.encode(None)
        for packet in packets:
            self._container.mux(packet)

    def _compute_pts(self, pts: Optional[int], time_base: Optional[Fraction]) -> tuple[float, int]:
        if pts is not None and time_base is not None:
            tb = _fraction_from_time_base(time_base)
            current_time_s = float(Fraction(pts) * tb)
            out_pts = _rescale_pts(pts, tb, _OUT_TIME_BASE)
            return current_time_s, out_pts

        now = time.time()
        if self._wallclock_start is None:
            self._wallclock_start = now
        current_time_s = now - self._wallclock_start
        return current_time_s, int(current_time_s * _OUT_TIME_BASE.denominator)

    def _schedule_pipe_reader(self, read_file: BinaryIO) -> None:
        def _start() -> None:
            task = self._loop.create_task(self._stream_pipe_to_trickle(read_file))
            self._segment_tasks.add(task)
            task.add_done_callback(self._segment_tasks.discard)

        self._loop.call_soon_threadsafe(_start)

    async def _stream_pipe_to_trickle(self, read_file: BinaryIO) -> None:
        segment_seq: Optional[int] = None
        try:
            segment = await self._publisher.next()
            segment_seq = segment.seq()
            self._stats["segments_started"] += 1
            async with segment:
                while True:
                    chunk = await asyncio.to_thread(read_file.read, _READ_CHUNK)
                    if not chunk:
                        break
                    self._stats["bytes_streamed_to_trickle"] += len(chunk)
                    # NB: the segment writer has its own chunk timeout, so
                    # lean on that instead of doing that here
                    await segment.write(chunk)
            self._stats["segments_completed"] += 1
            self._maybe_log_publish_summary()
        except TricklePublisherTerminalError as e:
            # At this point, publisher.next() has exhausted its retries and the
            # publisher cannot be used for future segments.
            if self._error is None:
                self._error = e
            self._stats["segments_failed"] += 1
            self._stats["terminal_failures"] += 1
            _LOG.error("MediaPublish terminal failure while streaming", exc_info=True)
        except TrickleSegmentWriteError:
            self._stats["segments_failed"] += 1
            _LOG.warning("MediaPublish dropped segment seq=%s", segment_seq, exc_info=True)
        except Exception:
            self._stats["segments_failed"] += 1
            _LOG.exception(
                "MediaPublish unexpected failure while streaming segment seq=%s",
                segment_seq,
            )
        finally:
            # This block is critical for clean-up; always drain and close file
            # Because not all exceptions will be caught (eg, CancelledError)
            try:
                await self._drain_pipe(read_file)
                read_file.close()
            except Exception:
                pass

    async def _drain_pipe(self, read_file: BinaryIO) -> None:
        async def _drain() -> None:
            while True:
                chunk = await asyncio.to_thread(read_file.read, _READ_CHUNK)
                if not chunk:
                    break

        # Best-effort cleanup: absorb TimeoutError and CancelledError
        # so drain never blocks shutdown
        try:
            await asyncio.wait_for(_drain(), timeout=_DRAIN_TIMEOUT_S)
        except BaseException:
            pass

    def _maybe_log_publish_summary(self) -> None:
        now = time.time()
        if now - self._last_summary_at >= _PUBLISH_SUMMARY_INTERVAL_S:
            self._last_summary_at = now
            self._log_publish_summary(prefix="periodic")

    def _log_publish_summary(self, *, prefix: str) -> None:
        publisher_stats = self._publisher.get_stats()
        elapsed_s = max(0.0, time.time() - self._started_at)
        _LOG.info(
            "MediaPublish summary (%s): elapsed=%.1fs "
            "frames_in=%d frames_dropped_before_encode=%d "
            "segments_started=%d segments_completed=%d segments_failed=%d "
            "bytes_streamed_to_trickle=%d "
            "post_attempts=%d post_retries_no_body_consumed=%d post_http_failures=%d "
            "post_exceptions=%d post_404=%d segment_writer_put_timeouts=%d "
            "terminal_failures=%d encoder_errors=%d terminal_error=%s",
            prefix,
            elapsed_s,
            self._stats["frames_in"],
            self._stats["frames_dropped_before_encode"],
            self._stats["segments_started"],
            self._stats["segments_completed"],
            self._stats["segments_failed"],
            self._stats["bytes_streamed_to_trickle"],
            publisher_stats.get("post_attempts", 0),
            publisher_stats.get("post_retries_no_body_consumed", 0),
            publisher_stats.get("post_http_failures", 0),
            publisher_stats.get("post_exceptions", 0),
            publisher_stats.get("post_404", 0),
            publisher_stats.get("segment_writer_put_timeouts", 0),
            max(
                self._stats["terminal_failures"],
                int(publisher_stats.get("terminal_failures", 0)),
            ),
            self._stats["encoder_errors"],
            publisher_stats.get("terminal_error", False),
        )

