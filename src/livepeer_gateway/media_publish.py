from __future__ import annotations

import asyncio
import logging
import os
import queue
import threading
import time
from fractions import Fraction
from typing import Optional, Set

import av
from av.video.frame import PictureType

from .errors import LivepeerGatewayError
from .trickle_publisher import TricklePublisher

_OUT_TIME_BASE = Fraction(1, 90_000)
_READ_CHUNK = 64 * 1024
_STOP = object()


def _fraction_from_time_base(time_base: Fraction) -> Fraction:
    if isinstance(time_base, Fraction):
        return time_base
    numerator = getattr(time_base, "numerator", None)
    denominator = getattr(time_base, "denominator", None)
    if numerator is not None and denominator is not None:
        return Fraction(int(numerator), int(denominator))
    return Fraction(time_base)


def _rescale_pts(pts: int, src_tb: Fraction, dst_tb: Fraction) -> int:
    if src_tb == dst_tb:
        return int(pts)
    return int(round(float((Fraction(pts) * src_tb) / dst_tb)))


class MediaPublish:
    def __init__(
        self,
        publish_url: str,
        *,
        mime_type: str = "video/mp2t",
        keyframe_interval_s: float = 2.0,
    ) -> None:
        self.publish_url = publish_url
        self._publisher = TricklePublisher(publish_url, mime_type)
        self._keyframe_interval_s = float(keyframe_interval_s)

        self._queue: queue.Queue[object] = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._segment_tasks: Set[asyncio.Task[None]] = set()
        self._start_lock = threading.Lock()

        self._closed = False
        self._error: Optional[BaseException] = None

        # Encoder state (owned by the encoder thread).
        self._container: Optional[av.container.OutputContainer] = None
        self._video_stream: Optional[av.video.stream.VideoStream] = None
        self._wallclock_start: Optional[float] = None
        self._last_keyframe_time: Optional[float] = None

    async def write_frame(self, frame: av.VideoFrame) -> None:
        if self._closed:
            raise LivepeerGatewayError("MediaPublish is closed")
        if not isinstance(frame, av.VideoFrame):
            raise TypeError(f"write_frame expects av.VideoFrame, got {type(frame).__name__}")
        if self._error:
            raise LivepeerGatewayError("MediaPublish encoder failed") from self._error

        if self._loop is None:
            self._loop = asyncio.get_running_loop()

        self._ensure_thread()
        await asyncio.to_thread(self._queue.put, frame)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        if self._thread is not None:
            await asyncio.to_thread(self._queue.put, _STOP)
            await asyncio.to_thread(self._thread.join)

        if self._segment_tasks:
            await asyncio.gather(*list(self._segment_tasks), return_exceptions=True)

        await self._publisher.close()

        if self._error:
            raise LivepeerGatewayError("MediaPublish encoder failed") from self._error

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
                if item is _STOP:
                    break
                frame = item
                if self._container is None:
                    self._open_container(frame)
                self._encode_frame(frame)

            self._flush_encoder()
        except Exception as e:
            self._error = e
            logging.error("MediaPublish encoder error", exc_info=True)
        finally:
            if self._container is not None:
                try:
                    self._container.close()
                except Exception:
                    logging.exception("MediaPublish failed to close container")
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
            "reset_timestamps": "1",
            "segment_format": "mpegts",
        }

        self._container = av.open(
            "%d.ts",
            format="segment",
            mode="w",
            io_open=custom_io_open,
            options=segment_options,
        )

        video_opts = {"bf": "0"}
        if self._container.format.name == "segment":
            video_opts["preset"] = "superfast"
            video_opts["tune"] = "zerolatency"
            video_opts["forced-idr"] = "1"

        self._video_stream = self._container.add_stream("libx264", options=video_opts)
        self._video_stream.time_base = _OUT_TIME_BASE
        self._video_stream.width = first_frame.width
        self._video_stream.height = first_frame.height
        self._video_stream.pix_fmt = "yuv420p"
        self._video_stream.codec_context.max_b_frames = 0

    def _encode_frame(self, frame: av.VideoFrame) -> None:
        if self._video_stream is None or self._container is None:
            raise RuntimeError("MediaPublish encoder is not initialized")

        source_pts = frame.pts
        source_tb = frame.time_base

        if frame.format.name != "yuv420p":
            frame = frame.reformat(format="yuv420p")

        current_time_s, out_pts = self._compute_pts(source_pts, source_tb)
        frame.pts = out_pts
        frame.time_base = _OUT_TIME_BASE

        if (
            self._last_keyframe_time is None
            or current_time_s - self._last_keyframe_time >= self._keyframe_interval_s
        ):
            frame.pict_type = PictureType.I
            self._last_keyframe_time = current_time_s

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

    def _schedule_pipe_reader(self, read_file: object) -> None:
        def _start() -> None:
            task = self._loop.create_task(self._stream_pipe_to_trickle(read_file))
            self._segment_tasks.add(task)
            task.add_done_callback(self._segment_tasks.discard)

        self._loop.call_soon_threadsafe(_start)

    async def _stream_pipe_to_trickle(self, read_file: object) -> None:
        try:
            async with await self._publisher.next() as segment:
                while True:
                    chunk = await asyncio.to_thread(read_file.read, _READ_CHUNK)
                    if not chunk:
                        break
                    await segment.write(chunk)
        except Exception:
            logging.error("MediaPublish pipe stream failed", exc_info=True)
        finally:
            try:
                read_file.close()
            except Exception:
                pass

