from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from fractions import Fraction
from typing import Optional, Union

import av


@dataclass(frozen=True)
class DecodedMediaFrame:
    kind: str
    frame: Union[av.VideoFrame, av.AudioFrame]
    pts: Optional[int]
    time_base: Optional[Fraction]
    # pts_time: presentation timestamp in seconds (pts * time_base). Not wall-clock time.
    pts_time: Optional[float]
    demuxed_at: float
    decoded_at: float
    source_segment_seq: Optional[int]
    width: Optional[int] = None
    height: Optional[int] = None
    pix_fmt: Optional[str] = None
    sample_rate: Optional[int] = None
    layout: Optional[str] = None
    format: Optional[str] = None
    samples: Optional[int] = None


class _SegmentMarker:
    __slots__ = ("seq",)

    def __init__(self, seq: Optional[int]) -> None:
        self.seq = seq


_EOF = object()
_END = object()


# Internal adapter that turns async-fed byte chunks into a blocking, file-like
# stream for PyAV. It also carries segment sequence markers so decoded frames
# can be annotated with the source segment.
class _BlockingByteStream:
    def __init__(self) -> None:
        # TODO: add backpressure (bounded queue + blocking feed) to avoid unbounded
        # memory growth if the decoder thread can't keep up with the producer.
        self._queue: "queue.Queue[object]" = queue.Queue()
        self._buffer = bytearray()
        self._closed = False
        self._current_seq: Optional[int] = None

    def feed(self, data: bytes) -> None:
        # Called from the async producer to enqueue raw bytes for decoding.
        if not data:
            return
        self._queue.put(data)

    def mark_seq(self, seq: Optional[int]) -> None:
        # Marker inserted when a new segment starts (best-effort attribution).
        self._queue.put(_SegmentMarker(seq))

    def close(self) -> None:
        # Signal EOF to the blocking reader.
        self._queue.put(_EOF)

    def current_seq(self) -> Optional[int]:
        return self._current_seq

    def read(self, size: int = -1) -> bytes:
        # Blocking read interface consumed by PyAV demuxer.
        if size is None or size < 0:
            size = 64 * 1024

        while len(self._buffer) < size and not self._closed:
            item = self._queue.get()
            if item is _EOF:
                self._closed = True
                break
            if isinstance(item, _SegmentMarker):
                self._current_seq = item.seq
                continue
            if not item:
                continue
            self._buffer.extend(item)  # type: ignore[arg-type]

        if not self._buffer and self._closed:
            return b""

        out = self._buffer[:size]
        del self._buffer[:size]
        return bytes(out)


class _DecoderError:
    __slots__ = ("error",)

    def __init__(self, error: BaseException) -> None:
        self.error = error


def _fraction_from_time_base(time_base: object) -> Optional[Fraction]:
    numerator = getattr(time_base, "numerator", None)
    denominator = getattr(time_base, "denominator", None)
    if numerator is not None and denominator is not None:
        try:
            return Fraction(int(numerator), int(denominator))
        except Exception:
            return None
    try:
        return Fraction(time_base)  # type: ignore[arg-type]
    except Exception:
        return None


def _time_from_pts(pts: Optional[int], time_base: Optional[Fraction]) -> Optional[float]:
    if pts is None or time_base is None:
        return None
    try:
        return float(Fraction(pts) * time_base)
    except Exception:
        return None


def _build_decoded_frame(
    frame: Union[av.VideoFrame, av.AudioFrame],
    *,
    demuxed_at: float,
    decoded_at: float,
    source_segment_seq: Optional[int],
) -> DecodedMediaFrame:
    pts = frame.pts
    time_base = _fraction_from_time_base(frame.time_base) if frame.time_base is not None else None
    pts_time = _time_from_pts(pts, time_base)

    if isinstance(frame, av.VideoFrame):
        return DecodedMediaFrame(
            kind="video",
            frame=frame,
            pts=pts,
            time_base=time_base,
            pts_time=pts_time,
            demuxed_at=demuxed_at,
            decoded_at=decoded_at,
            source_segment_seq=source_segment_seq,
            width=frame.width,
            height=frame.height,
            pix_fmt=frame.format.name if frame.format else None,
        )

    return DecodedMediaFrame(
        kind="audio",
        frame=frame,
        pts=pts,
        time_base=time_base,
        pts_time=pts_time,
        demuxed_at=demuxed_at,
        decoded_at=decoded_at,
        source_segment_seq=source_segment_seq,
        sample_rate=frame.sample_rate,
        layout=frame.layout.name if frame.layout else None,
        format=frame.format.name if frame.format else None,
        samples=frame.samples,
    )


class MpegTsDecoder:
    def __init__(self) -> None:
        self._reader = _BlockingByteStream()
        # TODO: add backpressure here too (bounded queue) if callers are slow to consume.
        self._output: "queue.Queue[object]" = queue.Queue()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="MpegTsDecoder", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def feed(self, data: bytes) -> None:
        self._reader.feed(data)

    def mark_seq(self, seq: Optional[int]) -> None:
        self._reader.mark_seq(seq)

    def close(self) -> None:
        self._reader.close()

    def stop(self) -> None:
        self._stop.set()
        self._reader.close()

    def join(self) -> None:
        self._thread.join()

    def output_queue(self) -> "queue.Queue[object]":
        return self._output

    def _run(self) -> None:
        container: Optional[av.container.InputContainer] = None
        try:
            container = av.open(self._reader, format="mpegts", mode="r")
            for packet in container.demux():
                if self._stop.is_set():
                    break
                demuxed_at = time.time()
                if packet is None:
                    continue
                try:
                    frames = packet.decode()
                except Exception as e:
                    self._output.put(_DecoderError(e))
                    break
                for frame in frames:
                    decoded_at = time.time()
                    decoded = _build_decoded_frame(
                        frame,
                        demuxed_at=demuxed_at,
                        decoded_at=decoded_at,
                        source_segment_seq=self._reader.current_seq(),
                    )
                    self._output.put(decoded)
        except Exception as e:
            self._output.put(_DecoderError(e))
        finally:
            if container is not None:
                try:
                    container.close()
                except Exception:
                    pass
            self._output.put(_END)


def is_decoder_end(item: object) -> bool:
    return item is _END


def decoder_error(item: object) -> Optional[BaseException]:
    if isinstance(item, _DecoderError):
        return item.error
    return None

