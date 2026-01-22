"""
Capture MacOS camera frames, publish to Livepeer, and show a 2x2 composite
of input, loopback, output, and an empty tile with latency overlays.
"""

import argparse
import asyncio
from collections import deque
import logging
import queue
import threading
import time
from contextlib import suppress
from fractions import Fraction
from typing import Optional

import av

from livepeer_gateway.media_publish import MediaPublishConfig
from livepeer_gateway.media_output import MediaOutput
from livepeer_gateway.orchestrator import (
    GetOrchestratorInfo,
    LivepeerGatewayError,
    StartJob,
    StartJobRequest,
)

DEFAULT_ORCH = "localhost:8935"
DEFAULT_MODEL_ID = "noop"  # fix
DEFAULT_DEVICE = "0"
DEFAULT_FPS = 30.0
DEFAULT_VIDEO_SIZE = "640x480"
DEFAULT_PIXEL_FORMAT = "nv12"
_TIME_BASE = 90_000
_STOP = object()


class _OneLineExceptionFormatter(logging.Formatter):
    def formatException(self, ei) -> str:
        return ""


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(name)s:%(message)s",
        force=True,
    )
    for handler in logging.getLogger().handlers:
        handler.setFormatter(_OneLineExceptionFormatter("%(levelname)s:%(name)s:%(message)s"))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Capture camera frames and show a 2x2 composite with latency overlays."
    )
    p.add_argument(
        "orchestrator",
        nargs="?",
        default=DEFAULT_ORCH,
        help=f"Orchestrator gRPC target (host:port). Default: {DEFAULT_ORCH}",
    )
    p.add_argument(
        "--signer",
        default=None,
        help="Remote signer base URL (no path). If omitted, runs in offchain mode.",
    )
    p.add_argument(
        "--model-id",
        default=DEFAULT_MODEL_ID,
        help=f"Pipeline model_id to start via /live-video-to-video. Default: {DEFAULT_MODEL_ID}",
    )
    p.add_argument(
        "--device",
        default=DEFAULT_DEVICE,
        help=(
            "Camera device index for avfoundation (default: 0). "
            'List devices with: ffmpeg -f avfoundation -list_devices true -i ""'
        ),
    )
    p.add_argument("--fps", type=float, default=DEFAULT_FPS, help="Frames per second (default: 30).")
    p.add_argument(
        "--video-size",
        default=DEFAULT_VIDEO_SIZE,
        help=f"Capture size (e.g. '1920x1080'). Default: {DEFAULT_VIDEO_SIZE}.",
    )
    p.add_argument(
        "--pixel-format",
        default=DEFAULT_PIXEL_FORMAT,
        help=(
            "Capture pixel format for avfoundation. "
            "Supported formats vary by device; common options: uyvy422, yuyv422, nv12."
        ),
    )
    p.add_argument(
        "--avg-window",
        type=float,
        default=3.5,
        help="Moving average window in seconds for latency metrics (default: 3.5).",
    )
    return p.parse_args()


def _capture_frames(
    input_: av.container.InputContainer,
    frame_queue: "queue.Queue[object]",
    stop_event: threading.Event,
) -> None:
    try:
        print("Running camera capture...")
        while not stop_event.is_set():
            try:
                for frame in input_.decode(video=0):
                    if stop_event.is_set():
                        break
                    frame_queue.put(frame)
            except av.BlockingIOError:
                continue
    finally:
        frame_queue.put(_STOP)


def _bgr_from_frame(frame: av.VideoFrame):
    return frame.to_ndarray(format="bgr24")


def _resize_to_height(img, height: int):
    import cv2

    if img is None:
        return None
    h, w = img.shape[:2]
    if h == height:
        return img
    width = max(1, int(round(w * (height / h))))
    return cv2.resize(img, (width, height))


def _letterbox_to_square(img, size: int):
    import cv2
    import numpy as np

    tile = np.zeros((size, size, 3), dtype=np.uint8)
    if img is None:
        return tile
    h, w = img.shape[:2]
    if h <= 0 or w <= 0:
        return tile
    scale = min(size / w, size / h)
    new_w = max(1, int(round(w * scale)))
    new_h = max(1, int(round(h * scale)))
    resized = cv2.resize(img, (new_w, new_h))
    x = (size - new_w) // 2
    y = (size - new_h) // 2
    tile[y : y + new_h, x : x + new_w] = resized
    return tile


def _draw_labels(img, lines):
    import cv2

    y = 28
    for line in lines:
        cv2.putText(
            img,
            line,
            (12, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (0, 255, 0),
            2,
            cv2.LINE_AA,
        )
        y += 28


async def main() -> None:
    _configure_logging()
    args = _parse_args()

    try:
        import cv2  # type: ignore[import-not-found]
        import numpy as np  # type: ignore[import-not-found]
    except ImportError as e:
        print(
            "This example requires opencv-python and numpy.\n"
            "Install with: pip install opencv-python numpy\n"
            f"ImportError: {e}"
        )
        return

    job = None
    input_ = None
    stop_event = threading.Event()
    stop_async = asyncio.Event()
    output_task: Optional[asyncio.Task[None]] = None
    loopback_task: Optional[asyncio.Task[None]] = None
    display_task: Optional[asyncio.Task[None]] = None

    latest = {
        "cam_img": None,
        "cam_pts_time": None,
        "loop_img": None,
        "loop_pts_time": None,
        "out_img": None,
        "out_pts_time": None,
    }
    latest_lock = threading.Lock()
    avg_window_s = max(0.1, float(args.avg_window))
    rtt_samples: "deque[tuple[float, float]]" = deque()
    total_samples: "deque[tuple[float, float]]" = deque()
    inference_samples: "deque[tuple[float, float]]" = deque()

    def _push_avg(samples: "deque[tuple[float, float]]", now: float, value: float) -> float:
        samples.append((now, value))
        cutoff = now - avg_window_s
        while samples and samples[0][0] < cutoff:
            samples.popleft()
        return sum(v for _, v in samples) / len(samples)

    def _update_cam(img, pts_time: Optional[float]) -> None:
        with latest_lock:
            latest["cam_img"] = img
            latest["cam_pts_time"] = pts_time

    def _update_out(img, pts_time: Optional[float]) -> None:
        with latest_lock:
            latest["out_img"] = img
            latest["out_pts_time"] = pts_time

    def _update_loop(img, pts_time: Optional[float]) -> None:
        with latest_lock:
            latest["loop_img"] = img
            latest["loop_pts_time"] = pts_time

    async def _subscribe_output() -> None:
        if job is None:
            return
        output = job.media_output()
        async for decoded in output.frames():
            if stop_async.is_set():
                break
            if decoded.kind != "video":
                continue
            try:
                img = _bgr_from_frame(decoded.frame)
            except Exception:
                continue
            _update_out(img, decoded.pts_time)

    async def _subscribe_loopback() -> None:
        if job is None or not job.publish_url:
            return
        loopback = MediaOutput(job.publish_url)
        async for decoded in loopback.frames():
            if stop_async.is_set():
                break
            if decoded.kind != "video":
                continue
            try:
                img = _bgr_from_frame(decoded.frame)
            except Exception:
                continue
            _update_loop(img, decoded.pts_time)

    async def _display_loop() -> None:
        title = "input | loopback | output | empty (press q to quit)"
        while not stop_async.is_set():
            with latest_lock:
                cam_img = latest["cam_img"]
                out_img = latest["out_img"]
                cam_pts_time = latest["cam_pts_time"]
                out_pts_time = latest["out_pts_time"]
                loop_img = latest["loop_img"]
                loop_pts_time = latest["loop_pts_time"]

            if cam_img is not None:
                base_img = cam_img
            elif loop_img is not None:
                base_img = loop_img
            else:
                base_img = out_img
            if base_img is not None:
                tile_size = min(base_img.shape[0], base_img.shape[1])
            else:
                tile_size = 480

            if tile_size > 0:
                now = time.time()
                if cam_pts_time is not None and loop_pts_time is not None:
                    rtt_s = cam_pts_time - loop_pts_time
                    rtt_avg = _push_avg(rtt_samples, now, rtt_s)
                    rtt_label = f"RTT: {rtt_s:+.3f}s (avg {rtt_avg:+.3f}s)"
                else:
                    rtt_s = None
                    rtt_label = "RTT: n/a"

                if cam_pts_time is not None and out_pts_time is not None:
                    total_delta_s = cam_pts_time - out_pts_time
                    total_avg = _push_avg(total_samples, now, total_delta_s)
                    total_label = f"Total Delta: {total_delta_s:+.3f}s (avg {total_avg:+.3f}s)"
                else:
                    total_delta_s = None
                    total_label = "Total Delta: n/a"

                if total_delta_s is not None and rtt_s is not None:
                    inference_s = total_delta_s - rtt_s
                    inference_avg = _push_avg(inference_samples, now, inference_s)
                    inference_label = f"Inference: {inference_s:+.3f}s (avg {inference_avg:+.3f}s)"
                else:
                    inference_label = "Inference: n/a"

                input_tile = _letterbox_to_square(cam_img, tile_size)
                loop_tile = _letterbox_to_square(loop_img, tile_size)
                out_tile = _letterbox_to_square(out_img, tile_size)
                empty_tile = _letterbox_to_square(None, tile_size)

                if cam_img is None:
                    _draw_labels(input_tile, ["CAMERA PENDING"])
                else:
                    _draw_labels(input_tile, ["INPUT"])

                if loop_img is None:
                    _draw_labels(loop_tile, ["LOOPBACK PENDING"])
                else:
                    _draw_labels(loop_tile, ["LOOPBACK", rtt_label])

                if out_img is None:
                    _draw_labels(out_tile, ["OUTPUT PENDING"])
                else:
                    _draw_labels(out_tile, ["OUTPUT", inference_label, total_label])

                _draw_labels(empty_tile, ["EMPTY"])

                top = np.hstack([input_tile, loop_tile])
                bottom = np.hstack([out_tile, empty_tile])
                composite = np.vstack([top, bottom])
                cv2.imshow(title, composite)

            key = cv2.waitKey(1) & 0xFF
            if key == ord("q"):
                stop_event.set()
                stop_async.set()
                break
            await asyncio.sleep(0.005)

        cv2.destroyAllWindows()

    try:
        info = GetOrchestratorInfo(args.orchestrator, signer_url=args.signer)
        job = StartJob(
            info,
            StartJobRequest(model_id=args.model_id),
            signer_base_url=args.signer,
        )

        print("=== LiveVideoToVideo ===")
        print("publish_url:", job.publish_url)
        print("subscribe_url:", job.subscribe_url)
        print()

        media = job.start_media(MediaPublishConfig(fps=args.fps))

        av.logging.set_level(av.logging.ERROR)
        input_ = av.open(
            args.device,
            format="avfoundation",
            container_options={
                "framerate": str(args.fps),
                "video_size": args.video_size,
                "pixel_format": args.pixel_format,
            },
        )

        frame_queue: "queue.Queue[object]" = queue.Queue(maxsize=8)
        capture_thread = threading.Thread(
            target=_capture_frames,
            args=(input_, frame_queue, stop_event),
            name="CameraCapture",
            daemon=True,
        )
        capture_thread.start()

        output_task = asyncio.create_task(_subscribe_output())
        loopback_task = asyncio.create_task(_subscribe_loopback())
        display_task = asyncio.create_task(_display_loop())

        time_base = Fraction(1, _TIME_BASE)
        last_pts = 0
        last_time: Optional[float] = None

        print("Running publish...")
        while not stop_async.is_set():
            item = await asyncio.to_thread(frame_queue.get)
            if item is _STOP:
                break
            frame = item
            now = time.time()
            if last_time is not None:
                last_pts += int((now - last_time) * _TIME_BASE)
            else:
                last_pts = 0
            last_time = now

            frame.pts = last_pts
            frame.time_base = time_base
            cam_pts_time = last_pts / _TIME_BASE
            try:
                cam_img = _bgr_from_frame(frame)
                _update_cam(cam_img, cam_pts_time)
            except Exception:
                _update_cam(None, cam_pts_time)
            await media.write_frame(frame)
    except KeyboardInterrupt:
        print("Recording stopped by user")
    except LivepeerGatewayError as e:
        print(f"Error processing frame ({args.orchestrator}): {e}")
    finally:
        stop_event.set()
        stop_async.set()
        if output_task is not None:
            output_task.cancel()
            with suppress(asyncio.CancelledError):
                await output_task
        if loopback_task is not None:
            loopback_task.cancel()
            with suppress(asyncio.CancelledError):
                await loopback_task
        if display_task is not None:
            display_task.cancel()
            with suppress(asyncio.CancelledError):
                await display_task
        if input_ is not None:
            try:
                input_.close()
            except Exception:
                pass
        if job is not None:
            try:
                await job.close()
            except LivepeerGatewayError as e:
                print(f"Error closing job ({args.orchestrator}): {e}")


if __name__ == "__main__":
    asyncio.run(main())

