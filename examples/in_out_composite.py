"""
Capture MacOS camera frames, publish to Livepeer, and show a side-by-side
composite of camera input and decoded media output with a PTS delta overlay.
"""

import argparse
import asyncio
import logging
import queue
import threading
import time
from contextlib import suppress
from fractions import Fraction
from typing import Optional

import av

from livepeer_gateway.media_publish import MediaPublishConfig
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
        description="Capture camera frames and show camera/output composite with PTS delta."
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
    display_task: Optional[asyncio.Task[None]] = None

    latest = {
        "cam_img": None,
        "cam_pts_time": None,
        "out_img": None,
        "out_pts_time": None,
    }
    latest_lock = threading.Lock()

    def _update_cam(img, pts_time: Optional[float]) -> None:
        with latest_lock:
            latest["cam_img"] = img
            latest["cam_pts_time"] = pts_time

    def _update_out(img, pts_time: Optional[float]) -> None:
        with latest_lock:
            latest["out_img"] = img
            latest["out_pts_time"] = pts_time

    async def _subscribe_output() -> None:
        if job is None:
            return
        out = job.media_output()
        async for decoded in out.frames():
            if stop_async.is_set():
                break
            if decoded.kind != "video":
                continue
            try:
                img = _bgr_from_frame(decoded.frame)
            except Exception:
                continue
            _update_out(img, decoded.pts_time)

    async def _display_loop() -> None:
        title = "camera | output (press q to quit)"
        while not stop_async.is_set():
            with latest_lock:
                cam_img = latest["cam_img"]
                out_img = latest["out_img"]
                cam_pts_time = latest["cam_pts_time"]
                out_pts_time = latest["out_pts_time"]

            if cam_img is not None and out_img is not None:
                target_height = min(cam_img.shape[0], out_img.shape[0])
                cam_disp = _resize_to_height(cam_img, target_height)
                out_disp = _resize_to_height(out_img, target_height)
                if cam_disp is not None and out_disp is not None:
                    composite = np.hstack([cam_disp, out_disp])
                    if cam_pts_time is not None and out_pts_time is not None:
                        delta_s = cam_pts_time - out_pts_time
                        label = f"PTS delta: {delta_s:+.3f}s"
                    else:
                        label = "PTS delta: n/a"
                    cv2.putText(
                        composite,
                        "INPUT",
                        (12, 28),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.8,
                        (0, 255, 0),
                        2,
                        cv2.LINE_AA,
                    )
                    cv2.putText(
                        composite,
                        "OUTPUT",
                        (cam_disp.shape[1] + 12, 28),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.8,
                        (0, 255, 0),
                        2,
                        cv2.LINE_AA,
                    )
                    cv2.putText(
                        composite,
                        label,
                        (cam_disp.shape[1] + 12, 58),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.8,
                        (0, 255, 0),
                        2,
                        cv2.LINE_AA,
                    )
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

