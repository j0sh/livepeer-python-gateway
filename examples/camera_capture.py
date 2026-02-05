# Camera capture and streaming to Livepeer; works on MacOS only

import argparse
import asyncio
import logging
import queue
import threading
import sys
from contextlib import suppress
from fractions import Fraction

import av

from livepeer_gateway.media_publish import MediaPublishConfig
from livepeer_gateway.orchestrator import LivepeerGatewayError, StartJobRequest, start_lv2v

DEFAULT_MODEL_ID = "noop" # fix
DEFAULT_DEVICE = "0"
DEFAULT_FPS = 30.0
DEFAULT_VIDEO_SIZE = "640x480"
DEFAULT_PIXEL_FORMAT = "nv12"

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
        description="Capture camera frames on MacOS (avfoundation) and publish via write_frame."
    )
    p.add_argument(
        "orchestrator",
        nargs="?",
        default=None,
        help="Orchestrator (host:port). If omitted, discovery is used.",
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
        "--output",
        default=None,
        help="Write subscribed media output to '-' (stdout) or a file path.",
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


async def _write_media_output(job, output: str) -> None:
    if output == "-" or output == "stdout":
        out = sys.stdout.buffer
        close_out = False
    else:
        out = open(output, "wb")
        close_out = True

    try:
        sub = job.media_output()
        async for chunk in sub.bytes():
            await asyncio.to_thread(out.write, chunk)
    finally:
        if close_out:
            out.close()


async def main() -> None:
    _configure_logging()
    args = _parse_args()
    job = None
    input_ = None
    stop_event = threading.Event()
    output_task: asyncio.Task[None] | None = None

    try:
        job = start_lv2v(
            args.orchestrator,
            StartJobRequest(model_id=args.model_id),
            signer_base_url=args.signer,
        )

        print("=== LiveVideoToVideo ===")
        print("publish_url:", job.publish_url)
        if args.output:
            print("subscribe_url:", job.subscribe_url)
        print()

        media = job.start_media(MediaPublishConfig(fps=args.fps))
        if args.output:
            output_task = asyncio.create_task(_write_media_output(job, args.output))

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

        print("Running publish...")
        while True:
            item = await asyncio.to_thread(frame_queue.get)
            if item is _STOP:
                break
            frame = item
            frame.pts = None
            await media.write_frame(frame)
    except KeyboardInterrupt:
        print("Recording stopped by user")
    except LivepeerGatewayError as e:
        print(f"Error processing frame: {e}")
    finally:
        stop_event.set()
        if output_task is not None:
            output_task.cancel()
            with suppress(asyncio.CancelledError):
                await output_task
        if input_ is not None:
            try:
                input_.close()
            except Exception:
                pass
        if job is not None:
            try:
                await job.close()
            except LivepeerGatewayError as e:
                print(f"Error closing job: {e}")


if __name__ == "__main__":
    asyncio.run(main())

