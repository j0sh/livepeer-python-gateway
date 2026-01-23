import argparse
import asyncio
from fractions import Fraction

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


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Start an LV2V job and publish raw frames via publish_url.")
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
    p.add_argument("--width", type=int, default=320, help="Frame width (default: 320).")
    p.add_argument("--height", type=int, default=180, help="Frame height (default: 180).")
    p.add_argument("--fps", type=float, default=30.0, help="Frames per second (default: 30).")
    p.add_argument("--count", type=int, default=90, help="Number of frames to send (default: 90).")
    return p.parse_args()


def _solid_rgb_frame(width: int, height: int, rgb: tuple[int, int, int]) -> av.VideoFrame:
    frame = av.VideoFrame(width, height, "rgb24")
    r, g, b = rgb
    frame.planes[0].update(bytes([r, g, b]) * (width * height))
    return frame


async def main() -> None:
    args = _parse_args()
    frame_interval = 1.0 / max(1e-6, args.fps)

    job = None
    try:
        info = GetOrchestratorInfo(
            args.orchestrator,
            signer_url=args.signer,
            typ="lv2v",
            model_id=args.model_id,
        )
        job = StartJob(
            info,
            StartJobRequest(model_id=args.model_id),
            signer_base_url=args.signer,
        )

        print("=== LiveVideoToVideo ===")
        print("publish_url:", job.publish_url)
        print()

        media = job.start_media(MediaPublishConfig(fps=args.fps))

        time_base = Fraction(1, int(round(args.fps)))
        for i in range(max(0, args.count)):
            color = (i * 5) % 255
            frame = _solid_rgb_frame(args.width, args.height, (color, 0, 255 - color))
            frame.pts = i
            frame.time_base = time_base
            await media.write_frame(frame)
            await asyncio.sleep(frame_interval)
    except LivepeerGatewayError as e:
        print(f"ERROR ({args.orchestrator}): {e}")
    finally:
        if job is not None:
            await job.close()


if __name__ == "__main__":
    asyncio.run(main())

