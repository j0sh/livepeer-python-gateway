import argparse
import asyncio
from fractions import Fraction

import av

from livepeer_gateway.errors import LivepeerGatewayError
from livepeer_gateway.lv2v import StartJobRequest, start_lv2v
from livepeer_gateway.media_publish import MediaPublishConfig

DEFAULT_MODEL_ID = "noop"  # fix


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Start an LV2V job and publish raw frames via publish_url.")
    p.add_argument(
        "orchestrator",
        nargs="?",
        default=None,
        help="Orchestrator (host:port). If omitted, discovery is used.",
    )
    p.add_argument(
        "--signer",
        default=None,
        help="Remote signer URL (no path). If omitted, runs in offchain mode.",
    )
    p.add_argument(
        "--model",
        default=DEFAULT_MODEL_ID,
        help=f"Pipeline model to start via /live-video-to-video. Default: {DEFAULT_MODEL_ID}",
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
        job = start_lv2v(
            args.orchestrator,
            StartJobRequest(model_id=args.model),
            signer_url=args.signer,
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
            await job.send_payment_if_due()
            await asyncio.sleep(frame_interval)
    except LivepeerGatewayError as e:
        print(f"ERROR: {e}")
    finally:
        if job is not None:
            await job.close()


if __name__ == "__main__":
    asyncio.run(main())

