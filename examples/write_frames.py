import argparse
import asyncio
from fractions import Fraction

import av

from livepeer_gateway import (
    LivePaymentConfig,
    LivepeerGatewayError,
    MediaPublishConfig,
    OrchestratorSession,
    StartJobRequest,
)

DEFAULT_ORCH = "localhost:8935"
DEFAULT_MODEL_ID = "noop"
DEFAULT_PAYMENT_INTERVAL = 5.0


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
        help="Remote signer base URL (no path). If omitted, runs in offchain mode.",
    )
    p.add_argument(
        "--model-id",
        default=DEFAULT_MODEL_ID,
        help=f"Pipeline model to start via /live-video-to-video. Default: {DEFAULT_MODEL_ID}",
    )
    p.add_argument("--width", type=int, default=320, help="Frame width (default: 320).")
    p.add_argument("--height", type=int, default=180, help="Frame height (default: 180).")
    p.add_argument("--fps", type=float, default=30.0, help="Frames per second (default: 30).")
    p.add_argument("--count", type=int, default=90, help="Number of frames to send (default: 90).")
    p.add_argument(
        "--payment-interval",
        type=float,
        default=DEFAULT_PAYMENT_INTERVAL,
        help=f"Payment interval in seconds (default: {DEFAULT_PAYMENT_INTERVAL}).",
    )
    return p.parse_args()


def _solid_rgb_frame(width: int, height: int, rgb: tuple[int, int, int]) -> av.VideoFrame:
    frame = av.VideoFrame(width, height, "rgb24")
    r, g, b = rgb
    frame.planes[0].update(bytes([r, g, b]) * (width * height))
    return frame


async def main() -> None:
    args = _parse_args()
    frame_interval = 1.0 / max(1e-6, args.fps)

    # Configure live payments if signer is provided
    live_payment_config = None
    if args.signer:
        live_payment_config = LivePaymentConfig(
            interval_s=args.payment_interval,
            width=args.width,
            height=args.height,
            fps=args.fps,
        )

    session = OrchestratorSession(
        args.orchestrator,
        signer_url=args.signer,
        live_payment_config=live_payment_config,
    )

    job = None
    try:
        job = session.start_job(StartJobRequest(model_id=args.model_id))

        print("=== LiveVideoToVideo ===")
        print("publish_url:", job.publish_url)
        print("subscribe_url:", job.subscribe_url)
        if args.signer and live_payment_config:
            print(f"live payments: enabled (interval={args.payment_interval}s)")
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

        print(f"Sent {args.count} frames successfully")
    except LivepeerGatewayError as e:
        print(f"ERROR: {e}")
    finally:
        await session.aclose()


if __name__ == "__main__":
    asyncio.run(main())

