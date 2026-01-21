import argparse
import asyncio
import json

from livepeer_gateway.orchestrator import (
    GetOrchestratorInfo,
    LivepeerGatewayError,
    StartJob,
    StartJobRequest,
)


DEFAULT_ORCH = "localhost:8935"
DEFAULT_MODEL_ID = "noop"  # fix


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Start an LV2V job and subscribe to its events channel.")
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
    p.add_argument("--count", type=int, default=0, help="Max events to print (0 = no limit).")
    p.add_argument("--buffer", type=int, default=256, help="Max buffered events (default: 256).")
    p.add_argument(
        "--overflow",
        default="drop_oldest",
        choices=("error", "drop_oldest", "drop_newest"),
        help="Overflow policy when the buffer is full.",
    )
    return p.parse_args()


async def main() -> None:
    args = _parse_args()

    try:
        info = GetOrchestratorInfo(args.orchestrator, signer_url=args.signer)
        job = StartJob(
            info,
            StartJobRequest(model_id=args.model_id),
            signer_base_url=args.signer,
        )

        print("=== LiveVideoToVideo ===")
        print("events_url:", job.events_url)
        print()

        if not job.events:
            raise LivepeerGatewayError("No events_url present on this LiveVideoToVideo job")

        seen = 0
        async for event in job.events(
            max_buffered_events=args.buffer,
            overflow=args.overflow,
        ):
            print(json.dumps(event, indent=2, sort_keys=True))
            print()
            seen += 1
            if args.count and seen >= args.count:
                break

    except LivepeerGatewayError as e:
        print(f"ERROR ({args.orchestrator}): {e}")
    finally:
        try:
            if "job" in locals():
                await job.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())

