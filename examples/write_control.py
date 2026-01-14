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
    p = argparse.ArgumentParser(description="Start an LV2V job and write JSON messages to its control channel.")
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
        "--message",
        default='{"type":"ping"}',
        help='JSON object to send on the control channel (default: {"type":"ping"}).',
    )
    p.add_argument("--count", type=int, default=1, help="How many times to send the message (default: 1).")
    p.add_argument("--interval", type=float, default=0.2, help="Seconds between messages (default: 0.2).")
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
        print("control_url:", job.control_url)
        print()

        if not job.control:
            raise LivepeerGatewayError("No control_url present on this LiveVideoToVideo job")

        msg = json.loads(args.message)
        if not isinstance(msg, dict):
            raise ValueError("--message must be a JSON object")

        for i in range(max(0, args.count)):
            await job.control.write_control({**msg, "n": i})
            if i + 1 < args.count:
                await asyncio.sleep(args.interval)

    except LivepeerGatewayError as e:
        print(f"ERROR ({args.orchestrator}): {e}")
    finally:
        try:
            # Best-effort (no-op if control was never opened).
            if "job" in locals():
                if job.control:
                    await job.control.close_control()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())


