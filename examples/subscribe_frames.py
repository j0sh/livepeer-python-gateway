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
    p = argparse.ArgumentParser(description="Start an LV2V job and subscribe to decoded media frames.")
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
    p.add_argument("--count", type=int, default=0, help="Max frames to print (0 = no limit).")
    return p.parse_args()


async def main() -> None:
    args = _parse_args()
    job = None
    try:
        info = GetOrchestratorInfo(args.orchestrator, signer_url=args.signer)
        job = StartJob(
            info,
            StartJobRequest(model_id=args.model_id),
            signer_base_url=args.signer,
        )

        print("=== LiveVideoToVideo ===")
        print("subscribe_url:", job.subscribe_url)
        print()

        sub = job.media_output()

        seen = 0
        async for frame in sub.frames():
            payload = {
                "kind": frame.kind,
                "pts": frame.pts,
                "time_s": frame.time_s,
                "time_base": str(frame.time_base) if frame.time_base else None,
                "demuxed_at": frame.demuxed_at,
                "decoded_at": frame.decoded_at,
                "source_segment_seq": frame.source_segment_seq,
            }
            if frame.kind == "video":
                payload.update(
                    {
                        "width": frame.width,
                        "height": frame.height,
                        "pix_fmt": frame.pix_fmt,
                    }
                )
            else:
                payload.update(
                    {
                        "sample_rate": frame.sample_rate,
                        "layout": frame.layout,
                        "format": frame.format,
                        "samples": frame.samples,
                    }
                )
            print(json.dumps(payload, sort_keys=True))
            seen += 1
            if args.count and seen >= args.count:
                break
    except LivepeerGatewayError as e:
        print(f"ERROR ({args.orchestrator}): {e}")
    finally:
        if job is not None:
            try:
                await job.close()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())

