import argparse
import json

from livepeer_gateway.orchestrator import GetOrchestratorInfo, LivepeerGatewayError, StartJob, StartJobRequest

DEFAULT_ORCH = "localhost:8935"
DEFAULT_SIGNER_URL = "https://vyt5g5r8tu9hrv.transfix.ai"  # base URL; adjust
DEFAULT_MODEL_ID = "noop" # fix

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch orchestrator info via Livepeer gRPC.")
    p.add_argument(
        "orchestrator",
        nargs="?",
        default=DEFAULT_ORCH,
        help=f"Orchestrator gRPC target (host:port). Default: {DEFAULT_ORCH}",
    )
    p.add_argument(
        "--signer-url",
        default=DEFAULT_SIGNER_URL,
        help="Remote signer base URL (no path). Used for /generate-live-payment.",
    )
    p.add_argument(
        "--model-id",
        default=DEFAULT_MODEL_ID,
        help=f"Pipeline model_id to start via /live-video-to-video. Default: {DEFAULT_MODEL_ID}",
    )
    return p.parse_args()

def main() -> None:
    args = _parse_args()

    orch_url = args.orchestrator
    try:
        info = GetOrchestratorInfo(orch_url, signer_url=args.signer_url)

        print("=== OrchestratorInfo ===")
        print("Orchestrator:", orch_url)
        print("Transcoder URI:", info.transcoder)
        print("ETH Address:", info.address.hex())
        print()

        url = info.transcoder
        job = StartJob(
            info,
            StartJobRequest(
                model_id=args.model_id,
            ),
            signer_base_url=args.signer_url,
        )
        print("=== StartJob ===")
        print("Endpoint:", f"{url}/live-video-to-video")
        print(json.dumps(job.raw, indent=2, sort_keys=True))
        print()

    except LivepeerGatewayError as e:
        print(f"ERROR ({orch_url}): {e}")
        print()

if __name__ == "__main__":
    main()
