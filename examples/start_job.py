import argparse
import json
import logging

from livepeer_gateway.orchestrator import LivepeerGatewayError, StartJobRequest, start_lv2v

DEFAULT_MODEL_ID = "noop" # fix

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch orchestrator info via Livepeer gRPC.")
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
        "--model-id",
        default=DEFAULT_MODEL_ID,
        help=f"Pipeline model_id to start via /live-video-to-video. Default: {DEFAULT_MODEL_ID}",
    )
    p.add_argument(
        "--discovery",
        default=None,
        help="Discovery endpoint for orchestrators.",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging.",
    )
    return p.parse_args()

def main() -> None:
    args = _parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")

    try:
        job = start_lv2v(
            args.orchestrator,
            StartJobRequest(
                model_id=args.model_id,
            ),
            signer_base_url=args.signer,
            discovery_url=args.discovery_url,
        )
        info = job.orchestrator_info
        if info is None:
            raise LivepeerGatewayError("start_lv2v did not return orchestrator info")

        print("=== OrchestratorInfo ===")
        print("Orchestrator:", info.transcoder)
        print("ETH Address:", info.address.hex())
        print()

        print("=== start_lv2v ===")
        print(json.dumps(job.raw, indent=2, sort_keys=True))
        print()

    except LivepeerGatewayError as e:
        print(f"ERROR: {e}")
        print()

if __name__ == "__main__":
    main()
