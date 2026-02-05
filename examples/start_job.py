import argparse
import json
import logging

from livepeer_gateway import (
    GetOrchestratorInfo,
    LivepeerGatewayError,
    StartJob,
    StartJobRequest,
)

DEFAULT_MODEL_ID = "noop"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Start an LV2V job and display the response.")
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

    orch_url = args.orchestrator
    try:
        # Request orchestrator info with model_id to get capability-specific pricing
        info = GetOrchestratorInfo(
            orch_url,
            signer_url=args.signer,
            model_id=args.model_id,
        )

        print("=== OrchestratorInfo ===")
        print("Orchestrator:", orch_url)
        print("Transcoder URI:", info.transcoder)
        print("ETH Address:", info.address.hex())
        print()

        url = info.transcoder
        job = StartJob(
            info,
            StartJobRequest(model_id=args.model_id),
            signer_base_url=args.signer,
            discovery_url=args.discovery,
        )
        orch = job.orchestrator_info

        print("=== OrchestratorInfo ===")
        print("Orchestrator:", orch.transcoder)
        print("ETH Address:", orch.address.hex())
        print()

        print("=== start_lv2v ===")
        print(json.dumps(job.raw, indent=2, sort_keys=True))
        print()

    except LivepeerGatewayError as e:
        print(f"ERROR: {e}")
        print()


if __name__ == "__main__":
    main()
