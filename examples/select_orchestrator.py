import argparse
import logging

from livepeer_gateway.capabilities import CapabilityId, build_capabilities
from livepeer_gateway.errors import LivepeerGatewayError, NoOrchestratorAvailableError
from livepeer_gateway.selection import orchestrator_selector

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Select an orchestrator via discovery or explicit list.",
        epilog=(
            "Examples, in priority order of application (highest first):\n"
            "  # Orchestrator list\n"
            "  python examples/select_orchestrator.py localhost:8935 localhost:8936\n"
            "  python examples/select_orchestrator.py 'localhost:8935,localhost:8936'\n"
            "  python examples/select_orchestrator.py localhost:8935 --signer https://signer.example.com\n"
            "\n"
            "  # Discover via given URL\n"
            "  python examples/select_orchestrator.py --discovery https://discover.example.com/orchestrators\n"
            "  python examples/select_orchestrator.py --discovery https://discover.example.com/orchestrators \\\n"
            "    --signer https://signer.example.com\n"
            "\n"
            "  # Discover via signer URL\n"
            "  python examples/select_orchestrator.py --signer https://signer.example.com\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "orchestrators",
        nargs="*",
        help="Optional list of orchestrators (host:port) or comma-delimited string.",
    )
    p.add_argument(
        "--discovery",
        default=None,
        help="Explicit discovery endpoint URL (overrides signer discovery).",
    )
    p.add_argument(
        "--model",
        default=None,
        help="Pipeline model ID for capability filtering. If omitted, no capability filter is applied.",
    )
    p.add_argument(
        "--signer",
        default=None,
        help="Remote signer base URL (no path). Can be combined with list/discovery.",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging for selection diagnostics.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    capabilities = build_capabilities(CapabilityId.LIVE_VIDEO_TO_VIDEO, args.model) if args.model else None

    try:
        cursor = orchestrator_selector(
            args.orchestrators or None,
            signer_url=args.signer,
            discovery_url=args.discovery,
            capabilities=capabilities,
        )
        orch_url, info = cursor.next()
        if args.orchestrators:
            mode = "orchestrator list"
        elif args.discovery:
            mode = "discovery URL"
        elif args.signer:
            mode = "signer discovery"
        else:
            mode = "no options"

        print("=== Selected Orchestrator ===")
        print("Mode:", mode)
        if args.model:
            print("Model:", args.model)
        print("Orchestrator:", orch_url)
        print("Transcoder URI:", info.transcoder)
        print("ETH Address:", info.address.hex())
    except NoOrchestratorAvailableError as e:
        print(f"ERROR: {e}")
        for r in e.rejections:
            print(f"  rejected {r.url}: {r.reason}")
    except LivepeerGatewayError as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    main()
