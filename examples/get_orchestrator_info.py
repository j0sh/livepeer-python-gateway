import argparse

from livepeer_gateway.capabilities import (
    compute_available,
    format_capability,
    get_capacity_in_use,
    get_per_capability_map,
)
from livepeer_gateway.orchestrator import GetOrchestratorInfo, LivepeerGatewayError

DEFAULT_ORCH = "localhost:8935"

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch orchestrator info via Livepeer gRPC.")
    p.add_argument(
        "orchestrators",
        nargs="*",
        default=[DEFAULT_ORCH],
        help=f"One or more orchestrator gRPC targets (host:port). Default: {DEFAULT_ORCH}",
    )
    p.add_argument(
        "--signer",
        default=None,
        help="Remote signer base URL (no path). If omitted, runs in offchain mode.",
    )
    return p.parse_args()

def main() -> None:
    args = _parse_args()

    for orch_url in args.orchestrators:
        try:
            info = GetOrchestratorInfo(orch_url, signer_url=args.signer)

            print("=== OrchestratorInfo ===")
            print("Orchestrator:", orch_url)
            print("Transcoder URI:", info.transcoder)
            print("ETH Address:", info.address.hex())
            if info.HasField("capabilities") and info.capabilities.version:
                print("Version:", info.capabilities.version)
            else:
                print("Capabilities: not provided")
                print()
                continue
            print()

            caps = info.capabilities
            per_capability = get_per_capability_map(caps)

            cap_ids = set(caps.capacities.keys())
            cap_ids.update(per_capability.keys())

            if not cap_ids:
                print("Capabilities: none advertised")
                print()
                continue

            print("Capabilities:")
            for cap_id in sorted(cap_ids):
                print(f"- {format_capability(cap_id)}")

                has_capacity = cap_id in caps.capacities
                if has_capacity and caps.capacities[cap_id] > 1:
                    print(f"  Total capacity: {caps.capacities[cap_id]}")

                cap_constraints = per_capability.get(cap_id)
                models = getattr(cap_constraints, "models", None) if cap_constraints else None
                if models:
                    print("  Models:")
                    for model_name, model_constraint in models.items():
                        warm = bool(getattr(model_constraint, "warm", False))
                        runner = str(getattr(model_constraint, "runnerVersion", "")) or "-"
                        capacity = int(getattr(model_constraint, "capacity", 0) or 0)
                        in_use = get_capacity_in_use(model_constraint)
                        available = compute_available(capacity, in_use)
                        print(
                            "    "
                            f"{model_name}: warm={warm} runner={runner} "
                            f"capacity={capacity} in_use={in_use} available={available}"
                        )

                if not has_capacity and not models:
                    print("  Capacity: not provided")

            print()

        except LivepeerGatewayError as e:
            print(f"ERROR ({orch_url}): {e}")
            print()

if __name__ == "__main__":
    main()
