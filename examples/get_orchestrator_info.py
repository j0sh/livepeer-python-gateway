import argparse

from livepeer_gateway import lp_rpc_pb2
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
        nargs="+",
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
            info = GetOrchestratorInfo(
                orch_url,
                signer_url=args.signer,
            )

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

            # Display pricing information
            has_general_price = info.HasField("price_info") and info.price_info.pricePerUnit > 0
            has_cap_prices = bool(info.capabilities_prices)
            
            if has_general_price or has_cap_prices:
                print("Pricing:")
                
                if has_general_price:
                    price_per_unit = info.price_info.pricePerUnit
                    pixels_per_unit = info.price_info.pixelsPerUnit if info.price_info.pixelsPerUnit > 0 else 1
                    print(f"  General: {price_per_unit} wei per {pixels_per_unit} pixel(s)")
                
                if has_cap_prices:
                    if has_general_price:
                        print("  Capability-specific prices:")
                    for cap_price in info.capabilities_prices:
                        if cap_price.pricePerUnit > 0:
                            cap_id = cap_price.capability if cap_price.capability else "?"
                            cap_name = format_capability(cap_id) if cap_id != "?" else "unknown"
                            price_per_unit = cap_price.pricePerUnit
                            pixels_per_unit = cap_price.pixelsPerUnit if cap_price.pixelsPerUnit > 0 else 1
                            constraint = f" [{cap_price.constraint}]" if cap_price.constraint else ""
                            indent = "    " if has_general_price else "  "
                            print(f"{indent}{cap_name} ({cap_id}){constraint}: {price_per_unit} wei per {pixels_per_unit} pixel(s)")
                
                print()
            else:
                print("Pricing: not provided")
                print()

            if info.hardware:
                print("Hardware / GPU:")
                for hw in info.hardware:
                    pipeline = hw.pipeline or "-"
                    model_id = hw.model_id or "-"
                    print(f"- Pipeline: {pipeline} | Model: {model_id}")
                    if not hw.gpu_info:
                        print("  GPU info: not provided")
                        continue
                    for key, gpu in hw.gpu_info.items():
                        gpu_id = gpu.id or "-"
                        name = gpu.name or "-"
                        compute = f"{gpu.major}.{gpu.minor}"
                        mem_free = format_bytes(int(gpu.memory_free))
                        mem_total = format_bytes(int(gpu.memory_total))
                        print(
                            "  "
                            f"{key}: id={gpu_id} name={name} "
                            f"compute={compute} mem_free={mem_free} mem_total={mem_total}"
                        )
                print()
            else:
                print("Hardware / GPU: not provided")
                print()

        except LivepeerGatewayError as e:
            print(f"ERROR ({orch_url}): {e}")
            print()


def format_bytes(num_bytes: int) -> str:
    if num_bytes < 0:
        return f"{num_bytes} B"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(num_bytes)
    unit_idx = 0
    while size >= 1024.0 and unit_idx < len(units) - 1:
        size /= 1024.0
        unit_idx += 1
    if unit_idx == 0:
        return f"{int(size)} {units[unit_idx]}"
    return f"{size:.2f} {units[unit_idx]} ({num_bytes} B)"

if __name__ == "__main__":
    main()
