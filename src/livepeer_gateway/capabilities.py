from __future__ import annotations

from typing import Any, Mapping

CAPABILITY_ID_TO_NAME: dict[int, str] = {
    -2: "Invalid",
    -1: "Unused",
    0: "H.264",
    1: "MPEGTS",
    2: "MP4",
    3: "Fractional framerates",
    4: "Storage direct",
    5: "Storage S3",
    6: "Storage GCS",
    7: "H264 Baseline profile",
    8: "H264 Main profile",
    9: "H264 High profile",
    10: "H264 Constrained, Contained High profile",
    11: "GOP",
    12: "Auth token",
    14: "MPEG7 signature",
    15: "HEVC decode",
    16: "HEVC encode",
    17: "VP8 decode",
    18: "VP9 decode",
    19: "VP8 encode",
    20: "VP9 encode",
    21: "H264 Decode YUV444 8-bit",
    22: "H264 Decode YUV422 8-bit",
    23: "H264 Decode YUV444 10-bit",
    24: "H264 Decode YUV422 10-bit",
    25: "H264 Decode YUV420 10-bit",
    26: "Segment slicing",
    27: "Text to image",
    28: "Image to image",
    29: "Image to video",
    30: "Upscale",
    31: "Audio to text",
    32: "Segment anything 2",
    33: "Llm",
    34: "Image to text",
    35: "Live video to video",
    36: "Text to speech",
}


def capability_name(cap_id: int) -> str:
    return CAPABILITY_ID_TO_NAME.get(cap_id, "Unknown capability")


def format_capability(cap_id: int) -> str:
    return f"{capability_name(cap_id)} ({cap_id})"


def compute_available(capacity: int, in_use: int) -> int:
    return max(0, capacity - in_use)


def get_per_capability_map(capabilities: Any) -> Mapping[int, Any]:
    """
    Best-effort access to the PerCapability map across protobuf field name variants.
    """
    constraints = getattr(capabilities, "constraints", None)
    if constraints is None:
        return {}

    for name in ("PerCapability", "per_capability", "perCapability"):
        if hasattr(constraints, name):
            return getattr(constraints, name)

    return {}


def get_capacity_in_use(model_constraint: Any) -> int:
    if hasattr(model_constraint, "capacityInUse"):
        return int(getattr(model_constraint, "capacityInUse") or 0)
    if hasattr(model_constraint, "capacity_in_use"):
        return int(getattr(model_constraint, "capacity_in_use") or 0)
    return 0

