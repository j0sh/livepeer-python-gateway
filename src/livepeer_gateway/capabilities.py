from __future__ import annotations

from enum import IntEnum
from typing import Any, Mapping, Optional

from . import lp_rpc_pb2


class CapabilityId(IntEnum):
    INVALID = -2
    UNUSED = -1
    H264 = 0
    MPEGTS = 1
    MP4 = 2
    FRACTIONAL_FRAMERATES = 3
    STORAGE_DIRECT = 4
    STORAGE_S3 = 5
    STORAGE_GCS = 6
    H264_BASELINE_PROFILE = 7
    H264_MAIN_PROFILE = 8
    H264_HIGH_PROFILE = 9
    H264_CONSTRAINED_CONTAINED_HIGH_PROFILE = 10
    GOP = 11
    AUTH_TOKEN = 12
    MPEG7_SIGNATURE = 14
    HEVC_DECODE = 15
    HEVC_ENCODE = 16
    VP8_DECODE = 17
    VP9_DECODE = 18
    VP8_ENCODE = 19
    VP9_ENCODE = 20
    H264_DECODE_YUV444_8BIT = 21
    H264_DECODE_YUV422_8BIT = 22
    H264_DECODE_YUV444_10BIT = 23
    H264_DECODE_YUV422_10BIT = 24
    H264_DECODE_YUV420_10BIT = 25
    SEGMENT_SLICING = 26
    TEXT_TO_IMAGE = 27
    IMAGE_TO_IMAGE = 28
    IMAGE_TO_VIDEO = 29
    UPSCALE = 30
    AUDIO_TO_TEXT = 31
    SEGMENT_ANYTHING_2 = 32
    LLM = 33
    IMAGE_TO_TEXT = 34
    LIVE_VIDEO_TO_VIDEO = 35
    TEXT_TO_SPEECH = 36

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


def build_capabilities(
    capability: CapabilityId,
    constraint: Optional[str],
) -> lp_rpc_pb2.Capabilities:
    """
    Build a capabilities message with an optional per-capability model constraint.
    """
    caps = lp_rpc_pb2.Capabilities()
    cap_id = int(capability)
    caps.capacities[cap_id] = 1
    if constraint:
        caps.constraints.PerCapability[cap_id].models[constraint]
    return caps


def capability_pipeline_id(cap_id: int) -> Optional[str]:
    """
    Convert a capability ID to a discovery pipeline ID.

    Example:
        LIVE_VIDEO_TO_VIDEO -> live-video-to-video
    """
    try:
        enum_name = CapabilityId(cap_id).name
    except ValueError:
        return None
    return enum_name.lower().replace("_", "-")


def capabilities_to_query(caps: Optional[lp_rpc_pb2.Capabilities]) -> list[str]:
    """
    Build discovery query values in `pipeline-id/model` form.

    Models are taken as-is from the protobuf constraints map keys.
    """
    if caps is None:
        return []

    constraints = getattr(caps, "constraints", None)
    if constraints is None:
        return []

    per_capability = getattr(constraints, "PerCapability", None)
    if per_capability is None:
        return []

    query_values: list[str] = []
    seen: set[str] = set()
    for cap_id in sorted(per_capability.keys()):
        pipeline_id = capability_pipeline_id(int(cap_id))
        if not pipeline_id:
            continue

        cap_constraint = per_capability[cap_id]
        models = getattr(cap_constraint, "models", None)
        if models is None:
            continue

        for model in sorted(models.keys()):
            if not isinstance(model, str) or not model:
                continue
            value = f"{pipeline_id}/{model}"
            if value in seen:
                continue
            seen.add(value)
            query_values.append(value)

    return query_values

