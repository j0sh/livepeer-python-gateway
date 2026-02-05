from __future__ import annotations

import asyncio
import base64
import json
import os
import ipaddress
import logging
import re
import socket
import ssl
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Optional, Sequence, Tuple
from urllib.parse import urlparse
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

import grpc

from . import lp_rpc_pb2
from . import lp_rpc_pb2_grpc

from .capabilities import CapabilityId, build_capabilities
from .control import Control
from .events import Events
from .media_publish import MediaPublish, MediaPublishConfig
from .media_output import MediaOutput
from .errors import LivepeerGatewayError, NoOrchestratorAvailableError, SessionRefreshRequired

_HEX_RE = re.compile(r"^(0x)?[0-9a-fA-F]*$")
_LOG = logging.getLogger(__name__)

CAPABILITY_LIVE_VIDEO_TO_VIDEO = 35

def _truncate(s: str, max_len: int = 2000) -> str:
    if len(s) <= max_len:
        return s
    return s[:max_len] + f"...(+{len(s) - max_len} chars)"

def _http_error_body(e: HTTPError) -> str:
    """
    Best-effort read of an HTTPError response body for debugging.
    """
    try:
        b = e.read()
        if not b:
            return ""
        if isinstance(b, bytes):
            return b.decode("utf-8", errors="replace")
        return str(b)
    except Exception:
        return ""

def _extract_error_message(e: HTTPError) -> str:
    """
    Best-effort extraction of a useful error message from an HTTPError body.

    If the body is JSON and matches {"error": {"message": "..."}}, return that message.
    Otherwise return the full body.

    Always truncates the returned value for readability.
    """
    body = _http_error_body(e)
    s = body.strip()
    if not s:
        return ""

    try:
        data = json.loads(s)
    except Exception:
        return _truncate(body)

    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            msg = err.get("message")
            if isinstance(msg, str) and msg:
                return _truncate(msg)

    return _truncate(body)


def request_json(
    url: str,
    *,
    method: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> Any:
    """
    Make a JSON HTTP request and parse the JSON response.

    If method is None, defaults to POST when payload is provided, otherwise GET.

    Raises LivepeerGatewayError on HTTP/network/JSON parsing errors.
    """
    req_headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": "livepeer-python-gateway/0.1",
    }
    body: Optional[bytes] = None
    if payload is not None:
        req_headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode("utf-8")
    if headers:
        req_headers.update(headers)

    resolved_method = method.upper() if method else ("POST" if payload is not None else "GET")
    req = Request(url, data=body, headers=req_headers, method=resolved_method)

    # Always ignore HTTPS certificate validation (matches our gRPC behavior).
    ssl_ctx = ssl._create_unverified_context()

    try:
        with urlopen(req, timeout=timeout, context=ssl_ctx) as resp:
            raw = resp.read().decode("utf-8")
        data: Any = json.loads(raw)
    except HTTPError as e:
        # HTTP 480 is HTTPStatusRefreshSession - signals need to refresh OrchestratorInfo
        if e.code == 480:
            raise SessionRefreshRequired(
                f"Remote signer requires session refresh (HTTP 480)"
            ) from e
        body = _extract_error_message(e)
        body_part = f"; body={body!r}" if body else ""
        raise LivepeerGatewayError(
            f"HTTP JSON error: HTTP {e.code} from endpoint (url={url}){body_part}"
        ) from e
    except ConnectionRefusedError as e:
        raise LivepeerGatewayError(
            f"HTTP JSON error: connection refused (is the server running? is the host/port correct?) (url={url})"
        ) from e
    except URLError as e:
        raise LivepeerGatewayError(
            f"HTTP JSON error: failed to reach endpoint: {getattr(e, 'reason', e)} (url={url})"
        ) from e
    except json.JSONDecodeError as e:
        raise LivepeerGatewayError(f"HTTP JSON error: endpoint did not return valid JSON: {e} (url={url})") from e
    except Exception as e:
        raise LivepeerGatewayError(
            f"HTTP JSON error: unexpected error: {e.__class__.__name__}: {e} (url={url})"
        ) from e

    return data


def post_json(
    url: str,
    payload: dict[str, Any],
    *,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """
    POST JSON to `url` and parse a JSON object response.
    """
    data = request_json(
        url,
        payload=payload,
        headers=headers,
        timeout=timeout,
    )
    if not isinstance(data, dict):
        raise LivepeerGatewayError(
            f"HTTP JSON error: expected JSON object, got {type(data).__name__} (url={url})"
        )
    return data


def get_json(
    url: str,
    *,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> Any:
    """
    GET JSON from `url` and parse the response.
    """
    return request_json(url, headers=headers, timeout=timeout)

def _parse_http_url(url: str, *, context: str = "URL") -> ParseResult:
    """
    Normalize a URL for HTTP(S) endpoints.

    Accepts:
    - "host:port" (implicitly https://host:port)
    - "http://host:port[/...]"
    - "https://host:port[/...]"
    """
    url = url.strip()
    normalized = url if "://" in url else f"https://{url}"
    parsed = urlparse(normalized)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Only http:// or https:// {context}s are supported (got {parsed.scheme!r})")
    if not parsed.netloc:
        raise ValueError(f"Invalid {context}: {url!r}")
    return parsed


def _http_origin(url: str) -> str:
    """
    Normalize a URL (possibly with a path) into a scheme:// origin (scheme + host:port).

    Accepts:
    - "host:port" (implicitly https://host:port)
    - "http://host:port[/...]" (path/query/fragment are ignored)
    - "https://host:port[/...]" (path/query/fragment are ignored)
    """
    parsed = _parse_http_url(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def build_capabilities(capability: int, constraint: Optional[str]) -> lp_rpc_pb2.Capabilities:
    """
    Build a capabilities message with an optional constraint for a specific capability.
    """
    caps = lp_rpc_pb2.Capabilities()
    if capability:
        caps.capacities[capability] = 1
        if constraint:
            if not caps.HasField("constraints"):
                caps.constraints.CopyFrom(lp_rpc_pb2.Capabilities.Constraints())
            constraints = caps.constraints
            per_cap = getattr(constraints, "PerCapability", None) or getattr(constraints, "per_capability", None)
            if per_cap is None:
                per_cap = constraints.PerCapability
            per_cap[capability].models[constraint].warm = False
    return caps


def _select_price_info(
    info: lp_rpc_pb2.OrchestratorInfo,
    *,
    typ: str,
    model_id: Optional[str],
) -> lp_rpc_pb2.PriceInfo:
    """
    Choose the price info to use for a payment request.

    For LV2V, prefer a capability-scoped price that matches the requested model ID.
    Fallback to the general price_info only if no matching capability price exists.
    """
    if typ == "lv2v":
        if not model_id:
            raise LivepeerGatewayError("GetPayment requires model_id for LV2V pricing.")

        # Capability 35 corresponds to Live video to video.
        for pi in info.capabilities_prices:
            if (
                pi.capability == CAPABILITY_LIVE_VIDEO_TO_VIDEO
                and pi.pricePerUnit > 0
                and pi.pixelsPerUnit > 0
                and pi.constraint == model_id
            ):
                return pi

        # No matching capability price; fall back to general price if valid.
        if info.HasField("price_info") and info.price_info.pricePerUnit > 0 and info.price_info.pixelsPerUnit > 0:
            return info.price_info

        raise LivepeerGatewayError(
            f"No capability price found for LV2V model_id={model_id}; orchestrator did not return usable pricing."
        )

    # Non-LV2V: use general price_info first, otherwise any valid capability price.
    if info.HasField("price_info") and info.price_info.pricePerUnit > 0 and info.price_info.pixelsPerUnit > 0:
        return info.price_info

    for pi in info.capabilities_prices:
        if pi.pricePerUnit > 0 and pi.pixelsPerUnit > 0:
            return pi

    raise LivepeerGatewayError("Orchestrator did not return usable pricing information.")


@dataclass(frozen=True)
class StartJobRequest:
    # The ID of the Gateway request (for logging purposes).
    request_id: Optional[str] = None
    # ModelId Name of the pipeline to run in the live video to video job.
    model_id: Optional[str] = None
    # Params Initial parameters for the pipeline.
    params: Optional[dict[str, Any]] = None
    # StreamId The Stream ID (for logging purposes).
    stream_id: Optional[str] = None

    def to_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.request_id is not None:
            payload["gateway_request_id"] = self.request_id
        if self.model_id is not None:
            payload["model_id"] = self.model_id
        if self.params is not None:
            payload["params"] = self.params
        if self.stream_id is not None:
            payload["stream_id"] = self.stream_id
        return payload


@dataclass(frozen=True)
class LiveVideoToVideo:
    raw: dict[str, Any]
    manifest_id: Optional[str] = None
    publish_url: Optional[str] = None
    subscribe_url: Optional[str] = None
    control_url: Optional[str] = None
    events_url: Optional[str] = None
    orchestrator_info: Optional[lp_rpc_pb2.OrchestratorInfo] = None
    control: Optional[Control] = None
    events: Optional[Events] = None
    _media: Optional[MediaPublish] = field(default=None, repr=False, compare=False)

    @staticmethod
    def from_json(
        data: dict[str, Any],
        *,
        orchestrator_info: Optional[lp_rpc_pb2.OrchestratorInfo] = None,
    ) -> "LiveVideoToVideo":
        control_url = data.get("control_url") if isinstance(data.get("control_url"), str) else None
        control = Control(control_url) if control_url else None
        publish_url = data.get("publish_url") if isinstance(data.get("publish_url"), str) else None
        events_url = data.get("events_url") if isinstance(data.get("events_url"), str) else None
        events = Events(events_url) if events_url else None
        return LiveVideoToVideo(
            raw=data,
            control_url=control_url,
            events_url=events_url,
            manifest_id=data.get("manifest_id") if isinstance(data.get("manifest_id"), str) else None,
            publish_url=publish_url,
            subscribe_url=data.get("subscribe_url") if isinstance(data.get("subscribe_url"), str) else None,
            orchestrator_info=orchestrator_info,
            control=control,
            events=events,
        )

    def start_media(self, config: MediaPublishConfig) -> MediaPublish:
        """
        Instantiate and return a MediaPublish helper for this job.
        """
        if not self.publish_url:
            raise LivepeerGatewayError("No publish_url present on this LiveVideoToVideo job")
        if self._media is None:
            media = MediaPublish(
                self.publish_url,
                mime_type=config.mime_type,
                keyframe_interval_s=config.keyframe_interval_s,
                fps=config.fps,
            )
            object.__setattr__(self, "_media", media)
        return self._media

    def media_output(
        self,
        *,
        start_seq: int = -2,
        max_retries: int = 5,
        max_segment_bytes: Optional[int] = None,
        connection_close: bool = False,
        chunk_size: int = 64 * 1024,
    ) -> MediaOutput:
        """
        Convenience helper to create a `MediaOutput` for this job.

        This uses `subscribe_url` from the job response and raises if missing.
        Subscription tuning is configured once here (and stored on the returned `MediaOutput`).
        """
        if not self.subscribe_url:
            raise LivepeerGatewayError("No subscribe_url present on this LiveVideoToVideo job")
        return MediaOutput(
            self.subscribe_url,
            start_seq=start_seq,
            max_retries=max_retries,
            max_segment_bytes=max_segment_bytes,
            connection_close=connection_close,
            chunk_size=chunk_size,
        )


    async def close(self) -> None:
        """
        Close any nested helpers (control, media, etc) best-effort.
        """
        tasks = []
        if self.control is not None:
            tasks.append(self.control.close_control())
        if self._media is not None:
            tasks.append(self._media.close())
        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result


@dataclass
class PaymentState:
    """
    Opaque state blob returned by the remote signer that must be sent with
    subsequent payment requests to ensure unique ticket nonces.

    Note: Go's RemotePaymentStateSig struct uses capitalized field names (State, Sig)
    with no JSON tags, so we must match that casing in JSON serialization.
    """
    state: Optional[bytes] = None
    sig: Optional[bytes] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict for the request payload.

        Uses capitalized keys (State, Sig) to match Go's JSON marshaling.
        """
        if self.state is None and self.sig is None:
            return {}
        result = {}
        if self.state is not None:
            result["State"] = base64.b64encode(self.state).decode("ascii")
        if self.sig is not None:
            result["Sig"] = base64.b64encode(self.sig).decode("ascii")
        return result

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "PaymentState":
        """Parse from JSON response.

        Expects capitalized keys (State, Sig) from Go's JSON marshaling.
        """
        state_b64 = data.get("State")
        sig_b64 = data.get("Sig")
        return PaymentState(
            state=base64.b64decode(state_b64) if state_b64 else None,
            sig=base64.b64decode(sig_b64) if sig_b64 else None,
        )


@dataclass(frozen=True)
class GetPaymentResponse:
    payment: str
    seg_creds: Optional[str] = None
    state: Optional[PaymentState] = None


def GetPayment(
    signer_base_url: str,
    info: lp_rpc_pb2.OrchestratorInfo,
    *,
    typ: str = "lv2v",
    model_id: Optional[str] = None,
    state: Optional[PaymentState] = None,
    manifest_id: Optional[str] = None,
) -> GetPaymentResponse:
    """
    Call the remote signer to generate an automatic payment for a job.

    POST {signer_base_url}/generate-live-payment with:
      orchestrator: base64 protobuf bytes of OrchestratorInfo
      priceInfo: pricing selected by the gateway (capability + constraint/model)
      state: optional opaque state from previous payment (for nonce tracking)
      manifestId: optional manifest ID for the payment

    The returned GetPaymentResponse includes the updated state which must be
    passed to subsequent calls to ensure unique ticket nonces.
    """
    if typ == "lv2v" and not model_id:
        raise LivepeerGatewayError(
            "GetPayment requires model_id when requesting LV2V payments."
        )

    if not signer_base_url:
        # Offchain mode: still send the expected headers, but with empty content.
        seg = lp_rpc_pb2.SegData()
        if not info.HasField("auth_token"):
            raise LivepeerGatewayError(
                "Orchestrator did not provide an auth token."
            )
        seg.auth_token.CopyFrom(info.auth_token)
        seg = base64.b64encode(seg.SerializeToString()).decode("ascii")
        return GetPaymentResponse(seg_creds=seg, payment="")

    # Select the appropriate price info based on type and model_id
    price_info = _select_price_info(info, typ=typ, model_id=model_id)

    # Validate the selected price has non-zero values
    if price_info.pricePerUnit <= 0 or price_info.pixelsPerUnit <= 0:
        raise LivepeerGatewayError(
            f"Selected price_info has zero values: pricePerUnit={price_info.pricePerUnit}, "
            f"pixelsPerUnit={price_info.pixelsPerUnit}, model_id={model_id}"
        )

    # Populate price_info on the OrchestratorInfo protobuf that will be sent to the signer.
    info_for_payment = lp_rpc_pb2.OrchestratorInfo()
    info_for_payment.CopyFrom(info)
    info_for_payment.price_info.pricePerUnit = price_info.pricePerUnit
    info_for_payment.price_info.pixelsPerUnit = price_info.pixelsPerUnit
    info_for_payment.price_info.capability = price_info.capability
    info_for_payment.price_info.constraint = price_info.constraint

    # Verify the copy worked correctly
    if info_for_payment.price_info.pricePerUnit <= 0 or info_for_payment.price_info.pixelsPerUnit <= 0:
        raise LivepeerGatewayError(
            f"Failed to set price_info on OrchestratorInfo: pricePerUnit={info_for_payment.price_info.pricePerUnit}, "
            f"pixelsPerUnit={info_for_payment.price_info.pixelsPerUnit}"
        )

    base = _http_origin(signer_base_url)
    url = f"{base}/generate-live-payment"

    # base64 protobuf bytes of OrchestratorInfo (NOT wrapped in PaymentResult)
    pb = info_for_payment.SerializeToString()
    orch_b64 = base64.b64encode(pb).decode("ascii")

    capability = price_info.capability
    if typ == "lv2v" and capability == 0:
        capability = CAPABILITY_LIVE_VIDEO_TO_VIDEO
    constraint = price_info.constraint or (model_id or "")

    payload: dict[str, Any] = {
        "orchestrator": orch_b64,
        "priceInfo": {
            "pricePerUnit": price_info.pricePerUnit,
            "pixelsPerUnit": price_info.pixelsPerUnit,
            "capability": capability,
            "constraint": constraint,
        },
        "type": typ,
    }

    # Include state if provided (required for subsequent payments to get unique nonces)
    if state is not None:
        state_dict = state.to_dict()
        if state_dict:
            payload["state"] = state_dict

    # Include manifest ID if provided
    if manifest_id:
        payload["manifestId"] = manifest_id

    data = post_json(url, payload)

    payment = data.get("payment")
    if not isinstance(payment, str) or not payment:
        raise LivepeerGatewayError(f"GetPayment error: missing/invalid 'payment' in response (url={url})")

    seg_creds = data.get("segCreds")
    if seg_creds is not None and not isinstance(seg_creds, str):
        raise LivepeerGatewayError(f"GetPayment error: invalid 'segCreds' in response (url={url})")

    # Parse the returned state for use in subsequent calls
    new_state = None
    state_data = data.get("state")
    if isinstance(state_data, dict):
        new_state = PaymentState.from_dict(state_data)

    return GetPaymentResponse(payment=payment, seg_creds=seg_creds, state=new_state)


def _start_job_with_headers(
    info: lp_rpc_pb2.OrchestratorInfo,
    req: StartJobRequest,
    headers: dict[str, Optional[str]],
) -> LiveVideoToVideo:
    """Internal helper to start a job with pre-built headers."""
    base = _http_origin(info.transcoder)
    url = f"{base}/live-video-to-video"
    data = post_json(url, req.to_json(), headers=headers)
    return LiveVideoToVideo.from_json(data)


def start_lv2v(
    orch_url: Optional[Sequence[str] | str],
    req: StartJobRequest,
    *,
    signer_base_url: Optional[str] = None,
    discovery_url: Optional[str] = None,
) -> LiveVideoToVideo:
    """
    Start a live video-to-video job.

    Selects an orchestrator with LV2V capability and calls
    POST {info.transcoder}/live-video-to-video with JSON body.
    """
    if not req.model_id:
        raise LivepeerGatewayError("start_lv2v requires model_id")

    capabilities = build_capabilities(CapabilityId.LIVE_VIDEO_TO_VIDEO, req.model_id)
    _, info = SelectOrchestrator(
        orch_url,
        signer_url=signer_base_url,
        discovery_url=discovery_url,
        capabilities=capabilities,
    )

    p = GetPayment(signer_base_url, info, model_id=req.model_id)
    headers: dict[str, Optional[str]] = {
        "Livepeer-Payment": p.payment,
        "Livepeer-Segment": p.seg_creds,
    }

    return _start_job_with_headers(info, req, headers)




@dataclass(frozen=True)
class SignerMaterial:
    """
    Material returned by the remote signer.
    address: 20-byte broadcaster ETH address
    sig: signature bytes (length depends on scheme; commonly 65 bytes for ECDSA)
    """
    address: bytes
    sig: bytes


def _hex_to_bytes(s: str, *, expected_len: Optional[int] = None) -> bytes:
    s = s.strip()
    if not _HEX_RE.match(s):
        raise ValueError(f"Not a hex string: {s!r}")
    if s.startswith(("0x", "0X")):
        s = s[2:]
    if len(s) % 2 == 1:
        # allow odd-length hex (pad left)
        s = "0" + s
    b = bytes.fromhex(s)
    if expected_len is not None and len(b) != expected_len:
        raise ValueError(f"Expected {expected_len} bytes, got {len(b)} bytes")
    return b


def _split_host_port(target: str) -> Tuple[str, int]:
    """
    Parse a gRPC target in the form "host:port" or "[ipv6]:port".
    """
    target = target.strip()
    if target.startswith("["):
        # [::1]:8935
        host, rest = target[1:].split("]", 1)
        if not rest.startswith(":"):
            raise ValueError(f"Invalid gRPC target (missing port): {target!r}")
        return host, int(rest[1:])

    # host:port (we don't support bare host here)
    if target.count(":") != 1:
        raise ValueError(f"Invalid gRPC target (expected host:port): {target!r}")
    host, port_s = target.split(":", 1)
    return host, int(port_s)


def _parse_grpc_target(orch_url: str) -> str:
    """
    Normalize user input into a gRPC target string suitable for grpc.secure_channel().

    Accepts:
    - "host:port"
    - "[ipv6]:port"
    - "https://host:port" (scheme is stripped; TLS is still used by the channel)
    """
    orch_url = orch_url.strip()
    # If no scheme is provided, treat it as https://... implicitly.
    # Also avoids `urlparse("localhost:8935")` interpreting "localhost" as a scheme.
    url = orch_url if "://" in orch_url else f"https://{orch_url}"

    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"Only https:// orchestrator URLs are supported (got {parsed.scheme!r})")
    if not parsed.netloc:
        raise ValueError(f"Invalid orchestrator URL: {orch_url!r}")
    if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
        # gRPC targets are host:port; ignore any path-like components by rejecting explicitly.
        raise ValueError(f"Orchestrator URL must not include a path/query/fragment: {orch_url!r}")
    return parsed.netloc

def _is_ip_address(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def _pick_cert_authority(cert: dict) -> Optional[str]:
    """
    Choose an authority value that will satisfy gRPC hostname verification.
    Prefers DNS SAN, then IP SAN, then Common Name.
    """
    san = cert.get("subjectAltName") or []
    for typ, val in san:
        if typ == "DNS" and val:
            return val
    for typ, val in san:
        if typ in ("IP Address", "IP") and val:
            return val

    # subject is a tuple of RDN tuples: ((('commonName','example.com'),), ...)
    for rdn in cert.get("subject") or []:
        for key, val in rdn:
            if key == "commonName" and val:
                return val
    return None


def _decode_pem_cert(pem: bytes) -> dict:
    """
    Decode a PEM cert into a dict containing subject / subjectAltName.

    Note: Python's `SSLSocket.getpeercert()` returns `{}` when verify_mode=CERT_NONE,
    so we decode the PEM ourselves via a CPython helper.
    """
    try:
        decode = ssl._ssl._test_decode_cert  # pyright: ignore[reportAttributeAccessIssue]
    except Exception:
        return {}

    tmp_path: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(pem)
            tmp_path = f.name
        return decode(tmp_path) or {}
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


@lru_cache(maxsize=None)
def _trust_on_first_use_root_cert_target(target: str) -> Tuple[bytes, str]:
    """
    Fetch the server certificate via a TLS handshake with verification disabled,
    then "trust" that exact certificate by using it as the root cert for gRPC.

    Returns (root_cert_pem_bytes, authority_override).
    """
    host, port = _split_host_port(target)

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    # gRPC requires HTTP/2; many gRPC-TLS servers expect ALPN "h2" in the ClientHello.
    try:
        ctx.set_alpn_protocols(["h2"])
    except NotImplementedError:
        # Some Python/OpenSSL builds may not support ALPN; best effort.
        pass

    # Only send SNI for DNS names; IP SNI can trigger "Invalid server name indication".
    server_hostname = None if _is_ip_address(host) else host

    with socket.create_connection((host, port), timeout=5.0) as sock:
        with ctx.wrap_socket(sock, server_hostname=server_hostname) as ssock:
            der = ssock.getpeercert(binary_form=True)

    pem = ssl.DER_cert_to_PEM_cert(der).encode("ascii")
    decoded = _decode_pem_cert(pem)
    authority = _pick_cert_authority(decoded) or host
    return pem, authority


def _trust_on_first_use_root_cert(orch_url: str) -> Tuple[bytes, str, str]:
    """
    Wrapper that:
    - accepts https://host:port or host:port
    - returns (root_pem, authority, target)
    - re-raises failures as OrchestratorRpcError with the *original* orch_url
    """
    try:
        target = _parse_grpc_target(orch_url)
        root_pem, authority = _trust_on_first_use_root_cert_target(target)
        return root_pem, authority, target
    except Exception as e:
        raise OrchestratorRpcError(
            orch_url,
            f"TLS trust-on-first-use probe failed: {e.__class__.__name__}: {e}",
            cause=e,
        ) from None


@lru_cache(maxsize=None)
def _get_signer_material(signer_base_url: str) -> SignerMaterial:
    """
    Fetch signer material exactly once per signer_base_url for the lifetime of the process.
    Subsequent calls return cached data.
    """

    # check for offchain mode
    if not signer_base_url:
        return SignerMaterial(address=None, sig=None)

    # Accept either a base URL or a full URL that includes /sign-orchestrator-info.
    # Normalize to an https:// origin and append the expected path.
    signer_url = f"{_http_origin(signer_base_url)}/sign-orchestrator-info"

    try:
        # Some signers accept/expect POST with an empty JSON object.
        data = post_json(signer_url, {}, timeout=5.0)

        # Expected response shape (example):
        # {
        #   "address": "0x0123...abcd",   # 20-byte ETH address hex
        #   "signature": "0x..."          # signature hex
        # }
        if "address" not in data or "signature" not in data:
            raise RemoteSignerError(
                    signer_url,
                    f"Remote signer JSON must contain 'address' and 'signature': {data!r}",
                    cause=None,
            ) from None

        address = _hex_to_bytes(str(data["address"]), expected_len=20)
        sig = _hex_to_bytes(str(data["signature"]))  # signature length may vary

    except LivepeerGatewayError as e:
        # post_json wraps the underlying exception as __cause__; convert back into
        # a signer-specific error message.
        cause = e.__cause__ or e

        if isinstance(cause, HTTPError):
            body = _extract_error_message(cause)
            body_part = f"; body={body!r}" if body else ""
            raise RemoteSignerError(
                signer_url,
                f"HTTP {cause.code} from signer{body_part}",
                cause=cause,
            ) from None

        if isinstance(cause, ConnectionRefusedError):
            raise RemoteSignerError(
                signer_url,
                "connection refused (is the signer running? is the host/port correct?)",
                cause=cause,
            ) from None

        if isinstance(cause, URLError):
            raise RemoteSignerError(
                signer_url,
                f"failed to reach signer: {getattr(cause, 'reason', cause)}",
                cause=cause,
            ) from None

        if isinstance(cause, json.JSONDecodeError):
            raise RemoteSignerError(
                signer_url,
                f"signer did not return valid JSON: {cause}",
                cause=cause,
            ) from None

        raise RemoteSignerError(
            signer_url,
            f"unexpected error: {cause.__class__.__name__}: {cause}",
            cause=cause if isinstance(cause, BaseException) else e,
        ) from None

    return SignerMaterial(address=address, sig=sig)


class OrchestratorClient:
    def __init__(
        self,
        orch_url: str,
        *,
        signer_url: Optional[str] = None,
        capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
    ) -> None:
        self.orch_url = orch_url
        self.signer_url = signer_url
        self.capabilities = capabilities

        # Always use TLS. "Ignore" invalid/self-signed certs by trusting the exact
        # certificate the server presents (trust-on-first-use) and overriding the
        # expected authority to match that cert.
        root_pem, authority, target = _trust_on_first_use_root_cert(orch_url)
        credentials = grpc.ssl_channel_credentials(root_certificates=root_pem)
        options = [
            ("grpc.ssl_target_name_override", authority),
            ("grpc.default_authority", authority),
        ]
        self._channel = grpc.secure_channel(target, credentials, options=options)
        self._stub = lp_rpc_pb2_grpc.OrchestratorStub(self._channel)

    def GetOrchestratorInfo(
        self,
        *,
        caps: Optional[lp_rpc_pb2.Capabilities] = None,
    ) -> lp_rpc_pb2.OrchestratorInfo:
        """
        Wrapper for the Orchestrator RPC method GetOrchestrator(...)
        but exposed as GetOrchestratorInfo() for convenience.

        If caps is provided, the orchestrator will return pricing specific to
        the requested capability and constraint (e.g., model_id for LV2V).
        """
        try:
            signer = _get_signer_material(self.signer_url)
        except Exception as e:
            # Ensure caller sees a clean library error (no raw traceback).
            raise OrchestratorRpcError(
                self.orch_url,
                f"{e.__class__.__name__}: {e}",
                cause=e,
            ) from None

        request = lp_rpc_pb2.OrchestratorRequest(
            address=signer.address,
            sig=signer.sig,
            capabilities=caps,
            ignoreCapacityCheck=True,
        )
        if self.capabilities is not None:
            request.capabilities.CopyFrom(self.capabilities)

        try:
            return self._stub.GetOrchestrator(request, timeout=5.0)
        except grpc.RpcError as e:
            # e.details() may be None; be defensive
            details = ""
            try:
                details = e.details() or ""
            except Exception:
                details = ""

            code = ""
            try:
                code = str(e.code())
            except Exception:
                code = "UNKNOWN"

            msg = details or repr(e)
            raise OrchestratorRpcError(self.orch_url, f"{code}: {msg}", cause=e) from None


def GetOrchestratorInfo(
    orch_url: str,
    *,
    signer_url: Optional[str] = None,
    capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
    caps: Optional[lp_rpc_pb2.Capabilities] = None,
    typ: str = "lv2v",
    model_id: Optional[str] = None,
) -> lp_rpc_pb2.OrchestratorInfo:
    """
    Public functional API:
        GetOrchestratorInfo(orch_url, signer_url=..., typ="lv2v", model_id=...)
    Remote signer is called once per process (cached).
    Always uses secure channel (TLS) with certificate verification disabled.

    For live-video-to-video (typ="lv2v"), if a model_id is provided we
    request orchestrator info with capability 35 and constraint=model_id so
    ticket params align with the requested model.
    """
    effective_caps = caps
    if typ == "lv2v" and model_id:
        effective_caps = build_capabilities(CAPABILITY_LIVE_VIDEO_TO_VIDEO, model_id)
    return OrchestratorClient(
        orch_url,
        signer_url=signer_url,
        capabilities=capabilities,
    ).GetOrchestratorInfo(caps=effective_caps)


def DiscoverOrchestrators(
    orchestrators: Optional[Sequence[str] | str] = None,
    *,
    signer_url: Optional[str] = None,
    discovery_url: Optional[str] = None,
) -> list[str]:
    """
    Discover orchestrators and return a list of addresses.

    This discovery can happen via the following parameters in priority order (highest first):
    - orchestrators: list or comma-delimited string
      (empty/whitespace-only input falls through)
    - discovery_url: use this discovery endpoint
    - signer_url: use signer-provided discovery service
    """
    if orchestrators is not None:
        if isinstance(orchestrators, str):
            orch_list = [orch.strip() for orch in orchestrators.split(",")]
        else:
            try:
                orch_list = list(orchestrators)
            except TypeError as e:
                raise LivepeerGatewayError(
                    "DiscoverOrchestrators requires a list of orchestrator URLs or a comma-delimited string"
                ) from e
        orch_list = [orch.strip() for orch in orch_list if isinstance(orch, str) and orch.strip()]
        if orch_list:
            return orch_list

    if discovery_url:
        discovery_endpoint = _parse_http_url(discovery_url).geturl()
    elif signer_url:
        discovery_endpoint = f"{_http_origin(signer_url)}/discover-orchestrators"
    else:
        _LOG.debug("DiscoverOrchestrators failed: no discovery inputs")
        raise LivepeerGatewayError("DiscoverOrchestrators requires discovery_url or signer_url")

    try:
        _LOG.debug("DiscoverOrchestrators running discovery: %s", discovery_endpoint)
        data = get_json(discovery_endpoint)
    except LivepeerGatewayError as e:
        _LOG.debug("DiscoverOrchestrators discovery failed: %s", e)
        raise RemoteSignerError(
            discovery_endpoint,
            str(e),
            cause=e.__cause__ or e,
        ) from None

    if not isinstance(data, list):
        _LOG.debug(
            "DiscoverOrchestrators discovery response not list: type=%s",
            type(data).__name__,
        )
        raise RemoteSignerError(
            discovery_endpoint,
            f"Discovery response must be a JSON list, got {type(data).__name__}",
            cause=None,
        ) from None

    _LOG.debug("DiscoverOrchestrators discovery response: %s", data)

    orch_list = []
    for item in data:
        if not isinstance(item, dict):
            continue
        address = item.get("address")
        if isinstance(address, str) and address.strip():
            orch_list.append(address.strip())
    _LOG.debug("DiscoverOrchestrators discovered %d orchestrators", len(orch_list))

    return orch_list


def SelectOrchestrator(
    orchestrators: Optional[Sequence[str] | str] = None,
    *,
    signer_url: Optional[str] = None,
    discovery_url: Optional[str] = None,
    capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
) -> Tuple[str, lp_rpc_pb2.OrchestratorInfo]:
    """
    Select an orchestrator by trying up to ~5 candidates in parallel.

    If orchestrators is empty/None, a discovery endpoint is used:
    - discovery_url, if provided
    - otherwise {signer_url}/discover-orchestrators
    """
    orch_list = DiscoverOrchestrators(
        orchestrators,
        signer_url=signer_url,
        discovery_url=discovery_url,
    )

    if not orch_list:
        _LOG.debug("SelectOrchestrator failed: empty orchestrator list")
        raise NoOrchestratorAvailableError("No orchestrators available to select")

    candidates = orch_list[:5]

    _LOG.debug("SelectOrchestrator trying candidates: %s", candidates)
    with ThreadPoolExecutor(max_workers=len(candidates)) as executor:
        futures = {
            executor.submit(
                GetOrchestratorInfo,
                url,
                signer_url=signer_url,
                capabilities=capabilities,
            ): url
            for url in candidates
        }

        for future in as_completed(futures):
            url = futures[future]
            try:
                info = future.result()
            except LivepeerGatewayError as e:
                _LOG.debug("SelectOrchestrator candidate failed: %s (%s)", url, e)
                continue
            _LOG.debug("SelectOrchestrator selected: %s", url)
            return url, info

    _LOG.debug("SelectOrchestrator failed: all candidates errored")
    raise NoOrchestratorAvailableError("All orchestrators failed")


@dataclass
class RemoteSignerError(LivepeerGatewayError):
    signer_url: str
    message: str
    cause: Optional[BaseException] = None

    def __str__(self) -> str:
        return f"Remote signer error: {self.message} (url={self.signer_url})"


@dataclass
class OrchestratorRpcError(LivepeerGatewayError):
    orch_url: str
    message: str
    cause: Optional[BaseException] = None

    def __str__(self) -> str:
        return f"Orchestrator RPC error: {self.message} (orch={self.orch_url})"

