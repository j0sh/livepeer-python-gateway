from __future__ import annotations

import base64
import json
import os
import ipaddress
import re
import socket
import ssl
import tempfile
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Optional, Tuple
from urllib.parse import urlparse
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

import grpc

from . import lp_rpc_pb2
from . import lp_rpc_pb2_grpc

from .control import Control
from .errors import LivepeerGatewayError

_HEX_RE = re.compile(r"^(0x)?[0-9a-fA-F]*$")

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


def post_json(
    url: str,
    payload: dict[str, Any],
    *,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """
    POST JSON to `url` and parse a JSON object response.

    Raises LivepeerGatewayError on HTTP/network/JSON parsing errors.
    """
    req_headers: dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "livepeer-python-gateway/0.1",
    }
    if headers:
        req_headers.update(headers)

    body = json.dumps(payload).encode("utf-8")
    req = Request(url, data=body, headers=req_headers, method="POST")

    # Always ignore HTTPS certificate validation (matches our gRPC behavior).
    ssl_ctx = ssl._create_unverified_context()

    try:
        with urlopen(req, timeout=timeout, context=ssl_ctx) as resp:
            raw = resp.read().decode("utf-8")
        data: Any = json.loads(raw)
    except HTTPError as e:
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

    if not isinstance(data, dict):
        raise LivepeerGatewayError(
            f"HTTP JSON error: expected JSON object, got {type(data).__name__} (url={url})"
        )

    return data


def _normalize_https_base_url(orch_url: str) -> str:
    """
    Normalize an orchestrator base URL to an https:// URL with no path/query/fragment.

    Accepts:
    - "host:port" (implicitly treated as https://host:port)
    - "https://host:port"
    """
    orch_url = orch_url.strip().rstrip("/")
    url = orch_url if "://" in orch_url else f"https://{orch_url}"

    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"Only https:// orchestrator URLs are supported (got {parsed.scheme!r})")
    if not parsed.netloc:
        raise ValueError(f"Invalid https orchestrator URL: {orch_url!r}")
    if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
        raise ValueError(f"Orchestrator URL must not include a path/query/fragment: {orch_url!r}")
    return f"https://{parsed.netloc}"


def _normalize_https_origin(url: str) -> str:
    """
    Normalize a URL (possibly with a path) into an https:// origin (scheme + host:port).

    Accepts:
    - "host:port" (implicitly https://host:port)
    - "https://host:port[/...]" (path/query/fragment are ignored)
    """
    url = url.strip()
    u = url if "://" in url else f"https://{url}"
    parsed = urlparse(u)
    if parsed.scheme != "https":
        raise ValueError(f"Only https:// URLs are supported (got {parsed.scheme!r})")
    if not parsed.netloc:
        raise ValueError(f"Invalid https URL: {url!r}")
    return f"https://{parsed.netloc}"


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
    control: Optional[Control] = None

    @staticmethod
    def from_json(data: dict[str, Any]) -> "LiveVideoToVideo":
        control_url = data.get("control_url") if isinstance(data.get("control_url"), str) else None
        control = Control(control_url) if control_url else None
        return LiveVideoToVideo(
            raw=data,
            control_url=control_url,
            events_url=data.get("events_url") if isinstance(data.get("events_url"), str) else None,
            manifest_id=data.get("manifest_id") if isinstance(data.get("manifest_id"), str) else None,
            publish_url=data.get("publish_url") if isinstance(data.get("publish_url"), str) else None,
            subscribe_url=data.get("subscribe_url") if isinstance(data.get("subscribe_url"), str) else None,
            control=control,
        )


@dataclass(frozen=True)
class GetPaymentResponse:
    payment: str
    seg_creds: Optional[str] = None


def GetPayment(
    signer_base_url: str,
    info: lp_rpc_pb2.OrchestratorInfo,
    *,
    typ: str = "lv2v",
) -> GetPaymentResponse:
    """
    Call the remote signer to generate an automatic payment for a job.

    POST {signer_base_url}/generate-live-payment with:
      orchestrator: base64 protobuf bytes of net.PaymentResult containing OrchestratorInfo
      type: job type (default: lv2v)
    """

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

    # Price info must be valid
    has_price_info = info.HasField("price_info")
    price_is_zero = has_price_info and info.price_info.pricePerUnit == 0
    if not has_price_info or price_is_zero:
        raise LivepeerGatewayError(
            "Valid price info required when using remote signer. "
            "The orchestrator returned missing or zero price_info."
        )

    base = _normalize_https_base_url(signer_base_url)
    url = f"{base}/generate-live-payment"

    # base64 protobuf bytes of net.PaymentResult containing OrchestratorInfo
    pb = lp_rpc_pb2.PaymentResult(info=info).SerializeToString()
    orch_b64 = base64.b64encode(pb).decode("ascii")

    data = post_json(url, {"orchestrator": orch_b64, "type": typ})

    payment = data.get("payment")
    if not isinstance(payment, str) or not payment:
        raise LivepeerGatewayError(f"GetPayment error: missing/invalid 'payment' in response (url={url})")

    seg_creds = data.get("segCreds")
    if seg_creds is not None and not isinstance(seg_creds, str):
        raise LivepeerGatewayError(f"GetPayment error: invalid 'segCreds' in response (url={url})")

    return GetPaymentResponse(payment=payment, seg_creds=seg_creds)


def StartJob(
    info: lp_rpc_pb2.OrchestratorInfo,
    req: StartJobRequest,
    *,
    signer_base_url: Optional[str] = None,
    typ: str = "lv2v",
) -> LiveVideoToVideo:
    """
    Start a live video-to-video job.

    Calls POST {info.transcoder}/live-video-to-video with JSON body.
    """
    if not req.model_id:
        raise LivepeerGatewayError("StartJob requires model_id")

    p = GetPayment(signer_base_url, info)
    headers: dict[str, str] = {
        "Livepeer-Payment": p.payment,
        "Livepeer-Segment": p.seg_creds,
    }

    base = _normalize_https_base_url(info.transcoder)
    url = f"{base}/live-video-to-video"
    data = post_json(url, req.to_json(), headers=headers)
    return LiveVideoToVideo.from_json(data)




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


def _normalize_grpc_target(orch_url: str) -> str:
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
        raise ValueError(f"Invalid https orchestrator URL: {orch_url!r}")
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
        target = _normalize_grpc_target(orch_url)
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
    signer_url = f"{_normalize_https_origin(signer_base_url)}/sign-orchestrator-info"

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
    ) -> None:
        self.orch_url = orch_url
        self.signer_url = signer_url

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

    def GetOrchestratorInfo(self) -> lp_rpc_pb2.OrchestratorInfo:
        """
        Wrapper for the Orchestrator RPC method GetOrchestrator(...)
        but exposed as GetOrchestratorInfo() for convenience.
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
            ignoreCapacityCheck=True,
            # capabilities=...  # can be added later
        )

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


def GetOrchestratorInfo(orch_url: str, *, signer_url: Optional[str] = None) -> lp_rpc_pb2.OrchestratorInfo:
    """
    Public functional API:
        GetOrchestratorInfo(orch_url, signer_url=...)
    Remote signer is called once per process (cached).
    Always uses secure channel (TLS) with certificate verification disabled.
    """
    return OrchestratorClient(orch_url, signer_url=signer_url).GetOrchestratorInfo()


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

