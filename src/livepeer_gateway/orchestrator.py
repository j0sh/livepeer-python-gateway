from __future__ import annotations

import json
import os
import ipaddress
import re
import socket
import ssl
import tempfile
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Tuple
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

import grpc

from . import lp_rpc_pb2
from . import lp_rpc_pb2_grpc

from .errors import LivepeerGatewayError

_HEX_RE = re.compile(r"^(0x)?[0-9a-fA-F]*$")

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
def _trust_on_first_use_root_cert(target: str) -> Tuple[bytes, str]:
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


@lru_cache(maxsize=None)
def _get_signer_material(signer_url: str) -> SignerMaterial:
    """
    Fetch signer material exactly once per signer_url for the lifetime of the process.
    Subsequent calls return cached data.
    """
    req = Request(
        signer_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "livepeer-python-gateway/0.1",
        },
        method="POST",
    )

    try:
        with urlopen(req, timeout=5.0) as resp:
            body = resp.read().decode("utf-8")
        data = json.loads(body)

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

    except ConnectionRefusedError as e:
        raise RemoteSignerError(
            signer_url,
            "connection refused (is the signer running? is the host/port correct?)",
            cause=e,
        ) from None
    except HTTPError as e:
        raise RemoteSignerError(
            signer_url,
            f"HTTP {e.code} from signer",
            cause=e,
        ) from None
    except URLError as e:
        # Includes DNS failures, refused connections wrapped as URLError, timeouts, etc.
        raise RemoteSignerError(
            signer_url,
            f"failed to reach signer: {getattr(e, 'reason', e)}",
            cause=e,
        ) from None
    except json.JSONDecodeError as e:
        raise RemoteSignerError(
               signer_url,
               f"signer did not return valid JSON: {e}",
               cause=e,
        ) from None
    except Exception as e:
        raise RemoteSignerError(
            signer_url,
            f"unexpected error: {e.__class__.__name__}: {e}",
            cause=e,
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
        self.signer_url = signer_url or os.getenv("LIVEPEER_SIGNER_URL")

        # Always use TLS. "Ignore" invalid/self-signed certs by trusting the exact
        # certificate the server presents (trust-on-first-use) and overriding the
        # expected authority to match that cert.
        root_pem, authority = _trust_on_first_use_root_cert(orch_url)
        credentials = grpc.ssl_channel_credentials(root_certificates=root_pem)
        options = [
            ("grpc.ssl_target_name_override", authority),
            ("grpc.default_authority", authority),
        ]
        self._channel = grpc.secure_channel(orch_url, credentials, options=options)
        self._stub = lp_rpc_pb2_grpc.OrchestratorStub(self._channel)

    def GetOrchestratorInfo(self) -> lp_rpc_pb2.OrchestratorInfo:
        """
        Wrapper for the Orchestrator RPC method GetOrchestrator(...)
        but exposed as GetOrchestratorInfo() for convenience.
        """
        if not self.signer_url:
            raise ValueError(
                "Remote signer URL not configured. "
                "Pass signer_url=... or set LIVEPEER_SIGNER_URL."
            )

        signer = _get_signer_material(self.signer_url)

        request = lp_rpc_pb2.OrchestratorRequest(
            address=signer.address,
            sig=signer.sig,
            ignoreCapacityCheck=True,
            # capabilities=...  # can be added later
        )

        return self._stub.GetOrchestrator(request, timeout=5.0)


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

