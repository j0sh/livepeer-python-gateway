from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional
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
        insecure: bool = True,
    ) -> None:
        self.orch_url = orch_url
        self.signer_url = signer_url or os.getenv("LIVEPEER_SIGNER_URL")

        if insecure:
            self._channel = grpc.insecure_channel(orch_url)
        else:
            raise NotImplementedError("Secure channel not implemented yet")

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

