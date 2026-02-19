from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Sequence, Tuple

from . import lp_rpc_pb2
from .errors import NoOrchestratorAvailableError, OrchestratorRejection
from .orch_info import get_orch_info
from .orchestrator import discover_orchestrators

_LOG = logging.getLogger(__name__)

_BATCH_SIZE = 5


class SelectionCursor:
    """
    Stateful selector that advances through orchestrators in batches.

    For each batch, all ``get_orch_info`` calls run in parallel and successful
    responses are cached in arrival order. Consumers pop cached successes one at
    a time and only trigger the next batch once the current cache is exhausted.
    """

    def __init__(
        self,
        orch_list: Sequence[str],
        *,
        signer_url: Optional[str] = None,
        signer_headers: Optional[dict[str, str]] = None,
        capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
    ) -> None:
        self._orch_list = list(orch_list)
        self._signer_url = signer_url
        self._signer_headers = signer_headers
        self._capabilities = capabilities
        self._batch_start = 0
        self._pending_successes: list[Tuple[str, lp_rpc_pb2.OrchestratorInfo]] = []
        self.rejections: list[OrchestratorRejection] = []

    def next(self) -> Tuple[str, lp_rpc_pb2.OrchestratorInfo]:
        while True:
            if self._pending_successes:
                selected = self._pending_successes.pop(0)
                _LOG.debug("select_orchestrator selected: %s", selected[0])
                return selected

            if self._batch_start >= len(self._orch_list):
                _LOG.debug(
                    "select_orchestrator failed: all %d orchestrators rejected",
                    len(self._orch_list),
                )
                raise NoOrchestratorAvailableError(
                    f"All orchestrators failed ({len(self.rejections)} tried)",
                    rejections=list(self.rejections),
                )

            self._populate_next_batch_successes()

    def _populate_next_batch_successes(self) -> None:
        batch = self._orch_list[self._batch_start : self._batch_start + _BATCH_SIZE]
        batch_start = self._batch_start
        self._batch_start += _BATCH_SIZE
        _LOG.debug(
            "select_orchestrator trying batch %d-%d: %s",
            batch_start,
            batch_start + len(batch) - 1,
            batch,
        )

        with ThreadPoolExecutor(max_workers=len(batch)) as executor:
            futures = {
                executor.submit(
                    get_orch_info,
                    url,
                    signer_url=self._signer_url,
                    signer_headers=self._signer_headers,
                    capabilities=self._capabilities,
                ): url
                for url in batch
            }

            batch_successes: list[Tuple[str, lp_rpc_pb2.OrchestratorInfo]] = []
            for future in as_completed(futures):
                url = futures[future]
                try:
                    info = future.result()
                except Exception as e:
                    reason = str(e)
                    _LOG.debug(
                        "select_orchestrator candidate failed: %s (%s)",
                        url,
                        reason,
                    )
                    self.rejections.append(OrchestratorRejection(url=url, reason=reason))
                    continue
                batch_successes.append((url, info))

        self._pending_successes.extend(batch_successes)
        if not batch_successes:
            _LOG.debug("select_orchestrator batch exhausted, trying next batch")


def orchestrator_selector(
    orchestrators: Optional[Sequence[str] | str] = None,
    *,
    signer_url: Optional[str] = None,
    signer_headers: Optional[dict[str, str]] = None,
    discovery_url: Optional[str] = None,
    discovery_headers: Optional[dict[str, str]] = None,
    capabilities: Optional[lp_rpc_pb2.Capabilities] = None,
) -> SelectionCursor:
    orch_list = discover_orchestrators(
        orchestrators,
        signer_url=signer_url,
        signer_headers=signer_headers,
        discovery_url=discovery_url,
        discovery_headers=discovery_headers,
        capabilities=capabilities,
    )

    if not orch_list:
        _LOG.debug("select_orchestrator failed: empty orchestrator list")
        raise NoOrchestratorAvailableError("No orchestrators available to select")

    return SelectionCursor(
        orch_list,
        signer_url=signer_url,
        signer_headers=signer_headers,
        capabilities=capabilities,
    )
