from __future__ import annotations

import json
import random
import time
from collections.abc import Callable
import httpx

from src.config import KagiSettings
from src.model import KagiSearchResponse


class KagiSearchError(RuntimeError):
    def __init__(self, reason_code: str, *, retryable: bool) -> None:
        self.reason_code = reason_code
        self.retryable = retryable
        super().__init__(reason_code)


def _reject_constant(_value: str) -> None:
    raise ValueError("non-standard JSON constant")


class KagiSearchClient:
    def __init__(
        self,
        settings: KagiSettings,
        *,
        max_result_bytes: int,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._settings = settings
        self._max_result_bytes = max_result_bytes
        self._sleep = sleep
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(settings.timeout),
        )

    def search(self, query: str) -> str:
        last_error: KagiSearchError | None = None
        for attempt in range(1, self._settings.max_attempts + 1):
            try:
                return self._request(query)
            except KagiSearchError as exc:
                last_error = exc
                if not exc.retryable or attempt == self._settings.max_attempts:
                    raise
            delay = self._settings.retry_base_delay * (2 ** (attempt - 1))
            self._sleep(delay + random.uniform(0, delay * 0.2))
        if last_error is not None:  # pragma: no cover - loop exhaustiveness guard
            raise last_error
        raise KagiSearchError("upstream_unknown", retryable=False)

    def _request(self, query: str) -> str:
        try:
            with self._client.stream(
                "POST",
                self._settings.endpoint,
                json={"query": query, "workflow": "search"},
                headers={
                    "Authorization": f"Bearer {self._settings.api_key}",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
            ) as response:
                if response.status_code == 429 or response.status_code >= 500:
                    raise KagiSearchError("upstream_unavailable", retryable=True)
                if response.status_code in (401, 403):
                    raise KagiSearchError("upstream_auth_failed", retryable=False)
                if response.status_code < 200 or response.status_code >= 300:
                    raise KagiSearchError("upstream_rejected", retryable=False)

                chunks: list[bytes] = []
                size = 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > self._max_result_bytes:
                        raise KagiSearchError("result_too_large", retryable=False)
                    chunks.append(chunk)
        except KagiSearchError:
            raise
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            raise KagiSearchError("upstream_unavailable", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise KagiSearchError("upstream_transport_error", retryable=True) from exc

        try:
            result = json.loads(b"".join(chunks), parse_constant=_reject_constant)
            serialized = KagiSearchResponse.model_validate(result).to_result_json()
        except (UnicodeDecodeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise KagiSearchError("upstream_invalid_response", retryable=False) from exc
        if len(serialized.encode("utf-8")) > self._max_result_bytes:
            raise KagiSearchError("result_too_large", retryable=False)
        return serialized

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
