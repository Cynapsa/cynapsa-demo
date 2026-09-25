from __future__ import annotations

from typing import Any

import httpx


class ModelError(Exception):
    """A safe model failure that never includes credentials or upstream bodies."""

    def __init__(self, status_code: int | None) -> None:
        self.status_code = status_code
        super().__init__("model request failed")


class ModelClient:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str,
        model: str,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not api_key or not model:
            raise ValueError("model API key and model are required")
        root = base_url.rstrip("/")
        url = httpx.URL(root)
        if url.scheme != "https" or not url.host:
            raise ValueError("model base URL must use HTTPS")
        if not root.endswith("/v1"):
            root += "/v1"
        self.model = model
        self._client = httpx.Client(
            base_url=root + "/",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=25.0,
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def complete(
        self,
        messages: list[dict[str, Any]],
        *,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {"model": self.model, "messages": messages}
        if tools is not None:
            request["tools"] = tools
        if tool_choice is not None:
            request["tool_choice"] = tool_choice
        try:
            response = self._client.post("chat/completions", json=request)
        except httpx.HTTPError as exc:
            raise ModelError(None) from exc
        if response.status_code >= 400:
            raise ModelError(response.status_code)
        try:
            result = response.json()
            message = result["choices"][0]["message"]
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise ModelError(None) from exc
        if not isinstance(message, dict):
            raise ModelError(None)
        return message
