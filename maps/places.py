from __future__ import annotations

from urllib.parse import quote

import httpx


SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
SEARCH_FIELDS = "places.id,places.displayName,places.formattedAddress,places.googleMapsUri"
DETAIL_FIELDS = "id,displayName,formattedAddress,googleMapsUri,rating,userRatingCount"


class PlacesError(Exception):
    """A safe error category that never includes keys or upstream bodies."""


def _place(raw: dict) -> dict:
    display = raw.get("displayName") or {}
    return {
        "id": raw.get("id"),
        "name": display.get("text") if isinstance(display, dict) else None,
        "address": raw.get("formattedAddress"),
        "google_maps_url": raw.get("googleMapsUri"),
        "rating": raw.get("rating"),
        "rating_count": raw.get("userRatingCount"),
    }


class GooglePlaces:
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client = httpx.Client(timeout=10.0)

    def close(self) -> None:
        self._client.close()

    def _headers(self, fields: str) -> dict[str, str]:
        return {"X-Goog-Api-Key": self._api_key, "X-Goog-FieldMask": fields}

    @staticmethod
    def _result(response: httpx.Response) -> dict:
        try:
            response.raise_for_status()
            result = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise PlacesError("Google Places request failed") from exc
        if not isinstance(result, dict):
            raise PlacesError("Google Places returned an invalid response")
        return result

    def search(self, query: str) -> dict:
        if not isinstance(query, str) or not 1 <= len(query.strip()) <= 300:
            raise ValueError("query must contain 1–300 characters")
        try:
            response = self._client.post(
                SEARCH_URL,
                headers=self._headers(SEARCH_FIELDS),
                json={"textQuery": query.strip(), "pageSize": 5},
            )
        except httpx.HTTPError as exc:
            raise PlacesError("Google Places request failed") from exc
        raw = self._result(response)
        return {"places": [_place(p) for p in raw.get("places", []) if isinstance(p, dict)]}

    def details(self, place_id: str) -> dict:
        if not isinstance(place_id, str) or not 1 <= len(place_id) <= 256:
            raise ValueError("invalid place ID")
        try:
            response = self._client.get(
                f"https://places.googleapis.com/v1/places/{quote(place_id, safe='')}",
                headers=self._headers(DETAIL_FIELDS),
            )
        except httpx.HTTPError as exc:
            raise PlacesError("Google Places request failed") from exc
        return _place(self._result(response))
