from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from typing import Any


HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
MAGIC_CODE_PATTERN = re.compile(r"^[A-Za-z0-9_-]{43}$")
EXPECTED_FIELDS = frozenset(
    {"schemaVersion", "jobId", "magicCode", "query", "queryHash"}
)


class InvalidSearchMessage(ValueError):
    SAFE_REASON_CODES = frozenset(
        {
            "body_too_large",
            "invalid_encoding",
            "invalid_json",
            "invalid_properties",
            "invalid_schema",
            "invalid_fields",
            "invalid_job_id",
            "invalid_magic_code",
            "invalid_query",
            "hash_mismatch",
        }
    )

    def __init__(self, reason_code: str) -> None:
        self.reason_code = (
            reason_code if reason_code in self.SAFE_REASON_CODES else "invalid_fields"
        )
        super().__init__(self.reason_code)


@dataclass(frozen=True)
class SearchCommand:
    schema_version: int
    job_id: str
    magic_code: str
    query: str
    query_hash: str


def _reject_constant(_value: str) -> None:
    raise ValueError("non-standard JSON constant")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _normalize_query(query: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", query).split()).casefold()


def parse_search_command(
    body: bytes,
    properties: Any,
    *,
    max_body_bytes: int = 65_536,
) -> SearchCommand:
    if len(body) > max_body_bytes:
        raise InvalidSearchMessage("body_too_large")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidSearchMessage("invalid_encoding") from exc
    try:
        payload = json.loads(
            text,
            parse_constant=_reject_constant,
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise InvalidSearchMessage("invalid_json") from exc
    if not isinstance(payload, dict):
        raise InvalidSearchMessage("invalid_json")

    if (
        getattr(properties, "content_type", None) != "application/json"
        or getattr(properties, "content_encoding", None) != "utf-8"
        or getattr(properties, "delivery_mode", None) != 2
        or getattr(properties, "type", None) != "search.requested.v1"
    ):
        raise InvalidSearchMessage("invalid_properties")
    if payload.get("schemaVersion") != 1 or isinstance(
        payload.get("schemaVersion"), bool
    ):
        raise InvalidSearchMessage("invalid_schema")
    if frozenset(payload) != EXPECTED_FIELDS:
        raise InvalidSearchMessage("invalid_fields")

    job_id = payload.get("jobId")
    query_hash = payload.get("queryHash")
    if (
        not isinstance(job_id, str)
        or not HASH_PATTERN.fullmatch(job_id)
        or not isinstance(query_hash, str)
        or not HASH_PATTERN.fullmatch(query_hash)
        or job_id != query_hash
        or getattr(properties, "message_id", None) != job_id
    ):
        raise InvalidSearchMessage("invalid_job_id")

    magic_code = payload.get("magicCode")
    if not isinstance(magic_code, str) or not MAGIC_CODE_PATTERN.fullmatch(magic_code):
        raise InvalidSearchMessage("invalid_magic_code")

    query = payload.get("query")
    if (
        not isinstance(query, str)
        or not 1 <= len(query) <= 200
        or any(ord(character) < 32 for character in query)
        or _normalize_query(query) != query
    ):
        raise InvalidSearchMessage("invalid_query")
    calculated_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()
    if calculated_hash != query_hash:
        raise InvalidSearchMessage("hash_mismatch")

    return SearchCommand(
        schema_version=1,
        job_id=job_id,
        magic_code=magic_code,
        query=query,
        query_hash=query_hash,
    )
