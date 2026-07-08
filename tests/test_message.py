import hashlib
import json
import unittest
from types import SimpleNamespace

from src.message import InvalidSearchMessage, parse_search_command


QUERY = "hufs 날씨"
QUERY_HASH = hashlib.sha256(QUERY.encode("utf-8")).hexdigest()
MAGIC_CODE = "A" * 43


def properties(**overrides):
    values = {
        "content_type": "application/json",
        "content_encoding": "utf-8",
        "delivery_mode": 2,
        "message_id": QUERY_HASH,
        "type": "search.requested.v1",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def body(**overrides):
    values = {
        "schemaVersion": 1,
        "jobId": QUERY_HASH,
        "magicCode": MAGIC_CODE,
        "query": QUERY,
        "queryHash": QUERY_HASH,
    }
    values.update(overrides)
    return json.dumps(values, ensure_ascii=False).encode()


class SearchCommandValidationTest(unittest.TestCase):
    def test_parses_backend_contract(self):
        command = parse_search_command(body(), properties())

        self.assertEqual(command.job_id, QUERY_HASH)
        self.assertEqual(command.query, QUERY)

    def test_rejects_hash_mismatch(self):
        different_hash = "a" * 64

        with self.assertRaises(InvalidSearchMessage) as raised:
            parse_search_command(
                body(jobId=different_hash, queryHash=different_hash),
                properties(message_id=different_hash),
            )

        self.assertEqual(raised.exception.reason_code, "hash_mismatch")

    def test_rejects_non_normalized_query(self):
        with self.assertRaises(InvalidSearchMessage) as raised:
            parse_search_command(body(query="  HUFS   날씨 "), properties())

        self.assertEqual(raised.exception.reason_code, "invalid_query")

    def test_rejects_wrong_event_type(self):
        with self.assertRaises(InvalidSearchMessage) as raised:
            parse_search_command(body(), properties(type="search.requested.v2"))

        self.assertEqual(raised.exception.reason_code, "invalid_properties")

    def test_rejects_duplicate_json_keys(self):
        duplicate = (
            '{"schemaVersion":1,"schemaVersion":1,"jobId":"'
            + QUERY_HASH
            + '","magicCode":"'
            + MAGIC_CODE
            + '","query":"hufs 날씨","queryHash":"'
            + QUERY_HASH
            + '"}'
        ).encode()

        with self.assertRaises(InvalidSearchMessage) as raised:
            parse_search_command(duplicate, properties())

        self.assertEqual(raised.exception.reason_code, "invalid_json")


if __name__ == "__main__":
    unittest.main()
