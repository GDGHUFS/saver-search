import json
import unittest

import httpx

from src.config import KagiSettings
from src.kagi import KagiSearchClient, KagiSearchError


def settings(*, max_attempts=3):
    return KagiSettings(
        api_key="test-key",
        endpoint="https://kagi.test/search",
        timeout=1,
        max_attempts=max_attempts,
        retry_base_delay=0.01,
    )


class KagiSearchClientTest(unittest.TestCase):
    def test_returns_compact_standard_json(self):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(
                200,
                json={"data": {"search": [{"title": "검색 결과"}]}},
            )

        http_client = httpx.Client(
            transport=httpx.MockTransport(handler),
        )
        client = KagiSearchClient(
            settings(),
            max_result_bytes=1024,
            client=http_client,
            sleep=lambda _delay: None,
        )

        result = client.search("hufs 날씨")

        self.assertEqual(
            json.loads(result)["data"]["search"][0]["title"],
            "검색 결과",
        )
        self.assertEqual(requests[0].method, "POST")
        self.assertEqual(requests[0].url.query, b"")
        self.assertEqual(
            json.loads(requests[0].content),
            {"query": "hufs 날씨", "workflow": "search"},
        )
        self.assertEqual(requests[0].headers["Authorization"], "Bearer test-key")
        http_client.close()

    def test_retries_transient_response(self):
        statuses = iter((503, 200))
        sleeps = []

        def handler(_request):
            status = next(statuses)
            return httpx.Response(status, json={"data": {"search": []}})

        http_client = httpx.Client(transport=httpx.MockTransport(handler))
        client = KagiSearchClient(
            settings(),
            max_result_bytes=1024,
            client=http_client,
            sleep=sleeps.append,
        )

        client.search("query")

        self.assertEqual(len(sleeps), 1)
        http_client.close()

    def test_does_not_retry_authentication_failure(self):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(401, json={"error": "secret detail"})

        http_client = httpx.Client(transport=httpx.MockTransport(handler))
        client = KagiSearchClient(
            settings(),
            max_result_bytes=1024,
            client=http_client,
            sleep=lambda _delay: None,
        )

        with self.assertRaises(KagiSearchError) as raised:
            client.search("query")

        self.assertEqual(raised.exception.reason_code, "upstream_auth_failed")
        self.assertEqual(len(calls), 1)
        http_client.close()

    def test_rejects_oversized_response(self):
        def handler(_request):
            return httpx.Response(200, json={"data": {"search": "x" * 200}})

        http_client = httpx.Client(transport=httpx.MockTransport(handler))
        client = KagiSearchClient(
            settings(),
            max_result_bytes=50,
            client=http_client,
            sleep=lambda _delay: None,
        )

        with self.assertRaises(KagiSearchError) as raised:
            client.search("query")

        self.assertEqual(raised.exception.reason_code, "result_too_large")
        http_client.close()

    def test_rejects_legacy_array_response_shape(self):
        http_client = httpx.Client(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, json={"data": []})
            )
        )
        client = KagiSearchClient(
            settings(),
            max_result_bytes=1024,
            client=http_client,
            sleep=lambda _delay: None,
        )

        with self.assertRaises(KagiSearchError) as raised:
            client.search("query")

        self.assertEqual(raised.exception.reason_code, "upstream_invalid_response")
        http_client.close()


if __name__ == "__main__":
    unittest.main()
