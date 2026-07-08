# SAVER Search

SAVER backend가 RabbitMQ로 발행한 검색 작업을 소비하고, Kagi Search API 결과를 Redis에 기록하는 장기 실행 worker다.

Kagi v1 Search API에는 Bearer 인증과 `{"query": "...", "workflow": "search"}` JSON body를 사용하는 `POST /api/v1/search`로 요청한다.

worker는 Kagi 응답을 검증하고 필요한 필드만 다음 형태로 Redis에 저장한다. 연관 검색어의 `title`, 검색 결과의 필수 `url`/`title`과 선택 `snippet`/`image` 외 필드는 저장하지 않는다.

```json
{
  "meta": {"ms": 123},
  "data": {
    "related_search": [{"title": "연관 검색어"}],
    "search": [
      {
        "url": "https://example.com",
        "title": "검색 결과",
        "snippet": "선택 설명",
        "image": {"url": "https://example.com/image.png"}
      }
    ]
  }
}
```

## 실행

Python 의존성을 설치하고 저장소 루트의 `.env`에 최소한 `APIKEY`를 설정한다.

```shell
python -m pip install -r requirements.txt
python -m src.main
```

기본 연결값은 Redis `localhost:6379`, RabbitMQ `localhost:5672`, queue `saver.search.requests`다. 다음 환경 변수로 조정할 수 있다.

- Redis: `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_PASSWORD`, `SEARCH_QUERY_TTL`, `SEARCH_LEASE_TTL`
- RabbitMQ: `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_VHOST`, `SEARCH_QUEUE`, `RABBITMQ_HEARTBEAT`, `SEARCH_PREFETCH_COUNT`
- Kagi: `APIKEY`, `KAGI_SEARCH_URL`, `KAGI_TIMEOUT`, `KAGI_MAX_ATTEMPTS`, `KAGI_RETRY_BASE_DELAY`
- worker: `SEARCH_MAX_MESSAGE_BYTES`, `SEARCH_MAX_RESULT_BYTES`, `SEARCH_BUSY_REQUEUE_DELAY`, `LOG_LEVEL`

메시지와 Redis 계약의 상세 내용은 [`AGENTS.md`](AGENTS.md)를 참고한다.

## 검증

기본 테스트는 외부 API를 호출하지 않는다.

```shell
python -m unittest discover -s tests -v
python -m compileall -q src tests
```

로컬 Redis와 RabbitMQ를 사용하는 통합 테스트는 임시 queue와 key를 만들었다가 삭제한다.

```shell
SAVER_INTEGRATION_TEST=1 python -m unittest tests.test_integration -v
```
