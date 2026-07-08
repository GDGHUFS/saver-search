# SAVER Search 작업 지침

## 프로젝트 역할

- 이 저장소는 SAVER의 비동기 검색 worker를 구현한다.
- 주 진입점은 `src/main.py`이며, HTTP API 서버가 아니라 항상 실행되면서 RabbitMQ 메시지를 기다리는 장기 실행 프로세스다.
- `saver-backend`가 검색 요청을 검증하고 RabbitMQ에 작업을 발행한다. 이 worker는 작업을 소비하고 Kagi Search API를 호출한 뒤, backend가 polling 응답에 사용할 결과를 Redis에 기록한다.
- 이 저장소는 사용자 인증, `magicCode` 발급, HTTP 검색 endpoint 제공 또는 검색 요청 발행을 담당하지 않는다.
- 연동 기준은 [GDGHUFS/saver-backend](https://github.com/GDGHUFS/saver-backend)다. 아래 계약은 확인 당시 backend 커밋 `5153f769253284c33d72944af6f3e1efdc5ba5b4`의 `README.md`와 `src/search/` 구현을 기준으로 한다. 연동 코드를 바꿀 때는 backend의 최신 구현을 다시 확인한다.

## 현재 코드의 해석

- `src/main.py`는 설계 초안이지 고정된 구조나 반드시 유지해야 할 구현이 아니다.
- 현재 파일에 포함된 `RabbitMQSearchPublisher`는 backend의 발행 코드를 참고한 것으로, 이 worker의 핵심 책임과 반대 방향이다. 이를 이유로 worker에서 검색 요청을 다시 발행하지 않는다.
- 필요하면 책임별 module, 설정 객체, domain exception 및 테스트로 코드를 분리한다. 단, 불필요한 framework나 새 의존성을 먼저 도입하지 않는다.

## 전체 검색 흐름

1. 로그인한 사용자가 backend의 `POST /search`에 검색어를 보낸다.
2. backend는 검색어를 NFKC 정규화하고, 연속 공백을 하나로 줄이며, `casefold()`를 적용한다.
3. backend는 정규화된 검색어의 UTF-8 SHA-256을 `queryHash`와 `jobId`로 사용한다.
4. backend는 Redis에 짧은 수명의 ticket/query 상태를 만든 뒤, 캐시된 완료 결과가 없으면 RabbitMQ에 persistent 메시지를 발행한다.
5. 이 worker는 메시지를 검증하고 Kagi를 호출한다.
6. 성공 또는 최종 실패 상태를 Redis query hash에 기록한다.
7. Redis 기록이 성공한 뒤 RabbitMQ 메시지를 ACK한다.
8. backend는 Redis만 조회하여 frontend polling에 `202`, `200` 또는 오류를 반환한다. RabbitMQ ACK 자체는 사용자에게 검색 완료를 의미하지 않는다.

## RabbitMQ 입력 계약

기본 queue는 `saver.search.requests`이고 durable queue로 선언된다. backend가 보내는 JSON payload는 다음과 같다.

```json
{
  "schemaVersion": 1,
  "jobId": "정규화된 검색어의 SHA-256 64자리 소문자 hex",
  "magicCode": "43자리 URL-safe token",
  "query": "backend가 정규화한 검색어",
  "queryHash": "정규화된 검색어의 SHA-256 64자리 소문자 hex"
}
```

backend의 AMQP properties는 다음과 같다.

- `content_type`: `application/json`
- `content_encoding`: `utf-8`
- `delivery_mode`: persistent
- `message_id`: `jobId`
- `type`: `search.requested.v1`
- exchange: default exchange (`""`)
- routing key: 설정된 검색 queue 이름

소비 경계에서 다음을 검증한다.

- body가 크기 제한 안의 UTF-8 JSON object인지 확인한다.
- `schemaVersion == 1`, `jobId == queryHash`, `message_id == jobId`인지 확인한다.
- `jobId`와 `queryHash`가 64자리 소문자 SHA-256 hex인지 확인한다.
- `query`가 비어 있지 않은 문자열이며 길이 제한 안인지 확인하고, `sha256(query.encode("utf-8")) == queryHash`인지 확인한다.
- `magicCode`는 현재 결과 key 작성에는 사용하지 않더라도 계약 위반을 탐지할 수 있도록 43자리 URL-safe token인지 확인한다.
- 지원하지 않는 schema, 손상된 JSON, 누락/추가 필드, hash 불일치 같은 영구 오류는 무한 재시도하지 않는다. 안전한 사유 코드와 함께 dead-letter 또는 reject 정책으로 보낸다.

메시지 스키마, queue, exchange 또는 AMQP property를 독자적으로 변경하지 않는다. 변경이 필요하면 backend와 함께 version을 올리고 호환/배포 순서를 정한다.

## Redis 출력 계약

backend가 읽는 검색 결과 key는 다음 Redis hash다.

```text
key: saver:search:query:{queryHash}
fields:
  status: PENDING | COMPLETED | FAILED
  result: UTF-8 JSON 문자열  # COMPLETED일 때 필수
  error_code: 안전한 내부 사유 코드  # FAILED일 때 선택
TTL: backend 기본값 180초
```

- 성공 시 `status=COMPLETED`와 `result=<유효한 JSON 문자열>`을 원자적으로 기록하고 query TTL을 갱신한다.
- backend는 결과를 UTF-8 기준 최대 2,000,000 bytes로 제한한다. 그보다 큰 결과는 저장하지 말고 필요한 필드만 선별하거나 명시적 실패로 처리한다.
- JSON에는 `NaN`, `Infinity`처럼 표준 JSON이 아닌 값을 포함하지 않는다.
- 최종 실패 시 `status=FAILED`와 외부에 노출해도 안전한 고정 `error_code`를 원자적으로 기록하고 TTL을 설정한다. API key, 검색어, endpoint 응답 본문 및 exception message를 저장하지 않는다.
- backend가 발급하는 `saver:search:ticket:{magicCode}`는 backend 소유다. 현재 backend 조회는 ticket이 참조하는 query key의 상태를 읽으므로 worker는 ticket을 생성하거나 삭제하지 않는다.
- Redis key prefix, 상태 문자열, JSON 형식 또는 TTL 의미를 변경할 때는 backend 호환성을 먼저 확인한다.

## 멱등성, ACK 및 재시도

- RabbitMQ는 at-least-once delivery로 가정한다. 동일 `jobId`가 여러 번 도착해도 결과가 손상되거나 불필요한 외부 과금이 반복되지 않도록 처리한다.
- 처리 전에 query key가 이미 유효한 `COMPLETED` 상태인지 확인하고, 그렇다면 Kagi를 다시 호출하지 않고 ACK할 수 있다.
- 동시에 같은 query가 처리될 수 있으므로 단순한 read-then-write만으로 중복 실행이 방지된다고 가정하지 않는다. Redis의 원자 연산, 짧은 lease/lock 또는 동등한 방식으로 작업 소유권과 만료를 설계한다.
- Kagi 성공 결과 또는 최종 실패 상태를 Redis에 확실히 기록한 다음 ACK한다.
- Kagi 호출이 성공했더라도 Redis 기록이 실패하면 ACK하지 않는다. 재전달 시 안전하게 다시 처리할 수 있어야 한다.
- timeout, 연결 단절, HTTP 429 및 일시적 5xx는 제한된 횟수의 exponential backoff와 jitter로 재시도한다. broker를 막는 무제한/긴 sleep은 피한다.
- 인증 실패, 잘못된 요청, 지원하지 않는 schema 같은 영구 실패와 일시적 인프라 장애를 분리한다.
- 소비 실패의 최종 `nack(requeue=...)` 또는 reject 결정은 명시적 retry/dead-letter 정책을 따른다. 설정되지 않은 상태에서 poison message를 즉시 무한 재전달하는 구현을 만들지 않는다.
- consumer에는 적절한 prefetch를 설정하고, 동시 처리량이 Kagi rate limit, Redis 용량 및 정상 종료 시간보다 커지지 않게 한다.

## 외부 I/O와 프로세스 수명주기

- Kagi, RabbitMQ, Redis는 모두 timeout을 명시하고 연결 실패, 응답 지연, rate limit, 잘못된 payload, 부분 실패를 처리한다.
- Kagi HTTP status를 확인한 뒤 JSON을 파싱한다. 성공 status라고 해서 응답 schema가 항상 정상이라고 가정하지 않는다.
- API key와 접속 자격 증명은 환경 변수에서만 읽고 저장소, 테스트 fixture, 로그 또는 예외 응답에 넣지 않는다. 현재 초안의 Kagi 환경 변수 이름은 `APIKEY`다. 이름을 바꾸면 실행 환경과 문서를 함께 갱신한다.
- 시작 시 필수 설정을 검증하고 RabbitMQ/Redis 연결 가능 여부를 확인한다. 필수 의존성을 사용할 수 없으면 메시지를 소비하는 척 실행하지 말고 명확히 실패한다.
- 연결은 처리 건마다 새로 만들지 말고 수명주기에 맞게 재사용하되, 끊어진 연결의 재연결을 지원한다.
- SIGINT/SIGTERM을 받으면 새 delivery 수신을 중단하고, 진행 중 작업을 제한 시간 안에 마무리하거나 안전하게 재전달되도록 한 뒤 HTTP/Redis/RabbitMQ client를 닫는다.
- event loop를 blocking I/O로 막지 않는다. `pika.BlockingConnection`을 유지한다면 connection과 channel은 생성한 단일 전용 thread에서만 다루고 heartbeat/frame 처리를 지속한다. 가능하면 프로젝트의 기존 의존성 안에서 검증된 소비 방식으로 구현한다.

## 보안과 관측성

- RabbitMQ payload, Redis 값 및 Kagi 응답은 신뢰하지 않는 외부 입력으로 취급한다.
- 로그에는 event 이름, 안전한 reason code, exception class, 필요하면 원문이 아닌 `jobId`/`queryHash`를 남긴다.
- `magicCode`, 검색어 원문, Kagi API key, Redis/RabbitMQ 비밀번호, 전체 Kagi 응답 및 원본 exception message는 로그에 남기지 않는다.
- 정상 처리, cache hit, validation reject, retry, 최종 실패, reconnect 및 graceful shutdown을 구분할 수 있게 기록한다.
- 예상 가능한 외부 장애만 구체적으로 잡는다. 광범위한 `except Exception`으로 programming error를 정상 장애처럼 숨기지 않는다. cleanup 경계에서 포괄 처리가 꼭 필요하면 기록 후 process 상태와 message disposition을 명확히 한다.

## 구현 및 테스트 원칙

- 새 도구나 의존성을 추가하기 전에 표준 라이브러리와 현재 의존성(`httpx`, `python-dotenv`, `pika`, `redis`)으로 해결 가능한지 확인한다.
- transport, Kagi client, message validation, Redis result store 및 orchestration을 분리하여 외부 서비스 없이 단위 테스트할 수 있게 한다.
- 시간, retry delay, network client와 broker delivery를 주입하거나 mock할 수 있게 설계한다.
- 변경에는 최소한 다음 경우를 검증하는 테스트를 포함한다.
  - 정상 메시지 소비, Kagi 성공, 원자적 Redis 완료 기록, ACK
  - 이미 완료된 `jobId` 재전달 시 Kagi 호출 생략과 ACK
  - malformed JSON, 잘못된 schema/property/hash 및 oversized payload
  - Kagi timeout, 429, 5xx, 인증 오류, 비 JSON/비정상 JSON 응답
  - Redis 기록 실패 시 ACK하지 않음
  - retry 소진과 FAILED 기록, poison message의 비재순환 처리
  - 결과 JSON 및 2,000,000-byte 제한
  - RabbitMQ/Redis 연결 단절과 재연결
  - 종료 signal 중 진행 작업 처리와 resource cleanup
- 실제 Kagi API를 호출하는 테스트는 기본 test suite와 분리하고 명시적 opt-in으로만 실행한다. 기본 테스트는 비용과 네트워크 없이 결정적으로 동작해야 한다.
- 변경 후 저장소에 정의된 formatter/linter/test 명령을 사용한다. 아직 표준 명령이 없다면 최소한 `python -m compileall src`를 실행하고, 추가한 테스트 runner로 관련 테스트를 실행한다.

## 작업 절차와 Git 규칙

- 작업 시작과 종료 시 `git status --short`와 `git diff`를 확인하여 변경 범위를 점검한다.
- 사용자의 기존 변경을 보존한다. 관련 없는 파일을 수정하거나 되돌리지 않는다.
- 사용자가 명시적으로 요청하지 않으면 commit, push, merge, rebase 또는 branch 생성/변경을 수행하지 않는다.
- 생성물, 가상 환경, dependency directory, cache, editor 임시 파일, 로그 및 비밀 정보는 Git에 추가하지 않는다.
- 커밋을 요청받으면 하나의 논리적 변경 단위로 구성한다.
- 커밋 메시지는 한국어로 상세하게 작성한다. 제목에 변경 목적을 명확히 쓰고, 본문에 주요 구현 내용과 필요하면 검증 방법 및 영향 범위를 설명한다.
