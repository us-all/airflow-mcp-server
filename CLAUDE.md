# CLAUDE.md

이 파일은 Claude Code가 이 저장소에서 작업할 때 참고하는 컨텍스트입니다.

## 프로젝트 개요

`@us-all/airflow-mcp` — Airflow Stable REST API를 stdio MCP로 노출. **7 도구 + 2 Prompts**. read-only 기본, trigger/clear는 `AIRFLOW_ALLOW_WRITE=true` 게이트.

- **타겟**: Airflow 3.x `/api/v2` + JWT (SimpleAuthManager). Airflow 2.x basic auth는 v0.1.x로 핀.
- **표준**: [@us-all MCP Standard](https://github.com/us-all/mcp-toolkit/blob/main/STANDARD.md) 준수
- **Companion**: dbt 자산은 별도 [`@us-all/dbt-mcp`](https://github.com/us-all/dbt-mcp-server)

## 디렉토리

```
src/
├── index.ts                # 카테고리별 tool() 등록 + Prompts
├── config.ts               # AIRFLOW_API_URL/USERNAME/PASSWORD/ALLOW_WRITE/TOOLS/DISABLE
├── tool-registry.ts        # CATEGORIES = airflow / meta
├── clients/
│   └── airflow.ts          # fetch wrapper + basic auth + AirflowApiError
├── tools/
│   ├── utils.ts            # wrapToolHandler + WriteBlocked / AirflowApi 에러 클래스
│   ├── dags.ts             # 6 도구 (list-dags, list-runs, task-instances, task-logs, trigger, clear)
│   └── aggregations.ts     # dag-health-rollup
└── prompts/
    └── index.ts            # 2 Prompts (dag-failure-triage, dag-schedule-audit)

tests/
└── airflow.test.ts         # 5 케이스 (모킹된 fetch + write-gate + aggregation)
```

## Build & Run

```bash
pnpm install
pnpm build              # tsc → dist/
pnpm test               # vitest (5 케이스)
pnpm smoke              # AIRFLOW_API_URL 설정 후 spawn + tools/list + airflow-list-dags
```

## 카테고리 (2)

| 카테고리 | 도구 수 | 토글 키 |
|---------|--------|---------|
| `airflow` | 6 + 1 aggregation | `AIRFLOW_TOOLS=airflow` |
| `meta`    | 1 (always) | — |

## 설계 원칙

- **Read-only by default**: trigger/clear만 write-gated (`AIRFLOW_ALLOW_WRITE=true`).
- **Schema-first**: 모든 도구 `<name>Schema` (zod) + `<name>` handler 페어. 모든 필드 `.describe()`.
- **Log tail**: `airflow-get-task-logs`는 기본 마지막 16 kB. 토큰 폭주 방지.
- **Aggregation caveat 패턴**: `dag-health-rollup`은 fan-out 시 task instances 가져오기 실패해도 부분 응답 반환.
- **민감정보 redaction**: `wrapToolHandler` redactionPatterns에 `AIRFLOW_PASSWORD` + `Authorization: Basic` 헤더 마스킹.

## 인증

v0.2부터 **JWT via SimpleAuthManager**. `AIRFLOW_USERNAME` + `AIRFLOW_PASSWORD`로 `POST /auth/token` 호출 → access_token 받아 `Authorization: Bearer <token>` 헤더 첨부. 토큰은 in-process 캐시 + JWT exp 클레임 기반 자동 갱신 (만료 1분 전 재발급, 401 응답 시 캐시 무효화).

`AIRFLOW_API_URL`은 host base만 받고 (`http://host:port`) 내부에서 `/api/v2` prepend. 트레일링 `/api/v1` `/api/v2`는 strip.

## 알려진 제약

- Airflow 2.x 미지원 (basic auth + `/api/v1` 의존성 제거됨). 2.x 사용자는 `@us-all/airflow-mcp@0.1.0`로 pin.
- `airflow-get-task-logs`는 단일 try_number 한 호출. 다중 try 비교는 클라이언트 측에서 여러 번 호출.
- OAuth/외부 IDP 인증은 미지원. SimpleAuthManager만 검증됨.

## 표준 가이드

`@us-all` MCP 작성 표준은 [mcp-toolkit/STANDARD.md](https://github.com/us-all/mcp-toolkit/blob/main/STANDARD.md)에 있음.
