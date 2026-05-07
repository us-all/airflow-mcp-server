# CLAUDE.md

이 파일은 Claude Code가 이 저장소에서 작업할 때 참고하는 컨텍스트입니다.

## 프로젝트 개요

`@us-all/airflow-mcp` — Airflow Stable REST API를 stdio MCP로 노출. **7 도구 + 2 Prompts**. read-only 기본, trigger/clear는 `AIRFLOW_ALLOW_WRITE=true` 게이트.

- **타겟**: Airflow 2.x Stable API (Airflow 3.x도 호환 — `/api/v1/` 그대로 사용)
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

v0.1.0은 **basic auth만**. `AIRFLOW_USERNAME` + `AIRFLOW_PASSWORD`로 `Authorization: Basic <base64>` 헤더 자동 첨부. 토큰 기반 인증(JWT/OAuth)은 v0.2 후보.

## 알려진 제약

- Airflow 3.x의 새로운 `/api/v2/`는 미지원 (v0.2). v1 Stable API는 모든 동작에 충분 — Airflow 3.x도 v1 엔드포인트 호환 유지.
- `airflow-get-task-logs`는 단일 try_number 한 호출. 다중 try 비교는 클라이언트 측에서 여러 번 호출.

## 표준 가이드

`@us-all` MCP 작성 표준은 [mcp-toolkit/STANDARD.md](https://github.com/us-all/mcp-toolkit/blob/main/STANDARD.md)에 있음.
