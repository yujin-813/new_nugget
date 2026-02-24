# Changelog

## v0.9.12 - 2026-02-23
- Fixed drill-down behavior after summary/channel context:
- `더 내려서` now promotes summary context to acquisition and advances hierarchy as expected.
- Fixed explicit intent switching under comparison:
- Queries like `지난달 매출 비교` now switch analysis type to revenue even when compare tokens exist.
- Added comparison opt-out phrase handling:
- Expressions like `비교 말고`, `단일로` now force non-comparison execution mode.
- Adjusted intent priority for mixed product+revenue phrases:
- `상품별 매출 ...` now prefers product analysis type.

## v0.9.11 - 2026-02-23
- Added correction-trigger handling for conversational repair:
- Inputs like `아니`, `왜 자꾸`, `말한 건`, `물어봤잖아` now force state rebuild from current sentence with minimal carry-over.
- Added metric conflict swap logic:
- If user intent switches (e.g., user-count question while revenue metric is active), metrics are auto-swapped.
- Multi-metric append remains supported for `함께/같이/추가` expressions.
- Comparison default-off hardening:
- Comparison is recomputed per question; when absent, previous comparison period is cleared.
- Added intent/result mismatch self-retry (one-shot):
- When response shape clearly mismatches user intent, system rebuilds state from current sentence and reruns once.

## v0.9.10 - 2026-02-23
- State persistence hardened to DB-single-source for query state:
- Added dedicated source key `ga4_query`; analysis state now loads from DB first and session is treated as cache-only copy.
- Replaced direct session merge flow with rebuild flow:
- Added `_build_state_from_scratch(user_input, previous_state)` and top-level detection `_is_top_level_analysis_request(...)`.
- Top-level analysis requests now reset base state before applying parsed constraints.
- Comparison execution decoupled from persisted state:
- Added runtime `execution_mode` and question-based comparison detection `_is_comparison_request_text(...)`.
- Persisted state now strips transient flags (`comparison`, `cause_analysis`, runtime mode) via `_sanitize_analysis_state_for_db(...)`.
- Query building/period resolution now derive comparison from current question, reducing stale-flag leakage between turns.
- `ask_question` flow updated:
- state pipeline is now `DB load -> rebuild -> DB save -> session cache`.
- follow-up selection also rebuilds/saves state through the same DB pipeline.

## v0.9.9 - 2026-02-24
- Added structured QueryState execution model for GA4 flows:
- `analysisType`, `period`, `dimension`, `metrics`, `sort`, `limit`, `comparison.previousPeriod`.
- Comparison execution strategy standardized:
- current query and previous-period query now run from the same base QueryState (period override only), then diff/rate are post-processed.
- Dimension swap behavior stabilized:
- when users request only a new dimension (e.g., `채널별로`), state performs dimension swap without forcing analysis-type switch.
- Non-silent fallback for dimension-missing data:
- if dimension-level compare data is missing, response explicitly asks whether to fallback to total compare.

## v0.9.8 - 2026-02-24
- Added explicit drill-down hierarchy rules for acquisition analysis:
- canonical flow: `channel -> source -> campaign -> keyword` (keyword falls back to campaign when metadata is unavailable).
- Updated drill-down request handling:
- `더 내려서 / drill down` now follows hierarchy progression instead of broad fallback.
- Period parser hardening:
- `지난주` = Monday~Sunday, `이번주` = Monday~today, `최근 7일` = today-6~today.
- Default summary period aligned to recent-7-day rule.
- Clarify fallback tone/action improved:
- Replaced generic “분석 기준 부족” message with actionable suggestion
  (e.g., “현재 채널 기준으로 캠페인까지 내려서 보여드릴까요?”).

## v0.9.7 - 2026-02-24
- Metrics are now managed as an array in analysis state:
- `함께/같이/추가` style requests append metrics instead of replacing existing ones.
- Added derived-metric post-processing layer:
- `conversionRate` is calculated after query execution from base metrics (`totalPurchasers`, `activeUsers`) to avoid query failures.
- Comparison flow hardened:
- Comparison is treated as a state flag (`comparison=true`), not a new intent.
- Current/previous periods are executed with same query shape, then diff/rate are calculated in post-processing.
- Compare without dimension is explicitly allowed and handled as total comparison.
- Failure fallback improved:
- Instead of silent/empty response, failed data queries now return condition-aware explanation (source/period/dimension/metrics).
- Cause-analysis handling refined:
- `원인 분석` is now treated as context-preserving metric expansion (no analysis-type jump), with support metrics appended (`sessions`, `engagementRate`, `conversionRate`).
- Added GA4 cause-analysis fallback that executes from current state and reports concentration/share with auxiliary metrics.

## v0.9.6 - 2026-02-23
- Fixed comparison-intent handling for ambiguous follow-up queries:
- Comparison requests now preserve current analysis context and set `comparison=true` instead of reclassifying analysis type.
- Added state-based GA4 comparison fallback:
- If compare intent is detected, the engine now auto-computes same-length previous period and returns executable compare output.
- Enabled total comparison without dimension:
- Even when no dimension is provided, compare response returns total values.
- Enforced compare output fields:
- Compare responses now include `현재값`, `이전값`, `증감`, `증감률` consistently.
- Clarify-message safety improved:
- Clarify UI now has a safe fallback message to prevent empty bot text.

## v0.9.5 - 2026-02-23
- Fixed follow-up misclassification for summary queries:
- `지난주 요약` style questions are now treated as new analysis (not stale follow-up).
- Summary intent normalization added:
- Time-scoped summary requests are rewritten to executable KPI bundles (users + revenue + top events).
- Guardrail tuning:
- Reduced over-blocking on summary and channel+user queries.
- Improved GA4 routing for channel user requests:
- Channel/user style questions now avoid repetitive source-choice loops more reliably.

## v0.9.4 - 2026-02-23
- Follow-up recommendation quality improved:
- Converted vague suggestions into directly executable prompts (e.g., channel/source/period split queries).
- Limited follow-up count for tap-first UX (mobile-first 4 items).
- Added stronger fallback follow-ups when data match fails.
- Routing stability improved for ambiguous GA4/File cases:
- Added GA4-preferred rule for channel/user style questions (e.g., "채널별 사용자수").
- Reduced unnecessary "GA4 vs 파일" choice prompts when intent is clearly GA4.
- Added labeled-feedback route hint:
- Recent `good`/`bad` labels now influence route hinting for similar future questions.
- Mobile UX refresh (tone preserved):
- Follow-up cards optimized as large tap targets.
- Updated visual polish: glass navbar/input feel, floating bottom navigation, clearer active states.

## v0.9.3 - 2026-02-23
- Mobile app-like navigation introduced (bottom tab / overlay / report panel transitions).
- Initial optional-login flow completed:
- No forced login on first entry.
- Google login requested only when GA4 connection is attempted.

## v0.9.2 - 2026-02-23
- Responsive layout stabilized across mobile/tablet breakpoints.
- GA4 connect modal/login UX improved.
