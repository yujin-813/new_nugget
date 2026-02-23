# Changelog

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
