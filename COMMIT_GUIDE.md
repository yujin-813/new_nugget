# Git Commit Guide

## 커밋할 파일 목록

### 새로 추가된 핵심 파일 (v2.0 Pipeline)
- `pipeline.py` - 전체 파이프라인 오케스트레이터
- `planner.py` - ExecutionPlan 생성 (scope 기반)
- `plan_executor.py` - GA4 API 실행
- `candidate_extractor.py` - 후보 추출 레이어
- `response_adapter.py` - 응답 형식 변환
- `integration_wrapper.py` - 신규/레거시 전환

### 업데이트된 파일
- `ga4_metadata.py` - scope 필드 추가, itemRevenue 메타데이터 개선
- `app.py` - 라우팅 및 통합
- `db_manager.py` - 데이터베이스 관리
- `file_engine.py` - 파일 분석
- `mixed_engine.py` - 혼합 분석
- `qa_module.py` - 레거시 엔진
- `query_parser.py` - 레거시 파서
- `state_resolver.py` - 상태 관리
- `semantic_matcher.py` - 의미 매칭
- `ml_module.py` - ML 유틸리티
- `insight_presenter.py` - 결과 표시
- `prompt_builder.py` - 프롬프트 생성
- `relation_classifier.py` - 관계 분류
- `ollama_intent_parser.py` - 의도 파싱
- `state_policy.py` - 상태 정책

### 문서 및 설정
- `README.md` - 프로젝트 문서
- `.gitignore` - Git 제외 파일 목록
- `.env.example` - 환경 변수 예시
- `service_account.json.example` - 인증 파일 예시
- `requirements.txt` - Python 패키지 목록

### 프론트엔드
- `static/` - HTML, CSS, JavaScript

### 스크립트
- `script/` - 유틸리티 스크립트

## 권장 커밋 메시지

```
feat: Implement GA4 Pipeline v2.0 with scope-based architecture

Major refactoring to support complex queries and scope-based data integrity:

- Add Extract->Plan->Execute pipeline architecture
- Implement scope-aware (event/item/user) query planning
- Support complex multi-block queries (e.g., "total + breakdown")
- Add natural language response formatting
- Create integration wrapper for gradual migration
- Update GA4 metadata with scope information
- Improve item-scoped metric handling (itemRevenue)

Breaking changes:
- New pipeline can be toggled via USE_NEW_PIPELINE flag
- Legacy engine remains available for backward compatibility

Closes #[issue-number]
```

## 커밋 명령어

```bash
# 1. 모든 변경사항 확인
git status

# 2. 커밋
git commit -m "feat: Implement GA4 Pipeline v2.0 with scope-based architecture"

# 3. 푸시 (원격 저장소가 설정된 경우)
git push origin main
```

## 주의사항

**절대 커밋하지 말아야 할 파일:**
- `service_account.json` - GA4 인증 정보
- `client_secret.json` - OAuth 클라이언트 시크릿
- `.env` - 환경 변수
- `*.db` - 데이터베이스 파일
- `uploaded_files/` - 업로드된 파일
- `uploads/` - 업로드 디렉토리
- `flask_session/` - 세션 파일
- `__pycache__/` - Python 캐시
- `*.pyc` - 컴파일된 Python 파일
- `.DS_Store` - macOS 시스템 파일

이 파일들은 `.gitignore`에 이미 추가되어 있습니다.
