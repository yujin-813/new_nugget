# GA4 AI Data Reporter

GA4 데이터 분석을 위한 AI 기반 질의응답 시스템입니다. 자연어 질문을 통해 Google Analytics 4 데이터를 분석하고, 파일 업로드를 통한 데이터 분석도 지원합니다.

## 주요 기능

### 1. GA4 데이터 분석
- **자연어 질의**: "지난주 활성 사용자 수를 알려줘", "상품별 매출 TOP 10" 등 자연어로 질문
- **복합 질문 처리**: "총 매출과 상품별 매출"과 같은 복합 질문을 자동으로 분리하여 처리
- **Scope 기반 분석**: Event, Item, User scope를 자동으로 인식하여 올바른 API 호출
- **대화형 컨텍스트**: 이전 질문의 맥락을 이해하고 연속적인 대화 지원

### 2. 파일 분석
- CSV, Excel 파일 업로드 및 분석
- LLM 기반 인사이트 생성

### 3. 혼합 분석
- GA4 데이터와 업로드된 파일 데이터를 결합한 분석

## 아키텍처

### 새로운 파이프라인 구조 (Extract → Plan → Execute)

```
질문 입력
    ↓
[Candidate Extractor]  ← 후보 추출 (결정 X)
    ↓
[GA4 Planner]          ← ExecutionPlan 생성
    ↓
[Plan Executor]        ← GA4 API 호출
    ↓
[Response Adapter]     ← 응답 형식 변환
    ↓
자연스러운 응답
```

### 핵심 컴포넌트

- **`candidate_extractor.py`**: 질문에서 metric/dimension 후보 추출
- **`planner.py`**: ExecutionPlan 생성 (scope 기반 블록 분리)
- **`plan_executor.py`**: PlanBlock 단위로 GA4 API 호출
- **`pipeline.py`**: 전체 파이프라인 오케스트레이터
- **`response_adapter.py`**: 새 파이프라인 응답을 기존 형식으로 변환
- **`integration_wrapper.py`**: 신규/레거시 엔진 전환 래퍼

### 레거시 컴포넌트

- **`qa_module.py`**: 기존 GA4 분석 엔진
- **`query_parser.py`**: 기존 쿼리 파싱 로직
- **`state_resolver.py`**: 상태 관리

## 설치 및 실행

### 1. 환경 설정

```bash
# 가상환경 생성
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 패키지 설치
pip install -r requirements.txt
```

### 2. 인증 설정

#### GA4 OAuth 설정
1. [Google Cloud Console](https://console.cloud.google.com/)에서 프로젝트 생성
2. Google Analytics Data API 활성화
3. OAuth 2.0 클라이언트 ID 생성 (웹 애플리케이션)
4. `client_secret.json` 파일을 프로젝트 루트에 저장

#### 환경 변수 설정
`.env` 파일 생성:
```
FLASK_SECRET_KEY=your-secret-key-here
```

### 3. 실행

```bash
python app.py
```

브라우저에서 `http://localhost:5001` 접속

## 사용 예시

### 단일 지표 조회
```
Q: 지난주 활성 사용자 수를 알려줘
A: 활성 사용자은 **293,323**명입니다.
```

### 복합 질문
```
Q: 총 매출과 상품별 매출 TOP 10은?
A: 구매 수익은 **74,572,596**원입니다.
   상위 10개 항목은 다음과 같습니다:
   [테이블 표시]
```

### Breakdown 분석
```
Q: 디바이스별 사용자 수
A: 디바이스별 분석 (3개 항목):
   [테이블 표시]
```

## 기술 스택

- **Backend**: Flask, Python 3.11+
- **GA4 API**: Google Analytics Data API v1beta
- **Database**: SQLite
- **LLM**: OpenAI GPT (optional, for semantic matching)
- **Frontend**: HTML, JavaScript, CSS

## 프로젝트 구조

```
my_project/
├── app.py                      # Flask 애플리케이션
├── pipeline.py                 # 새 파이프라인 오케스트레이터
├── candidate_extractor.py      # 후보 추출 레이어
├── planner.py                  # ExecutionPlan 생성
├── plan_executor.py            # GA4 API 실행
├── response_adapter.py         # 응답 형식 변환
├── integration_wrapper.py      # 신규/레거시 전환
├── ga4_metadata.py             # GA4 metrics/dimensions 메타데이터
├── semantic_matcher.py         # 의미 기반 매칭
├── db_manager.py               # 데이터베이스 관리
├── file_engine.py              # 파일 분석 엔진
├── mixed_engine.py             # 혼합 분석 엔진
├── static/                     # 프론트엔드 파일
└── requirements.txt            # Python 패키지 목록
```

## 주요 개선 사항 (v2.0)

### 1. Scope 기반 아키텍처
- Event, Item, User scope를 명확히 구분
- Scope 불일치로 인한 GA4 API 오류 방지

### 2. 복합 질문 처리
- "총 매출과 상품별 매출"과 같은 질문을 자동으로 2개 블록으로 분리
- 각 블록을 독립적으로 실행하여 결과 통합

### 3. 자연스러운 응답
- Metric 카테고리에 따라 적절한 단위 사용 (원/명/회)
- 대화체 응답 생성

### 4. Item-scoped Metric 지원
- `itemRevenue`, `itemName` 등 item-scoped 분석 지원
- "상품별" 키워드 감지 시 item-scoped metric 우선 선택

## 릴리즈 스모크 체크

릴리즈 전 아래 명령으로 핵심 질의 라우팅/플랜 회귀를 점검하세요.

```bash
python3 scripts/release_smoke.py
```

## 운영/배포 준비

### 1) 토큰/세션 보안
- `.env`에 `FLASK_SECRET_KEY`, `ADMIN_API_TOKEN` 설정
- 운영 환경에서는 `SESSION_COOKIE_SECURE=1` 권장

### 2) 학습 데이터 관리
- 학습 JSONL 추출:
```bash
python3 scripts/export_training_data.py --days 30 --limit 5000 --label-filter good --output training_examples.jsonl
```
- 오래된 로그 정리:
```bash
python3 scripts/prune_learning_data.py --retention-days 180
```

### 3) 운영 API (Bearer 토큰 필요)
- `GET /admin/token_status`
- `GET /admin/label_status?days=30`
- `POST /admin/label_interaction` with `{"interaction_id":123,"label":"good|bad|unknown","note":"..."}`
- `GET /admin/export_training_jsonl?days=30&limit=5000&label_filter=good`
- `POST /admin/prune_learning_data` with `{\"retention_days\":180}`

## Docker 배포 테스트

### 1) 환경 변수 준비
```bash
cp .env.example .env
```
- 필수: `FLASK_SECRET_KEY`, `ADMIN_API_TOKEN`
- OAuth: `GOOGLE_CLIENT_SECRET_PATH=client_secret.json` 또는 `GOOGLE_OAUTH_CLIENT_JSON*`

### 2) 빌드/실행
```bash
docker compose up --build -d
```

### 3) 상태 확인
```bash
docker compose ps
docker compose logs -f app
```

### 4) 종료
```bash
docker compose down
```

## 공개 URL 배포 (Render)

### 1) GitHub에 푸시
```bash
git add .
git commit -m "deploy: add render config"
git push
```

### 2) Render에서 배포
1. Render Dashboard -> New + -> Blueprint
2. GitHub repo 연결 후 `render.yaml` 선택
3. 환경변수(Secret) 입력:
   - `FLASK_SECRET_KEY`
   - `ADMIN_API_TOKEN`
   - `OPENAI_API_KEY` (사용 시)
   - `GOOGLE_OAUTH_CLIENT_JSON` (권장) 또는 `GOOGLE_OAUTH_CLIENT_JSON_BASE64`
   - `DB_PATH=/var/data/sqlite.db`
   - `UPLOAD_FOLDER=/var/data/uploaded_files`
4. Deploy 클릭

### 3) URL 확인
- 배포 완료 후 Render가 `https://<service-name>.onrender.com` URL을 발급
- 헬스체크: `https://<service-name>.onrender.com/healthz`

### 4) Google OAuth 리디렉션 URI 등록
Google Cloud Console OAuth Client에 아래 URL 추가:
- `https://<service-name>.onrender.com/oauth2callback`

## 라이선스

MIT License

## 기여

이슈 및 PR을 환영합니다!
