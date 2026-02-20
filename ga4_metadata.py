# ga4_metadata.py
# GA4 Data API Metadata Registry (v6.3.0 Refined)

"""
목적:
- 한글 질문에서도 안정적으로 Dimension/Metric 매칭
- score를 confidence처럼 활용 가능한 구조 설계
- aliases는 표현 기반, kr_semantics는 의미 기반
- category/priority/concept를 통해 동점 처리 및 의도 추론 가능

주의:
- 이 파일은 "정의(Registry)"만 담당한다.
- 유사도 계산/매칭 알고리즘은 별도 파일에서 처리한다.
"""

# ------------------------------
# 공통 카테고리 정의
# ------------------------------
DIMENSION_CATEGORIES = {
    "time": "시간 관련",
    "event": "이벤트 관련",
    "page": "페이지 관련",
    "device": "디바이스 관련",
    "geo": "지역 관련",
    "traffic": "유입/채널 관련",
    "user": "사용자 관련",
    "ads": "광고 관련"
}

METRIC_CATEGORIES = {
    "user": "사용자 관련",
    "traffic": "트래픽 관련",
    "page": "페이지 관련",
    "engagement": "참여도 관련",
    "event": "이벤트 관련",
    "ecommerce": "이커머스 관련"
}

# ------------------------------
# GA4 Dimension Registry
# ------------------------------
GA4_DIMENSIONS = {
    # ------------------ Time ------------------
    "date": {
        "ui_name": "날짜",
        "aliases": ["날짜", "일자"],
        "kr_semantics": ["언제", "기간", "일별"],
        "category": "time",
        "priority": 5,
        "description": "YYYYMMDD 형식 날짜"
    },
    "dateHour": {
        "ui_name": "날짜 + 시간",
        "aliases": ["날짜시간"],
        "kr_semantics": ["시간별"],
        "category": "time",
        "priority": 4,
        "description": "YYYYMMDDHH"
    },
    "dateHourMinute": {
        "ui_name": "날짜 + 시간 + 분",
        "aliases": ["분단위"],
        "kr_semantics": ["분별"],
        "category": "time",
        "priority": 3,
        "description": "YYYYMMDDHHMM"
    },
    "day": {
        "ui_name": "일",
        "aliases": ["몇일"],
        "kr_semantics": [],
        "category": "time",
        "priority": 2,
        "description": "01~31"
    },
    "dayOfWeek": {
        "ui_name": "요일",
        "aliases": ["요일숫자"],
        "kr_semantics": [],
        "category": "time",
        "priority": 2,
        "description": "0~6 (일요일 시작)"
    },
    "dayOfWeekName": {
        "ui_name": "요일 이름",
        "aliases": ["요일"],
        "kr_semantics": ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"],
        "category": "time",
        "priority": 4,
        "description": "Sunday~Saturday"
    },
    "week": {
        "ui_name": "주",
        "aliases": ["주간", "주차"],
        "kr_semantics": ["주별"],
        "category": "time",
        "priority": 4,
        "description": "01~53"
    },
    "month": {
        "ui_name": "월",
        "aliases": ["월간"],
        "kr_semantics": ["월별"],
        "category": "time",
        "priority": 4,
        "description": "01~12"
    },
    "year": {
        "ui_name": "연도",
        "aliases": ["년도"],
        "kr_semantics": [],
        "category": "time",
        "priority": 4,
        "description": "YYYY"
    },
    "yearMonth": {
        "ui_name": "연도 월",
        "aliases": ["연월"],
        "kr_semantics": [],
        "category": "time",
        "priority": 3,
        "description": "YYYYMM"
    },
    "yearWeek": {
        "ui_name": "연도 주",
        "aliases": [],
        "kr_semantics": [],
        "category": "time",
        "priority": 3,
        "description": "YYYYWW"
    },

    # ------------------ Event ------------------
    "eventName": {
        "ui_name": "이벤트 이름",
        "aliases": ["이벤트"],
        "kr_semantics": ["가입", "구매", "클릭", "전환"],
        "category": "event",
        "priority": 5,
        "description": "이벤트 이름"
    },
    "achievementId": {
        "ui_name": "업적 ID",
        "aliases": ["업적"],
        "kr_semantics": [],
        "category": "event",
        "priority": 2,
        "description": "achievement_id 매개변수"
    },
    "character": {
        "ui_name": "캐릭터",
        "aliases": ["플레이어"],
        "kr_semantics": [],
        "category": "event",
        "priority": 2,
        "description": "character 매개변수"
    },
    "groupId": {
        "ui_name": "그룹 ID",
        "aliases": [],
        "kr_semantics": [],
        "category": "event",
        "priority": 2,
        "description": "group_id 매개변수"
    },

    # ------------------ Page ------------------
    "pagePath": {
        "ui_name": "페이지 경로",
        "aliases": ["URL", "경로"],
        "kr_semantics": ["페이지별"],
        "category": "page",
        "priority": 5,
        "description": "페이지 경로"
    },
    "pageTitle": {
        "ui_name": "페이지 제목",
        "aliases": ["제목"],
        "kr_semantics": [],
        "category": "page",
        "priority": 4,
        "description": "페이지 제목"
    },
    "landingPage": {
        "ui_name": "방문 페이지",
        "aliases": ["랜딩"],
        "kr_semantics": [],
        "category": "page",
        "priority": 4,
        "description": "세션 첫 페이지"
    },
    "fullPageUrl": {
        "ui_name": "전체 페이지 URL",
        "aliases": ["전체URL"],
        "kr_semantics": [],
        "category": "page",
        "priority": 2,
        "description": "Full URL"
    },

    # ------------------ Device ------------------
    "deviceCategory": {
        "ui_name": "기기 카테고리",
        "aliases": ["디바이스", "기기"],
        "kr_semantics": ["모바일", "PC", "태블릿"],
        "category": "device",
        "priority": 4,
        "description": "mobile/desktop/tablet"
    },
    "browser": {
        "ui_name": "브라우저",
        "aliases": [],
        "kr_semantics": ["크롬", "사파리"],
        "category": "device",
        "priority": 3,
        "description": "브라우저"
    },
    "operatingSystem": {
        "ui_name": "운영체제",
        "aliases": ["OS"],
        "kr_semantics": ["안드로이드", "iOS", "윈도우"],
        "category": "device",
        "priority": 3,
        "description": "운영체제"
    },
    "screenResolution": {
        "ui_name": "화면 해상도",
        "aliases": [],
        "kr_semantics": [],
        "category": "device",
        "priority": 2,
        "description": "예: 1920x1080"
    },

    # ------------------ Geo ------------------
    "country": {
        "ui_name": "국가",
        "aliases": ["나라"],
        "kr_semantics": ["국가별"],
        "category": "geo",
        "priority": 4,
        "description": "국가"
    },
    "city": {
        "ui_name": "도시",
        "aliases": [],
        "kr_semantics": ["도시별"],
        "category": "geo",
        "priority": 3,
        "description": "도시"
    },
    "region": {
        "ui_name": "지역",
        "aliases": [],
        "kr_semantics": [],
        "category": "geo",
        "priority": 3,
        "description": "지역"
    },
    "continent": {
        "ui_name": "대륙",
        "aliases": [],
        "kr_semantics": [],
        "category": "geo",
        "priority": 2,
        "description": "대륙"
    },

    # ------------------ Traffic ------------------
    "source": {
        "ui_name": "소스",
        "aliases": ["유입"],
        "kr_semantics": [],
        "category": "traffic",
        "priority": 4,
        "description": "트래픽 소스"
    },
    "medium": {
        "ui_name": "매체",
        "aliases": [],
        "kr_semantics": [],
        "category": "traffic",
        "priority": 4,
        "description": "트래픽 매체"
    },
    "sourceMedium": {
        "ui_name": "소스/매체",
        "aliases": [],
        "kr_semantics": [],
        "category": "traffic",
        "priority": 4,
        "description": "source + medium"
    },
    "defaultChannelGroup": {
        "ui_name": "기본 채널 그룹",
        "aliases": ["채널"],
        "kr_semantics": ["채널별"],
        "category": "traffic",
        "priority": 5,
        "description": "Default Channel Group"
    },

    # ------------------ User ------------------
    "userAgeBracket": {
        "ui_name": "연령",
        "aliases": ["나이"],
        "kr_semantics": [],
        "category": "user",
        "priority": 3,
        "description": "연령대"
    },
    "userGender": {
        "ui_name": "성별",
        "aliases": [],
        "kr_semantics": [],
        "category": "user",
        "priority": 3,
        "description": "성별"
    },
    "newVsReturning": {
        "ui_name": "신규/재사용자",
        "aliases": ["재방문"],
        "kr_semantics": ["신규", "재방문"],
        "category": "user",
        "priority": 4,
        "description": "new / returning"
    },
    "itemBrand": {
        "ui_name": "상품 브랜드",
        "aliases": ["브랜드"],
        "kr_semantics": ["브랜드명"],
        "category": "ecommerce",
        "scope": "item",
        "priority": 3,
        "description": "상품의 브랜드 이름"
    },

    "itemCategory": {
        "ui_name": "상품 카테고리",
        "aliases": ["카테고리1", "대분류"],
        "kr_semantics": ["상품분류"],
        "category": "ecommerce",
        "description": "상품의 1단계 카테고리"
    },

    "itemCategory2": {
        "ui_name": "상품 카테고리 2",
        "aliases": ["카테고리2", "중분류"],
        "kr_semantics": ["2단계카테고리"],
        "category": "ecommerce",
        "description": "상품의 2단계 카테고리"
    },

    "itemCategory3": {
        "ui_name": "상품 카테고리 3",
        "aliases": ["카테고리3"],
        "kr_semantics": ["3단계카테고리"],
        "category": "ecommerce",
        "description": "상품의 3단계 카테고리"
    },

    "itemCategory4": {
        "ui_name": "상품 카테고리 4",
        "aliases": ["카테고리4"],
        "kr_semantics": ["4단계카테고리"],
        "category": "ecommerce",
        "description": "상품의 4단계 카테고리"
    },

    "itemCategory5": {
        "ui_name": "상품 카테고리 5",
        "aliases": ["카테고리5"],
        "kr_semantics": ["5단계카테고리"],
        "category": "ecommerce",
        "description": "상품의 5단계 카테고리"
    },

    "itemId": {
        "ui_name": "상품 ID",
        "aliases": ["상품아이디"],
        "kr_semantics": ["제품ID"],
        "category": "ecommerce",
        "scope": "item",
        "priority": 4,
        "is_label": True,
        "description": "상품의 ID"
    },

    "itemListId": {
        "ui_name": "상품 목록 ID",
        "aliases": ["리스트ID"],
        "kr_semantics": ["목록아이디"],
        "category": "ecommerce",
        "description": "상품 목록의 ID"
    },

    "itemListName": {
        "ui_name": "상품 목록 이름",
        "aliases": ["리스트이름"],
        "kr_semantics": ["목록명"],
        "category": "ecommerce",
        "description": "상품 목록의 이름"
    },

    "itemListPosition": {
        "ui_name": "상품 목록 위치",
        "aliases": ["리스트위치", "순번"],
        "kr_semantics": ["목록순서"],
        "category": "ecommerce",
        "description": "목록에서 상품의 위치"
    },

    "itemLocationID": {
        "ui_name": "상품 위치 ID",
        "aliases": ["위치ID", "매장ID"],
        "kr_semantics": ["오프라인위치"],
        "category": "ecommerce",
        "description": "상품과 연결된 실제 위치 ID"
    },

    "itemName": {
        "ui_name": "항목 이름",
        "aliases": ["상품명", "제품명"],
        "kr_semantics": ["아이템이름"],
        "category": "ecommerce",
        "scope": "item",
        "priority": 5,
        "is_label": True,
        "description": "상품의 이름"
    },

    "itemPromotionCreativeName": {
        "ui_name": "상품 프로모션 광고 소재 이름",
        "aliases": ["프로모션소재명"],
        "kr_semantics": ["광고소재이름"],
        "category": "ecommerce",
        "description": "프로모션 광고 소재 이름"
    },

    "itemPromotionCreativeSlot": {
        "ui_name": "상품 프로모션 광고 소재 슬롯",
        "aliases": ["소재슬롯"],
        "kr_semantics": ["광고슬롯"],
        "category": "ecommerce",
        "description": "프로모션 광고 소재 슬롯 이름"
    },

    "itemPromotionId": {
        "ui_name": "상품 프로모션 ID",
        "aliases": ["프로모션ID"],
        "kr_semantics": ["프로모션아이디"],
        "category": "ecommerce",
        "description": "상품 프로모션 ID"
    },

    "itemPromotionName": {
        "ui_name": "상품 프로모션 이름",
        "aliases": ["프로모션이름"],
        "kr_semantics": ["프로모션명"],
        "category": "ecommerce",
        "description": "상품 프로모션 이름"
    },

    "itemVariant": {
        "ui_name": "상품 옵션",
        "aliases": ["옵션", "색상", "사이즈"],
        "kr_semantics": ["상품변형"],
        "category": "ecommerce",
        "description": "상품의 변형 옵션"
    },

    
    "landingPagePlusQueryString": {
        "ui_name": "방문 페이지 + 쿼리 문자열",
        "aliases": ["랜딩페이지쿼리"],
        "kr_semantics": ["쿼리포함페이지"],
        "category": "page",
        "description": "세션 첫 페이지 경로 + 쿼리 문자열"
    },

    "language": {
        "ui_name": "언어",
        "aliases": ["브라우저언어"],
        "kr_semantics": ["사용자언어"],
        "category": "user",
        "description": "사용자 브라우저 또는 기기 언어"
    },

    "languageCode": {
        "ui_name": "언어 코드",
        "aliases": ["언어코드"],
        "kr_semantics": ["ISO언어코드"],
        "category": "user",
        "description": "ISO 639 언어 코드"
    },

    "level": {
        "ui_name": "수준",
        "aliases": ["레벨", "게임레벨"],
        "kr_semantics": ["플레이어레벨"],
        "category": "event",
        "description": "게임 내 레벨"
    },

    "linkClasses": {
        "ui_name": "클래스 연결",
        "aliases": ["링크클래스"],
        "kr_semantics": ["html클래스"],
        "category": "event",
        "description": "링크의 HTML class 속성"
    },

    "linkDomain": {
        "ui_name": "도메인 연결",
        "aliases": ["링크도메인"],
        "kr_semantics": ["외부도메인"],
        "category": "event",
        "description": "외부 연결 대상 도메인"
    },

    "linkId": {
        "ui_name": "링크 ID",
        "aliases": ["링크아이디"],
        "kr_semantics": ["htmlid"],
        "category": "event",
        "description": "링크 또는 다운로드의 HTML ID"
    },

    "linkText": {
        "ui_name": "링크 텍스트",
        "aliases": ["링크문구"],
        "kr_semantics": ["다운로드텍스트"],
        "category": "event",
        "description": "링크 또는 다운로드의 텍스트"
    },

    "linkUrl": {
        "ui_name": "링크 URL",
        "aliases": ["링크주소"],
        "kr_semantics": ["외부링크"],
        "category": "event",
        "description": "링크 또는 다운로드의 전체 URL"
    }

}

# ------------------------------
# GA4 Ads Dimension Registry
# ------------------------------
GA4_ADS_DIMENSIONS = {
    # ------------------ Google Ads ------------------
    "googleAdsCampaignId": {
        "ui_name": "Google Ads 캠페인 ID",
        "aliases": ["구글캠페인ID"],
        "kr_semantics": [],
        "category": "ads",
        "priority": 3,
        "description": "Google Ads 캠페인 ID"
    },
    "googleAdsCampaignName": {
        "ui_name": "Google Ads 캠페인",
        "aliases": ["구글캠페인"],
        "kr_semantics": ["캠페인별"],
        "category": "ads",
        "priority": 5,
        "description": "Google Ads 캠페인 이름"
    },
    "googleAdsAdGroupName": {
        "ui_name": "Google Ads 광고그룹",
        "aliases": ["광고그룹"],
        "kr_semantics": [],
        "category": "ads",
        "priority": 4,
        "description": "Google Ads 광고 그룹 이름"
    },
    "googleAdsKeyword": {
        "ui_name": "Google Ads 키워드",
        "aliases": ["키워드"],
        "kr_semantics": ["검색어"],
        "category": "ads",
        "priority": 4,
        "description": "Google Ads 키워드 텍스트"
    },

    # ------------------ DV360 ------------------
    "dv360CampaignName": {
        "ui_name": "DV360 캠페인",
        "aliases": [],
        "kr_semantics": [],
        "category": "ads",
        "priority": 4,
        "description": "DV360 캠페인 이름"
    },
    "dv360LineItemName": {
        "ui_name": "DV360 광고 항목",
        "aliases": [],
        "kr_semantics": [],
        "category": "ads",
        "priority": 3,
        "description": "DV360 Line Item 이름"
    },

    # ------------------ CM360 ------------------
    "cm360CampaignName": {
        "ui_name": "CM360 캠페인",
        "aliases": [],
        "kr_semantics": [],
        "category": "ads",
        "priority": 4,
        "description": "CM360 캠페인 이름"
    },
    "cm360CreativeName": {
        "ui_name": "CM360 광고 소재",
        "aliases": [],
        "kr_semantics": [],
        "category": "ads",
        "priority": 3,
        "description": "CM360 Creative 이름"
    },
}

# ------------------------------
# GA4 Metric Registry
# ------------------------------
GA4_METRICS = {
    # ------------------ User ------------------
    "activeUsers": {
        "ui_name": "활성 사용자",
        "aliases": ["사용자", "유저", "방문자", "사람", "명수", "접속자"],
        "kr_semantics": ["사용자수", "방문자수", "몇명"],
        "category": "user",
        "priority": 5,
        "concept": "user",
        "description": "활성 사용자 수"
    },
    "active7DayUsers": {
        "ui_name": "7일 활성 사용자",
        "aliases": ["7일사용자", "7일활성", "최근7일사용자"],
        "kr_semantics": ["최근7일", "7일간"],
        "category": "user",
        "priority": 4,
        "concept": "user",
        "description": "최근 7일간 활성 사용자"
    },
    "active28DayUsers": {
        "ui_name": "28일 활성 사용자",
        "aliases": ["28일사용자", "28일활성", "최근28일사용자"],
        "kr_semantics": ["최근28일", "한달간"],
        "category": "user",
        "priority": 3,
        "concept": "user",
        "description": "최근 28일간 활성 사용자"
    },
    "newUsers": {
        "ui_name": "신규 사용자",
        "aliases": ["신규", "처음", "새로운"],
        "kr_semantics": ["신규유저", "첫 방문"],
        "category": "user",
        "priority": 5,
        "concept": "user",
        "description": "신규 사용자 수"
    },
    "totalUsers": {
        "ui_name": "총 사용자 수",
        "aliases": ["전체사용자", "누적사용자", "총유저"],
        "kr_semantics": ["누적", "전체"],
        "category": "user",
        "priority": 3,
        "concept": "user",
        "description": "전체 사용자 수"
    },
    "dauPerMau": {
        "ui_name": "DAU/MAU",
        "aliases": ["dau mau", "일간월간비율"],
        "kr_semantics": ["dau/mau", "월간 대비 일간 활성"],
        "category": "user",
        "priority": 3,
        "concept": "user",
        "description": "30일 활성 사용자 중 1일 활성 사용자의 롤링 비율"
    },
    "dauPerWau": {
        "ui_name": "DAU/WAU",
        "aliases": ["dau wau", "일간주간비율"],
        "kr_semantics": ["dau/wau", "주간 대비 일간 활성"],
        "category": "user",
        "priority": 3,
        "concept": "user",
        "description": "7일 활성 사용자 중 1일 활성 사용자의 롤링 비율"
    },

    # ------------------ Traffic ------------------
    "sessions": {
        "ui_name": "세션",
        "aliases": ["세션", "방문수", "접속", "연결"],
        "kr_semantics": ["방문횟수", "세션수"],
        "category": "traffic",
        "priority": 5,
        "concept": "traffic",
        "description": "세션 수"
    },
    "sessionsPerUser": {
        "ui_name": "사용자당 세션수",
        "aliases": ["유저당세션", "평균세션수"],
        "kr_semantics": ["사용자평균세션"],
        "category": "traffic",
        "priority": 4,
        "concept": "traffic",
        "description": "사용자당 평균 세션수"
    },

    # ------------------ Page ------------------
    "screenPageViews": {
        "ui_name": "조회수",
        "aliases": ["조회수", "뷰", "페이지뷰", "PV", "클릭수"],
        "kr_semantics": ["페이지뷰", "view", "pv"],
        "category": "page",
        "priority": 5,
        "concept": "page",
        "description": "페이지/스크린 조회수"
    },
    "screenPageViewsPerSession": {
        "ui_name": "세션당 조회수",
        "aliases": ["세션당페이지뷰", "세션당뷰"],
        "kr_semantics": ["세션평균조회수"],
        "category": "page",
        "priority": 3,
        "concept": "page",
        "description": "세션당 평균 조회수"
    },
    "screenPageViewsPerUser": {
        "ui_name": "사용자당 조회수",
        "aliases": ["유저당페이지뷰", "사용자평균조회수"],
        "kr_semantics": ["사용자평균뷰"],
        "category": "page",
        "priority": 3,
        "concept": "page",
        "description": "활성 사용자당 평균 조회수"
    },

    # ------------------ Engagement ------------------
    "engagedSessions": {
        "ui_name": "참여 세션수",
        "aliases": ["참여세션", "참여한세션"],
        "kr_semantics": ["참여세션수"],
        "category": "engagement",
        "priority": 3,
        "concept": "engagement",
        "description": "참여 세션 수"
    },
    "engagementRate": {
        "ui_name": "참여율",
        "aliases": ["참여율", "반응율", "관심도"],
        "kr_semantics": ["참여", "engagement"],
        "category": "engagement",
        "priority": 4,
        "concept": "engagement",
        "description": "참여율"
    },
    "bounceRate": {
        "ui_name": "이탈률",
        "aliases": ["이탈률", "이탈율", "바운스", "종료율"],
        "kr_semantics": ["이탈", "나감"],
        "category": "engagement",
        "priority": 4,
        "concept": "engagement",
        "description": "이탈률"
    },
    "averageSessionDuration": {
        "ui_name": "평균 세션 시간",
        "aliases": ["체류시간", "머문시간", "평균시간"],
        "kr_semantics": ["체류", "얼마나 오래"],
        "category": "engagement",
        "priority": 4,
        "concept": "engagement",
        "description": "평균 세션 시간 (초)"
    },
    "scrolledUsers": {
        "ui_name": "스크롤한 사용자",
        "aliases": ["스크롤사용자", "스크롤유저"],
        "kr_semantics": ["스크롤90퍼센트"],
        "category": "engagement",
        "priority": 3,
        "concept": "engagement",
        "description": "페이지의 90% 이상을 스크롤한 사용자 수"
    },

    # ------------------ Event ------------------
    "eventCount": {
        "ui_name": "이벤트 수",
        "aliases": ["이벤트횟수", "건수", "발생수", "횟수"],
        "kr_semantics": ["몇번", "몇 번", "발생"],
        "category": "event",
        "priority": 5,
        "concept": "event",
        "description": "이벤트 발생 횟수"
    },
    "eventCountPerUser": {
        "ui_name": "사용자당 이벤트 수",
        "aliases": ["유저당이벤트", "1인당 이벤트"],
        "kr_semantics": ["사용자 평균 이벤트"],
        "category": "event",
        "priority": 3,
        "concept": "event",
        "description": "이벤트 수 ÷ 활성 사용자 수"
    },
    "eventValue": {
        "ui_name": "이벤트 값",
        "aliases": ["이벤트 value", "이벤트금액"],
        "kr_semantics": ["value 합계"],
        "category": "event",
        "priority": 3,
        "concept": "event",
        "description": "value 매개변수의 합계"
    },
    "eventsPerSession": {
        "ui_name": "세션당 이벤트 수",
        "aliases": ["세션당이벤트", "세션 평균 이벤트"],
        "kr_semantics": ["세션 평균 이벤트"],
        "category": "event",
        "priority": 3,
        "concept": "event",
        "description": "이벤트 수 ÷ 세션 수"
    },
    "keyEvents": {
        "ui_name": "주요 이벤트",
        "aliases": ["전환", "전환수", "목표"],
        "kr_semantics": ["전환", "목표", "conversion"],
        "category": "event",
        "priority": 5,
        "concept": "event",
        "description": "주요 이벤트 수"
    },
    "sessionKeyEventRate": {
        "ui_name": "세션 주요 이벤트 비율",
        "aliases": ["세션전환율"],
        "kr_semantics": ["주요이벤트세션비율"],
        "category": "event",
        "priority": 3,
        "concept": "event",
        "description": "주요 이벤트가 발생한 세션의 비율"
    },

    # ------------------ Ecommerce ------------------
    "transactions": {
        "ui_name": "거래",
        "aliases": ["거래", "결제", "구매", "주문"],
        "kr_semantics": ["거래수", "구매건수", "결제건수"],
        "category": "ecommerce",
        "priority": 4,
        "concept": "ecommerce",
        "description": "수익이 있는 거래 수"
    },
    "purchaseRevenue": {
        "ui_name": "구매 수익",
        "aliases": ["수익", "매출", "금액", "돈"],
        "kr_semantics": ["매출", "수익", "revenue"],
        "category": "ecommerce",
        "priority": 5,
        "concept": "ecommerce",
        "scope": "event",  # Event-scoped (전체 구매 이벤트 수익)
        "description": "구매 수익"
    },
    "totalRevenue": {
        "ui_name": "총 수익",
        "aliases": ["총수익", "전체매출"],
        "kr_semantics": ["총매출", "전체수익"],
        "category": "ecommerce",
        "priority": 4,
        "concept": "ecommerce",
        "description": "총 수익"
    },
    "itemsPurchased": {
        "ui_name": "구매한 상품 수",
        "aliases": ["구매항목", "구매상품", "상품수"],
        "kr_semantics": ["상품개수", "몇개샀는지","많이팔린"],
        "category": "ecommerce",
        "priority": 4,
        "concept": "ecommerce",
        "scope": "item",
        "description": "구매 이벤트에 포함된 상품 수"
    },
    "ecommercePurchases": {
        "ui_name": "전자상거래 구매 건수",
        "aliases": ["이커머스구매", "구매완료수"],
        "kr_semantics": ["전자상거래 구매"],
        "category": "ecommerce",
        "priority": 4,
        "concept": "ecommerce",
        "description": "purchase 이벤트 집계 수"
    },
    "firstTimePurchaserRate": {
        "ui_name": "첫 구매자 비율",
        "aliases": ["첫구매율", "최초구매율"],
        "kr_semantics": ["첫 구매 비율"],
        "category": "ecommerce",
        "scope": "event",
        "priority": 4,
        "concept": "ecommerce",
        "description": "처음 구매한 활성 사용자 비율"
    },
    "firstTimePurchasers": {
        "ui_name": "최초 구매자 수",
        "aliases": ["첫구매자", "최초구매자", "신규구매자", "신규후원자", "첫후원자"],
        "kr_semantics": ["첫 구매 사용자", "신규 후원자"],
        "category": "ecommerce",
        "scope": "event",
        "priority": 4,
        "concept": "ecommerce",
        "description": "첫 구매 이벤트 완료 사용자 수"
    },
    "firstTimePurchasersPerNewUser": {
        "ui_name": "신규 사용자당 최초 구매자 수",
        "aliases": ["신규당첫구매"],
        "kr_semantics": ["신규 대비 첫 구매"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "신규 사용자당 평균 최초 구매자 수"
    },
    "grossItemRevenue": {
        "ui_name": "총 항목 수익",
        "aliases": ["상품총수익"],
        "kr_semantics": ["상품 매출 합계"],
        "category": "ecommerce",
        "priority": 4,
        "concept": "ecommerce",
        "scope": "item",
        "description": "상품에서 발생한 총 수익"
    },
    "grossPurchaseRevenue": {
        "ui_name": "총 구매 수익",
        "aliases": ["총구매매출"],
        "kr_semantics": ["전체 구매 매출"],
        "category": "ecommerce",
        "priority": 4,
        "concept": "ecommerce",
        "description": "구매 관련 이벤트 수익 합계"
    },
    "itemDiscountAmount": {
        "ui_name": "상품 할인 금액",
        "aliases": ["상품할인"],
        "kr_semantics": ["할인금액"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "상품 할인 금액"
    },
    "itemListClickEvents": {
        "ui_name": "상품 목록 클릭 이벤트",
        "aliases": ["목록클릭수"],
        "kr_semantics": ["상품목록 클릭"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "select_item 이벤트 수"
    },
    "itemListClickThroughRate": {
        "ui_name": "상품 목록 클릭률",
        "aliases": ["목록클릭률"],
        "kr_semantics": ["상품목록 CTR"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "목록 클릭률"
    },
    "itemListViewEvents": {
        "ui_name": "상품 목록 조회 이벤트",
        "aliases": ["목록조회수"],
        "kr_semantics": ["상품목록 조회"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "view_item_list 이벤트 수"
    },
    "itemPromotionClickThroughRate": {
        "ui_name": "상품 프로모션 클릭률",
        "aliases": ["프로모션클릭률"],
        "kr_semantics": ["프로모션 CTR"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "프로모션 클릭률"
    },
    "itemRefundAmount": {
        "ui_name": "상품 환불 금액",
        "aliases": ["상품환불"],
        "kr_semantics": ["환불금액"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "상품 환불 총액"
    },
    "itemRevenue": {
        "ui_name": "상품 수익",
        "aliases": ["상품매출", "아이템매출", "상품별매출", "제품매출", "상품수익"],
        "kr_semantics": ["상품 수익", "아이템 매출", "상품별 매출"],
        "category": "ecommerce",
        "priority": 5,  # 높은 우선순위
        "concept": "ecommerce",
        "scope": "item",
        "description": "상품 수익 (환불 제외)"
    },
    "itemViewEvents": {
        "ui_name": "상품 조회 이벤트",
        "aliases": ["상품조회수"],
        "kr_semantics": ["상품 상세조회"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "view_item 이벤트 수"
    },
    "itemsAddedToCart": {
        "ui_name": "장바구니 추가 상품 수",
        "aliases": ["장바구니수량", "카트추가"],
        "kr_semantics": ["add to cart 수량"],
        "category": "ecommerce",
        "priority": 4,
        "concept": "ecommerce",
        "scope": "item",
        "description": "add_to_cart 이벤트 상품 수량"
    },
    "itemsCheckedOut": {
        "ui_name": "결제된 상품 수",
        "aliases": ["결제수량"],
        "kr_semantics": ["checkout 수량"],
        "category": "ecommerce",
        "priority": 4,
        "concept": "ecommerce",
        "scope": "item",
        "description": "begin_checkout 상품 수량"
    },
    "itemsClickedInList": {
        "ui_name": "목록 클릭 상품 수",
        "aliases": ["목록클릭상품"],
        "kr_semantics": ["select_item 수량"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "목록 클릭 상품 수"
    },
    "itemsClickedInPromotion": {
        "ui_name": "프로모션 클릭 상품 수",
        "aliases": ["프로모션클릭상품"],
        "kr_semantics": ["select_promotion 수량"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "프로모션 클릭 상품 수"
    },
    "itemsViewed": {
        "ui_name": "조회된 상품 수",
        "aliases": ["상품조회수량"],
        "kr_semantics": ["view_item 수량"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "scope": "item",
        "description": "조회된 상품 수"
    },
    "itemsViewedInList": {
        "ui_name": "목록 조회 상품 수",
        "aliases": ["목록조회상품"],
        "kr_semantics": ["view_item_list 수량"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "목록 조회 상품 수"
    },
    "itemsViewedInPromotion": {
        "ui_name": "프로모션 조회 상품 수",
        "aliases": ["프로모션조회상품"],
        "kr_semantics": ["view_promotion 수량"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "프로모션 조회 상품 수"
    },
    "purchaseToViewRate": {
        "ui_name": "조회수 대비 구매 비율",
        "aliases": ["구매전환율", "조회대비구매", "상품전환율"],
        "kr_semantics": ["구매비율", "전환비율"],
        "category": "ecommerce",
        "priority": 4,
        "concept": "ecommerce",
        "description": "제품을 구매한 사용자 수를 조회 사용자 수로 나눈 비율"
    },
    "purchaserRate": {
        "ui_name": "구매자 비율",
        "aliases": ["구매율", "구매자비율", "후원자비율", "후원율"],
        "kr_semantics": ["구매한사용자비율", "후원자 비율"],
        "category": "ecommerce",
        "scope": "event",
        "priority": 4,
        "concept": "ecommerce",
        "description": "1회 이상 구매 거래를 한 활성 사용자의 비율"
    },
    "refundAmount": {
        "ui_name": "환불 금액",
        "aliases": ["환불", "환불액"],
        "kr_semantics": ["환불금액"],
        "category": "ecommerce",
        "priority": 4,
        "concept": "ecommerce",
        "description": "환불된 총 거래 수익"
    },
    "returnOnAdSpend": {
        "ui_name": "광고 투자수익",
        "aliases": ["roas", "광고수익률"],
        "kr_semantics": ["광고대비수익"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "총수익을 광고비로 나눈 값"
    },
    "shippingAmount": {
        "ui_name": "배송비",
        "aliases": ["배송금액"],
        "kr_semantics": ["배송비용"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "거래와 연결된 배송비"
    },
    "taxAmount": {
        "ui_name": "세액",
        "aliases": ["세금"],
        "kr_semantics": ["부가세"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "거래와 관련된 세액"
    },
    "totalAdRevenue": {
        "ui_name": "총 광고 수익",
        "aliases": ["광고수익", "애드수익"],
        "kr_semantics": ["광고매출"],
        "category": "ecommerce",
        "priority": 3,
        "concept": "ecommerce",
        "description": "AdMob 및 서드파티 광고 수익 합계"
    },
    "totalPurchasers": {
        "ui_name": "전체 구매자 수",
        "aliases": ["구매자수", "총구매자", "후원자수", "총후원자"],
        "kr_semantics": ["구매한사용자수", "후원한사용자수"],
        "category": "ecommerce",
        "scope": "event",
        "priority": 4,
        "concept": "ecommerce",
        "description": "구매 이벤트를 기록한 사용자 수"
    }
}

# ------------------------------
# Custom Definition Placeholders
# ------------------------------
GA4_CUSTOM_DIMENSIONS = {
    # purchase 이벤트 커스텀 파라미터 (등록된 속성에서만 조회 가능)
    "customEvent:is_regular_donation": {
        "ui_name": "정기후원 여부",
        "aliases": ["is_regular_donation", "정기후원여부", "정기후원", "정기/일시"],
        "kr_semantics": ["정기후원", "일시후원"],
        "category": "event",
        "scope": "event",
        "priority": 5,
        "description": "purchase 이벤트 커스텀 파라미터"
    },
    "customEvent:country_name": {
        "ui_name": "국가명(커스텀)",
        "aliases": ["country_name", "국가명", "국가"],
        "kr_semantics": ["국가별"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "purchase 이벤트 커스텀 파라미터"
    },
    "customEvent:domestic_children_count": {
        "ui_name": "국내 아동 수",
        "aliases": ["domestic_children_count", "국내아동수", "국내아동"],
        "kr_semantics": ["국내 아동"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "purchase 이벤트 커스텀 파라미터"
    },
    "customEvent:overseas_children_count": {
        "ui_name": "해외 아동 수",
        "aliases": ["overseas_children_count", "해외아동수", "해외아동"],
        "kr_semantics": ["해외 아동"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "purchase 이벤트 커스텀 파라미터"
    },
    "customEvent:letter_translation": {
        "ui_name": "편지 번역 여부",
        "aliases": ["letter_translation", "편지번역", "번역여부"],
        "kr_semantics": ["편지 번역"],
        "category": "event",
        "scope": "event",
        "priority": 3,
        "description": "purchase 이벤트 커스텀 파라미터"
    },
    "customEvent:donation_name": {
        "ui_name": "후원명",
        "aliases": ["donation_name", "후원명", "후원이름"],
        "kr_semantics": ["후원명"],
        "category": "event",
        "scope": "event",
        "priority": 5,
        "description": "purchase 이벤트 커스텀 파라미터"
    },
    "customEvent:menu_name": {
        "ui_name": "메뉴명",
        "aliases": ["menu_name", "menu name", "메뉴명", "메뉴 네임", "메뉴이름"],
        "kr_semantics": ["메뉴", "네비", "gnb", "메뉴 클릭"],
        "category": "event",
        "scope": "event",
        "priority": 5,
        "description": "커스텀 이벤트 파라미터 (예: gnb_click)"
    },
    "customEvent:banner_name": {
        "ui_name": "배너명",
        "aliases": ["banner_name", "배너명", "배너 이름"],
        "kr_semantics": ["배너"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:button_name": {
        "ui_name": "버튼명",
        "aliases": ["button_name", "버튼명", "버튼 이름"],
        "kr_semantics": ["버튼"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:click_button": {
        "ui_name": "클릭 버튼",
        "aliases": ["click_button", "클릭버튼", "click allbutton", "click_allbutton"],
        "kr_semantics": ["버튼 클릭"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "click_allButton 이벤트의 click_button 파라미터"
    },
    "customEvent:click_location": {
        "ui_name": "클릭 위치",
        "aliases": ["click_location", "클릭위치"],
        "kr_semantics": ["위치 클릭"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:click_section": {
        "ui_name": "클릭 섹션",
        "aliases": ["click_section", "클릭섹션"],
        "kr_semantics": ["섹션 클릭"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:click_text": {
        "ui_name": "클릭 텍스트",
        "aliases": ["click_text", "클릭텍스트", "클릭 문자열"],
        "kr_semantics": ["클릭 문구", "버튼 문구"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:content_category": {
        "ui_name": "콘텐츠 카테고리",
        "aliases": ["content_category", "콘텐츠카테고리"],
        "kr_semantics": ["콘텐츠 카테고리"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:content_name": {
        "ui_name": "콘텐츠 이름",
        "aliases": ["content_name", "콘텐츠이름", "콘텐츠명"],
        "kr_semantics": ["콘텐츠 이름"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:content_type": {
        "ui_name": "콘텐츠 유형",
        "aliases": ["content_type", "콘텐츠유형", "콘텐츠 타입"],
        "kr_semantics": ["콘텐츠 유형"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:detail_category": {
        "ui_name": "상세 카테고리",
        "aliases": ["detail_category", "상세카테고리"],
        "kr_semantics": ["상세 카테고리"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:event_category": {
        "ui_name": "Event Category",
        "aliases": ["event_category", "event category", "이벤트 카테고리"],
        "kr_semantics": ["이벤트 분류"],
        "category": "event",
        "scope": "event",
        "priority": 3,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:event_label": {
        "ui_name": "Event Label",
        "aliases": ["event_label", "event label", "이벤트 라벨"],
        "kr_semantics": ["라벨"],
        "category": "event",
        "scope": "event",
        "priority": 3,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:main_category": {
        "ui_name": "메인 카테고리",
        "aliases": ["main_category", "메인카테고리"],
        "kr_semantics": ["메인 카테고리"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:sub_category": {
        "ui_name": "서브 카테고리",
        "aliases": ["sub_category", "서브카테고리"],
        "kr_semantics": ["서브 카테고리"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:payment_type": {
        "ui_name": "결제 유형",
        "aliases": ["payment_type", "결제유형", "결제 타입"],
        "kr_semantics": ["결제"],
        "category": "event",
        "scope": "event",
        "priority": 4,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:percent_scrolled": {
        "ui_name": "스크롤 비율",
        "aliases": ["percent_scrolled", "스크롤비율", "스크롤 퍼센트"],
        "kr_semantics": ["스크롤"],
        "category": "event",
        "scope": "event",
        "priority": 3,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:referrer_host": {
        "ui_name": "유입 호스트",
        "aliases": ["referrer_host", "리퍼러 호스트", "유입호스트"],
        "kr_semantics": ["리퍼러 호스트"],
        "category": "event",
        "scope": "event",
        "priority": 3,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:referrer_pathname": {
        "ui_name": "유입 경로(Pathname)",
        "aliases": ["referrer_pathname", "리퍼러 경로", "유입경로"],
        "kr_semantics": ["리퍼러 경로"],
        "category": "event",
        "scope": "event",
        "priority": 3,
        "description": "이벤트 커스텀 파라미터"
    },
    "customEvent:step": {
        "ui_name": "단계",
        "aliases": ["step", "스텝", "단계"],
        "kr_semantics": ["단계"],
        "category": "event",
        "scope": "event",
        "priority": 3,
        "description": "이벤트 커스텀 파라미터"
    },
}
GA4_CUSTOM_METRICS = {}

# 커스텀 차원을 기본 차원 registry에 병합
GA4_DIMENSIONS.update(GA4_CUSTOM_DIMENSIONS)

# ------------------------------
# 기본값 / 추천 정책
# ------------------------------
DEFAULT_METRIC = "activeUsers"
DEFAULT_DIMENSION = None
DEFAULT_TIME_DIMENSION = "date"

# ------------------------------
# Metric / Dimension 빠른 검색용
# ------------------------------
ALL_DIMENSION_NAMES = list(GA4_DIMENSIONS.keys())
ALL_METRIC_NAMES = list(GA4_METRICS.keys())

# ------------------------------
# QA/Validation 용
# ------------------------------
def is_valid_dimension(name: str) -> bool:
    return name in GA4_DIMENSIONS or name in GA4_ADS_DIMENSIONS

def is_valid_metric(name: str) -> bool:
    return name in GA4_METRICS
