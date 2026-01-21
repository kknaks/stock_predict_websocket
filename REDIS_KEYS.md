# Redis 키 목록

이 문서는 `stock_predict_websocket` 서비스에서 사용하는 모든 Redis 키 패턴을 정리합니다.

## 1. 웹소켓 연결 정보 (WebSocket Connection)

### 1.1 가격 웹소켓 연결
- **키**: `websocket:price`
- **타입**: String (JSON)
- **TTL**: 24시간 (86400초)
- **설명**: 가격 웹소켓 연결 정보
- **데이터 구조**:
  ```json
  {
    "ws_token": "string",
    "appkey": "string",
    "env_dv": "string",
    "stocks": ["005930", "000660"],
    "status": "connected",
    "reconnect_attempts": 0,
    "connected_at": "2026-01-16T11:45:10.191705+09:00",
    "updated_at": "2026-01-16T11:45:10.191705+09:00"
  }
  ```

### 1.2 계좌 웹소켓 연결
- **키**: `websocket:account:{account_no}`
- **타입**: String (JSON)
- **TTL**: 24시간 (86400초)
- **설명**: 계좌별 웹소켓 연결 정보
- **예시**: `websocket:account:12345678`
- **데이터 구조**:
  ```json
  {
    "account_no": "12345678",
    "ws_token": "string",
    "appkey": "string",
    "env_dv": "string",
    "account_product_code": "01",
    "is_mock": false,
    "access_token": "string",
    "user_id": 1,
    "user_strategy_ids": [2, 3],
    "hts_id": "string",
    "status": "connected",
    "reconnect_attempts": 0,
    "connected_at": "2026-01-16T11:45:10.191705+09:00",
    "updated_at": "2026-01-16T11:45:10.191705+09:00"
  }
  ```

### 1.3 사용자 ID로 계좌 조회 인덱스
- **키**: `websocket:account:user:{user_id}`
- **타입**: String (account_no)
- **TTL**: 24시간 (86400초)
- **설명**: user_id로 account_no를 빠르게 조회하기 위한 인덱스
- **예시**: `websocket:account:user:1` → `"12345678"`

### 1.4 전략 ID로 계좌 조회 인덱스
- **키**: `websocket:account:strategy:{user_strategy_id}`
- **타입**: String (account_no)
- **TTL**: 24시간 (86400초)
- **설명**: user_strategy_id로 account_no를 빠르게 조회하기 위한 인덱스
- **예시**: `websocket:account:strategy:2` → `"12345678"`

## 2. 전략 테이블 정보 (Strategy Table)

### 2.1 전략 설정
- **키**: `strategy:config:{user_strategy_id}`
- **타입**: String (JSON)
- **TTL**: 24시간 (86400초)
- **설명**: 전략별 설정 정보
- **예시**: `strategy:config:2`
- **데이터 구조**:
  ```json
  {
    "user_strategy_id": 2,
    "user_id": 1,
    "is_mock": false,
    "strategy_id": 1,
    "strategy_name": "gap_rate",
    "ls_ratio": -1.0,
    "tp_ratio": 0.8
  }
  ```

### 2.2 종목별 목표가
- **키**: `strategy:target:{user_strategy_id}:{stock_code}`
- **타입**: String (JSON)
- **TTL**: 24시간 (86400초)
- **설명**: 전략별 종목의 목표가 정보
- **예시**: `strategy:target:2:005930`
- **데이터 구조**:
  ```json
  {
    "stock_code": "005930",
    "stock_name": "삼성전자",
    "exchange": "KRX",
    "stock_open": 75000.0,
    "target_price": 82500.0,
    "target_sell_price": 81000.0,
    "stop_loss_price": 74250.0,
    "gap_rate": 2.5,
    "take_profit_target": 10.0,
    "prob_up": 0.75,
    "signal": "BUY",
    "created_at": "2026-01-16T11:45:10.191705+09:00"
  }
  ```

### 2.3 전략의 종목 목록 (Set)
- **키**: `strategy:targets:{user_strategy_id}`
- **타입**: Set
- **TTL**: 24시간 (86400초)
- **설명**: 전략에 등록된 모든 종목 코드 목록
- **예시**: `strategy:targets:2` → `["005930", "000660", "035420"]`
- **용도**: 전략의 모든 종목 목표가를 빠르게 조회하기 위한 인덱스

## 3. 실시간 가격 데이터 (Price Data)

### 3.1 실시간 체결가 데이터
- **키**: `websocket:price_data:{stock_code}`
- **타입**: String (JSON)
- **TTL**: 1시간 (3600초)
- **설명**: 종목별 실시간 체결가 데이터 (최신 데이터만 유지, 덮어쓰기)
- **예시**: `websocket:price_data:005930`
- **데이터 구조**: 웹소켓에서 받은 체결가 데이터 + `updated_at` 타임스탬프

### 3.2 실시간 호가 데이터
- **키**: `websocket:asking_price_data:{stock_code}`
- **타입**: String (JSON)
- **TTL**: 1시간 (3600초)
- **설명**: 종목별 실시간 호가 데이터 (최신 데이터만 유지, 덮어쓰기)
- **예시**: `websocket:asking_price_data:005930`
- **데이터 구조**: 웹소켓에서 받은 호가 데이터 + `updated_at` 타임스탬프

## 4. 매도 시그널 (Sell Signal)

### 4.1 매도 시그널
- **키**: `signal:sell:{user_strategy_id}:{stock_code}`
- **타입**: String (JSON)
- **TTL**: 30분 (1800초)
- **설명**: 전략별 종목의 매도 시그널 정보
- **예시**: `signal:sell:2:005930`
- **데이터 구조**:
  ```json
  {
    "signal_type": "SELL",
    "stock_code": "005930",
    "current_price": 82000.0,
    "target_price": 82500.0,
    "stop_loss_price": 74250.0,
    "recommended_order_price": 81900.0,
    "recommended_order_type": "LIMIT",
    "expected_slippage_pct": 0.12,
    "urgency": "HIGH",
    "reason": "목표가 도달",
    "created_at": "2026-01-16T11:45:10.191705+09:00",
    "user_strategy_id": 2
  }
  ```

## 키 패턴 요약

| 카테고리 | 키 패턴 | 타입 | TTL |
|---------|---------|------|-----|
| 웹소켓 연결 | `websocket:price` | String | 24시간 |
| 웹소켓 연결 | `websocket:account:{account_no}` | String | 24시간 |
| 웹소켓 인덱스 | `websocket:account:user:{user_id}` | String | 24시간 |
| 웹소켓 인덱스 | `websocket:account:strategy:{user_strategy_id}` | String | 24시간 |
| 전략 설정 | `strategy:config:{user_strategy_id}` | String | 24시간 |
| 목표가 | `strategy:target:{user_strategy_id}:{stock_code}` | String | 24시간 |
| 종목 목록 | `strategy:targets:{user_strategy_id}` | Set | 24시간 |
| 가격 데이터 | `websocket:price_data:{stock_code}` | String | 1시간 |
| 호가 데이터 | `websocket:asking_price_data:{stock_code}` | String | 1시간 |
| 매도 시그널 | `signal:sell:{user_strategy_id}:{stock_code}` | String | 30분 |

## 참고사항

- 모든 키는 JSON 문자열로 저장됩니다 (Set 타입 제외)
- TTL이 만료되면 자동으로 삭제됩니다
- 같은 키에 대해 덮어쓰기가 가능합니다 (가격 데이터, 호가 데이터 등)
- 인덱스 키들은 빠른 조회를 위한 참조용입니다
