# 매수/매도 로직 요약

## 전체 흐름

```
[Kafka 예측 메시지] → [PredictionHandler] → [StrategyTable] → [WebSocket 종목 구독]
                                                                       ↓
[주문 API] ← [SignalExecutor] ← [실시간 가격 데이터] ← [KIS WebSocket]
```

---

## 1. 예측 데이터 수신 및 테이블 생성

### 1.1 PredictionHandler (prediction_handler.py)

Kafka에서 예측 메시지를 수신하면:

1. **후보군 필터링**
   ```python
   candidate_stocks = [
       p for p in predictions
       if p.gap_rate < 28 and p.prob_up > 0.2
   ]
   # 상승 확률 상위 15개 선정
   candidate_stocks = sorted(candidate_stocks, key=lambda x: x.prob_up, reverse=True)[:15]
   ```

2. **StrategyTable에 후보군 전달**
   ```python
   await strategy_table.process_predictions(candidate_stocks)
   ```

3. **WebSocket 종목 업데이트**
   - 후보군 종목에 대해 실시간 가격 구독 시작

---

## 2. 전략 테이블 (strategy_table.py)

### 2.1 START 명령 시 전략 초기화

`WebSocketHandler._handle_start_command()`에서 호출:

```python
await strategy_table.initialize_from_start_command(all_strategies)
```

**StrategyConfig 구조:**
```python
@dataclass
class StrategyConfig:
    user_strategy_id: int
    user_id: int
    is_mock: bool
    strategy_id: int
    strategy_name: str
    strategy_weight_type: str  # EQUAL, MARKETCAP, PRICE
    ls_ratio: float           # 손절 비율 (%, 음수)
    tp_ratio: float           # 익절 비율 (0~1)
    total_investment: float   # 총 투자 금액
```

### 2.2 목표가 계산 (_calculate_target)

예측 데이터 수신 시 `process_predictions()`에서 계산:

```python
# 목표가 = 시가 * (1 + take_profit_target / 100)
target_price = stock_open * (1 + take_profit_target / 100)

# 매도가 = 시가 * (1 + take_profit_target * tp_ratio / 100)
sell_price = stock_open * (1 + take_profit_target * tp_ratio / 100)

# 손절가 = 시가 * (1 + ls_ratio / 100)
stop_loss_price = stock_open * (1 + ls_ratio / 100)

# 목표 수량 = (총 투자 금액 * 비중) / 시가
target_quantity = int((total_investment * weight) / stock_open)
```

**비중(weight) 계산:**
- `EQUAL`: 균등 분배 (1 / 종목 수)
- `MARKETCAP`: 시가총액 비례
- `PRICE`: 역가중치 (시가가 낮을수록 비중 높음)

**TargetPrice 구조:**
```python
@dataclass
class TargetPrice:
    stock_code: str
    stock_name: str
    exchange: str
    stock_open: float         # 시가
    target_price: float       # 목표가
    sell_price: float         # 매도가 (익절)
    stop_loss_price: float    # 손절가
    gap_rate: float
    take_profit_target: float
    prob_up: float
    signal: str
    weight: float             # 비중 (0.0 ~ 1.0)
    target_quantity: int      # 목표 수량
    created_at: datetime
```

---

## 3. 시그널 생성 및 실행 (signal_execute.py)

### 3.1 매수 시그널 생성 조건

`check_and_generate_buy_signal()`:

```python
# 조건 체크:
# 1. strategy_id == 1 인 전략만
# 2. 시가 존재 (STCK_OPRC > 0)
# 3. 현재가와 시가 차이 1% 미만
# 4. 장 시작 후 10분 이내
# 5. 중복 방지 (이미 BUY 시그널 생성된 조합 스킵)

price_diff_pct = abs((current_price - opening_price) / opening_price * 100)
if price_diff_pct >= 1.0:
    return  # 스킵
```

### 3.2 매도 시그널 생성 조건

`check_and_generate_sell_signal()`:

```python
# 조건 체크:
# 1. Position 기반: holding_quantity > 0
# 2. 활성 매도 주문 없음
# 3. 목표가 데이터 존재

# 매도 시그널 생성 시 조건 (signal_generator에서):
# - current_price >= sell_price (익절)
# - current_price <= stop_loss_price (손절)
```

### 3.3 최적화 로직

```python
# 1. 가격 변동 없으면 스킵
if current_price == self._last_prices.get(stock_code):
    return

# 2. 쓰로틀링 (0.5초 간격)
if (now - last_check).total_seconds() < 0.5:
    return

# 3. 중복 시그널 방지
signal_key = f"{user_strategy_id}_{stock_code}_{signal_type}"
if signal_key in self._generated_signals:
    return
```

---

## 4. Redis 데이터 구조

### 4.1 전략 설정

**Key:** `strategy:config:{user_strategy_id}`
```json
{
    "user_strategy_id": 1,
    "user_id": 100,
    "is_mock": false,
    "strategy_id": 1,
    "strategy_name": "시가매수전략",
    "strategy_weight_type": "EQUAL",
    "ls_ratio": -1.0,
    "tp_ratio": 0.8,
    "total_investment": 1000000
}
```

### 4.2 목표가 정보

**Key:** `strategy:target:{user_strategy_id}:{stock_code}`
```json
{
    "stock_code": "005930",
    "stock_name": "삼성전자",
    "exchange": "KOSPI",
    "stock_open": 70000,
    "target_price": 72100,
    "sell_price": 71680,
    "stop_loss_price": 69300,
    "gap_rate": 1.5,
    "take_profit_target": 3.0,
    "prob_up": 0.65,
    "signal": "BUY",
    "weight": 0.0667,
    "target_quantity": 14,
    "created_at": "2024-01-15T09:00:00",

    // 매수 후 추가되는 필드
    "buy_executed": true,
    "buy_quantity": 14,
    "buy_price": 70050,
    "buy_order_no": "0000123456",
    "buy_order_time": "2024-01-15T09:01:30",
    "buy_executed_quantity": 14,
    "buy_executed_price": 70050,
    "buy_execution_time": "2024-01-15T09:01:35",

    // 매도 후 추가되는 필드
    "sell_executed": false,
    "sell_quantity": 14,
    "sell_price": 71680,
    "sell_order_no": "0000123457"
}
```

**종목 목록 Set:** `strategy:targets:{user_strategy_id}`
- Set 자료구조로 해당 전략의 모든 종목 코드 저장

### 4.3 Daily Strategy

**Key:** `daily_strategy:{daily_strategy_id}`
```json
{
    "daily_strategy_id": 1,
    "user_strategy_id": 1,
    "is_mock": false,
    "user_id": 100,
    "strategy_id": 1,
    "created_at": "2024-01-15T08:30:00"
}
```

**인덱스:** `daily_strategy:by_user:{user_strategy_id}` → `daily_strategy_id`

### 4.4 Position (포지션)

**Key:** `position:{daily_strategy_id}:{stock_code}`
```json
{
    "daily_strategy_id": 1,
    "user_strategy_id": 1,
    "stock_code": "005930",
    "stock_name": "삼성전자",
    "holding_quantity": 14,
    "average_price": 70050,
    "total_investment": 980700,
    "total_buy_quantity": 14,
    "total_buy_amount": 980700,
    "total_sell_quantity": 0,
    "total_sell_amount": 0,
    "realized_pnl": 0,
    "created_at": "2024-01-15T09:01:35",
    "updated_at": "2024-01-15T09:01:35"
}
```

**인덱스:**
- `position:list:{daily_strategy_id}` → Set (종목 코드 목록)
- `position:by_user:{user_strategy_id}:{stock_code}` → `daily_strategy_id`

### 4.5 Order (주문)

**Key:** `order:{order_id}`
```json
{
    "order_id": "uuid-string",
    "daily_strategy_id": 1,
    "stock_code": "005930",
    "order_type": "BUY",
    "order_quantity": 14,
    "order_price": 70050,
    "order_no": "0000123456",
    "status": "filled",
    "executed_quantity": 14,
    "executed_price": 70050,
    "remaining_quantity": 0,
    "created_at": "2024-01-15T09:01:30",
    "updated_at": "2024-01-15T09:01:35"
}
```

**인덱스:**
- `order:by_no:{order_no}` → `order_id`
- `order:active:{daily_strategy_id}:{stock_code}` → Set (활성 주문 ID 목록)

### 4.6 시그널 (백업용)

**Key:** `signal:{buy|sell}:{user_strategy_id}:{stock_code}`
```json
{
    "signal_type": "BUY",
    "stock_code": "005930",
    "current_price": 70050,
    "target_price": 72100,
    "stop_loss_price": 69300,
    "recommended_order_price": 70100,
    "recommended_order_type": "limit",
    "expected_slippage_pct": 0.07,
    "urgency": "HIGH",
    "reason": "시가 매수 조건 충족",
    "created_at": "2024-01-15T09:01:30",
    "user_strategy_id": 1
}
```
- TTL: 30분

### 4.7 호가 데이터

**Key:** `websocket:asking_price_data:{stock_code}`
- KIS WebSocket에서 수신한 호가 데이터 저장

### 4.8 WebSocket 연결 정보

**계좌:** `websocket:account:{account_no}`
```json
{
    "account_no": "12345678",
    "ws_token": "...",
    "appkey": "...",
    "access_token": "...",
    "user_id": 100,
    "user_strategy_ids": [1, 2],
    "status": "connected"
}
```

**인덱스:**
- `websocket:account:user:{user_id}` → `account_no`
- `websocket:account:strategy:{user_strategy_id}` → `account_no`

---

## 5. 슬리피지 계산 (calculate_slippage.py)

### 5.1 매수 슬리피지

```python
# 단순 슬리피지 = (최우선 매도호가 - 현재가) / 현재가 * 100
simple_slippage = (best_ask - current_price) / current_price * 100

# 예상 슬리피지 = (가중평균 체결가 - 현재가) / 현재가 * 100
expected_slippage = (weighted_avg_price - current_price) / current_price * 100
```

### 5.2 매도 슬리피지

```python
# 단순 슬리피지 = (현재가 - 최우선 매수호가) / 현재가 * 100
simple_slippage = (current_price - best_bid) / current_price * 100

# 예상 슬리피지 = (현재가 - 가중평균 체결가) / 현재가 * 100
expected_slippage = (current_price - weighted_avg_price) / current_price * 100
```

### 5.3 주문 유형 추천

| 슬리피지 | 즉시 체결 가능 | 추천 주문 유형 |
|---------|--------------|--------------|
| <= 0.05% | O | 시장가 |
| <= 0.1% | - | 지정가 (최우선 호가) |
| > 0.1% & 2단계 초과 | - | 분할 주문 |

---

## 6. 장 마감 처리 (websocket_handler.py)

### 6.1 CLOSING 명령 처리

`_handle_close_command()`에서 각 전략별로:

```
_handle_close_command()
    └── for user_strategy_id in all_strategy_ids:
            ├── _close_active_buy_orders()    # Order 기반 미체결 매수 취소
            └── _close_unsold_positions()     # Position 기반 미매도 시장가 매도
```

### 6.2 Order 기반 미체결 매수 취소

`_close_active_buy_orders()`:

```python
# 1. daily_strategy_id 조회
daily_strategy_id = self._redis_manager.get_daily_strategy_id(user_strategy_id)

# 2. strategy:targets에서 종목 목록 조회
all_targets = self._redis_manager.get_strategy_all_targets(user_strategy_id)

# 3. 각 종목별 활성 매수 주문 조회 및 취소
for stock_code in all_targets.keys():
    active_buy = self._redis_manager.get_active_buy_order(daily_strategy_id, stock_code)
    if active_buy and active_buy.get("order_no"):
        await self._order_api.cancel_order(...)
```

### 6.3 Position 기반 미매도 종목 시장가 매도

`_close_unsold_positions()`:

```python
# 1. 보유 수량이 있는 모든 Position 조회
positions_with_holdings = self._redis_manager.get_positions_with_holdings(daily_strategy_id)

# 2. 각 Position에 대해 시장가 매도
for position in positions_with_holdings:
    if position.get("holding_quantity", 0) > 0:
        market_sell_signal = SignalResult(
            signal_type="SELL",
            recommended_order_type=OrderType.MARKET,
            urgency="CRITICAL",
            reason="장 마감 시 Position 기반 강제 매도 (시장가)"
        )
        await self._order_api.process_sell_order(...)
```

---

## 7. TTL 정리

| 데이터 | TTL | 비고 |
|-------|-----|-----|
| strategy:config | 24시간 | |
| strategy:target | 24시간 | |
| daily_strategy | 25시간 | |
| position | 25시간 | |
| order | 25시간 | |
| signal | 30분 | 백업용 |
| websocket 연결 정보 | 24시간 | |
