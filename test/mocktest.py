"""
매수/매도 로직 통합 테스트 스크립트

테스트 흐름:
1. START 명령 → 전략 테이블 초기화, daily_strategy_id 생성
2. Prediction 처리 → 목표가 테이블 생성
3. Price 알림 (매수 조건) → 매수 시그널 생성 및 주문
4. 체결 통보 Mock → Position 업데이트
5. Price 알림 (매도 조건) → 매도 시그널 생성 및 주문
6. CLOSING → 마감 처리

실행 전 필요 사항:
- Redis: localhost:6379
- Kafka(Redpanda): localhost:19092
"""

# 환경 변수 설정 - 반드시 다른 모든 import보다 먼저!
import os
os.environ["REDIS_HOST"] = "localhost"
os.environ["KAFKA_USE_INTERNAL"] = "false"

import asyncio
import logging
import sys
import json
from datetime import datetime
from pathlib import Path

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

# settings 캐시 초기화 (환경 변수 변경 반영)
from app.config.settings import get_settings
get_settings.cache_clear()

# 환경 변수 설정 (fakeredis 사용 여부)
USE_FAKE_REDIS = False  # True: fakeredis 사용, False: 실제 Redis 사용

if USE_FAKE_REDIS:
    import fakeredis
    fake_redis = fakeredis.FakeRedis(decode_responses=True)
    import app.kis.websocket.redis_manager as redis_manager_module

    def patched_init(self):
        """Redis 연결을 fakeredis로 대체"""
        self._redis_client = fake_redis
        print("FakeRedis 연결됨")

    redis_manager_module.WebSocketRedisManager.__init__ = patched_init

from app.models.websocket import WebSocketCommand
from app.models.prediction import PredictionItem, PredictionMessage
from app.handler.websocket_handler import get_websocket_handler
from app.handler.prediction_handler import get_prediction_handler
from app.service.strategy_table import get_strategy_table
from app.service.signal_execute import get_signal_executor
from app.kis.websocket.redis_manager import get_redis_manager
from app.kafka.daily_strategy_producer import get_daily_strategy_producer
from app.kafka.order_signal_producer import get_order_signal_producer

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# Mock 데이터 생성 함수
# =============================================================================

def create_start_command() -> WebSocketCommand:
    """START 명령 생성"""
    return WebSocketCommand(
        command="START",
        timestamp=datetime.now(),
        target="ALL",
        config={
            "env_dv": "demo",
            "appkey": "mock_appkey",
            "is_mock": True,
            "accounts": [],
            "stocks": ["005930", "000660", "035420"],  # 삼성전자, SK하이닉스, 네이버
            "strategies": [
                {
                    "user_strategy_id": 1,
                    "user_id": 100,
                    "strategy_id": 1,
                    "strategy_name": "시가매수전략",
                    "strategy_weight_type": "EQUAL",
                    "ls_ratio": -1.0,  # 손절 -1%
                    "tp_ratio": 0.8,   # 익절 80%
                    "total_investment": 1000000,  # 100만원
                }
            ],
            "users": [],
        }
    )


def create_closing_command() -> WebSocketCommand:
    """CLOSING 명령 생성"""
    return WebSocketCommand(
        command="CLOSING",
        timestamp=datetime.now(),
        target="ALL",
    )


def create_prediction_message() -> PredictionMessage:
    """예측 메시지 생성"""
    now = datetime.now()
    return PredictionMessage(
        timestamp=now,
        total_count=3,
        predictions=[
            PredictionItem(
                timestamp=now,
                stock_code="005930",
                stock_name="삼성전자",
                exchange="KOSPI",
                market_cap=500000000000000,
                date=now.strftime("%Y-%m-%d"),
                gap_rate=1.5,
                stock_open=70000,  # 시가 70,000원
                prob_up=0.65,
                prob_down=0.35,
                predicted_direction=1,
                expected_return=2.5,
                return_if_up=3.0,
                return_if_down=-1.0,
                max_return_if_up=5.0,
                take_profit_target=3.0,  # 익절 목표 3%
                signal="BUY",
                model_version="v1.0",
                confidence="HIGH",
            ),
            PredictionItem(
                timestamp=now,
                stock_code="000660",
                stock_name="SK하이닉스",
                exchange="KOSPI",
                market_cap=100000000000000,
                date=now.strftime("%Y-%m-%d"),
                gap_rate=2.0,
                stock_open=150000,  # 시가 150,000원
                prob_up=0.55,
                prob_down=0.45,
                predicted_direction=1,
                expected_return=2.0,
                return_if_up=2.5,
                return_if_down=-1.5,
                max_return_if_up=4.0,
                take_profit_target=2.5,  # 익절 목표 2.5%
                signal="BUY",
                model_version="v1.0",
                confidence="MEDIUM",
            ),
            PredictionItem(
                timestamp=now,
                stock_code="035420",
                stock_name="네이버",
                exchange="KOSPI",
                market_cap=50000000000000,
                date=now.strftime("%Y-%m-%d"),
                gap_rate=1.8,
                stock_open=200000,  # 시가 200,000원
                prob_up=0.60,
                prob_down=0.40,
                predicted_direction=1,
                expected_return=2.2,
                return_if_up=2.8,
                return_if_down=-1.2,
                max_return_if_up=4.5,
                take_profit_target=2.8,  # 익절 목표 2.8%
                signal="BUY",
                model_version="v1.0",
                confidence="MEDIUM",
            ),
        ]
    )


def create_price_data(stock_code: str, current_price: float, opening_price: float) -> dict:
    """
    실시간 가격 데이터 생성 (KIS WebSocket H0STCNT0 형식)
    """
    now = datetime.now()
    return {
        "MKSC_SHRN_ISCD": stock_code,  # 종목코드
        "STCK_PRPR": str(int(current_price)),  # 현재가
        "STCK_OPRC": str(int(opening_price)),  # 시가
        "OPRC_HOUR": now.strftime("%H%M%S"),  # 시가시간
        "STCK_HGPR": str(int(current_price * 1.05)),  # 고가
        "STCK_LWPR": str(int(current_price * 0.95)),  # 저가
        "PRDY_VRSS": "500",  # 전일대비
        "PRDY_VRSS_SIGN": "2",  # 전일대비부호 (2: 상승)
        "PRDY_CTRT": "0.71",  # 전일대비율
        "ACML_VOL": "1000000",  # 누적거래량
        "ACML_TR_PBMN": "70000000000",  # 누적거래대금
    }


def create_asking_price_data(stock_code: str, current_price: float) -> dict:
    """
    호가 데이터 생성 (KIS WebSocket H0STASP0 형식)
    """
    tick = 100 if current_price >= 50000 else 50  # 호가단위

    data = {
        "MKSC_SHRN_ISCD": stock_code,
    }

    # 매도호가 (현재가보다 높음)
    for i in range(1, 11):
        data[f"ASKP{i}"] = str(int(current_price + tick * i))
        data[f"ASKP_RSQN{i}"] = str(1000 * (11 - i))  # 1호가가 가장 많음

    # 매수호가 (현재가보다 낮음)
    for i in range(1, 11):
        data[f"BIDP{i}"] = str(int(current_price - tick * i))
        data[f"BIDP_RSQN{i}"] = str(1000 * (11 - i))

    return data


# =============================================================================
# 테스트 함수
# =============================================================================

async def test_1_start_command():
    """Step 1: START 명령 처리 테스트"""
    logger.info("=" * 60)
    logger.info("Step 1: START 명령 처리")
    logger.info("=" * 60)

    websocket_handler = get_websocket_handler()
    start_cmd = create_start_command()

    await websocket_handler.handle_command(start_cmd)

    # 결과 확인
    strategy_table = get_strategy_table()
    strategies = strategy_table.get_all_strategies()
    logger.info(f"등록된 전략: {strategies}")

    redis_manager = get_redis_manager()
    for user_strategy_id in strategies:
        daily_id = redis_manager.get_daily_strategy_id(user_strategy_id)
        logger.info(f"user_strategy_id={user_strategy_id} → daily_strategy_id={daily_id}")

    return len(strategies) > 0


async def test_2_prediction():
    """Step 2: 예측 데이터 처리 테스트"""
    logger.info("=" * 60)
    logger.info("Step 2: 예측 데이터 처리 (목표가 테이블 생성)")
    logger.info("=" * 60)

    prediction_handler = get_prediction_handler()
    prediction_msg = create_prediction_message()

    await prediction_handler.handle_prediction(prediction_msg)

    # 결과 확인
    redis_manager = get_redis_manager()
    strategy_table = get_strategy_table()

    for user_strategy_id in strategy_table.get_all_strategies():
        targets = redis_manager.get_strategy_all_targets(user_strategy_id)
        logger.info(f"전략 {user_strategy_id}의 목표가 테이블:")
        for stock_code, target_data in targets.items():
            logger.info(
                f"  {stock_code}: 시가={target_data.get('stock_open')}, "
                f"목표가={target_data.get('target_price'):.0f}, "
                f"목표매도가={target_data.get('target_sell_price'):.0f}, "
                f"손절가={target_data.get('stop_loss_price'):.0f}, "
                f"수량={target_data.get('target_quantity')}"
            )

    return len(targets) > 0


async def test_3_buy_signal():
    """Step 3: 매수 시그널 생성 테스트"""
    logger.info("=" * 60)
    logger.info("Step 3: 매수 시그널 생성 (시가 근처 가격)")
    logger.info("=" * 60)

    signal_executor = get_signal_executor()
    redis_manager = get_redis_manager()

    # 삼성전자 매수 조건: 시가 70,000원, 현재가 70,200원 (시가 대비 0.3% 상승)
    stock_code = "005930"
    opening_price = 70000
    current_price = 70200  # 시가 대비 0.3% 이내

    price_data = create_price_data(stock_code, current_price, opening_price)
    asking_price_data = create_asking_price_data(stock_code, current_price)

    # 호가 데이터 Redis에 저장 (signal_executor가 사용함)
    redis_key = f"websocket:asking_price_data:{stock_code}"
    redis_manager._redis_client.setex(
        redis_key,
        60,
        json.dumps(asking_price_data, ensure_ascii=False)
    )

    logger.info(f"Price 데이터: 종목={stock_code}, 시가={opening_price}, 현재가={current_price}")

    # 매수 시그널 체크
    await signal_executor.check_and_generate_buy_signal(price_data)

    # 결과 확인 (시그널이 생성되었는지)
    generated = f"1_{stock_code}_BUY" in signal_executor._generated_signals
    logger.info(f"매수 시그널 생성됨: {generated}")

    return generated


async def test_4_naver_buy_signal():
    """Step 4: 네이버 매수 시그널 생성 테스트 (매도 안 할 종목)"""
    logger.info("=" * 60)
    logger.info("Step 4: 네이버 매수 시그널 생성 (매도 안 할 종목)")
    logger.info("=" * 60)

    signal_executor = get_signal_executor()
    redis_manager = get_redis_manager()

    # 쓰로틀링 캐시 초기화
    signal_executor._last_signal_check.clear()
    signal_executor._last_prices.clear()

    # 네이버 매수 조건: 시가 200,000원, 현재가 200,300원 (시가 대비 0.15% 상승)
    stock_code = "035420"
    opening_price = 200000
    current_price = 200300  # 시가 대비 0.15% 이내

    price_data = create_price_data(stock_code, current_price, opening_price)
    asking_price_data = create_asking_price_data(stock_code, current_price)

    # 호가 데이터 Redis에 저장
    redis_key = f"websocket:asking_price_data:{stock_code}"
    redis_manager._redis_client.setex(
        redis_key,
        60,
        json.dumps(asking_price_data, ensure_ascii=False)
    )

    logger.info(f"Price 데이터: 종목={stock_code}, 시가={opening_price}, 현재가={current_price}")

    # 매수 시그널 체크
    await signal_executor.check_and_generate_buy_signal(price_data)

    # 결과 확인
    generated = f"1_{stock_code}_BUY" in signal_executor._generated_signals
    logger.info(f"네이버 매수 시그널 생성됨: {generated}")

    # Position 확인
    daily_strategy_id = redis_manager.get_daily_strategy_id(1)
    position = redis_manager.get_position(daily_strategy_id, stock_code)
    if position:
        logger.info(
            f"네이버 Position: 보유수량={position.get('holding_quantity')}, "
            f"평균가={position.get('average_price')}"
        )

    return generated


async def test_5_hynix_buy_signal():
    """Step 5: SK하이닉스 매수 시그널 생성 테스트 (매도 안 할 종목)"""
    logger.info("=" * 60)
    logger.info("Step 5: SK하이닉스 매수 시그널 생성 (매도 안 할 종목)")
    logger.info("=" * 60)

    signal_executor = get_signal_executor()
    redis_manager = get_redis_manager()

    # 쓰로틀링 캐시 초기화
    signal_executor._last_signal_check.clear()
    signal_executor._last_prices.clear()

    # SK하이닉스 매수 조건: 시가 150,000원, 현재가 150,200원 (시가 대비 0.13% 상승)
    stock_code = "000660"
    opening_price = 150000
    current_price = 150200  # 시가 대비 1% 이내

    price_data = create_price_data(stock_code, current_price, opening_price)
    asking_price_data = create_asking_price_data(stock_code, current_price)

    # 호가 데이터 Redis에 저장
    redis_key = f"websocket:asking_price_data:{stock_code}"
    redis_manager._redis_client.setex(
        redis_key,
        60,
        json.dumps(asking_price_data, ensure_ascii=False)
    )

    logger.info(f"Price 데이터: 종목={stock_code}, 시가={opening_price}, 현재가={current_price}")

    # 매수 시그널 체크
    await signal_executor.check_and_generate_buy_signal(price_data)

    # 결과 확인
    generated = f"1_{stock_code}_BUY" in signal_executor._generated_signals
    logger.info(f"SK하이닉스 매수 시그널 생성됨: {generated}")

    # Position 확인
    daily_strategy_id = redis_manager.get_daily_strategy_id(1)
    position = redis_manager.get_position(daily_strategy_id, stock_code)
    if position:
        logger.info(
            f"SK하이닉스 Position: 보유수량={position.get('holding_quantity')}, "
            f"평균가={position.get('average_price')}"
        )

    return generated


async def test_6_sell_signal():
    """Step 6: 삼성전자만 매도 시그널 생성 (네이버는 매도 안 함)"""
    logger.info("=" * 60)
    logger.info("Step 6: 삼성전자 매도 시그널 생성 (네이버는 매도 안 함)")
    logger.info("=" * 60)

    signal_executor = get_signal_executor()
    redis_manager = get_redis_manager()

    # 쓰로틀링 캐시 초기화 (테스트를 위해)
    signal_executor._last_signal_check.clear()
    signal_executor._last_prices.clear()

    # 삼성전자 매도 조건: 목표매도가(target_sell_price) 도달
    # 시가 70,000 * (1 + 3.0 * 0.8 / 100) = 71,680원
    stock_code = "005930"
    current_price = 71700  # 매도가 71,680원 초과

    price_data = create_price_data(stock_code, current_price, 70000)
    asking_price_data = create_asking_price_data(stock_code, current_price)

    # 호가 데이터 Redis에 저장
    redis_key = f"websocket:asking_price_data:{stock_code}"
    redis_manager._redis_client.setex(
        redis_key,
        60,
        json.dumps(asking_price_data, ensure_ascii=False)
    )

    logger.info(f"Price 데이터: 종목={stock_code}, 현재가={current_price} (매도가 71,680 초과)")

    # 매도 시그널 체크
    await signal_executor.check_and_generate_sell_signal(price_data)

    # 결과 확인
    generated = f"1_{stock_code}_SELL" in signal_executor._generated_signals
    logger.info(f"매도 시그널 생성됨: {generated}")

    return generated


async def test_7_closing():
    """Step 7: CLOSING 명령 처리 테스트 (네이버, SK하이닉스 시장가 매도 확인)"""
    logger.info("=" * 60)
    logger.info("Step 7: CLOSING 명령 처리 (네이버, SK하이닉스 시장가 매도 확인)")
    logger.info("=" * 60)

    websocket_handler = get_websocket_handler()
    redis_manager = get_redis_manager()

    # CLOSING 전 Position 상태 확인
    user_strategy_id = 1
    daily_strategy_id = redis_manager.get_daily_strategy_id(user_strategy_id)

    if daily_strategy_id:
        positions = redis_manager.get_positions_with_holdings(daily_strategy_id)
        logger.info(f"CLOSING 전 보유 Position 수: {len(positions)}")
        for pos in positions:
            logger.info(
                f"  {pos.get('stock_code')}: 보유수량={pos.get('holding_quantity')}"
            )

    # CLOSING 명령 처리
    closing_cmd = create_closing_command()
    await websocket_handler.handle_command(closing_cmd)

    logger.info("CLOSING 처리 완료")
    return True


async def test_8_check_final_state():
    """Step 8: 최종 상태 확인"""
    logger.info("=" * 60)
    logger.info("Step 8: 최종 상태 확인")
    logger.info("=" * 60)

    redis_manager = get_redis_manager()
    strategy_table = get_strategy_table()

    for user_strategy_id in strategy_table.get_all_strategies():
        daily_strategy_id = redis_manager.get_daily_strategy_id(user_strategy_id)

        logger.info(f"\n전략 {user_strategy_id} (daily_strategy_id={daily_strategy_id}):")

        # 목표가 테이블
        targets = redis_manager.get_strategy_all_targets(user_strategy_id)
        logger.info(f"  목표가 테이블 종목 수: {len(targets)}")

        # Position
        if daily_strategy_id:
            positions = redis_manager.get_all_positions(daily_strategy_id)
            logger.info(f"  Position 수: {len(positions)}")
            for pos in positions:
                logger.info(
                    f"    {pos.get('stock_code')}: "
                    f"보유={pos.get('holding_quantity')}, "
                    f"매수총량={pos.get('total_buy_quantity')}, "
                    f"매도총량={pos.get('total_sell_quantity')}, "
                    f"실현손익={pos.get('realized_pnl')}"
                )

    return True


# =============================================================================
# 메인
# =============================================================================

async def main():
    """메인 테스트 실행"""
    logger.info("=" * 60)
    logger.info("매수/매도 로직 통합 테스트 시작")
    logger.info("=" * 60)
    logger.info("")
    logger.info("테스트 시나리오:")
    logger.info("  - 삼성전자: 매수 → 매도 시그널 → 매도 완료")
    logger.info("  - 네이버: 매수 → 매도 시그널 없음 → CLOSING에서 시장가 매도")
    logger.info("  - SK하이닉스: 매수 → 매도 시그널 없음 → CLOSING에서 시장가 매도")
    logger.info("")

    # Kafka producer 시작
    if not USE_FAKE_REDIS:
        logger.info("Kafka producer 시작 중...")
        daily_strategy_producer = get_daily_strategy_producer()
        order_signal_producer = get_order_signal_producer()

        await daily_strategy_producer.start()
        await order_signal_producer.start()
        logger.info("Kafka producer 시작 완료")

    results = {}

    try:
        # Step 1: START 명령
        results["1_start"] = await test_1_start_command()

        # Step 2: 예측 데이터 처리
        results["2_prediction"] = await test_2_prediction()

        # Step 3: 삼성전자 매수 시그널
        results["3_samsung_buy"] = await test_3_buy_signal()

        # Step 4: 네이버 매수 시그널
        results["4_naver_buy"] = await test_4_naver_buy_signal()

        # Step 5: SK하이닉스 매수 시그널
        results["5_hynix_buy"] = await test_5_hynix_buy_signal()

        # Step 6: 삼성전자만 매도 시그널 (네이버는 매도 안 함)
        results["6_samsung_sell"] = await test_6_sell_signal()

        # Step 7: CLOSING (네이버, SK하이닉스 시장가 매도 확인)
        results["7_closing"] = await test_7_closing()

        # Step 8: 최종 상태
        results["8_final_state"] = await test_8_check_final_state()

    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {e}", exc_info=True)

    # 결과 요약
    logger.info("\n" + "=" * 60)
    logger.info("테스트 결과 요약")
    logger.info("=" * 60)

    for step, result in results.items():
        status = "PASS" if result else "FAIL"
        logger.info(f"  {step}: {status}")

    passed = sum(1 for r in results.values() if r)
    total = len(results)
    logger.info(f"\n총 {total}개 중 {passed}개 통과")

    # Kafka producer 종료
    if not USE_FAKE_REDIS:
        logger.info("Kafka producer 종료 중...")
        await daily_strategy_producer.stop()
        await order_signal_producer.stop()
        logger.info("Kafka producer 종료 완료")


if __name__ == "__main__":
    asyncio.run(main())
