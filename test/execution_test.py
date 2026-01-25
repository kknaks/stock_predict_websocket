"""
체결 시뮬레이션 테스트 스크립트

웹소켓 가격 정보 없이 직접 OrderAPI를 호출하여
매수/매도 체결을 시뮬레이션합니다.

테스트 흐름:
1. START 명령 → 전략 테이블 초기화
2. Prediction 처리 → 목표가 테이블 생성
3. 직접 매수 주문 (OrderAPI 호출) → 즉시 체결
4. 직접 매도 주문 (OrderAPI 호출) → 즉시 체결
5. 최종 Position/Order 상태 확인

실행 전 필요 사항:
- Redis: localhost:6379
- Kafka(Redpanda): localhost:19092 (선택사항)
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

from app.models.websocket import WebSocketCommand
from app.models.prediction import PredictionItem, PredictionMessage
from app.handler.websocket_handler import get_websocket_handler
from app.handler.prediction_handler import get_prediction_handler
from app.service.strategy_table import get_strategy_table
from app.kis.websocket.redis_manager import get_redis_manager
from app.kis.api.order_api import get_order_api
from app.service.calculate_slippage import SignalResult, OrderType
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


def create_buy_signal(stock_code: str, current_price: float) -> SignalResult:
    """매수 시그널 생성"""
    return SignalResult(
        stock_code=stock_code,
        signal_type="BUY",
        current_price=current_price,
        recommended_order_price=current_price,  # 지정가 = 현재가
        recommended_order_type=OrderType.LIMIT,
        reason="테스트 매수 시그널"
    )


def create_sell_signal(stock_code: str, current_price: float) -> SignalResult:
    """매도 시그널 생성"""
    return SignalResult(
        stock_code=stock_code,
        signal_type="SELL",
        current_price=current_price,
        recommended_order_price=current_price,  # 지정가 = 현재가
        recommended_order_type=OrderType.LIMIT,
        reason="테스트 매도 시그널"
    )


def create_market_sell_signal(stock_code: str, current_price: float) -> SignalResult:
    """시장가 매도 시그널 생성"""
    return SignalResult(
        stock_code=stock_code,
        signal_type="SELL",
        current_price=current_price,
        recommended_order_price=0,  # 시장가
        recommended_order_type=OrderType.MARKET,
        reason="시장가 매도 시그널"
    )


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


async def test_3_direct_buy_order():
    """Step 3: 직접 매수 주문 테스트 (웹소켓 가격 정보 없이)"""
    logger.info("=" * 60)
    logger.info("Step 3: 직접 매수 주문 (OrderAPI 직접 호출)")
    logger.info("=" * 60)

    order_api = get_order_api()
    redis_manager = get_redis_manager()

    test_cases = [
        {"stock_code": "005930", "stock_name": "삼성전자", "price": 70000, "quantity": 5},
        {"stock_code": "000660", "stock_name": "SK하이닉스", "price": 150000, "quantity": 3},
        {"stock_code": "035420", "stock_name": "네이버", "price": 200000, "quantity": 2},
    ]

    user_strategy_id = 1
    success_count = 0

    for case in test_cases:
        stock_code = case["stock_code"]
        stock_name = case["stock_name"]
        price = case["price"]
        quantity = case["quantity"]

        logger.info(f"\n매수 주문: {stock_name}({stock_code}) {quantity}주 @ {price:,}원")

        # 매수 시그널 생성
        buy_signal = create_buy_signal(stock_code, price)

        # OrderAPI로 직접 주문 (mock 모드이므로 즉시 체결)
        result = await order_api.process_buy_order(
            user_strategy_id=user_strategy_id,
            signal=buy_signal,
            order_quantity=quantity,
            stock_name=stock_name
        )

        if result.get("success"):
            success_count += 1
            logger.info(f"  ✓ 매수 체결 완료: order_id={result.get('order_id')}")

            # Position 확인
            daily_strategy_id = result.get("daily_strategy_id")
            position = redis_manager.get_position(daily_strategy_id, stock_code)
            if position:
                logger.info(
                    f"  Position: 보유={position.get('holding_quantity')}주, "
                    f"평균가={position.get('average_price'):,.0f}원"
                )
        else:
            logger.error(f"  ✗ 매수 실패: {result.get('error')}")

    logger.info(f"\n매수 결과: {success_count}/{len(test_cases)} 성공")
    return success_count == len(test_cases)


async def test_4_direct_sell_order():
    """Step 4: 직접 매도 주문 테스트 (삼성전자만 매도)"""
    logger.info("=" * 60)
    logger.info("Step 4: 직접 매도 주문 (삼성전자만 지정가 매도)")
    logger.info("=" * 60)

    order_api = get_order_api()
    redis_manager = get_redis_manager()

    user_strategy_id = 1
    stock_code = "005930"
    stock_name = "삼성전자"
    sell_price = 72000  # 익절 가격
    sell_quantity = 5

    logger.info(f"\n매도 주문: {stock_name}({stock_code}) {sell_quantity}주 @ {sell_price:,}원")

    # 매도 시그널 생성
    sell_signal = create_sell_signal(stock_code, sell_price)

    # OrderAPI로 직접 매도 주문
    result = await order_api.process_sell_order(
        user_strategy_id=user_strategy_id,
        signal=sell_signal,
        order_quantity=sell_quantity,
        stock_name=stock_name
    )

    if result.get("success"):
        logger.info(f"  ✓ 매도 체결 완료: order_id={result.get('order_id')}")

        # Position 확인
        daily_strategy_id = result.get("daily_strategy_id")
        position = redis_manager.get_position(daily_strategy_id, stock_code)
        if position:
            realized_pnl = position.get('realized_pnl', 0)
            logger.info(
                f"  Position: 보유={position.get('holding_quantity')}주, "
                f"평균가={position.get('average_price'):,.0f}원, "
                f"실현손익={realized_pnl:,.0f}원"
            )
        return True
    else:
        logger.error(f"  ✗ 매도 실패: {result.get('error')}")
        return False


async def test_5_market_sell_remaining():
    """Step 5: 시장가 매도 테스트 (남은 종목들)"""
    logger.info("=" * 60)
    logger.info("Step 5: 시장가 매도 (SK하이닉스, 네이버)")
    logger.info("=" * 60)

    order_api = get_order_api()
    redis_manager = get_redis_manager()

    user_strategy_id = 1
    daily_strategy_id = redis_manager.get_daily_strategy_id(user_strategy_id)

    if not daily_strategy_id:
        logger.error("daily_strategy_id를 찾을 수 없습니다")
        return False

    # 보유 중인 Position 조회
    positions = redis_manager.get_positions_with_holdings(daily_strategy_id)
    logger.info(f"보유 중인 Position 수: {len(positions)}")

    success_count = 0
    total_count = 0

    for pos in positions:
        stock_code = pos.get("stock_code")
        stock_name = pos.get("stock_name", stock_code)
        holding_qty = pos.get("holding_quantity", 0)
        avg_price = pos.get("average_price", 0)

        if holding_qty <= 0:
            continue

        total_count += 1

        # 시장가 = 평균가 * 1.01 (1% 상승 가정)
        current_price = avg_price * 1.01

        logger.info(f"\n시장가 매도: {stock_name}({stock_code}) {holding_qty}주 @ 시장가 (현재가 약 {current_price:,.0f}원)")

        # 시장가 매도 시그널 생성
        sell_signal = create_market_sell_signal(stock_code, current_price)

        # OrderAPI로 시장가 매도 주문
        result = await order_api.process_sell_order(
            user_strategy_id=user_strategy_id,
            signal=sell_signal,
            order_quantity=holding_qty,
            stock_name=stock_name
        )

        if result.get("success"):
            success_count += 1
            logger.info(f"  ✓ 시장가 매도 체결 완료")

            # Position 확인
            position = redis_manager.get_position(daily_strategy_id, stock_code)
            if position:
                logger.info(
                    f"  Position: 보유={position.get('holding_quantity')}주, "
                    f"실현손익={position.get('realized_pnl', 0):,.0f}원"
                )
        else:
            logger.error(f"  ✗ 시장가 매도 실패: {result.get('error')}")

    if total_count > 0:
        logger.info(f"\n시장가 매도 결과: {success_count}/{total_count} 성공")
        return success_count == total_count
    else:
        logger.info("매도할 포지션이 없습니다")
        return True


async def test_6_check_final_state():
    """Step 6: 최종 상태 확인"""
    logger.info("=" * 60)
    logger.info("Step 6: 최종 상태 확인")
    logger.info("=" * 60)

    redis_manager = get_redis_manager()
    strategy_table = get_strategy_table()

    for user_strategy_id in strategy_table.get_all_strategies():
        daily_strategy_id = redis_manager.get_daily_strategy_id(user_strategy_id)

        logger.info(f"\n전략 {user_strategy_id} (daily_strategy_id={daily_strategy_id}):")

        # Position
        if daily_strategy_id:
            positions = redis_manager.get_all_positions(daily_strategy_id)
            logger.info(f"  Position 수: {len(positions)}")

            total_realized_pnl = 0
            for pos in positions:
                realized_pnl = pos.get('realized_pnl', 0)
                total_realized_pnl += realized_pnl
                logger.info(
                    f"    {pos.get('stock_code')}: "
                    f"보유={pos.get('holding_quantity')}주, "
                    f"매수총량={pos.get('total_buy_quantity')}주, "
                    f"매도총량={pos.get('total_sell_quantity')}주, "
                    f"실현손익={realized_pnl:,.0f}원"
                )

            logger.info(f"\n  총 실현손익: {total_realized_pnl:,.0f}원")

    return True


# =============================================================================
# 단순화된 테스트 (전략 초기화 없이 직접 주문만 테스트)
# =============================================================================

async def simple_order_test():
    """
    간단한 주문 테스트

    전략 테이블/예측 처리 없이 직접 OrderAPI로 주문만 테스트합니다.
    Redis에 전략 설정을 수동으로 저장한 후 주문을 실행합니다.
    """
    logger.info("=" * 60)
    logger.info("간단한 주문 테스트 (직접 OrderAPI 호출)")
    logger.info("=" * 60)

    redis_manager = get_redis_manager()
    order_api = get_order_api()

    user_strategy_id = 999  # 테스트용 전략 ID

    # 1. 전략 설정을 수동으로 Redis에 저장
    strategy_config = {
        "user_strategy_id": user_strategy_id,
        "user_id": 100,
        "is_mock": True,
        "account_type": "mock",  # mock 모드 (즉시 체결)
        "total_investment": 1000000,
    }
    redis_manager.save_strategy_config(user_strategy_id, strategy_config)
    logger.info(f"전략 설정 저장: user_strategy_id={user_strategy_id}")

    # 2. 매수 주문
    logger.info("\n--- 매수 주문 ---")
    buy_signal = create_buy_signal("005930", 70000)
    buy_result = await order_api.process_buy_order(
        user_strategy_id=user_strategy_id,
        signal=buy_signal,
        order_quantity=10,
        stock_name="삼성전자"
    )

    if buy_result.get("success"):
        daily_strategy_id = buy_result.get("daily_strategy_id")
        logger.info(f"✓ 매수 체결: daily_strategy_id={daily_strategy_id}")

        position = redis_manager.get_position(daily_strategy_id, "005930")
        logger.info(f"  Position: {json.dumps(position, ensure_ascii=False, indent=2)}")
    else:
        logger.error(f"✗ 매수 실패: {buy_result.get('error')}")
        return False

    # 3. 매도 주문
    logger.info("\n--- 매도 주문 ---")
    sell_signal = create_sell_signal("005930", 72000)  # 2000원 이익
    sell_result = await order_api.process_sell_order(
        user_strategy_id=user_strategy_id,
        signal=sell_signal,
        order_quantity=10,
        stock_name="삼성전자"
    )

    if sell_result.get("success"):
        logger.info("✓ 매도 체결")

        position = redis_manager.get_position(daily_strategy_id, "005930")
        logger.info(f"  Position: {json.dumps(position, ensure_ascii=False, indent=2)}")

        # 손익 확인
        realized_pnl = position.get("realized_pnl", 0)
        logger.info(f"\n실현손익: {realized_pnl:,.0f}원")
        logger.info(f"  매수가: 70,000원 x 10주 = 700,000원")
        logger.info(f"  매도가: 72,000원 x 10주 = 720,000원")
        logger.info(f"  예상손익: 20,000원")
    else:
        logger.error(f"✗ 매도 실패: {sell_result.get('error')}")
        return False

    return True


# =============================================================================
# 메인
# =============================================================================

async def main():
    """메인 테스트 실행"""
    import argparse

    parser = argparse.ArgumentParser(description="체결 시뮬레이션 테스트")
    parser.add_argument(
        "--mode",
        choices=["full", "simple"],
        default="full",
        help="테스트 모드: full(전체 흐름), simple(단순 주문만)"
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("체결 시뮬레이션 테스트 시작")
    logger.info(f"모드: {args.mode}")
    logger.info("=" * 60)

    if args.mode == "simple":
        # 간단한 주문 테스트
        success = await simple_order_test()
        logger.info(f"\n테스트 결과: {'PASS' if success else 'FAIL'}")
        return

    # 전체 흐름 테스트
    logger.info("")
    logger.info("테스트 시나리오:")
    logger.info("  1. START 명령 → 전략 테이블 초기화")
    logger.info("  2. Prediction 처리 → 목표가 테이블 생성")
    logger.info("  3. 직접 매수 주문 (3종목) → 즉시 체결")
    logger.info("  4. 삼성전자 지정가 매도 → 즉시 체결")
    logger.info("  5. 나머지 종목 시장가 매도 → 즉시 체결")
    logger.info("  6. 최종 상태 확인")
    logger.info("")

    # Kafka producer 시작
    logger.info("Kafka producer 시작 중...")
    daily_strategy_producer = get_daily_strategy_producer()
    order_signal_producer = get_order_signal_producer()

    try:
        await daily_strategy_producer.start()
        await order_signal_producer.start()
        logger.info("Kafka producer 시작 완료")
    except Exception as e:
        logger.warning(f"Kafka 연결 실패 (무시하고 계속): {e}")

    results = {}

    try:
        # Step 1: START 명령
        results["1_start"] = await test_1_start_command()

        # Step 2: 예측 데이터 처리
        results["2_prediction"] = await test_2_prediction()

        # Step 3: 직접 매수 주문
        results["3_buy_orders"] = await test_3_direct_buy_order()

        # Step 4: 삼성전자 매도
        results["4_sell_samsung"] = await test_4_direct_sell_order()

        # Step 5: 나머지 시장가 매도
        results["5_market_sell"] = await test_5_market_sell_remaining()

        # Step 6: 최종 상태
        results["6_final_state"] = await test_6_check_final_state()

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
    try:
        logger.info("Kafka producer 종료 중...")
        await daily_strategy_producer.stop()
        await order_signal_producer.stop()
        logger.info("Kafka producer 종료 완료")
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
