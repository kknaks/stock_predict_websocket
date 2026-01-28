"""
Mock 체결 테스트 서버

웹소켓 가격 정보 없이 Kafka 메시지를 수신하여 체결을 시뮬레이션합니다.

동작:
1. Kafka에서 START/CLOSING 메시지 수신 → 전략 초기화/마감
2. Kafka에서 Prediction 메시지 수신 → 목표가 테이블 생성 → 즉시 매수 체결
3. 매수 완료 후 일정 시간 뒤 매도 체결 시뮬레이션
4. 모든 체결 결과를 Kafka로 발행

실행:
    python test/mock_server.py

필요 사항:
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
import signal
from pathlib import Path
from datetime import datetime
from typing import Optional

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

# settings 캐시 초기화 (환경 변수 변경 반영)
from app.config.settings import get_settings
get_settings.cache_clear()

from app.kafka import get_predict_consumer, get_websocket_consumer
from app.kafka.daily_strategy_producer import get_daily_strategy_producer
from app.kafka.order_signal_producer import get_order_signal_producer
from app.handler.prediction_handler import get_prediction_handler
from app.kis.websocket.redis_manager import get_redis_manager
from app.kis.api.order_api import get_order_api
from app.service.strategy_table import get_strategy_table
from app.service.calculate_slippage import SignalResult, OrderType
from app.models.websocket import WebSocketCommand

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockWebSocketHandler:
    """
    Mock 웹소켓 핸들러

    실제 웹소켓 연결 없이 전략 초기화만 수행합니다.
    """

    def __init__(self):
        self._redis_manager = get_redis_manager()
        self._order_api = get_order_api()
        self._strategy_table = get_strategy_table()

    async def handle_command(self, websocket_msg: WebSocketCommand) -> None:
        """웹소켓 메시지 처리"""
        try:
            if websocket_msg.command == "START":
                await self._handle_start_command(websocket_msg)
            elif websocket_msg.command == "CLOSING":
                await self._handle_closing_command(websocket_msg)
            elif websocket_msg.command == "STOP":
                await self._handle_stop_command(websocket_msg)
            else:
                logger.warning(f"Unknown command: {websocket_msg.command}")

        except Exception as e:
            logger.error(f"Error handling command {websocket_msg.command}: {e}", exc_info=True)

    async def _handle_start_command(self, websocket_msg: WebSocketCommand) -> None:
        """START 명령 처리 (웹소켓 연결 스킵)"""
        try:
            start_command = websocket_msg.to_start_command()
            logger.info(
                f"[MOCK] Processing START: "
                f"stocks={len(start_command.config.stocks)}, "
                f"strategies={len(start_command.config.strategies) if start_command.config.strategies else 0}"
            )

            # 전략 테이블 초기화
            all_strategies = []

            if start_command.config.strategies:
                for strategy in start_command.config.strategies:
                    strategy_dict = strategy.copy() if isinstance(strategy, dict) else dict(strategy)
                    strategy_dict["is_mock"] = True  # 항상 mock 모드
                    strategy_dict["account_type"] = "mock"
                    all_strategies.append(strategy_dict)

            if start_command.config.users:
                for user in start_command.config.users:
                    if user.strategies:
                        for strategy in user.strategies:
                            strategy_dict = strategy.copy() if isinstance(strategy, dict) else dict(strategy)
                            strategy_dict["user_id"] = user.user_id
                            strategy_dict["is_mock"] = True
                            strategy_dict["account_type"] = "mock"
                            all_strategies.append(strategy_dict)

            if all_strategies:
                await self._strategy_table.initialize_from_start_command(all_strategies)
                logger.info(f"[MOCK] Strategy table initialized: {len(all_strategies)} strategies")

                # daily_strategy_id 생성
                for strategy in all_strategies:
                    user_strategy_id = strategy.get("user_strategy_id")
                    if user_strategy_id:
                        existing_id = self._redis_manager.get_daily_strategy_id(user_strategy_id)
                        if not existing_id:
                            daily_strategy_id = self._redis_manager.generate_daily_strategy_id()
                            self._redis_manager.save_daily_strategy(
                                daily_strategy_id=daily_strategy_id,
                                user_strategy_id=user_strategy_id,
                                data={
                                    "is_mock": True,
                                    "account_type": "mock",
                                    "user_id": strategy.get("user_id"),
                                }
                            )
                            logger.info(
                                f"[MOCK] Created daily_strategy_id={daily_strategy_id} "
                                f"for user_strategy_id={user_strategy_id}"
                            )
                        else:
                            logger.info(f"[MOCK] Using existing daily_strategy_id={existing_id}")

            logger.info("[MOCK] START command processed (WebSocket connection skipped)")

        except Exception as e:
            logger.error(f"[MOCK] Error processing START: {e}", exc_info=True)

    async def _handle_closing_command(self, websocket_msg: WebSocketCommand) -> None:
        """CLOSING 명령 처리"""
        try:
            logger.info("[MOCK] Processing CLOSING command")

            all_strategy_ids = self._strategy_table.get_all_strategies()
            logger.info(f"[MOCK] Closing {len(all_strategy_ids)} strategies")

            for user_strategy_id in all_strategy_ids:
                await self._close_unsold_positions(user_strategy_id)

            self._strategy_table.clear_all_strategies()
            logger.info("[MOCK] CLOSING command processed")

        except Exception as e:
            logger.error(f"[MOCK] Error processing CLOSING: {e}", exc_info=True)

    async def _close_unsold_positions(self, user_strategy_id: int) -> None:
        """미매도 Position 시장가 매도"""
        try:
            daily_strategy_id = self._redis_manager.get_daily_strategy_id(user_strategy_id)
            if not daily_strategy_id:
                return

            positions = self._redis_manager.get_positions_with_holdings(daily_strategy_id)
            if not positions:
                return

            logger.info(f"[MOCK] Closing {len(positions)} positions for strategy {user_strategy_id}")

            for position in positions:
                stock_code = position.get("stock_code")
                holding_qty = position.get("holding_quantity", 0)
                avg_price = position.get("average_price", 0)
                stock_name = position.get("stock_name", stock_code)

                if holding_qty <= 0:
                    continue

                # 시장가 매도 (현재가 = 평균가 * 1.01 가정)
                current_price = avg_price * 1.01

                sell_signal = SignalResult(
                    signal_type="SELL",
                    stock_code=stock_code,
                    current_price=current_price,
                    target_price=current_price,
                    target_quantity=holding_qty,
                    stop_loss_price=0,
                    recommended_order_price=0,
                    recommended_order_type=OrderType.MARKET,
                    expected_slippage_pct=0.0,
                    urgency="CRITICAL",
                    reason="[MOCK] 장마감 시장가 매도"
                )

                result = await self._order_api.process_sell_order(
                    user_strategy_id=user_strategy_id,
                    signal=sell_signal,
                    order_quantity=holding_qty,
                    stock_name=stock_name
                )

                if result.get("success"):
                    logger.info(f"[MOCK] ✓ Sold {stock_code}: {holding_qty}주 @ 시장가")
                else:
                    logger.error(f"[MOCK] ✗ Failed to sell {stock_code}: {result.get('error')}")

        except Exception as e:
            logger.error(f"[MOCK] Error closing positions: {e}", exc_info=True)

    async def _handle_stop_command(self, websocket_msg: WebSocketCommand) -> None:
        """STOP 명령 처리"""
        logger.info("[MOCK] Processing STOP command (no-op)")


class MockPredictionHandler:
    """
    Mock 예측 핸들러

    예측 메시지 수신 시 즉시 매수 체결을 시뮬레이션합니다.
    """

    def __init__(self, auto_sell_delay: float = 5.0):
        """
        Args:
            auto_sell_delay: 매수 후 자동 매도까지 대기 시간 (초). 0이면 자동 매도 안 함.
        """
        self._redis_manager = get_redis_manager()
        self._order_api = get_order_api()
        self._strategy_table = get_strategy_table()
        self._prediction_handler = get_prediction_handler()
        self._auto_sell_delay = auto_sell_delay
        self._pending_sells = []  # (sell_time, user_strategy_id, stock_code, quantity, buy_price)

    async def handle_prediction(self, prediction_msg) -> None:
        """
        예측 메시지 처리

        1. 목표가 테이블 생성 (기존 prediction_handler 사용)
        2. 각 예측 종목에 대해 즉시 매수 체결
        """
        try:
            logger.info(f"[MOCK] Received prediction: {prediction_msg.total_count} stocks")

            # 1. 기존 prediction_handler로 목표가 테이블 생성
            await self._prediction_handler.handle_prediction(prediction_msg)

            # 2. 각 전략별로 매수 주문 실행
            all_strategies = self._strategy_table.get_all_strategies()

            for user_strategy_id in all_strategies:
                await self._execute_buy_orders(user_strategy_id, prediction_msg)

        except Exception as e:
            logger.error(f"[MOCK] Error handling prediction: {e}", exc_info=True)

    async def _execute_buy_orders(self, user_strategy_id: int, prediction_msg) -> None:
        """예측 종목들에 대해 매수 주문 실행"""
        try:
            # 목표가 테이블에서 수량 정보 가져오기
            targets = self._redis_manager.get_strategy_all_targets(user_strategy_id)
            if not targets:
                logger.warning(f"[MOCK] No targets found for strategy {user_strategy_id}")
                return

            for prediction in prediction_msg.predictions:
                stock_code = prediction.stock_code
                stock_name = prediction.stock_name

                target_data = targets.get(stock_code)
                if not target_data:
                    continue

                buy_price = prediction.stock_open  # 시가로 매수
                buy_quantity = target_data.get("target_quantity", 1)

                if buy_quantity <= 0:
                    continue

                logger.info(
                    f"[MOCK] Executing BUY: {stock_name}({stock_code}) "
                    f"{buy_quantity}주 @ {buy_price:,}원"
                )

                buy_signal = SignalResult(
                    signal_type="BUY",
                    stock_code=stock_code,
                    current_price=buy_price,
                    target_price=buy_price,
                    target_quantity=buy_quantity,
                    stop_loss_price=buy_price * 0.99,  # 1% 손절
                    recommended_order_price=buy_price,
                    recommended_order_type=OrderType.LIMIT,
                    expected_slippage_pct=0.0,
                    urgency="NORMAL",
                    reason=f"[MOCK] 예측 기반 매수 (confidence={prediction.confidence})"
                )

                result = await self._order_api.process_buy_order(
                    user_strategy_id=user_strategy_id,
                    signal=buy_signal,
                    order_quantity=buy_quantity,
                    stock_name=stock_name
                )

                if result.get("success"):
                    logger.info(f"[MOCK] ✓ BUY executed: {stock_code} {buy_quantity}주")

                    # 자동 매도 예약
                    if self._auto_sell_delay > 0:
                        sell_time = asyncio.get_event_loop().time() + self._auto_sell_delay
                        self._pending_sells.append(
                            (sell_time, user_strategy_id, stock_code, stock_name, buy_quantity, buy_price)
                        )
                        logger.info(f"[MOCK] Auto-sell scheduled in {self._auto_sell_delay}s")
                else:
                    logger.error(f"[MOCK] ✗ BUY failed: {result.get('error')}")

        except Exception as e:
            logger.error(f"[MOCK] Error executing buy orders: {e}", exc_info=True)

    async def process_pending_sells(self) -> None:
        """예약된 매도 주문 처리"""
        if not self._pending_sells:
            return

        current_time = asyncio.get_event_loop().time()
        executed = []

        for i, (sell_time, user_strategy_id, stock_code, stock_name, quantity, buy_price) in enumerate(self._pending_sells):
            if current_time >= sell_time:
                # 매도 가격 = 매수 가격 * 1.02 (2% 수익 가정)
                sell_price = buy_price * 1.02

                logger.info(
                    f"[MOCK] Executing auto SELL: {stock_name}({stock_code}) "
                    f"{quantity}주 @ {sell_price:,.0f}원"
                )

                sell_signal = SignalResult(
                    signal_type="SELL",
                    stock_code=stock_code,
                    current_price=sell_price,
                    target_price=sell_price,
                    target_quantity=quantity,
                    stop_loss_price=0,
                    recommended_order_price=sell_price,
                    recommended_order_type=OrderType.LIMIT,
                    expected_slippage_pct=0.0,
                    urgency="NORMAL",
                    reason="[MOCK] 자동 익절 매도"
                )

                result = await self._order_api.process_sell_order(
                    user_strategy_id=user_strategy_id,
                    signal=sell_signal,
                    order_quantity=quantity,
                    stock_name=stock_name
                )

                if result.get("success"):
                    profit = (sell_price - buy_price) * quantity
                    logger.info(f"[MOCK] ✓ SELL executed: {stock_code}, profit={profit:,.0f}원")
                else:
                    logger.error(f"[MOCK] ✗ SELL failed: {result.get('error')}")

                executed.append(i)

        # 실행된 항목 제거 (역순으로)
        for i in reversed(executed):
            self._pending_sells.pop(i)


# 전역 변수
shutdown_event = asyncio.Event()


def signal_handler(sig, frame):
    """시그널 핸들러"""
    logger.info(f"Received signal {sig}, shutting down...")
    shutdown_event.set()


async def main():
    """메인 실행 함수"""
    import argparse

    parser = argparse.ArgumentParser(description="Mock 체결 테스트 서버")
    parser.add_argument(
        "--auto-sell-delay",
        type=float,
        default=0,
        help="매수 후 자동 매도까지 대기 시간 (초). 0이면 자동 매도 안 함. (기본값: 0)"
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Mock 체결 테스트 서버 시작")
    logger.info("=" * 60)
    logger.info("")
    logger.info("설정:")
    logger.info(f"  - 자동 매도 딜레이: {args.auto_sell_delay}초")
    logger.info("")
    logger.info("대기 중인 Kafka 토픽:")
    logger.info("  - kis_websocket_commands (START/CLOSING)")
    logger.info("  - predict_result (Prediction)")
    logger.info("")
    logger.info("발행하는 Kafka 토픽:")
    logger.info("  - order_signal (체결 결과)")
    logger.info("  - daily_strategy (전략 정보)")
    logger.info("")

    # 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Kafka producers 시작
    daily_strategy_producer = get_daily_strategy_producer()
    order_signal_producer = get_order_signal_producer()

    await daily_strategy_producer.start()
    await order_signal_producer.start()
    logger.info("Kafka producers started")

    # Kafka consumers 설정
    websocket_consumer = get_websocket_consumer()
    predict_consumer = get_predict_consumer()

    # Mock 핸들러 생성
    mock_ws_handler = MockWebSocketHandler()
    mock_pred_handler = MockPredictionHandler(auto_sell_delay=args.auto_sell_delay)

    # 핸들러 등록
    websocket_consumer.add_handler(mock_ws_handler.handle_command)
    predict_consumer.add_handler(mock_pred_handler.handle_prediction)

    # Consumers 시작
    ws_connected = await websocket_consumer.start()
    pred_connected = await predict_consumer.start()

    if not ws_connected:
        logger.error("Failed to connect websocket consumer")
        return

    if not pred_connected:
        logger.error("Failed to connect predict consumer")
        return

    logger.info("Kafka consumers started")
    logger.info("")
    logger.info("서버 준비 완료. Kafka 메시지 대기 중...")
    logger.info("종료하려면 Ctrl+C를 누르세요.")
    logger.info("")

    # Consumer 태스크 시작
    ws_task = asyncio.create_task(websocket_consumer.consume())
    pred_task = asyncio.create_task(predict_consumer.consume())

    # 자동 매도 처리 태스크
    async def auto_sell_loop():
        while not shutdown_event.is_set():
            await mock_pred_handler.process_pending_sells()
            await asyncio.sleep(1)

    auto_sell_task = asyncio.create_task(auto_sell_loop())

    # 종료 대기
    try:
        await shutdown_event.wait()
    except asyncio.CancelledError:
        pass

    # 정리
    logger.info("Shutting down...")

    ws_task.cancel()
    pred_task.cancel()
    auto_sell_task.cancel()

    try:
        await ws_task
    except asyncio.CancelledError:
        pass

    try:
        await pred_task
    except asyncio.CancelledError:
        pass

    try:
        await auto_sell_task
    except asyncio.CancelledError:
        pass

    await websocket_consumer.stop()
    await predict_consumer.stop()
    await daily_strategy_producer.stop()
    await order_signal_producer.stop()

    logger.info("Server shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
