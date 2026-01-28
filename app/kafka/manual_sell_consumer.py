"""
Kafka Consumer for Manual Sell Signal

사용자의 수동 매도 요청을 처리합니다.
"""

import asyncio
import json
import logging
from typing import Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from app.config.kafka_connections import get_kafka_config
from app.kis.api.order_api import OrderAPI
from app.kis.websocket.redis_manager import get_redis_manager
from app.service.calculate_slippage import SignalResult, OrderType

logger = logging.getLogger(__name__)


class KafkaManualSellConsumer:
    """수동 매도 시그널을 수신하는 Kafka Consumer"""

    def __init__(self):
        self._config = get_kafka_config()
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._order_api: Optional[OrderAPI] = None
        self._redis_manager = get_redis_manager()

    async def start(self) -> bool:
        """Consumer 시작 및 연결 확인"""
        try:
            self._consumer = AIOKafkaConsumer(
                self._config.topic_manual_sell,
                bootstrap_servers=self._config.bootstrap_servers_list,
                group_id=f"{self._config.kafka_group_id}-manual-sell",
                auto_offset_reset=self._config.kafka_auto_offset_reset,
                enable_auto_commit=self._config.kafka_enable_auto_commit,
                value_deserializer=lambda m: m.decode('utf-8'),
            )

            await self._consumer.start()
            self._running = True

            # OrderAPI 초기화
            self._order_api = OrderAPI()

            logger.info(
                f"Manual sell consumer started. "
                f"Topic: {self._config.topic_manual_sell}, "
                f"Servers: {self._config.bootstrap_servers}"
            )
            return True

        except KafkaConnectionError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error starting consumer: {e}")
            return False

    async def stop(self) -> None:
        """Consumer 중지"""
        self._running = False
        if self._consumer:
            await self._consumer.stop()
            logger.info("Manual sell consumer stopped")

    async def consume(self) -> None:
        """메시지 수신 루프"""
        if not self._consumer:
            logger.error("Consumer not started")
            return

        logger.info("Starting manual sell consumption loop...")

        try:
            async for msg in self._consumer:
                if not self._running:
                    break

                try:
                    data = json.loads(msg.value)
                    logger.info(f"Received manual sell signal: {data}")
                    await self._handle_manual_sell(data)

                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                except Exception as e:
                    logger.error(f"Message processing error: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"Consumer loop error: {e}")
        finally:
            logger.info("Manual sell consumption loop ended")

    async def _handle_manual_sell(self, message: dict) -> None:
        """
        수동 매도 메시지 처리

        메시지 형식:
        {
            "user_strategy_id": 123,
            "stock_code": "357780",
            "order_type": "LIMIT" | "MARKET",
            "order_price": 340000,  # 지정가인 경우
            "order_quantity": 5     # 생략 시 전량 매도
        }

        처리 방식:
        - mock: Redis에 수동 매도 타겟 저장 → WebSocket에서 가격 감시 후 주문
        - real/paper: 현재가 검증 후 바로 API 호출
        """
        try:
            # 필수 필드 검증
            user_strategy_id = message.get("user_strategy_id")
            stock_code = message.get("stock_code")
            order_type_str = message.get("order_type", "LIMIT").upper()
            order_price = message.get("order_price", 0)
            order_quantity = message.get("order_quantity")

            if not user_strategy_id or not stock_code:
                logger.error(f"Missing required fields: user_strategy_id={user_strategy_id}, stock_code={stock_code}")
                return

            # daily_strategy_id 조회 (user_strategy_id로부터)
            daily_strategy_id = self._redis_manager.get_daily_strategy_id(user_strategy_id)
            if not daily_strategy_id:
                logger.error(f"daily_strategy_id not found for user_strategy_id={user_strategy_id}")
                return

            # daily_strategy에서 account_type 조회
            daily_strategy = self._redis_manager.get_daily_strategy(daily_strategy_id)
            if not daily_strategy:
                logger.error(f"daily_strategy not found: daily_strategy_id={daily_strategy_id}")
                return

            account_type = daily_strategy.get("account_type", "mock")

            # 주문 수량 결정 (없으면 전량)
            if not order_quantity:
                position = self._redis_manager.get_position(daily_strategy_id, stock_code)
                if position:
                    order_quantity = position.get("holding_quantity", 0)
                else:
                    logger.error(f"Position not found and order_quantity not specified")
                    return

            if order_quantity <= 0:
                logger.error(f"Invalid order_quantity: {order_quantity}")
                return

            # 지정가인 경우 가격 필수
            if order_type_str == "LIMIT" and not order_price:
                logger.error("order_price required for LIMIT order")
                return

            # account_type에 따라 분기
            if account_type == "mock":
                # Mock 모드: Redis에 수동 매도 타겟 저장 → WebSocket에서 가격 감시
                await self._handle_mock_manual_sell(
                    user_strategy_id=user_strategy_id,
                    stock_code=stock_code,
                    order_type_str=order_type_str,
                    order_price=order_price,
                    order_quantity=order_quantity,
                )
            else:
                # Real/Paper 모드: 현재가 검증 후 바로 API 호출
                await self._handle_real_manual_sell(
                    user_strategy_id=user_strategy_id,
                    daily_strategy_id=daily_strategy_id,
                    stock_code=stock_code,
                    order_type_str=order_type_str,
                    order_price=order_price,
                    order_quantity=order_quantity,
                )

        except Exception as e:
            logger.error(f"Error handling manual sell: {e}", exc_info=True)

    async def _handle_mock_manual_sell(
        self,
        user_strategy_id: int,
        stock_code: str,
        order_type_str: str,
        order_price: float,
        order_quantity: int,
    ) -> None:
        """
        Mock 모드 수동 매도 처리

        Redis에 수동 매도 타겟 저장 → WebSocket에서 가격 감시 후 주문
        """
        # Redis에 수동 매도 타겟 저장
        success = self._redis_manager.save_manual_sell_target(
            user_strategy_id=user_strategy_id,
            stock_code=stock_code,
            order_type=order_type_str,
            order_price=order_price,
            order_quantity=order_quantity,
        )

        if success:
            logger.info(
                f"[MOCK] Manual sell target registered: "
                f"user_strategy_id={user_strategy_id}, "
                f"stock_code={stock_code}, "
                f"order_type={order_type_str}, "
                f"order_price={order_price}, "
                f"order_quantity={order_quantity} "
                f"(waiting for price condition)"
            )
        else:
            logger.error(f"[MOCK] Failed to save manual sell target")

    async def _handle_real_manual_sell(
        self,
        user_strategy_id: int,
        daily_strategy_id: int,
        stock_code: str,
        order_type_str: str,
        order_price: float,
        order_quantity: int,
    ) -> None:
        """
        Real/Paper 모드 수동 매도 처리

        현재가 검증 후 바로 API 호출
        """
        # 현재가 조회
        current_price = self._redis_manager.get_current_price(stock_code)

        # 지정가 주문인 경우 현재가와 비교 (선택적 검증)
        if order_type_str == "LIMIT" and current_price:
            if order_price > current_price:
                logger.warning(
                    f"LIMIT sell price ({order_price}) is higher than current price ({current_price}). "
                    f"Order may not be executed immediately."
                )

        # OrderType 변환
        if order_type_str == "MARKET":
            order_type = OrderType.MARKET
            recommended_price = current_price if current_price else 0
        else:
            order_type = OrderType.LIMIT
            recommended_price = order_price

        # 종목명 조회
        position = self._redis_manager.get_position(daily_strategy_id, stock_code)
        stock_name = position.get("stock_name", "") if position else ""

        # SignalResult 생성
        signal = SignalResult(
            signal_type="SELL",
            stock_code=stock_code,
            current_price=current_price if current_price else 0,
            target_price=None,
            target_quantity=order_quantity,
            stop_loss_price=None,
            recommended_order_price=recommended_price,
            recommended_order_type=order_type,
            expected_slippage_pct=0.0,
            urgency="HIGH",
            reason="사용자 수동 매도"
        )

        # 매도 주문 처리
        result = await self._order_api.process_sell_order(
            user_strategy_id=user_strategy_id,
            signal=signal,
            order_quantity=order_quantity,
            stock_name=stock_name
        )

        if result.get("success"):
            logger.info(
                f"Manual sell order processed: "
                f"stock_code={stock_code}, "
                f"quantity={order_quantity}, "
                f"order_type={order_type_str}, "
                f"price={order_price}"
            )
        else:
            logger.error(f"Manual sell order failed: {result.get('error')}")

    async def check_connection(self) -> bool:
        """Kafka 연결 상태 확인"""
        if not self._consumer:
            return False

        try:
            await self._consumer.topics()
            return True
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False


# 싱글톤 인스턴스
_consumer_instance: Optional[KafkaManualSellConsumer] = None


def get_manual_sell_consumer() -> KafkaManualSellConsumer:
    """Manual Sell Consumer 싱글톤 인스턴스 반환"""
    global _consumer_instance
    if _consumer_instance is None:
        _consumer_instance = KafkaManualSellConsumer()
    return _consumer_instance
