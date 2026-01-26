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
            "daily_strategy_id": 32,
            "stock_code": "357780",
            "order_type": "LIMIT" | "MARKET",
            "order_price": 340000,  # 지정가인 경우
            "order_quantity": 5     # 생략 시 전량 매도
        }
        """
        try:
            # 필수 필드 검증
            daily_strategy_id = message.get("daily_strategy_id")
            stock_code = message.get("stock_code")
            order_type_str = message.get("order_type", "LIMIT").upper()
            order_price = message.get("order_price", 0)
            order_quantity = message.get("order_quantity")

            if not daily_strategy_id or not stock_code:
                logger.error(f"Missing required fields: daily_strategy_id={daily_strategy_id}, stock_code={stock_code}")
                return

            # user_strategy_id 조회 (daily_strategy_id로부터)
            user_strategy_id = self._redis_manager.get_user_strategy_id_by_daily(daily_strategy_id)
            if not user_strategy_id:
                logger.error(f"user_strategy_id not found for daily_strategy_id={daily_strategy_id}")
                return

            # 현재가 조회
            current_price = self._redis_manager.get_current_price(stock_code)
            if not current_price:
                logger.warning(f"Current price not found for {stock_code}, using order_price")
                current_price = float(order_price) if order_price else 0

            # 주문 수량 결정 (없으면 전량)
            if not order_quantity:
                position = self._redis_manager.get_position(daily_strategy_id, stock_code)
                if position:
                    order_quantity = position.get("holding_quantity", 0)
                else:
                    logger.error(f"Position not found and order_quantity not specified")
                    return

            # OrderType 변환
            if order_type_str == "MARKET":
                order_type = OrderType.MARKET
                order_price = 0
            else:
                order_type = OrderType.LIMIT
                if not order_price:
                    logger.error("order_price required for LIMIT order")
                    return

            # SignalResult 생성
            signal = SignalResult(
                signal_type="SELL",
                stock_code=stock_code,
                current_price=current_price,
                target_price=None,
                target_quantity=order_quantity,
                stop_loss_price=None,
                recommended_order_price=float(order_price) if order_price else current_price,
                recommended_order_type=order_type,
                expected_slippage_pct=0.0,
                urgency="HIGH",
                reason="사용자 수동 매도"
            )

            # 종목명 조회
            position = self._redis_manager.get_position(daily_strategy_id, stock_code)
            stock_name = position.get("stock_name", "") if position else ""

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

        except Exception as e:
            logger.error(f"Error handling manual sell: {e}", exc_info=True)

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
