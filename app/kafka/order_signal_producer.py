"""
Kafka Producer for Order Signal Messages

매도/매수 시그널을 Kafka로 발행하는 Producer
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.config.kafka_connections import get_kafka_config
from app.service.calculate_slippage import SignalResult

logger = logging.getLogger(__name__)


class OrderSignalProducer:
    """주문 시그널을 Kafka로 발행하는 Producer"""

    def __init__(self):
        self._config = get_kafka_config()
        self._producer: Optional[AIOKafkaProducer] = None
        self._topic = "order_signal"

    async def start(self) -> bool:
        """Producer 시작 및 연결 확인"""
        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._config.bootstrap_servers_list,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
            )

            await self._producer.start()
            
            # 토픽 존재 여부 확인 및 메타데이터 요청 (토픽 자동 생성 유도)
            try:
                await self._producer.client.cluster.request_metadata(topics=[self._topic])
                logger.info(f"Topic metadata requested for: {self._topic}")
            except Exception as e:
                logger.warning(f"Could not request metadata for topic {self._topic}: {e}. Topic will be auto-created on first send.")
            
            logger.info(
                f"Order signal producer started. "
                f"Topic: {self._topic}, "
                f"Servers: {self._config.bootstrap_servers}"
            )
            return True

        except KafkaConnectionError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error starting order signal producer: {e}", exc_info=True)
            return False

    async def stop(self) -> None:
        """Producer 중지"""
        if self._producer:
            await self._producer.stop()
            logger.info("Order signal producer stopped")

    async def send_signal(self, user_strategy_id: int, signal: SignalResult) -> bool:
        """
        주문 시그널을 Kafka로 발행

        Args:
            user_strategy_id: 사용자 전략 ID
            signal: 시그널 결과

        Returns:
            발행 성공 여부
        """
        if not self._producer:
            logger.error("Order signal producer not started")
            return False

        try:
            # 메시지 구성
            message = {
                "timestamp": datetime.now().isoformat(),
                "user_strategy_id": user_strategy_id,
                "signal_type": signal.signal_type,
                "stock_code": signal.stock_code,
                "current_price": signal.current_price,
                "target_price": signal.target_price,
                "target_quantity": signal.target_quantity,
                "stop_loss_price": signal.stop_loss_price,
                "recommended_order_price": signal.recommended_order_price,
                "recommended_order_type": signal.recommended_order_type.value,
                "expected_slippage_pct": signal.expected_slippage_pct,
                "urgency": signal.urgency,
                "reason": signal.reason,
            }

            # Kafka로 발행 (토픽이 없으면 자동 생성됨)
            try:
                await self._producer.send_and_wait(self._topic, message)
                logger.info(
                    f"Sent order signal to Kafka: "
                    f"topic={self._topic}, "
                    f"user_strategy_id={user_strategy_id}, "
                    f"signal_type={signal.signal_type}, "
                    f"stock_code={signal.stock_code}"
                )
                return True
            except Exception as send_error:
                # 토픽 관련 에러인 경우 더 자세한 로그
                error_msg = str(send_error)
                if "UnknownTopicOrPartitionException" in error_msg or "UNKNOWN_TOPIC_OR_PARTITION" in error_msg:
                    logger.error(
                        f"Topic {self._topic} does not exist and auto-creation failed. "
                        f"Please create the topic manually or check Kafka/Redpanda auto-creation settings. "
                        f"Error: {send_error}"
                    )
                else:
                    logger.error(f"Error sending order signal message: {send_error}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error preparing order signal message: {e}", exc_info=True)
            return False

    async def send_order_result(self, order_result: Dict[str, Any]) -> bool:
        """
        주문 결과를 Kafka로 발행 (주문번호 포함)

        Args:
            order_result: 주문 결과 딕셔너리
                - timestamp: 주문 시각
                - user_strategy_id: 사용자 전략 ID
                - order_type: 주문 유형 (BUY/SELL)
                - stock_code: 종목 코드
                - order_no: 주문번호 (필수)
                - order_quantity: 주문 수량
                - order_price: 주문 가격
                - order_dvsn: 주문구분
                - account_no: 계좌번호
                - is_mock: Mock 모드 여부
                - status: 주문 상태 (ordered/executed)
                - executed_quantity: 체결 수량
                - executed_price: 체결 가격

        Returns:
            발행 성공 여부
        """
        if not self._producer:
            logger.error("Order signal producer not started")
            return False

        try:
            # Kafka로 발행
            try:
                await self._producer.send_and_wait(self._topic, order_result)
                logger.info(
                    f"Sent order result to Kafka: "
                    f"topic={self._topic}, "
                    f"user_strategy_id={order_result.get('user_strategy_id')}, "
                    f"order_type={order_result.get('order_type')}, "
                    f"stock_code={order_result.get('stock_code')}, "
                    f"order_no={order_result.get('order_no')}, "
                    f"status={order_result.get('status')}"
                )
                return True
            except Exception as send_error:
                error_msg = str(send_error)
                if "UnknownTopicOrPartitionException" in error_msg or "UNKNOWN_TOPIC_OR_PARTITION" in error_msg:
                    logger.error(
                        f"Topic {self._topic} does not exist and auto-creation failed. "
                        f"Please create the topic manually or check Kafka/Redpanda auto-creation settings. "
                        f"Error: {send_error}"
                    )
                else:
                    logger.error(f"Error sending order result message: {send_error}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error preparing order result message: {e}", exc_info=True)
            return False

    async def check_connection(self) -> bool:
        """Kafka 연결 상태 확인"""
        if not self._producer:
            return False

        try:
            # 메타데이터 요청으로 연결 확인
            await self._producer.client.cluster.request_metadata()
            return True
        except Exception as e:
            logger.error(f"Order signal producer connection check failed: {e}")
            return False


# 싱글톤 인스턴스
_producer_instance: Optional[OrderSignalProducer] = None


def get_order_signal_producer() -> OrderSignalProducer:
    """Order Signal Producer 싱글톤 인스턴스 반환"""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = OrderSignalProducer()
    return _producer_instance
