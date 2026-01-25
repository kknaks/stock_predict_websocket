"""
Kafka Producer for Real-time Price Data

실시간 가격 데이터를 Kafka로 발행하는 Producer
"""

import json
import logging
from datetime import datetime
from typing import Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.config.kafka_connections import get_kafka_config
from app.models.price import PriceMessage

logger = logging.getLogger(__name__)


class PriceProducer:
    """실시간 가격 데이터를 Kafka로 발행하는 Producer"""

    def __init__(self):
        self._config = get_kafka_config()
        self._producer: Optional[AIOKafkaProducer] = None
        self._topic = self._config.topic_price

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
                f"Price producer started. "
                f"Topic: {self._topic}, "
                f"Servers: {self._config.bootstrap_servers}"
            )
            return True

        except KafkaConnectionError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error starting price producer: {e}", exc_info=True)
            return False

    async def stop(self) -> None:
        """Producer 중지"""
        if self._producer:
            await self._producer.stop()
            self._producer = None  # 재시작 가능하도록 None으로 설정
            logger.info("Price producer stopped")

    async def send_price(self, price_message: PriceMessage) -> bool:
        """
        실시간 가격 데이터를 Kafka로 발행

        Args:
            price_message: 가격 메시지

        Returns:
            발행 성공 여부
        """
        if not self._producer:
            logger.error("Price producer not started")
            return False

        try:
            # Pydantic 모델을 dict로 변환
            message_dict = price_message.model_dump(mode='json')

            # Kafka로 발행 (토픽이 없으면 자동 생성됨)
            try:
                await self._producer.send_and_wait(self._topic, message_dict)
                logger.debug(
                    f"Sent price data to Kafka: "
                    f"topic={self._topic}, "
                    f"stock_code={price_message.stock_code}, "
                    f"current_price={price_message.current_price}"
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
                    logger.error(f"Error sending price message: {send_error}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error preparing price message: {e}", exc_info=True)
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
            logger.error(f"Price producer connection check failed: {e}")
            return False


# 싱글톤 인스턴스
_producer_instance: Optional[PriceProducer] = None


def get_price_producer() -> PriceProducer:
    """Price Producer 싱글톤 인스턴스 반환"""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = PriceProducer()
    return _producer_instance
