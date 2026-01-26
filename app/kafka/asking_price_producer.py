"""
Kafka Producer for Real-time Asking Price Data

실시간 호가 데이터를 Kafka로 발행하는 Producer
"""

import json
import logging
from typing import Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.config.kafka_connections import get_kafka_config
from app.models.asking_price import AskingPriceMessage

logger = logging.getLogger(__name__)


class AskingPriceProducer:
    """실시간 호가 데이터를 Kafka로 발행하는 Producer"""

    def __init__(self):
        self._config = get_kafka_config()
        self._producer: Optional[AIOKafkaProducer] = None
        self._topic = self._config.topic_asking_price

    async def start(self) -> bool:
        """Producer 시작 및 연결 확인"""
        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._config.bootstrap_servers_list,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
            )

            await self._producer.start()

            # 토픽 존재 여부 확인 및 메타데이터 요청
            try:
                await self._producer.client.cluster.request_metadata(topics=[self._topic])
                logger.info(f"Topic metadata requested for: {self._topic}")
            except Exception as e:
                logger.warning(f"Could not request metadata for topic {self._topic}: {e}")

            logger.info(
                f"Asking price producer started. "
                f"Topic: {self._topic}, "
                f"Servers: {self._config.bootstrap_servers}"
            )
            return True

        except KafkaConnectionError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error starting asking price producer: {e}", exc_info=True)
            return False

    async def stop(self) -> None:
        """Producer 중지"""
        if self._producer:
            await self._producer.stop()
            self._producer = None
            logger.info("Asking price producer stopped")

    async def send_asking_price(self, asking_price_message: AskingPriceMessage) -> bool:
        """
        실시간 호가 데이터를 Kafka로 발행

        Args:
            asking_price_message: 호가 메시지

        Returns:
            발행 성공 여부
        """
        if not self._producer:
            logger.error("Asking price producer not started")
            return False

        try:
            message_dict = asking_price_message.model_dump(mode='json')

            try:
                await self._producer.send_and_wait(self._topic, message_dict)
                logger.debug(
                    f"Sent asking price data to Kafka: "
                    f"topic={self._topic}, "
                    f"stock_code={asking_price_message.stock_code}, "
                    f"askp1={asking_price_message.askp1}, "
                    f"bidp1={asking_price_message.bidp1}"
                )
                return True
            except Exception as send_error:
                error_msg = str(send_error)
                if "UnknownTopicOrPartitionException" in error_msg or "UNKNOWN_TOPIC_OR_PARTITION" in error_msg:
                    logger.error(
                        f"Topic {self._topic} does not exist and auto-creation failed. "
                        f"Please create the topic manually. Error: {send_error}"
                    )
                else:
                    logger.error(f"Error sending asking price message: {send_error}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error preparing asking price message: {e}", exc_info=True)
            return False

    async def check_connection(self) -> bool:
        """Kafka 연결 상태 확인"""
        if not self._producer:
            return False

        try:
            await self._producer.client.cluster.request_metadata()
            return True
        except Exception as e:
            logger.error(f"Asking price producer connection check failed: {e}")
            return False


# 싱글톤 인스턴스
_producer_instance: Optional[AskingPriceProducer] = None


def get_asking_price_producer() -> AskingPriceProducer:
    """Asking Price Producer 싱글톤 인스턴스 반환"""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = AskingPriceProducer()
    return _producer_instance
