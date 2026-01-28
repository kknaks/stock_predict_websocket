"""
Kafka Consumer for AI Prediction Results
"""

import asyncio
import json
import logging
from typing import Callable, Optional, List

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from app.config.kafka_connections import get_kafka_config
from app.models.prediction import PredictionMessage

logger = logging.getLogger(__name__)


class KafkaPredictionConsumer:
    """AI 예측 결과를 수신하는 Kafka Consumer"""

    def __init__(self):
        self._config = get_kafka_config()
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._handlers: List[Callable[[PredictionMessage], None]] = []

    def add_handler(self, handler: Callable[[PredictionMessage], None]) -> None:
        """메시지 수신 시 호출될 핸들러 등록"""
        self._handlers.append(handler)
        logger.info(f"Handler registered: {handler.__name__}")

    async def start(self) -> bool:
        """Consumer 시작 및 연결 확인"""
        try:
            self._consumer = AIOKafkaConsumer(
                self._config.topic_prediction_result,
                bootstrap_servers=self._config.bootstrap_servers_list,
                group_id=f"{self._config.kafka_group_id}-websocket",
                auto_offset_reset=self._config.kafka_auto_offset_reset,
                enable_auto_commit=self._config.kafka_enable_auto_commit,
                value_deserializer=lambda m: m.decode('utf-8'),
            )

            await self._consumer.start()
            self._running = True
            logger.info(
                f"Kafka consumer started. "
                f"Topic: {self._config.topic_prediction_result}, "
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
            logger.info("Kafka consumer stopped")

    async def consume(self) -> None:
        """메시지 수신 루프"""
        if not self._consumer:
            logger.error("Consumer not started")
            return

        logger.info("Starting message consumption loop...")

        try:
            async for msg in self._consumer:
                if not self._running:
                    break

                try:
                    # JSON 파싱
                    data = json.loads(msg.value)
                    prediction_msg = PredictionMessage(**data)

                    logger.info(
                        f"Received prediction: {prediction_msg.total_count} items "
                        f"at {prediction_msg.timestamp}"
                    )

                    # 등록된 핸들러들 호출
                    for handler in self._handlers:
                        try:
                            if asyncio.iscoroutinefunction(handler):
                                await handler(prediction_msg)
                            else:
                                handler(prediction_msg)
                        except Exception as e:
                            logger.error(f"Handler error: {e}")

                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                except Exception as e:
                    logger.error(f"Message processing error: {e}")

        except Exception as e:
            logger.error(f"Consumer loop error: {e}")
        finally:
            logger.info("Message consumption loop ended")

    async def check_connection(self) -> bool:
        """Kafka 연결 상태 확인"""
        if not self._consumer:
            return False

        try:
            # 메타데이터 요청으로 연결 확인
            await self._consumer.topics()
            return True
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False


# 싱글톤 인스턴스
_consumer_instance: Optional[KafkaPredictionConsumer] = None


def get_predict_consumer() -> KafkaPredictionConsumer:
    """Kafka Consumer 싱글톤 인스턴스 반환"""
    global _consumer_instance
    if _consumer_instance is None:
        _consumer_instance = KafkaPredictionConsumer()
    return _consumer_instance
