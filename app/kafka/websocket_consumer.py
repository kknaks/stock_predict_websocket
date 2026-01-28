"""
Kafka Consumer for KIS WebSocket Messages
"""

import asyncio
import json
import logging
from typing import Callable, Optional, List

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from app.config.kafka_connections import get_kafka_config
from app.models.websocket import WebSocketCommand

logger = logging.getLogger(__name__)


class KafkaWebSocketConsumer:
    """KIS 웹소켓 메시지를 수신하는 Kafka Consumer"""

    def __init__(self):
        self._config = get_kafka_config()
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._handlers: List[Callable[[WebSocketCommand], None]] = []
        self._topic = "kis_websocket_commands"  # 웹소켓 메시지 토픽

    def add_handler(self, handler: Callable[[WebSocketCommand], None]) -> None:
        """메시지 수신 시 호출될 핸들러 등록"""
        self._handlers.append(handler)
        logger.info(f"WebSocket handler registered: {handler.__name__}")

    async def start(self) -> bool:
        """Consumer 시작 및 연결 확인"""
        try:
            self._consumer = AIOKafkaConsumer(
                self._topic,
                bootstrap_servers=self._config.bootstrap_servers_list,
                group_id=f"{self._config.kafka_group_id}-websocket",
                auto_offset_reset=self._config.kafka_auto_offset_reset,
                enable_auto_commit=self._config.kafka_enable_auto_commit,
                value_deserializer=lambda m: m.decode('utf-8'),
            )

            await self._consumer.start()
            self._running = True
            logger.info(
                f"Kafka websocket consumer started. "
                f"Topic: {self._topic}, "
                f"Servers: {self._config.bootstrap_servers}"
            )
            return True

        except KafkaConnectionError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error starting websocket consumer: {e}")
            return False

    async def stop(self) -> None:
        """Consumer 중지"""
        self._running = False
        if self._consumer:
            await self._consumer.stop()
            logger.info("Kafka websocket consumer stopped")

    async def consume(self) -> None:
        """메시지 수신 루프"""
        if not self._consumer:
            logger.error("WebSocket consumer not started")
            return

        logger.info("Starting websocket consumption loop...")

        try:
            async for msg in self._consumer:
                if not self._running:
                    break

                try:
                    # JSON 파싱
                    data = json.loads(msg.value)
                    websocket_msg = WebSocketCommand(**data)

                    logger.info(
                        f"Received websocket message: {websocket_msg.command} "
                        f"target={websocket_msg.target} "
                        f"at {websocket_msg.timestamp}"
                    )

                    # 등록된 핸들러들 호출
                    for handler in self._handlers:
                        try:
                            if asyncio.iscoroutinefunction(handler):
                                await handler(websocket_msg)
                            else:
                                handler(websocket_msg)
                        except Exception as e:
                            logger.error(f"WebSocket handler error: {e}", exc_info=True)

                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}, message: {msg.value}")
                except Exception as e:
                    logger.error(f"WebSocket message processing error: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"WebSocket consumer loop error: {e}", exc_info=True)
        finally:
            logger.info("WebSocket consumption loop ended")

    async def check_connection(self) -> bool:
        """Kafka 연결 상태 확인"""
        if not self._consumer:
            return False

        try:
            # 메타데이터 요청으로 연결 확인
            await self._consumer.topics()
            return True
        except Exception as e:
            logger.error(f"WebSocket consumer connection check failed: {e}")
            return False


# 싱글톤 인스턴스
_websocket_consumer_instance: Optional[KafkaWebSocketConsumer] = None


def get_websocket_consumer() -> KafkaWebSocketConsumer:
    """Kafka WebSocket Consumer 싱글톤 인스턴스 반환"""
    global _websocket_consumer_instance
    if _websocket_consumer_instance is None:
        _websocket_consumer_instance = KafkaWebSocketConsumer()
    return _websocket_consumer_instance
