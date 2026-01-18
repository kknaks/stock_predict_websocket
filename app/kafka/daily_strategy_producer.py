"""
Kafka Producer for Daily Strategy Messages
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.config.kafka_connections import get_kafka_config

logger = logging.getLogger(__name__)


class DailyStrategyProducer:
    """일일 전략 정보를 Kafka로 발행하는 Producer"""

    def __init__(self):
        self._config = get_kafka_config()
        self._producer: Optional[AIOKafkaProducer] = None
        self._topic = "daily_strategy"

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
                f"Daily strategy producer started. "
                f"Topic: {self._topic}, "
                f"Servers: {self._config.bootstrap_servers}"
            )
            return True

        except KafkaConnectionError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error starting daily strategy producer: {e}", exc_info=True)
            return False

    async def stop(self) -> None:
        """Producer 중지"""
        if self._producer:
            await self._producer.stop()
            logger.info("Daily strategy producer stopped")

    async def send_strategies(self, strategies: List[Dict]) -> bool:
        """
        유저별 전략 정보를 Kafka로 발행

        Args:
            strategies: 전략 리스트 (각 전략은 user_id를 포함해야 함)

        Returns:
            발행 성공 여부
        """
        if not self._producer:
            logger.error("Daily strategy producer not started")
            return False

        if not strategies:
            logger.warning("No strategies to send, skipping")
            return False

        try:
            # 유저별로 전략 그룹화
            strategies_by_user: Dict[int, List[Dict]] = defaultdict(list)
            for strategy in strategies:
                user_id = strategy.get("user_id")
                if user_id is None:
                    logger.warning(f"Strategy missing user_id: {strategy}")
                    continue
                strategies_by_user[user_id].append(strategy)

            # 메시지 구성
            message = {
                "timestamp": datetime.now().isoformat(),
                "strategies_by_user": [
                    {
                        "user_id": user_id,
                        "strategies": user_strategies
                    }
                    for user_id, user_strategies in strategies_by_user.items()
                ]
            }

            # Kafka로 발행 (토픽이 없으면 자동 생성됨)
            try:
                await self._producer.send_and_wait(self._topic, message)
                logger.info(
                    f"Sent daily strategy message to Kafka: "
                    f"topic={self._topic}, "
                    f"users={len(strategies_by_user)}, "
                    f"total_strategies={len(strategies)}"
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
                    logger.error(f"Error sending daily strategy message: {send_error}", exc_info=True)
                return False

        except Exception as e:
            logger.error(f"Error preparing daily strategy message: {e}", exc_info=True)
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
            logger.error(f"Daily strategy producer connection check failed: {e}")
            return False


# 싱글톤 인스턴스
_producer_instance: Optional[DailyStrategyProducer] = None


def get_daily_strategy_producer() -> DailyStrategyProducer:
    """Daily Strategy Producer 싱글톤 인스턴스 반환"""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = DailyStrategyProducer()
    return _producer_instance
