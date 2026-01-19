"""
Kafka 연결 설정
"""

from typing import List
from .settings import settings


class KafkaConfig:
    """Kafka 연결 설정"""

    @property
    def bootstrap_servers(self) -> str:
        return settings.kafka_servers

    @property
    def bootstrap_servers_list(self) -> List[str]:
        return settings.kafka_servers_list

    @property
    def kafka_group_id(self) -> str:
        return settings.kafka_group_id

    @property
    def kafka_auto_offset_reset(self) -> str:
        return settings.kafka_auto_offset_reset

    @property
    def kafka_enable_auto_commit(self) -> bool:
        return settings.kafka_enable_auto_commit

    @property
    def topic_prediction_result(self) -> str:
        return settings.topic_prediction_result

    @property
    def topic_price(self) -> str:
        return settings.topic_price


_config_instance: KafkaConfig | None = None


def get_kafka_config() -> KafkaConfig:
    """Kafka 설정 싱글톤 인스턴스 반환"""
    global _config_instance
    if _config_instance is None:
        _config_instance = KafkaConfig()
    return _config_instance
