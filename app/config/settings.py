"""
WebSocket 서버 설정
"""

from functools import lru_cache
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """WebSocket 서버 설정"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # -------------------------------------------
    # Server
    # -------------------------------------------
    app_name: str = "Stock Predict WebSocket"
    app_version: str = "0.1.0"
    debug: bool = True
    host: str = "0.0.0.0"
    port: int = 8001

    # -------------------------------------------
    # Kafka / Redpanda
    # -------------------------------------------
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_bootstrap_servers_internal: str = "redpanda-0:9092"
    kafka_use_internal: bool = False

    kafka_group_id: str = "websocket-server-group"
    kafka_auto_offset_reset: str = "latest"
    kafka_enable_auto_commit: bool = True

    topic_prediction_result: str = "ai_prediction_result"
    topic_price: str = "stock_price"
    topic_asking_price: str = "stock_asking_price"
    topic_manual_sell: str = "manual-sell-signal"

    # -------------------------------------------
    # Redis
    # -------------------------------------------
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str = ""
    redis_decode_responses: bool = True
    redis_socket_timeout: int = 5
    redis_socket_connect_timeout: int = 5

    # -------------------------------------------
    # Slack
    # -------------------------------------------
    slack_webhook_url: str = ""

    @property
    def kafka_servers(self) -> str:
        """Kafka 브로커 주소"""
        return self.kafka_bootstrap_servers_internal if self.kafka_use_internal else self.kafka_bootstrap_servers

    @property
    def kafka_servers_list(self) -> List[str]:
        """Kafka 브로커 주소 리스트"""
        return self.kafka_servers.split(',')

    @property
    def redis_url(self) -> str:
        """Redis 연결 URL"""
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
