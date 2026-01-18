"""
Kafka package
"""

from .predict_consumer import KafkaPredictionConsumer, get_predict_consumer
from .websocket_consumer import KafkaWebSocketConsumer, get_websocket_consumer

__all__ = [
    "KafkaPredictionConsumer",
    "get_predict_consumer",
    "KafkaWebsocketConsumer",
    "get_websocket_consumer",
]
