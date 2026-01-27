"""
AI 예측 결과 모델
"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel


class PredictionItem(BaseModel):
    """개별 종목 예측 결과"""

    timestamp: datetime
    stock_code: str
    stock_name: str
    exchange: str
    market_cap: int
    date: str
    gap_rate: float
    stock_open: int
    avg_volume_20d: Optional[int] = None  # 20일 평균 거래량
    prob_up: float
    prob_down: float
    predicted_direction: int
    expected_return: float
    return_if_up: float
    return_if_down: float
    max_return_if_up: float
    take_profit_target: float
    signal: str
    model_version: str
    confidence: str


class PredictionMessage(BaseModel):
    """Kafka에서 받는 예측 결과 메시지"""

    timestamp: datetime
    total_count: int
    predictions: List[PredictionItem]
