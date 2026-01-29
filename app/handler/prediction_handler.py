"""
Prediction Message Handler

Kafka에서 수신한 예측 결과를 처리하고 저장
"""

import logging
from datetime import datetime
from typing import List, Optional

from app.models.prediction import PredictionMessage, PredictionItem
from app.kis.websocket.manager import get_websocket_manager
from app.service.strategy_table import get_strategy_table

logger = logging.getLogger(__name__)


class PredictionHandler:
    """예측 결과 핸들러"""

    def __init__(self):
        self._latest_predictions: Optional[PredictionMessage] = None
        self._prediction_history: List[PredictionMessage] = []
        self._max_history = 100  # 최대 보관 개수
        self._websocket_manager = get_websocket_manager()

    @property
    def latest_predictions(self) -> Optional[PredictionMessage]:
        """최신 예측 결과"""
        return self._latest_predictions

    @property
    def prediction_count(self) -> int:
        """히스토리에 저장된 예측 결과 수"""
        return len(self._prediction_history)

    async def handle_prediction(self, message: PredictionMessage) -> None:
        """
        예측 결과 메시지 처리

        Args:
            message: Kafka에서 수신한 예측 메시지
        """
        logger.info(
            f"Processing prediction message: "
            f"timestamp={message.timestamp}, "
            f"total_count={message.total_count}"
        )

        # 최신 예측 저장
        self._latest_predictions = message

        # 히스토리에 추가
        self._prediction_history.append(message)

        # 히스토리 크기 제한
        if len(self._prediction_history) > self._max_history:
            self._prediction_history = self._prediction_history[-self._max_history:]

        # 예측 결과 로깅
        self._log_predictions(message)

        # 후보군 산출
        candidate_stocks = [
            p for p in message.predictions
            if p.gap_rate < 28 and p.prob_up > 0.2
        ]
        candidate_stocks = sorted(candidate_stocks, key=lambda x: x.prob_up, reverse=True)[:15]

        logger.info(f"Candidate stocks: {candidate_stocks}")
        
        # 전략 테이블에 예측 데이터 처리 (목표가 계산) - 필터링된 후보군만 전달
        strategy_table = get_strategy_table()
        try:
            await strategy_table.process_predictions(candidate_stocks)
            logger.info(f"Processed {len(candidate_stocks)} candidate stocks for strategy tables")
        except Exception as e:
            logger.error(f"Error processing predictions for strategy tables: {e}", exc_info=True)
        
        # 웹소켓 종목 업데이트 (exchange_type 기반 분기)
        exchange_type = message.exchange_type.lower()
        if exchange_type == "nxt":
            await self._update_websocket_stocks(candidate_stocks)
        else:
            await self._add_websocket_stocks(candidate_stocks)

    def _log_predictions(self, message: PredictionMessage) -> None:
        """예측 결과 요약 로깅"""
        # 시그널별 분류
        buy_signals = [p for p in message.predictions if p.signal == "BUY"]
        hold_signals = [p for p in message.predictions if p.signal == "HOLD"]
        sell_signals = [p for p in message.predictions if p.signal == "SELL"]

        logger.info(
            f"Prediction summary: "
            f"BUY={len(buy_signals)}, "
            f"HOLD={len(hold_signals)}, "
            f"SELL={len(sell_signals)}"
        )

        # # BUY 시그널 상세 로깅
        # for pred in buy_signals:
        #     logger.info(
        #         f"  BUY: {pred.stock_name}({pred.stock_code}) "
        #         f"gap={pred.gap_rate}%, prob_up={pred.prob_up}"
        #     )

        # for pred in hold_signals:
        #     logger.info(
        #         f"  HOLD: {pred.stock_name}({pred.stock_code}) "
        #         f"gap={pred.gap_rate}%, prob_up={pred.prob_up}"
        #     )

        # for pred in sell_signals:
        #     logger.info(
        #         f"  SELL: {pred.stock_name}({pred.stock_code}) "
        #         f"gap={pred.gap_rate}%, prob_up={pred.prob_up}"
        #     )

    def get_by_signal(self, signal: str) -> List[PredictionItem]:
        """특정 시그널의 예측 결과 조회"""
        if not self._latest_predictions:
            return []
        return [p for p in self._latest_predictions.predictions if p.signal == signal]

    def get_by_exchange(self, exchange: str) -> List[PredictionItem]:
        """특정 거래소의 예측 결과 조회"""
        if not self._latest_predictions:
            return []
        return [p for p in self._latest_predictions.predictions if p.exchange == exchange]

    def get_top_by_prob_up(self, n: int = 10) -> List[PredictionItem]:
        """상승 확률 상위 N개 조회"""
        if not self._latest_predictions:
            return []
        sorted_preds = sorted(
            self._latest_predictions.predictions,
            key=lambda x: float(x.prob_up),
            reverse=True
        )
        return sorted_preds[:n]

    async def _update_websocket_stocks(self, candidate_stocks: dict[any, any]) -> None:
        """예측 결과의 종목 리스트로 웹소켓 종목 업데이트"""
        if not candidate_stocks:
            logger.warning("No predictions to update websocket stocks")
            return
        
        # 예측 결과에서 종목 코드 추출
        stock_codes = [pred.stock_code for pred in candidate_stocks]
        # 중복 제거
        unique_stocks = list(set(stock_codes))
        
        logger.info(
            f"Updating websocket stocks: {len(unique_stocks)} stocks "
            f"from {len(candidate_stocks)} predictions"
        )
        
        # 웹소켓 매니저에 종목 업데이트 요청
        success = await self._websocket_manager.update_price_stocks(unique_stocks)
        if success:
            logger.info(f"Websocket stocks updated successfully: {len(unique_stocks)} stocks")
        else:
            logger.warning("Failed to update websocket stocks")

    async def _add_websocket_stocks(self, candidate_stocks: dict[any, any]) -> None:
        """예측 결과의 종목 리스트를 기존 구독에 추가"""
        if not candidate_stocks:
            logger.warning("No predictions to add websocket stocks")
            return

        stock_codes = [pred.stock_code for pred in candidate_stocks]
        unique_stocks = list(set(stock_codes))

        logger.info(
            f"Adding websocket stocks: {len(unique_stocks)} stocks "
            f"from {len(candidate_stocks)} predictions"
        )

        success = await self._websocket_manager.add_price_stocks(unique_stocks)
        if success:
            logger.info(f"Websocket stocks added successfully: {len(unique_stocks)} stocks")
        else:
            logger.warning("Failed to add websocket stocks")

    def clear_history(self) -> None:
        """히스토리 초기화"""
        self._prediction_history.clear()
        logger.info("Prediction history cleared")


# 싱글톤 인스턴스
_handler_instance: Optional[PredictionHandler] = None


def get_prediction_handler() -> PredictionHandler:
    """Prediction Handler 싱글톤 인스턴스 반환"""
    global _handler_instance
    if _handler_instance is None:
        _handler_instance = PredictionHandler()
    return _handler_instance
