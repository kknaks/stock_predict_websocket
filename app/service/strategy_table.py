"""
전략별 목표가 테이블 관리

START 명령어로 전략 정보를 받아 기본 테이블을 생성하고,
예측 데이터가 들어오면 목표가, 매도가, 손절가를 계산하여 저장합니다.
실시간 데이터와 비교하여 매수/매도 주문을 낼 수 있도록 관리합니다.
"""

import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Union
from dataclasses import dataclass, asdict

from app.models.prediction import PredictionItem
from app.kafka.daily_strategy_producer import get_daily_strategy_producer
from app.kis.websocket.redis_manager import get_redis_manager

logger = logging.getLogger(__name__)


@dataclass
class StrategyConfig:
    """전략 설정 정보"""
    user_strategy_id: int
    user_id: int
    is_mock: bool
    strategy_id: int
    strategy_name: str
    strategy_weight_type: str
    ls_ratio: float  # 손절 비율 (%)
    tp_ratio: float  # 익절 비율 (%)
    total_investment: float = 0.0  # 총 투자 금액 (비중 기반 수량 계산용)


@dataclass
class TargetPrice:
    """목표가 정보"""
    stock_code: str
    stock_name: str
    exchange: str
    stock_open: float  # 시가
    target_price: float  # 목표가 (take_profit_target 기준)
    target_sell_price: float  # 목표 매도가 (tp_ratio 적용)
    stop_loss_price: float  # 손절가 (ls_ratio 적용)
    gap_rate: float  # 갭률
    take_profit_target: float  # 익절 목표 수익률 (%)
    prob_up: float  # 상승 확률
    signal: str  # 매매 신호
    weight: float  # 비중 (0.0 ~ 1.0)
    target_quantity: int  # 목표 구매 수량 (비중 기반 계산)
    created_at: datetime  # 생성 시간


class StrategyTable:
    """전략별 목표가 테이블 관리 클래스"""

    def __init__(self):
        # user_strategy_id별로 전략 설정 저장
        # 여러 전략이 동시에 실행될 수 있으며, 각 전략은 독립적으로 관리됩니다.
        # 예: {2: StrategyConfig(...), 3: StrategyConfig(...), ...}
        self._strategy_configs: Dict[int, StrategyConfig] = {}
        # user_strategy_id별로 종목별 목표가 저장
        # 구조: {user_strategy_id: {stock_code: TargetPrice}}
        # 예: {2: {"005930": TargetPrice(...), "000660": TargetPrice(...)}, ...}
        self._target_tables: Dict[int, Dict[str, TargetPrice]] = {}
        # Redis 매니저
        self._redis_manager = get_redis_manager()

    def _create_target_table(self, strategy_config: StrategyConfig) -> None:
        """
        전략별 기본 테이블 생성

        Args:
            strategy_config: 전략 설정 정보
        """
        user_strategy_id = strategy_config.user_strategy_id
        
        # 전략 설정 저장 (메모리)
        self._strategy_configs[user_strategy_id] = strategy_config
        
        # 목표가 테이블 초기화 (기존 데이터가 있으면 유지)
        if user_strategy_id not in self._target_tables:
            self._target_tables[user_strategy_id] = {}
        
        # Redis에 전략 설정 저장
        config_data = {
            "user_strategy_id": strategy_config.user_strategy_id,
            "user_id": strategy_config.user_id,
            "is_mock": strategy_config.is_mock,
            "strategy_id": strategy_config.strategy_id,
            "strategy_name": strategy_config.strategy_name,
            "strategy_weight_type": strategy_config.strategy_weight_type,
            "ls_ratio": strategy_config.ls_ratio,
            "tp_ratio": strategy_config.tp_ratio,
            "total_investment": strategy_config.total_investment,
        }
        self._redis_manager.save_strategy_config(user_strategy_id, config_data)
        
        logger.info(
            f"Created target table for strategy: "
            f"user_strategy_id={user_strategy_id}, "
            f"strategy_name={strategy_config.strategy_name}, "
            f"ls_ratio={strategy_config.ls_ratio}%, "
            f"tp_ratio={strategy_config.tp_ratio}"
        )

    def _calculate_target(
        self, 
        prediction: PredictionItem, 
        strategy_config: StrategyConfig,
        weight: float
    ) -> TargetPrice:
        """
        예측 데이터로부터 목표가, 매도가, 손절가 계산

        Args:
            prediction: 예측 데이터
            strategy_config: 전략 설정 정보
            weight: 비중 (0.0 ~ 1.0)

        Returns:
            TargetPrice: 계산된 목표가 정보
        """
        stock_open = float(prediction.stock_open)
        take_profit_target = float(prediction.take_profit_target)
        ls_ratio = strategy_config.ls_ratio
        tp_ratio = strategy_config.tp_ratio

        # 목표가 = 시가 * (1 + take_profit_target / 100)
        target_price = stock_open * (1 + take_profit_target / 100)
        # 목표 매도가 = 시가 * (1 + take_profit_target * tp_ratio / 100)
        # tp_ratio만큼 수익 시 매도 (예: 0.8이면 80% 수익 시 매도)
        target_sell_price = stock_open * (1 + take_profit_target * tp_ratio / 100)

        # 손절가 = 시가 * (1 + ls_ratio / 100)
        # ls_ratio는 음수 (예: -1이면 -1% 손절)
        stop_loss_price = stock_open * (1 + ls_ratio / 100)

        # 비중 기반 목표 구매 수량 계산
        # 수량 = (총 투자 금액 * 비중) / 시가
        if strategy_config.total_investment > 0 and stock_open > 0:
            target_quantity = int((strategy_config.total_investment * weight) / stock_open)
            # 최소 1주는 보장
            target_quantity = max(1, target_quantity)
        else:
            # 총 투자 금액이 설정되지 않은 경우, 비중만 저장하고 수량은 1로 설정
            # 실제 주문 시 비중을 기반으로 수량을 계산해야 함
            target_quantity = 1

        return TargetPrice(
            stock_code=prediction.stock_code,
            stock_name=prediction.stock_name,
            exchange=prediction.exchange,
            stock_open=stock_open,
            target_price=target_price,
            target_sell_price=target_sell_price,
            stop_loss_price=stop_loss_price,
            gap_rate=prediction.gap_rate,
            take_profit_target=take_profit_target,
            prob_up=float(prediction.prob_up),
            signal=prediction.signal,
            weight=weight,
            target_quantity=target_quantity,
            created_at=prediction.timestamp
        )

    def _update_target(
        self, 
        user_strategy_id: int, 
        target_price: TargetPrice
    ) -> None:
        """
        목표가 테이블 업데이트

        Args:
            user_strategy_id: 사용자 전략 ID
            target_price: 목표가 정보
        """
        if user_strategy_id not in self._target_tables:
            logger.warning(
                f"Target table not found for user_strategy_id={user_strategy_id}. "
                f"Creating new table."
            )
            self._target_tables[user_strategy_id] = {}

        # 종목별 목표가 저장/업데이트 (메모리)
        self._target_tables[user_strategy_id][target_price.stock_code] = target_price

        # Redis에 목표가 저장
        target_data = {
            "stock_code": target_price.stock_code,
            "stock_name": target_price.stock_name,
            "exchange": target_price.exchange,
            "stock_open": target_price.stock_open,
            "target_price": target_price.target_price,
            "target_sell_price": target_price.target_sell_price,
            "stop_loss_price": target_price.stop_loss_price,
            "gap_rate": target_price.gap_rate,
            "take_profit_target": target_price.take_profit_target,
            "prob_up": target_price.prob_up,
            "signal": target_price.signal,
            "weight": target_price.weight,
            "target_quantity": target_price.target_quantity,
            "created_at": target_price.created_at.isoformat() if isinstance(target_price.created_at, datetime) else str(target_price.created_at)
        }
        self._redis_manager.save_strategy_target(
            user_strategy_id, 
            target_price.stock_code, 
            target_data
        )

        logger.debug(
            f"Updated target price for strategy={user_strategy_id}, "
            f"stock={target_price.stock_code}({target_price.stock_name}): "
            f"target={target_price.target_price:.2f}, "
            f"target_sell={target_price.target_sell_price:.2f}, "
            f"stop_loss={target_price.stop_loss_price:.2f}"
        )

    def _get_target(
        self, 
        user_strategy_id: int, 
        stock_code: Optional[str] = None
    ) -> Optional[Union[TargetPrice, Dict[str, TargetPrice]]]:
        """
        목표가 조회 (메모리 우선, 없으면 Redis에서 조회)

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드 (None이면 전체 조회)

        Returns:
            TargetPrice 또는 Dict[str, TargetPrice] 또는 None
        """
        # 메모리에서 먼저 조회
        if user_strategy_id in self._target_tables:
            table = self._target_tables[user_strategy_id]
            if stock_code is None:
                # 전체 조회
                return table.copy()
            else:
                # 특정 종목 조회
                target = table.get(stock_code)
                if target:
                    return target
        
        # 메모리에 없으면 Redis에서 조회
        if stock_code is None:
            # 전체 조회
            redis_targets = self._redis_manager.get_strategy_all_targets(user_strategy_id)
            if redis_targets:
                # Redis 데이터를 TargetPrice 객체로 변환
                result = {}
                for code, data in redis_targets.items():
                    try:
                        result[code] = TargetPrice(
                            stock_code=data["stock_code"],
                            stock_name=data["stock_name"],
                            exchange=data["exchange"],
                            stock_open=float(data["stock_open"]),
                            target_price=float(data["target_price"]),
                            target_sell_price=float(data.get("target_sell_price", data.get("sell_price", 0))),
                            stop_loss_price=float(data["stop_loss_price"]),
                            gap_rate=float(data["gap_rate"]),
                            take_profit_target=float(data["take_profit_target"]),
                            prob_up=float(data["prob_up"]),
                            signal=data["signal"],
                            weight=float(data.get("weight", 0.0)),  # 기존 데이터 호환성
                            target_quantity=int(data.get("target_quantity", 1)),  # 기존 데이터 호환성
                            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data["created_at"], str) else data["created_at"]
                        )
                    except Exception as e:
                        logger.warning(f"Failed to convert Redis data to TargetPrice for {code}: {e}")
                return result if result else None
            return None
        else:
            # 특정 종목 조회
            redis_data = self._redis_manager.get_strategy_target(user_strategy_id, stock_code)
            if redis_data:
                try:
                    return TargetPrice(
                        stock_code=redis_data["stock_code"],
                        stock_name=redis_data["stock_name"],
                        exchange=redis_data["exchange"],
                        stock_open=float(redis_data["stock_open"]),
                        target_price=float(redis_data["target_price"]),
                        target_sell_price=float(redis_data.get("target_sell_price", redis_data.get("sell_price", 0))),
                        stop_loss_price=float(redis_data["stop_loss_price"]),
                        gap_rate=float(redis_data["gap_rate"]),
                        take_profit_target=float(redis_data["take_profit_target"]),
                        prob_up=float(redis_data["prob_up"]),
                        signal=redis_data["signal"],
                        weight=float(redis_data.get("weight", 0.0)),  # 기존 데이터 호환성
                        target_quantity=int(redis_data.get("target_quantity", 1)),  # 기존 데이터 호환성
                        created_at=datetime.fromisoformat(redis_data["created_at"]) if isinstance(redis_data["created_at"], str) else redis_data["created_at"]
                    )
                except Exception as e:
                    logger.warning(f"Failed to convert Redis data to TargetPrice for {stock_code}: {e}")
            return None

    def _delete_target(
        self, 
        user_strategy_id: int, 
        stock_code: Optional[str] = None
    ) -> None:
        """
        목표가 테이블 삭제

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드 (None이면 전체 삭제)
        """
        if stock_code is None:
            # 전체 삭제
            # 메모리에서 삭제
            if user_strategy_id in self._target_tables:
                del self._target_tables[user_strategy_id]
            if user_strategy_id in self._strategy_configs:
                del self._strategy_configs[user_strategy_id]
            # Redis에서 삭제
            self._redis_manager.delete_strategy_all_targets(user_strategy_id)
            logger.info(f"Deleted all target tables for user_strategy_id={user_strategy_id}")
        else:
            # 특정 종목 삭제
            # 메모리에서 삭제
            if user_strategy_id in self._target_tables:
                if stock_code in self._target_tables[user_strategy_id]:
                    del self._target_tables[user_strategy_id][stock_code]
            # Redis에서 삭제
            self._redis_manager.delete_strategy_target(user_strategy_id, stock_code)
            logger.info(
                f"Deleted target price for strategy={user_strategy_id}, "
                f"stock={stock_code}"
            )

    async def initialize_from_start_command(self, strategies: List[Dict]) -> None:
        """
        START 명령어로부터 전략 테이블 초기화
        
        여러 전략이 동시에 들어올 수 있으며, 각 전략은 독립적으로 관리됩니다.
        예: 10개의 전략이 들어오면 10개의 독립적인 목표가 테이블이 생성됩니다.
        
        초기화 완료 후 카프카로 daily_strategy 토픽에 유저별 전략 정보를 발행합니다.

        Args:
            strategies: 전략 리스트 (START 명령어의 config.strategies)
                      빈 리스트인 경우 아무 작업도 수행하지 않습니다.
        """
        if not strategies:
            logger.info("No strategies provided, skipping initialization")
            return

        logger.info(f"Initializing {len(strategies)} strategy tables")
        success_count = 0
        successful_strategies = []
        
        for strategy_data in strategies:
            try:
                strategy_config = StrategyConfig(
                    user_strategy_id=strategy_data["user_strategy_id"],
                    user_id=strategy_data["user_id"],
                    is_mock=strategy_data.get("is_mock", False),
                    strategy_id=strategy_data["strategy_id"],
                    strategy_name=strategy_data["strategy_name"],
                    strategy_weight_type=strategy_data.get("strategy_weight_type", "equal"),
                    ls_ratio=float(strategy_data["ls_ratio"]),
                    tp_ratio=float(strategy_data["tp_ratio"]),
                    total_investment=float(strategy_data.get("total_investment", 0.0))
                )
                self._create_target_table(strategy_config)
                success_count += 1
                successful_strategies.append(strategy_data)
            except KeyError as e:
                logger.error(f"Missing required field in strategy data: {e}, data={strategy_data}")
            except Exception as e:
                logger.error(f"Error initializing strategy table: {e}, data={strategy_data}", exc_info=True)
        
        logger.info(
            f"Strategy table initialization completed: "
            f"{success_count}/{len(strategies)} strategies initialized successfully"
        )

    async def process_predictions(self, predictions: List[PredictionItem]) -> None:
        """
        예측 데이터 처리 및 목표가 계산
        
        예측 데이터 처리 완료 후 카프카로 daily_strategy 토픽에 전략 정보를 발행합니다.
        
        총 투자 금액보다 시가가 높은 종목은 자동으로 제외됩니다.

        Args:
            predictions: 예측 데이터 리스트
        """
        # 각 전략별로 구매 가능한 종목만 필터링 (총 투자 금액보다 시가가 낮은 종목만)
        strategy_available_predictions = {}
        for user_strategy_id, strategy_config in self._strategy_configs.items():
            if strategy_config.total_investment > 0:
                # 총 투자 금액이 설정된 경우, 구매 가능한 종목만 필터링
                available = [
                    p for p in predictions 
                    if float(p.stock_open) <= strategy_config.total_investment
                ]
                if len(available) < len(predictions):
                    excluded_count = len(predictions) - len(available)
                    excluded_stocks = [
                        p.stock_code for p in predictions 
                        if float(p.stock_open) > strategy_config.total_investment
                    ]
                    logger.warning(
                        f"전략 {user_strategy_id}에서 구매 불가능한 종목 {excluded_count}개 제외: "
                        f"종목코드={excluded_stocks[:5]}{'...' if len(excluded_stocks) > 5 else ''}, "
                        f"총투자금액={strategy_config.total_investment:,.0f}원"
                    )
                strategy_available_predictions[user_strategy_id] = available
            else:
                # 총 투자 금액이 설정되지 않은 경우 모든 종목 포함
                strategy_available_predictions[user_strategy_id] = predictions
        
        # 각 전략별로 구매 가능한 종목만 처리
        for user_strategy_id, strategy_config in self._strategy_configs.items():
            available_predictions = strategy_available_predictions[user_strategy_id]
            
            if not available_predictions:
                logger.warning(
                    f"전략 {user_strategy_id}에 구매 가능한 종목이 없습니다. "
                    f"총투자금액={strategy_config.total_investment:,.0f}원"
                )
                continue
            
            # 비중 계산에 필요한 총합을 미리 계산 (해당 전략의 구매 가능한 종목만으로 계산)
            if strategy_config.strategy_weight_type == "MARKETCAP":
                total_market_cap = sum(p.market_cap for p in available_predictions)
            elif strategy_config.strategy_weight_type == "PRICE":
                # 역가중치: 시가가 낮을수록 비중 높음
                total_inverse_price = sum(1.0 / float(p.stock_open) for p in available_predictions if float(p.stock_open) > 0)
            else:
                total_market_cap = 0
                total_inverse_price = 0
            
            for prediction in available_predictions:
                try:
                    # 비중 계산 
                    if strategy_config.strategy_weight_type == "EQUAL":
                        # EQUAL 방식: 구매 가능한 종목 수로 균등 분배
                        weight = 1.0 / len(available_predictions)
                    elif strategy_config.strategy_weight_type == "MARKETCAP":
                        if total_market_cap > 0:
                            weight = prediction.market_cap / total_market_cap
                        else:
                            weight = 1.0 / len(available_predictions)
                    elif strategy_config.strategy_weight_type == "PRICE":
                        # 역가중치: 시가가 낮을수록 비중 높음
                        stock_open = float(prediction.stock_open)
                        if total_inverse_price > 0 and stock_open > 0:
                            weight = (1.0 / stock_open) / total_inverse_price
                        else:
                            weight = 1.0 / len(available_predictions)
                    else:
                        weight = 1.0 / len(available_predictions)
                    
                    # 목표가 계산
                    target_price = self._calculate_target(prediction, strategy_config, weight)
                    
                    # 테이블 업데이트
                    self._update_target(user_strategy_id, target_price)
                except Exception as e:
                    logger.error(
                        f"Error processing prediction for strategy={user_strategy_id}, "
                        f"stock={prediction.stock_code}: {e}",
                        exc_info=True
                    )
        
        # 예측 데이터 처리 완료 후 카프카로 전략 정보 발행 (종목별 목표가 포함)
        if self._strategy_configs:
            try:
                # 현재 전략 테이블의 모든 전략 정보와 종목별 목표가 정보를 포함
                strategies_data = []
                for user_strategy_id, strategy_config in self._strategy_configs.items():
                    # 전략 기본 정보
                    strategy_data = {
                        "user_strategy_id": strategy_config.user_strategy_id,
                        "user_id": strategy_config.user_id,
                        "is_mock": strategy_config.is_mock,
                        "strategy_id": strategy_config.strategy_id,
                        "strategy_name": strategy_config.strategy_name,
                        "strategy_weight_type": strategy_config.strategy_weight_type,
                        "ls_ratio": strategy_config.ls_ratio,
                        "tp_ratio": strategy_config.tp_ratio,
                        "stocks": []  # 종목별 목표가 정보
                    }
                    
                    # 해당 전략의 종목별 목표가 정보 추가
                    if user_strategy_id in self._target_tables:
                        target_table = self._target_tables[user_strategy_id]
                        for stock_code, target_price in target_table.items():
                            strategy_data["stocks"].append({
                                "stock_code": target_price.stock_code,
                                "stock_name": target_price.stock_name,
                                "exchange": target_price.exchange,
                                "stock_open": target_price.stock_open,
                                "target_price": target_price.target_price,
                                "target_sell_price": target_price.target_sell_price,
                                "stop_loss_price": target_price.stop_loss_price,
                                "gap_rate": target_price.gap_rate,
                                "take_profit_target": target_price.take_profit_target,
                                "prob_up": target_price.prob_up,
                                "signal": target_price.signal,
                                "weight": target_price.weight,
                                "target_quantity": target_price.target_quantity,
                                "created_at": target_price.created_at.isoformat() if isinstance(target_price.created_at, datetime) else str(target_price.created_at)
                            })
                    
                    strategies_data.append(strategy_data)
                
                producer = get_daily_strategy_producer()
                await producer.send_strategies(strategies_data)
                logger.info(
                    f"Sent {len(strategies_data)} strategies with stock targets to Kafka daily_strategy topic "
                    f"after processing {len(predictions)} predictions"
                )
            except Exception as e:
                logger.error(f"Error sending strategies to Kafka after processing predictions: {e}", exc_info=True)

    def get_target_for_comparison(
        self, 
        user_strategy_id: int, 
        stock_code: str,
        use_redis: bool = True
    ) -> Optional[TargetPrice]:
        """
        실시간 데이터와 비교하기 위한 목표가 조회
        
        실시간 조회가 필요한 경우 Redis에서 직접 가져와서 최신 데이터를 보장합니다.

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드
            use_redis: True면 Redis에서 직접 조회 (최신 데이터 보장), False면 메모리 우선

        Returns:
            TargetPrice 또는 None
        """
        if use_redis:
            # Redis에서 직접 조회 (최신 데이터 보장)
            redis_data = self._redis_manager.get_strategy_target(user_strategy_id, stock_code)
            if redis_data:
                try:
                    target = TargetPrice(
                        stock_code=redis_data["stock_code"],
                        stock_name=redis_data["stock_name"],
                        exchange=redis_data["exchange"],
                        stock_open=float(redis_data["stock_open"]),
                        target_price=float(redis_data["target_price"]),
                        target_sell_price=float(redis_data.get("target_sell_price", redis_data.get("sell_price", 0))),
                        stop_loss_price=float(redis_data["stop_loss_price"]),
                        gap_rate=float(redis_data["gap_rate"]),
                        take_profit_target=float(redis_data["take_profit_target"]),
                        prob_up=float(redis_data["prob_up"]),
                        signal=redis_data["signal"],
                        weight=float(redis_data.get("weight", 0.0)),  # 기존 데이터 호환성
                        target_quantity=int(redis_data.get("target_quantity", 1)),  # 기존 데이터 호환성
                        created_at=datetime.fromisoformat(redis_data["created_at"]) if isinstance(redis_data["created_at"], str) else redis_data["created_at"]
                    )
                    # 메모리 캐시도 업데이트 (다음 조회 시 빠르게)
                    if user_strategy_id not in self._target_tables:
                        self._target_tables[user_strategy_id] = {}
                    self._target_tables[user_strategy_id][stock_code] = target
                    return target
                except Exception as e:
                    logger.warning(f"Failed to convert Redis data to TargetPrice for {stock_code}: {e}")
            return None
        else:
            # 메모리 우선 조회 (기존 방식)
            target = self._get_target(user_strategy_id, stock_code)
            if isinstance(target, TargetPrice):
                return target
            return None

    def check_price_conditions(
        self, 
        user_strategy_id: int, 
        stock_code: str, 
        current_price: float
    ) -> Dict[str, bool]:
        """
        현재가와 목표가 비교하여 매수/매도 조건 확인

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드
            current_price: 현재가

        Returns:
            조건 체크 결과 딕셔너리
        """
        target = self.get_target_for_comparison(user_strategy_id, stock_code)
        if target is None:
            return {
                "has_target": False,
                "reached_target": False,
                "reached_sell": False,
                "reached_stop_loss": False
            }

        return {
            "has_target": True,
            "reached_target": current_price >= target.target_price,
            "reached_sell": current_price >= target.target_sell_price,
            "reached_stop_loss": current_price <= target.stop_loss_price,
            "target_price": target.target_price,
            "target_sell_price": target.target_sell_price,
            "stop_loss_price": target.stop_loss_price,
            "current_price": current_price
        }

    def get_all_strategies(self) -> List[int]:
        """
        등록된 모든 전략 ID 목록 조회

        Returns:
            user_strategy_id 리스트
        """
        return list(self._strategy_configs.keys())

    def get_strategy_info(self, user_strategy_id: int) -> Optional[StrategyConfig]:
        """
        특정 전략의 설정 정보 조회

        Args:
            user_strategy_id: 사용자 전략 ID

        Returns:
            StrategyConfig 또는 None
        """
        return self._strategy_configs.get(user_strategy_id)

    def get_strategy_statistics(self, user_strategy_id: int) -> Dict[str, any]:
        """
        전략별 통계 정보 조회

        Args:
            user_strategy_id: 사용자 전략 ID

        Returns:
            통계 정보 딕셔너리
        """
        if user_strategy_id not in self._target_tables:
            return {
                "user_strategy_id": user_strategy_id,
                "exists": False,
                "stock_count": 0
            }

        target_table = self._target_tables[user_strategy_id]
        strategy_config = self._strategy_configs.get(user_strategy_id)

        return {
            "user_strategy_id": user_strategy_id,
            "exists": True,
            "stock_count": len(target_table),
            "strategy_name": strategy_config.strategy_name if strategy_config else None,
            "ls_ratio": strategy_config.ls_ratio if strategy_config else None,
            "tp_ratio": strategy_config.tp_ratio if strategy_config else None,
            "stock_codes": list(target_table.keys())
        }

    def get_all_statistics(self) -> Dict[int, Dict[str, any]]:
        """
        모든 전략의 통계 정보 조회

        Returns:
            {user_strategy_id: 통계 정보} 딕셔너리
        """
        return {
            user_strategy_id: self.get_strategy_statistics(user_strategy_id)
            for user_strategy_id in self._strategy_configs.keys()
        }


# 싱글톤 인스턴스
_strategy_table_instance: Optional[StrategyTable] = None


def get_strategy_table() -> StrategyTable:
    """StrategyTable 싱글톤 인스턴스 반환"""
    global _strategy_table_instance
    if _strategy_table_instance is None:
        _strategy_table_instance = StrategyTable()
    return _strategy_table_instance