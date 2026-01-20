"""
슬리피지 계산 모듈

실시간 호가 데이터를 기반으로 매수/매도 시 예상 슬리피지를 계산합니다.
슬리피지 최소화를 위한 최적 주문 가격과 주문 유형을 추천합니다.
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class OrderType(Enum):
    """주문 유형"""
    MARKET = "market"  # 시장가
    LIMIT = "limit"    # 지정가
    SPLIT = "split"    # 분할 주문


class OrderSide(Enum):
    """주문 방향"""
    BUY = "buy"
    SELL = "sell"


@dataclass
class SlippageResult:
    """슬리피지 계산 결과"""
    order_side: OrderSide
    current_price: float           # 현재가
    best_price: float              # 최우선 호가 (매수:매도1호가, 매도:매수1호가)
    simple_slippage_pct: float     # 단순 슬리피지 (%)
    expected_slippage_pct: float   # 예상 슬리피지 (주문량 고려, %)
    weighted_avg_price: float      # 가중평균 체결 예상가
    can_fill_immediately: bool     # 즉시 체결 가능 여부
    available_quantity: int        # 호가창에서 사용 가능한 수량
    recommended_order_type: OrderType  # 추천 주문 유형
    recommended_price: float       # 추천 주문 가격
    depth_analysis: Dict           # 호가창 깊이 분석 결과


@dataclass
class SignalResult:
    """시그널 생성 결과"""
    signal_type: str               # BUY, SELL, HOLD
    stock_code: str
    current_price: float
    target_price: Optional[float]
    target_quantity: Optional[int]
    stop_loss_price: Optional[float]
    recommended_order_price: float
    recommended_order_type: OrderType
    expected_slippage_pct: float
    urgency: str                   # LOW, MEDIUM, HIGH, CRITICAL
    reason: str                    # 시그널 생성 사유


class SlippageCalculator:
    """슬리피지 계산기"""

    # 슬리피지 임계값 (%)
    SLIPPAGE_THRESHOLD_LOW = 0.05      # 0.05% 이하: 매우 좋음
    SLIPPAGE_THRESHOLD_MEDIUM = 0.1    # 0.1% 이하: 양호
    SLIPPAGE_THRESHOLD_HIGH = 0.3      # 0.3% 이하: 주의

    def __init__(self):
        pass

    def calculate_buy_slippage(
        self,
        price_data: Dict,
        asking_price_data: Dict,
        order_quantity: int
    ) -> SlippageResult:
        """
        매수 시 예상 슬리피지 계산

        매수 슬리피지 = (실제 체결 예상가 - 현재가) / 현재가 * 100

        Args:
            price_data: 체결가 데이터 (H0STCNT0)
            asking_price_data: 호가 데이터 (H0STASP0)
            order_quantity: 주문 수량

        Returns:
            SlippageResult: 슬리피지 계산 결과
        """
        current_price = self._parse_float(price_data.get('STCK_PRPR', 0))

        # 호가 데이터에서 매도호가 정보 추출 (매수 시 매도호가를 봐야 함)
        ask_prices, ask_quantities = self._extract_ask_book(asking_price_data)

        if current_price <= 0 or not ask_prices:
            return self._empty_result(OrderSide.BUY, current_price)

        best_ask = ask_prices[0] if ask_prices else current_price
        best_ask_qty = ask_quantities[0] if ask_quantities else 0

        # 단순 슬리피지: 최우선 매도호가 기준
        simple_slippage = ((best_ask - current_price) / current_price * 100) if current_price > 0 else 0

        # 주문량 대비 즉시 체결 가능 여부
        can_fill_immediately = order_quantity <= best_ask_qty

        # 가중평균 체결 예상가 계산 (호가창 깊이 분석)
        weighted_avg_price, total_available, depth_analysis = self._calculate_weighted_avg_buy_price(
            ask_prices, ask_quantities, order_quantity
        )

        # 예상 슬리피지 (가중평균 기준)
        expected_slippage = ((weighted_avg_price - current_price) / current_price * 100) if current_price > 0 else 0

        # 추천 주문 유형 및 가격 결정
        recommended_type, recommended_price = self._recommend_buy_order(
            current_price, best_ask, simple_slippage, can_fill_immediately, depth_analysis
        )

        return SlippageResult(
            order_side=OrderSide.BUY,
            current_price=current_price,
            best_price=best_ask,
            simple_slippage_pct=round(simple_slippage, 4),
            expected_slippage_pct=round(expected_slippage, 4),
            weighted_avg_price=round(weighted_avg_price, 2),
            can_fill_immediately=can_fill_immediately,
            available_quantity=total_available,
            recommended_order_type=recommended_type,
            recommended_price=recommended_price,
            depth_analysis=depth_analysis
        )

    def calculate_sell_slippage(
        self,
        price_data: Dict,
        asking_price_data: Dict,
        order_quantity: int
    ) -> SlippageResult:
        """
        매도 시 예상 슬리피지 계산

        매도 슬리피지 = (현재가 - 실제 체결 예상가) / 현재가 * 100

        Args:
            price_data: 체결가 데이터 (H0STCNT0)
            asking_price_data: 호가 데이터 (H0STASP0)
            order_quantity: 주문 수량

        Returns:
            SlippageResult: 슬리피지 계산 결과
        """
        current_price = self._parse_float(price_data.get('STCK_PRPR', 0))

        # 호가 데이터에서 매수호가 정보 추출 (매도 시 매수호가를 봐야 함)
        bid_prices, bid_quantities = self._extract_bid_book(asking_price_data)

        if current_price <= 0 or not bid_prices:
            return self._empty_result(OrderSide.SELL, current_price)

        best_bid = bid_prices[0] if bid_prices else current_price
        best_bid_qty = bid_quantities[0] if bid_quantities else 0

        # 단순 슬리피지: 최우선 매수호가 기준
        simple_slippage = ((current_price - best_bid) / current_price * 100) if current_price > 0 else 0

        # 주문량 대비 즉시 체결 가능 여부
        can_fill_immediately = order_quantity <= best_bid_qty

        # 가중평균 체결 예상가 계산 (호가창 깊이 분석)
        weighted_avg_price, total_available, depth_analysis = self._calculate_weighted_avg_sell_price(
            bid_prices, bid_quantities, order_quantity
        )

        # 예상 슬리피지 (가중평균 기준)
        expected_slippage = ((current_price - weighted_avg_price) / current_price * 100) if current_price > 0 else 0

        # 추천 주문 유형 및 가격 결정
        recommended_type, recommended_price = self._recommend_sell_order(
            current_price, best_bid, simple_slippage, can_fill_immediately, depth_analysis
        )

        return SlippageResult(
            order_side=OrderSide.SELL,
            current_price=current_price,
            best_price=best_bid,
            simple_slippage_pct=round(simple_slippage, 4),
            expected_slippage_pct=round(expected_slippage, 4),
            weighted_avg_price=round(weighted_avg_price, 2),
            can_fill_immediately=can_fill_immediately,
            available_quantity=total_available,
            recommended_order_type=recommended_type,
            recommended_price=recommended_price,
            depth_analysis=depth_analysis
        )

    def _extract_ask_book(self, asking_price_data: Dict) -> Tuple[List[float], List[int]]:
        """호가 데이터에서 매도호가 정보 추출 (10단계)"""
        prices = []
        quantities = []

        for i in range(1, 11):
            price = self._parse_float(asking_price_data.get(f'ASKP{i}', 0))
            qty = self._parse_int(asking_price_data.get(f'ASKP_RSQN{i}', 0))

            if price > 0:
                prices.append(price)
                quantities.append(qty)

        return prices, quantities

    def _extract_bid_book(self, asking_price_data: Dict) -> Tuple[List[float], List[int]]:
        """호가 데이터에서 매수호가 정보 추출 (10단계)"""
        prices = []
        quantities = []

        for i in range(1, 11):
            price = self._parse_float(asking_price_data.get(f'BIDP{i}', 0))
            qty = self._parse_int(asking_price_data.get(f'BIDP_RSQN{i}', 0))

            if price > 0:
                prices.append(price)
                quantities.append(qty)

        return prices, quantities

    def _calculate_weighted_avg_buy_price(
        self,
        ask_prices: List[float],
        ask_quantities: List[int],
        order_quantity: int
    ) -> Tuple[float, int, Dict]:
        """
        매수 시 가중평균 체결 예상가 계산

        주문 수량이 여러 호가 단계를 소진하는 경우,
        각 호가에서 체결되는 수량을 가중치로 평균가 계산
        """
        if not ask_prices or order_quantity <= 0:
            return 0, 0, {}

        remaining_qty = order_quantity
        total_cost = 0
        filled_qty = 0
        levels_used = []

        for i, (price, qty) in enumerate(zip(ask_prices, ask_quantities)):
            if remaining_qty <= 0:
                break

            fill_at_level = min(remaining_qty, qty)
            total_cost += price * fill_at_level
            filled_qty += fill_at_level
            remaining_qty -= fill_at_level

            levels_used.append({
                "level": i + 1,
                "price": price,
                "available_qty": qty,
                "fill_qty": fill_at_level
            })

        weighted_avg = total_cost / filled_qty if filled_qty > 0 else (ask_prices[0] if ask_prices else 0)
        total_available = sum(ask_quantities)

        depth_analysis = {
            "levels_needed": len(levels_used),
            "total_levels": len(ask_prices),
            "levels_detail": levels_used,
            "unfilled_qty": remaining_qty if remaining_qty > 0 else 0,
            "total_available_qty": total_available,
            "spread_pct": self._calculate_spread(ask_prices) if len(ask_prices) >= 2 else 0
        }

        return weighted_avg, total_available, depth_analysis

    def _calculate_weighted_avg_sell_price(
        self,
        bid_prices: List[float],
        bid_quantities: List[int],
        order_quantity: int
    ) -> Tuple[float, int, Dict]:
        """
        매도 시 가중평균 체결 예상가 계산
        """
        if not bid_prices or order_quantity <= 0:
            return 0, 0, {}

        remaining_qty = order_quantity
        total_revenue = 0
        filled_qty = 0
        levels_used = []

        for i, (price, qty) in enumerate(zip(bid_prices, bid_quantities)):
            if remaining_qty <= 0:
                break

            fill_at_level = min(remaining_qty, qty)
            total_revenue += price * fill_at_level
            filled_qty += fill_at_level
            remaining_qty -= fill_at_level

            levels_used.append({
                "level": i + 1,
                "price": price,
                "available_qty": qty,
                "fill_qty": fill_at_level
            })

        weighted_avg = total_revenue / filled_qty if filled_qty > 0 else (bid_prices[0] if bid_prices else 0)
        total_available = sum(bid_quantities)

        depth_analysis = {
            "levels_needed": len(levels_used),
            "total_levels": len(bid_prices),
            "levels_detail": levels_used,
            "unfilled_qty": remaining_qty if remaining_qty > 0 else 0,
            "total_available_qty": total_available,
            "spread_pct": self._calculate_spread(bid_prices) if len(bid_prices) >= 2 else 0
        }

        return weighted_avg, total_available, depth_analysis

    def _calculate_spread(self, prices: List[float]) -> float:
        """호가 스프레드 계산 (첫 호가 대비 마지막 호가 차이 %)"""
        if len(prices) < 2 or prices[0] <= 0:
            return 0
        return abs(prices[-1] - prices[0]) / prices[0] * 100

    def _recommend_buy_order(
        self,
        current_price: float,
        best_ask: float,
        slippage: float,
        can_fill_immediately: bool,
        depth_analysis: Dict
    ) -> Tuple[OrderType, float]:
        """
        매수 주문 추천

        슬리피지 최소화를 위한 최적 주문 유형과 가격 결정
        """
        # 슬리피지가 매우 낮으면 시장가 (빠른 체결 우선)
        if slippage <= self.SLIPPAGE_THRESHOLD_LOW and can_fill_immediately:
            return OrderType.MARKET, best_ask

        # 슬리피지가 낮으면 최우선 매도호가로 지정가
        if slippage <= self.SLIPPAGE_THRESHOLD_MEDIUM:
            return OrderType.LIMIT, best_ask

        # 슬리피지가 높으면 분할 주문 추천
        # 분할 주문 시 best_ask 사용 (체결 확률 높임)
        if depth_analysis.get('levels_needed', 1) > 2:
            return OrderType.SPLIT, best_ask

        return OrderType.LIMIT, best_ask

    def _recommend_sell_order(
        self,
        current_price: float,
        best_bid: float,
        slippage: float,
        can_fill_immediately: bool,
        depth_analysis: Dict
    ) -> Tuple[OrderType, float]:
        """
        매도 주문 추천

        슬리피지 최소화를 위한 최적 주문 유형과 가격 결정
        """
        # 슬리피지가 매우 낮으면 시장가 (빠른 체결 우선)
        if slippage <= self.SLIPPAGE_THRESHOLD_LOW and can_fill_immediately:
            return OrderType.MARKET, best_bid

        # 슬리피지가 낮으면 최우선 매수호가로 지정가
        if slippage <= self.SLIPPAGE_THRESHOLD_MEDIUM:
            return OrderType.LIMIT, best_bid

        # 슬리피지가 높으면 분할 주문 추천
        # 분할 주문 시 best_bid 사용 (체결 확률 높임)
        if depth_analysis.get('levels_needed', 1) > 2:
            return OrderType.SPLIT, best_bid

        return OrderType.LIMIT, best_bid

    def _empty_result(self, side: OrderSide, current_price: float) -> SlippageResult:
        """빈 결과 반환"""
        return SlippageResult(
            order_side=side,
            current_price=current_price,
            best_price=0,
            simple_slippage_pct=0,
            expected_slippage_pct=0,
            weighted_avg_price=0,
            can_fill_immediately=False,
            available_quantity=0,
            recommended_order_type=OrderType.LIMIT,
            recommended_price=current_price,
            depth_analysis={}
        )

    @staticmethod
    def _parse_float(value) -> float:
        """문자열을 float로 변환"""
        try:
            return float(str(value).strip()) if value else 0.0
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _parse_int(value) -> int:
        """문자열을 int로 변환"""
        try:
            return int(float(str(value).strip())) if value else 0
        except (ValueError, TypeError):
            return 0


# 싱글톤 인스턴스
_slippage_calculator_instance: Optional[SlippageCalculator] = None


def get_slippage_calculator() -> SlippageCalculator:
    """SlippageCalculator 싱글톤 인스턴스 반환"""
    global _slippage_calculator_instance
    if _slippage_calculator_instance is None:
        _slippage_calculator_instance = SlippageCalculator()
    return _slippage_calculator_instance
