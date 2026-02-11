"""
시그널 생성 모듈

매수/매도 시그널을 생성합니다.
"""

import logging
from typing import Dict, Optional

from app.service.calculate_slippage import SlippageCalculator, SignalResult, OrderType, get_slippage_calculator

logger = logging.getLogger(__name__)


class SignalGenerator:
    """매수/매도 시그널 생성기"""

    # 긴급도 기준 (손절가 대비 현재가 위치)
    URGENCY_CRITICAL_PCT = 0.3   # 손절가까지 0.3% 이하
    URGENCY_HIGH_PCT = 0.5       # 손절가까지 0.5% 이하
    URGENCY_MEDIUM_PCT = 1.0     # 손절가까지 1% 이하

    def __init__(self, slippage_calculator: Optional[SlippageCalculator] = None):
        self.slippage_calculator = slippage_calculator or SlippageCalculator()

    def generate_buy_signal(
        self,
        stock_code: str,
        price_data: Dict,
        asking_price_data: Dict,
        order_quantity: int = 1
    ) -> SignalResult:
        """
        매수 시그널 생성

        실시간 가격 수신 시 즉시 매수하되, 슬리피지 최소화

        Args:
            stock_code: 종목 코드
            price_data: 체결가 데이터
            asking_price_data: 호가 데이터
            order_quantity: 주문 수량

        Returns:
            SignalResult: 시그널 결과
        """
        current_price = self.slippage_calculator._parse_float(price_data.get('STCK_PRPR', 0))

        # 슬리피지 계산
        slippage_result = self.slippage_calculator.calculate_buy_slippage(
            price_data, asking_price_data, order_quantity
        )

        # 매수 시그널 생성
        return SignalResult(
            signal_type="BUY",
            stock_code=stock_code,
            current_price=current_price,
            target_price=None,
            target_quantity=order_quantity,
            stop_loss_price=None,
            recommended_order_price=slippage_result.recommended_price,
            recommended_order_type=slippage_result.recommended_order_type,
            expected_slippage_pct=slippage_result.expected_slippage_pct,
            urgency="HIGH",  # 매수는 즉시 체결이 목표
            reason=f"즉시 매수 - 예상 슬리피지: {slippage_result.expected_slippage_pct:.3f}%"
        )

    def generate_sell_signal(
        self,
        stock_code: str,
        price_data: Dict,
        asking_price_data: Dict,
        target_price: float,
        stop_loss_price: float,
        order_quantity: int = 1
    ) -> Optional[SignalResult]:
        """
        매도 시그널 생성

        목표가 도달 또는 손절가 도달 시 매도 시그널 생성

        Args:
            stock_code: 종목 코드
            price_data: 체결가 데이터
            asking_price_data: 호가 데이터
            target_price: 목표가 (익절)
            stop_loss_price: 손절가
            order_quantity: 주문 수량

        Returns:
            SignalResult or None: 조건 미충족 시 None
        """
        current_price = self.slippage_calculator._parse_float(price_data.get('STCK_PRPR', 0))

        if current_price <= 0:
            return None

        # 슬리피지 계산
        slippage_result = self.slippage_calculator.calculate_sell_slippage(
            price_data, asking_price_data, order_quantity
        )

        # 1. 목표가 도달 체크 (익절)
        if current_price >= target_price:
            return SignalResult(
                signal_type="SELL",
                stock_code=stock_code,
                current_price=current_price,
                target_price=target_price,
                target_quantity=order_quantity,
                stop_loss_price=stop_loss_price,
                recommended_order_price=slippage_result.recommended_price,
                recommended_order_type=slippage_result.recommended_order_type,
                expected_slippage_pct=slippage_result.expected_slippage_pct,
                urgency="MEDIUM",  # 익절은 급하지 않음
                reason=f"목표가 도달 - 현재가 {current_price:,.0f} >= 목표가 {target_price:,.0f}"
            )

        # 2. 손절가 도달 체크
        if current_price <= stop_loss_price:
            # 긴급도 계산: 손절가 아래로 얼마나 내려갔는지
            loss_pct = (stop_loss_price - current_price) / stop_loss_price * 100

            if loss_pct >= 1.0:
                urgency = "CRITICAL"
            elif loss_pct >= 0.5:
                urgency = "HIGH"
            else:
                urgency = "MEDIUM"

            return SignalResult(
                signal_type="SELL",
                stock_code=stock_code,
                current_price=current_price,
                target_price=target_price,
                target_quantity=order_quantity,
                stop_loss_price=stop_loss_price,
                recommended_order_price=slippage_result.recommended_price,
                recommended_order_type=OrderType.MARKET if urgency == "CRITICAL" else slippage_result.recommended_order_type,
                expected_slippage_pct=slippage_result.expected_slippage_pct,
                urgency=urgency,
                reason=f"손절가 도달 - 현재가 {current_price:,.0f} <= 손절가 {stop_loss_price:,.0f}"
            )

        # 3. 손절가 접근 경고 (HOLD 시그널)
        distance_to_stop = (current_price - stop_loss_price) / current_price * 100

        if distance_to_stop <= self.URGENCY_MEDIUM_PCT:
            urgency = "HIGH" if distance_to_stop <= self.URGENCY_HIGH_PCT else "MEDIUM"

            return SignalResult(
                signal_type="HOLD",
                stock_code=stock_code,
                current_price=current_price,
                target_price=target_price,
                target_quantity=order_quantity,
                stop_loss_price=stop_loss_price,
                recommended_order_price=slippage_result.recommended_price,
                recommended_order_type=slippage_result.recommended_order_type,
                expected_slippage_pct=slippage_result.expected_slippage_pct,
                urgency=urgency,
                reason=f"손절가 접근 경고 - 손절가까지 {distance_to_stop:.2f}%"
            )

        return None  # 아무 조건도 충족하지 않음

    def generate_manual_sell_signal(
        self,
        stock_code: str,
        price_data: Dict,
        asking_price_data: Dict,
        order_type: str,
        order_price: float,
        order_quantity: int
    ) -> SignalResult:
        """
        수동 매도 시그널 생성 (Mock 모드용)

        사용자가 지정한 조건으로 매도 시그널 생성
        SlippageCalculator를 통해 최적 가격 계산

        Args:
            stock_code: 종목 코드
            price_data: 체결가 데이터
            asking_price_data: 호가 데이터
            order_type: 주문 유형 (LIMIT/MARKET)
            order_price: 주문 가격 (지정가인 경우)
            order_quantity: 주문 수량

        Returns:
            SignalResult: 시그널 결과
        """
        current_price = self.slippage_calculator._parse_float(price_data.get('STCK_PRPR', 0))

        # 슬리피지 계산
        slippage_result = self.slippage_calculator.calculate_sell_slippage(
            price_data, asking_price_data, order_quantity
        )

        # 주문 유형에 따른 가격/유형 결정
        if order_type == "MARKET":
            recommended_order_type = OrderType.MARKET
            recommended_price = slippage_result.recommended_price
        else:
            # 지정가: 사용자가 지정한 가격과 슬리피지 계산 결과 중 유리한 가격 선택
            # 매도의 경우 더 높은 가격이 유리
            recommended_order_type = OrderType.LIMIT
            recommended_price = max(order_price, slippage_result.recommended_price)

        return SignalResult(
            signal_type="SELL",
            stock_code=stock_code,
            current_price=current_price,
            target_price=None,
            target_quantity=order_quantity,
            stop_loss_price=None,
            recommended_order_price=recommended_price,
            recommended_order_type=recommended_order_type,
            expected_slippage_pct=slippage_result.expected_slippage_pct,
            urgency="HIGH",
            reason=f"사용자 수동 매도 - {order_type} {order_quantity}주"
        )


# 싱글톤 인스턴스
_signal_generator_instance: Optional[SignalGenerator] = None


def get_signal_generator() -> SignalGenerator:
    """SignalGenerator 싱글톤 인스턴스 반환"""
    global _signal_generator_instance
    if _signal_generator_instance is None:
        _signal_generator_instance = SignalGenerator()
    return _signal_generator_instance
