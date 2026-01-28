"""
Position 및 Order 데이터 모델

Redis에 저장되는 Position(보유 현황)과 Order(주문) 데이터 구조 정의
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from enum import Enum


class OrderStatus(str, Enum):
    """주문 상태"""
    PENDING = "pending"       # 주문 대기
    SUBMITTED = "submitted"   # 주문 제출됨
    ORDERED = "ordered"       # 주문 접수됨
    PARTIAL = "partial"       # 부분 체결
    FILLED = "filled"         # 전량 체결
    CANCELLED = "cancelled"   # 취소됨
    REJECTED = "rejected"     # 거부됨


class OrderType(str, Enum):
    """주문 유형"""
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class Position:
    """
    보유 현황 (Position)

    Redis 키: position:{daily_strategy_id}:{stock_code}
    TTL: 25시간 (90000초)
    """
    daily_strategy_id: int
    user_strategy_id: int
    stock_code: str
    stock_name: str

    # 현재 보유 정보
    holding_quantity: int = 0
    average_price: float = 0.0
    total_investment: float = 0.0

    # 누적 매수/매도 정보
    total_buy_quantity: int = 0
    total_buy_amount: float = 0.0
    total_sell_quantity: int = 0
    total_sell_amount: float = 0.0

    # 실현 손익
    realized_pnl: float = 0.0

    # 타임스탬프
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()

    def to_dict(self) -> dict:
        """딕셔너리로 변환"""
        return {
            "daily_strategy_id": self.daily_strategy_id,
            "user_strategy_id": self.user_strategy_id,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "holding_quantity": self.holding_quantity,
            "average_price": self.average_price,
            "total_investment": self.total_investment,
            "total_buy_quantity": self.total_buy_quantity,
            "total_buy_amount": self.total_buy_amount,
            "total_sell_quantity": self.total_sell_quantity,
            "total_sell_amount": self.total_sell_amount,
            "realized_pnl": self.realized_pnl,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Position":
        """딕셔너리에서 Position 생성"""
        created_at = data.get("created_at")
        updated_at = data.get("updated_at")

        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)

        return cls(
            daily_strategy_id=data["daily_strategy_id"],
            user_strategy_id=data["user_strategy_id"],
            stock_code=data["stock_code"],
            stock_name=data.get("stock_name", ""),
            holding_quantity=data.get("holding_quantity", 0),
            average_price=data.get("average_price", 0.0),
            total_investment=data.get("total_investment", 0.0),
            total_buy_quantity=data.get("total_buy_quantity", 0),
            total_buy_amount=data.get("total_buy_amount", 0.0),
            total_sell_quantity=data.get("total_sell_quantity", 0),
            total_sell_amount=data.get("total_sell_amount", 0.0),
            realized_pnl=data.get("realized_pnl", 0.0),
            created_at=created_at,
            updated_at=updated_at,
        )


@dataclass
class Order:
    """
    주문 정보 (Order)

    Redis 키: order:{order_id}
    인덱스: order:by_no:{order_no} → order_id
    인덱스: order:active:{daily_strategy_id}:{stock_code} (Set)
    TTL: 25시간 (90000초)
    """
    order_id: str
    daily_strategy_id: int
    user_strategy_id: int
    stock_code: str

    # 주문 정보
    order_type: OrderType  # BUY or SELL
    order_quantity: int
    order_price: float
    order_dvsn: str  # 00: 지정가, 01: 시장가

    # 주문 상태
    order_no: Optional[str] = None  # KIS API 주문번호
    status: OrderStatus = OrderStatus.PENDING

    # 체결 정보
    executed_quantity: int = 0
    executed_price: float = 0.0
    remaining_quantity: int = 0

    # 타임스탬프
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()
        if self.remaining_quantity == 0:
            self.remaining_quantity = self.order_quantity

    def to_dict(self) -> dict:
        """딕셔너리로 변환"""
        return {
            "order_id": self.order_id,
            "daily_strategy_id": self.daily_strategy_id,
            "user_strategy_id": self.user_strategy_id,
            "stock_code": self.stock_code,
            "order_type": self.order_type.value if isinstance(self.order_type, OrderType) else self.order_type,
            "order_quantity": self.order_quantity,
            "order_price": self.order_price,
            "order_dvsn": self.order_dvsn,
            "order_no": self.order_no,
            "status": self.status.value if isinstance(self.status, OrderStatus) else self.status,
            "executed_quantity": self.executed_quantity,
            "executed_price": self.executed_price,
            "remaining_quantity": self.remaining_quantity,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Order":
        """딕셔너리에서 Order 생성"""
        created_at = data.get("created_at")
        updated_at = data.get("updated_at")

        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)

        order_type = data.get("order_type")
        if isinstance(order_type, str):
            order_type = OrderType(order_type)

        status = data.get("status", "pending")
        if isinstance(status, str):
            status = OrderStatus(status)

        return cls(
            order_id=data["order_id"],
            daily_strategy_id=data["daily_strategy_id"],
            user_strategy_id=data["user_strategy_id"],
            stock_code=data["stock_code"],
            order_type=order_type,
            order_quantity=data.get("order_quantity", 0),
            order_price=data.get("order_price", 0.0),
            order_dvsn=data.get("order_dvsn", "00"),
            order_no=data.get("order_no"),
            status=status,
            executed_quantity=data.get("executed_quantity", 0),
            executed_price=data.get("executed_price", 0.0),
            remaining_quantity=data.get("remaining_quantity", 0),
            created_at=created_at,
            updated_at=updated_at,
        )

    @property
    def is_fully_executed(self) -> bool:
        """전량 체결 여부"""
        return self.executed_quantity >= self.order_quantity

    @property
    def is_active(self) -> bool:
        """활성 주문 여부 (미체결)"""
        return self.status in [OrderStatus.ORDERED, OrderStatus.PARTIAL, OrderStatus.SUBMITTED]
