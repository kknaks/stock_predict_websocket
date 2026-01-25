"""
KIS 웹소켓 메시지 모델
"""

from datetime import datetime
from typing import List, Literal, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator


class TokenInfo(BaseModel):
    """KIS 토큰 정보"""

    access_token: str = Field(..., description="OAuth 접근 토큰")
    token_type: str = Field(default="Bearer", description="토큰 타입")
    expires_in: int = Field(..., description="토큰 만료 시간(초)")
    ws_token: str = Field(..., alias="ws_token", description="WebSocket 접속키")
    ws_expires_in: int = Field(..., alias="ws_expires_in", description="WebSocket 토큰 만료 시간(초)")

    class Config:
        populate_by_name = True


class AccountConfig(BaseModel):
    """계좌 설정"""

    account_no: str = Field(..., description="계좌번호")
    account_product_code: str = Field(..., description="계좌상품코드")


class UserAccount(BaseModel):
    """사용자 계좌 정보"""

    account_id: int = Field(..., description="계좌 ID")
    account_type: str = Field(..., description="계좌 유형 (real/paper/mock)")
    account_no: str = Field(..., description="계좌번호")
    account_product_code: str = Field(..., description="계좌상품코드")
    account_balance: Optional[float] = Field(default=None, description="계좌 잔액")
    app_key: str = Field(..., description="앱키")
    app_secret: str = Field(..., description="앱 시크릿")
    access_token: str = Field(..., description="액세스 토큰")
    ws_token: str = Field(..., description="웹소켓 토큰")
    expires_in: int = Field(..., description="토큰 만료 시간(초)")
    hts_id: Optional[str] = Field(default=None, description="고객ID (체결통보 구독용)")

    @field_validator("account_type", mode="before")
    @classmethod
    def lowercase_account_type(cls, v: str) -> str:
        """account_type을 소문자로 변환 (DB에서 대문자로 올 수 있음)"""
        return v.lower() if isinstance(v, str) else v


class UserInfo(BaseModel):
    """사용자 정보"""

    user_id: int = Field(..., description="사용자 ID")
    nickname: str = Field(..., description="닉네임")
    account: UserAccount = Field(..., description="계좌 정보")
    strategies: List[Dict[str, Any]] = Field(default_factory=list, description="전략 리스트")


class StartConfig(BaseModel):
    """START 명령 설정"""

    env_dv: str = Field(..., description="환경구분 (real: 실전, demo: 모의)")
    appkey: str = Field(..., description="앱키")
    is_mock: bool = Field(default=True, description="모의 사용자 여부")
    accounts: List[AccountConfig] = Field(default_factory=list, description="계좌 리스트")
    stocks: List[str] = Field(default_factory=list, description="모니터링 종목 코드 리스트")
    strategies: List[Dict[str, Any]] = Field(default_factory=list, description="전략 리스트")
    users: List[UserInfo] = Field(default_factory=list, description="사용자 리스트 (ACCOUNT 타겟용)")


class StartCommand(BaseModel):
    """START 명령 메시지"""

    command: Literal["START"] = Field(..., description="명령 타입")
    timestamp: datetime = Field(..., description="명령 시간")
    target: str = Field(default="ALL", description="대상 (ALL, PRICE, ACCOUNT, account_no)")
    tokens: Optional[TokenInfo] = Field(default=None, description="KIS 토큰 정보 (PRICE 타겟용)")
    config: StartConfig = Field(..., description="설정 정보")

class ClosingCommand(BaseModel):
    """CLOSING 명령 메시지"""

    command: Literal["CLOSING"] = Field(..., description="명령 타입")
    timestamp: datetime = Field(..., description="명령 시간")
    target: str = Field(default="ALL", description="대상 (ALL, PRICE, ACCOUNT, account_no)")


class StopCommand(BaseModel):
    """STOP 명령 메시지"""

    command: Literal["STOP"] = Field(..., description="명령 타입")
    timestamp: datetime = Field(..., description="명령 시간")
    target: str = Field(default="ALL", description="대상 (ALL, PRICE, ACCOUNT, account_no)")


class WebSocketCommand(BaseModel):
    """웹소켓 메시지 (Union 타입)"""

    command: str = Field(..., description="명령 타입")
    timestamp: datetime = Field(..., description="명령 시간")
    target: str = Field(default="ALL", description="대상")
    tokens: Optional[TokenInfo] = Field(default=None, description="KIS 토큰 정보 (START 명령에만 필요)")
    config: Optional[Dict[str, Any]] = Field(default=None, description="설정 정보 (START 명령에만 필요)")

    def to_start_command(self) -> StartCommand:
        """START 명령으로 변환"""
        if self.command != "START":
            raise ValueError(f"Cannot convert {self.command} to StartCommand")
        if not self.config:
            raise ValueError("config is required for START command")

        # PRICE 타겟인 경우 tokens 필수
        if self.target == "PRICE" and not self.tokens:
            raise ValueError("tokens is required for PRICE target")

        return StartCommand(
            command="START",
            timestamp=self.timestamp,
            target=self.target,
            tokens=self.tokens,
            config=StartConfig(**self.config),
        )

    def to_stop_command(self) -> StopCommand:
        """STOP 명령으로 변환"""
        if self.command != "STOP":
            raise ValueError(f"Cannot convert {self.command} to StopCommand")

        return StopCommand(
            command="STOP",
            timestamp=self.timestamp,
            target=self.target,
        )

    def to_closing_command(self) -> ClosingCommand:
        """CLOSING 명령으로 변환"""
        if self.command != "CLOSING":
            raise ValueError(f"Cannot convert {self.command} to ClosingCommand")

        return ClosingCommand(
            command="CLOSING",
            timestamp=self.timestamp,
            target=self.target,
        )