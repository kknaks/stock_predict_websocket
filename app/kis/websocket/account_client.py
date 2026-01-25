"""
KIS 계좌 WebSocket Client

실시간 체결통보 수신
"""

import asyncio
import base64
import json
import logging
from datetime import datetime
from typing import Optional

import websockets
from websockets.client import WebSocketClientProtocol
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

from app.kis.websocket.exceptions import (
    WebSocketConnectionError,
    WebSocketAuthError,
    WebSocketMessageError,
    WebSocketSubscriptionError,
    WebSocketTimeoutError,
)
from app.kis.websocket.error_stats import get_error_stats
from app.kis.websocket.redis_manager import get_redis_manager
from app.kafka.order_signal_producer import get_order_signal_producer

logger = logging.getLogger(__name__)


class AccountWebSocketClient:
    """계좌 웹소켓 클라이언트"""

    def __init__(
        self,
        ws_token: str,
        appkey: str,
        env_dv: str,
        account_no: str,
        account_product_code: str,
        account_type: str = "mock",
        is_mock: bool = False,
        access_token: Optional[str] = None,
        user_id: Optional[int] = None,
        user_strategy_ids: Optional[list] = None,
        hts_id: Optional[str] = None,
    ):
        """
        Args:
            ws_token: WebSocket 접속키
            appkey: 앱키
            env_dv: 환경구분 (real: 실전, demo: 모의)
            account_no: 계좌번호
            account_product_code: 계좌상품코드
            account_type: 계좌유형 (real: 실전, paper: 모의, mock: 테스트)
            access_token: OAuth 액세스 토큰 (API 주문용)
            user_id: 사용자 ID
            user_strategy_ids: 사용자 전략 ID 리스트
            hts_id: 고객ID (체결통보 구독용, tr_key에 사용)
        """
        self.ws_token = ws_token
        self.appkey = appkey
        self.env_dv = env_dv
        self.account_no = account_no
        self.account_product_code = account_product_code
        self.account_type = account_type
        self.is_mock = is_mock
        self.access_token = access_token
        self.user_id = user_id
        self.user_strategy_ids = user_strategy_ids or []
        self.hts_id = hts_id

        # WebSocket URL 설정 (account_type에 따라)
        # real: 실전투자 (21000)
        # paper: 모의투자 (31000)
        if account_type == "real":
            self.ws_url = "ws://ops.koreainvestment.com:21000"
        else:
            # paper (모의투자)
            self.ws_url = "ws://ops.koreainvestment.com:31000"

        self._websocket: Optional[WebSocketClientProtocol] = None
        self._running = False
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._error_stats = get_error_stats()
        self._connection_failed = False
        self._redis_manager = get_redis_manager()
        self._order_signal_producer = get_order_signal_producer()
        
        # 복호화 키 및 IV 저장 (문자열로 저장, MCP 공식 코드 기준)
        self._decrypt_key: Optional[str] = None
        self._decrypt_iv: Optional[str] = None

    async def connect_and_run(self) -> None:
        """연결 및 실행"""
        # 시작 시 running 플래그 설정
        self._running = True

        # 재연결 시 Redis에서 정보 조회 시도
        if self._reconnect_attempts > 0:
            await self._load_from_redis()

        while self._reconnect_attempts < self._max_reconnect_attempts and self._running:
            try:
                await self._connect()
                await self._subscribe()
                await self._run()
                # _run()이 정상 종료된 경우 (예외 없이) 루프 종료
                break
            except websockets.ConnectionClosed as e:
                logger.warning(f"Account websocket connection closed: {self.account_no}")
                self._error_stats.record_error(
                    WebSocketConnectionError(
                        f"Account websocket connection closed: {self.account_no}",
                        details={
                            "account_no": self.account_no,
                            "close_code": e.code,
                            "close_reason": e.reason
                        }
                    )
                )
                # Redis 상태 업데이트
                self._redis_manager.update_connection_status(
                    "account",
                    account_no=self.account_no,
                    status="disconnected",
                    reconnect_attempts=self._reconnect_attempts
                )
                if self._running:
                    await self._reconnect()
                else:
                    break
            except WebSocketAuthError as e:
                logger.error(f"Account websocket auth error ({self.account_no}): {e}")
                self._error_stats.record_error(e, error_type=None, error_code=None, details={"account_no": self.account_no})
                self._connection_failed = True
                self._running = False
                # Redis에서 삭제 (인증 에러는 재연결 불가)
                self._redis_manager.delete_account_connection(self.account_no)
                break  # 인증 에러는 재시도 불가
            except WebSocketConnectionError as e:
                logger.error(f"Account websocket connection error ({self.account_no}): {e}")
                self._error_stats.record_error(e, error_type=None, error_code=None, details={"account_no": self.account_no})
                # Redis 상태 업데이트
                self._redis_manager.update_connection_status(
                    "account",
                    account_no=self.account_no,
                    status="reconnecting",
                    reconnect_attempts=self._reconnect_attempts
                )
                if self._running:
                    await self._reconnect()
                else:
                    break
            except Exception as e:
                logger.error(f"Account websocket error ({self.account_no}): {e}", exc_info=True)
                self._error_stats.record_error(e, error_type=None, error_code=None, details={"account_no": self.account_no})
                if self._running:
                    await self._reconnect()
                else:
                    break

    async def _connect(self) -> None:
        """WebSocket 연결"""
        try:
            uri = f"{self.ws_url}?appkey={self.appkey}&appsecret={self.ws_token}&custtype=P"
            logger.info(
                f"Connecting to account websocket ({self.account_no}): "
                f"env_dv={self.env_dv}, "
                f"hts_id={self.hts_id}, "
                f"ws_token_length={len(self.ws_token) if self.ws_token else 0}"
            )

            try:
                self._websocket = await asyncio.wait_for(
                    websockets.connect(uri),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                raise WebSocketTimeoutError(
                    f"Account websocket connection timeout: {self.account_no}",
                    details={"account_no": self.account_no, "uri": uri[:50]}
                )
            except websockets.InvalidStatusCode as e:
                # 401, 403 등 인증 관련 에러
                logger.error(
                    f"Account websocket invalid status code ({self.account_no}): "
                    f"status_code={e.status_code}, "
                    f"response={getattr(e, 'response', None)}"
                )
                if e.status_code in (401, 403):
                    raise WebSocketAuthError(
                        f"Account websocket authentication failed: {e.status_code}",
                        details={"account_no": self.account_no, "status_code": e.status_code}
                    )
                else:
                    raise WebSocketConnectionError(
                        f"Account websocket connection failed: {e.status_code}",
                        details={"account_no": self.account_no, "status_code": e.status_code}
                    )
            except websockets.ConnectionClosed as e:
                logger.error(
                    f"Account websocket connection closed during connect ({self.account_no}): "
                    f"code={e.code}, reason={e.reason}"
                )
                raise WebSocketConnectionError(
                    f"Account websocket connection closed: {self.account_no}",
                    details={"account_no": self.account_no, "close_code": e.code, "close_reason": e.reason}
                )

            self._running = True
            self._reconnect_attempts = 0
            self._connection_failed = False
            logger.info(f"Account websocket connected: {self.account_no}")

            # Redis에 연결 정보 저장
            self._redis_manager.save_account_connection(
                account_no=self.account_no,
                ws_token=self.ws_token,
                appkey=self.appkey,
                env_dv=self.env_dv,
                is_mock=self.is_mock,
                account_type=self.account_type,
                account_product_code=self.account_product_code,
                access_token=self.access_token,
                user_id=self.user_id,
                user_strategy_ids=self.user_strategy_ids,
                hts_id=self.hts_id,
                status="connected",
                reconnect_attempts=0,
            )

        except (WebSocketAuthError, WebSocketConnectionError, WebSocketTimeoutError):
            raise
        except Exception as e:
            logger.error(f"Failed to connect account websocket ({self.account_no}): {e}")
            raise WebSocketConnectionError(
                f"Unexpected connection error: {str(e)}",
                details={"account_no": self.account_no, "error_type": type(e).__name__}
            ) from e

    async def _subscribe(self) -> None:
        """체결통보 구독"""
        if not self._websocket:
            raise WebSocketConnectionError("WebSocket not connected")

        try:
            # tr_key 결정: hts_id 우선, 없으면 계좌번호 사용
            tr_key = self.hts_id if self.hts_id else f"{self.account_no};{self.account_product_code}"

            # TR_ID 결정 (account_type에 따라)
            # real: H0STCNI0 (실전)
            # paper: H0STCNI9 (모의)
            tr_id = "H0STCNI0" if self.account_type == "real" else "H0STCNI9"

            # 구독 메시지 생성
            subscribe_message = {
                "header": {
                    "approval_key": self.ws_token,
                    "custtype": "P",
                    "tr_type": "1",  # 등록
                    "content-type": "utf-8",
                },
                "body": {
                    "input": {
                        "tr_id": tr_id,  # 실시간 체결통보
                        "tr_key": tr_key,
                    }
                }
            }

            logger.info(
                f"Sending subscription message ({self.account_no}): "
                f"tr_id={tr_id}, tr_key={tr_key}, "
                f"account_type={self.account_type}, "
                f"hts_id={self.hts_id}"
            )

            try:
                await asyncio.wait_for(
                    self._websocket.send(json.dumps(subscribe_message)),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                raise WebSocketTimeoutError(
                    f"Account subscription message send timeout: {self.account_no}",
                    details={"account_no": self.account_no}
                )
            except websockets.ConnectionClosed as e:
                logger.error(
                    f"Account websocket connection closed during subscription ({self.account_no}): "
                    f"code={e.code}, reason={e.reason}, "
                    f"tr_key={tr_key}"
                )
                raise WebSocketConnectionError(
                    f"Account websocket connection closed during subscription: {self.account_no}",
                    details={
                        "account_no": self.account_no,
                        "close_code": e.code,
                        "close_reason": e.reason,
                        "tr_key": tr_key
                    }
                )

            # 구독 메시지 전송 후 서버 응답 확인 (최대 3초 대기)
            try:
                response = await asyncio.wait_for(
                    self._websocket.recv(),
                    timeout=3.0
                )
                response_data = json.loads(response)
                logger.info(
                    f"Subscription response ({self.account_no}): {response_data}"
                )
                
                # 응답에서 에러 확인 및 복호화 키/IV 저장
                if isinstance(response_data, dict):
                    header = response_data.get("header", {})
                    body = response_data.get("body", {})
                    
                    # 에러 코드 확인
                    if "tr_cd" in header and header["tr_cd"] != "000000":
                        error_msg = body.get("rt_msg") or body.get("msg") or "Unknown error"
                        logger.error(
                            f"Subscription failed ({self.account_no}): "
                            f"tr_cd={header.get('tr_cd')}, msg={error_msg}"
                        )
                        raise WebSocketSubscriptionError(
                            f"Account subscription failed: {error_msg}",
                            details={
                                "account_no": self.account_no,
                                "tr_cd": header.get("tr_cd"),
                                "error_msg": error_msg,
                                "tr_key": tr_key
                            }
                        )
                    
                    # 복호화 키 및 IV 저장
                    output = body.get("output", {})
                    if output:
                        key_str = output.get("key")
                        iv_str = output.get("iv")
                        if key_str and iv_str:
                            try:
                                # 문자열 그대로 저장 (MCP 공식 코드 기준)
                                self._decrypt_key = key_str
                                self._decrypt_iv = iv_str
                                logger.info(
                                    f"Decryption key and IV saved ({self.account_no}): "
                                    f"key_length={len(self._decrypt_key)}, "
                                    f"iv_length={len(self._decrypt_iv)}"
                                )
                                
                                # Redis에 복호화 키 저장 (Base64 문자열로 저장)
                                self._redis_manager.save_account_connection(
                                    account_no=self.account_no,
                                    ws_token=self.ws_token,
                                    appkey=self.appkey,
                                    env_dv=self.env_dv,
                                    is_mock=self.is_mock,
                                    account_type=self.account_type,
                                    account_product_code=self.account_product_code,
                                    access_token=self.access_token,
                                    user_id=self.user_id,
                                    user_strategy_ids=self.user_strategy_ids,
                                    hts_id=self.hts_id,
                                    status="connected",
                                    reconnect_attempts=0,
                                    decrypt_key=key_str,  # Base64 문자열 그대로 저장
                                    decrypt_iv=iv_str,    # Base64 문자열 그대로 저장
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Failed to decode encryption key/IV ({self.account_no}): {e}"
                                )
                        else:
                            logger.warning(
                                f"No encryption key/IV in subscription response ({self.account_no})"
                            )
            except asyncio.TimeoutError:
                # 응답이 없어도 계속 진행 (일부 API는 응답이 없을 수 있음)
                logger.warning(
                    f"No subscription response received ({self.account_no}), continuing..."
                )
            except json.JSONDecodeError as e:
                logger.warning(
                    f"Failed to parse subscription response ({self.account_no}): {e}, "
                    f"response={response[:100] if 'response' in locals() else 'N/A'}"
                )
            except WebSocketSubscriptionError:
                raise

            logger.info(f"Subscribed to account websocket: {self.account_no} (tr_key={tr_key})")

        except (WebSocketTimeoutError, WebSocketConnectionError, WebSocketSubscriptionError):
            raise
        except Exception as e:
            logger.error(f"Failed to subscribe account websocket ({self.account_no}): {e}", exc_info=True)
            self._error_stats.record_error(
                WebSocketSubscriptionError(
                    f"Account subscription failed: {str(e)}",
                    details={"account_no": self.account_no, "error_type": type(e).__name__}
                )
            )
            raise WebSocketSubscriptionError(
                f"Account subscription failed: {str(e)}",
                details={"account_no": self.account_no}
            ) from e

    async def _run(self) -> None:
        """메시지 수신 루프"""
        if not self._websocket:
            raise WebSocketConnectionError("WebSocket not connected")

        logger.info(f"Starting account websocket message loop: {self.account_no}")

        try:
            async for message in self._websocket:
                if not self._running:
                    break

                try:
                    await self._handle_message(message)
                except WebSocketMessageError as e:
                    # 메시지 에러는 로깅 후 계속 진행
                    logger.warning(f"Message error ({self.account_no}, continuing): {e}")
                    self._error_stats.record_error(e, error_type=None, error_code=None, details={"account_no": self.account_no})
                except Exception as e:
                    logger.error(f"Error handling message ({self.account_no}): {e}", exc_info=True)
                    self._error_stats.record_error(
                        WebSocketMessageError(
                            f"Unexpected message error: {str(e)}",
                            details={"account_no": self.account_no, "error_type": type(e).__name__}
                        )
                    )

        except websockets.ConnectionClosed as e:
            logger.warning(f"Account websocket connection closed: {self.account_no}")
            self._error_stats.record_error(
                WebSocketConnectionError(
                    f"Account websocket connection closed during message loop: {self.account_no}",
                    details={
                        "account_no": self.account_no,
                        "close_code": e.code,
                        "close_reason": e.reason
                    }
                )
            )
            raise WebSocketConnectionError(
                f"Account websocket connection closed: {self.account_no}",
                details={"account_no": self.account_no, "close_code": e.code, "close_reason": e.reason}
            )
        except Exception as e:
            logger.error(f"Account websocket run error ({self.account_no}): {e}", exc_info=True)
            self._error_stats.record_error(e, details={"account_no": self.account_no})
            raise WebSocketConnectionError(
                f"Account websocket run error: {str(e)}",
                details={"account_no": self.account_no, "error_type": type(e).__name__}
            ) from e

    def _decrypt_message(self, encrypted_data: str) -> str:
        """
        AES256 CBC 모드로 암호화된 메시지 복호화 (MCP 공식 코드 기준)

        Args:
            encrypted_data: Base64로 인코딩된 암호화된 데이터

        Returns:
            복호화된 문자열 (^로 구분된 필드들)
        """
        if not self._decrypt_key or not self._decrypt_iv:
            raise ValueError("Decryption key or IV not available")

        try:
            # MCP 공식 코드: key.encode('utf-8'), iv.encode('utf-8') 사용
            cipher = AES.new(
                self._decrypt_key.encode('utf-8'),
                AES.MODE_CBC,
                self._decrypt_iv.encode('utf-8')
            )
            decrypted_bytes = cipher.decrypt(base64.b64decode(encrypted_data))
            decrypted_text = unpad(decrypted_bytes, AES.block_size).decode('utf-8')

            return decrypted_text
        except Exception as e:
            logger.error(f"Failed to decrypt message ({self.account_no}): {e}")
            raise

    def _parse_execution_notice(self, decrypted_data: str, tr_id: str = "", data_count: str = "") -> list[dict]:
        """
        복호화된 체결통보 메시지 파싱 (MCP 공식 코드 기준)

        MCP 공식 코드에서는 pd.read_csv를 사용하여 여러 건을 처리합니다.
        여러 건의 데이터는 줄바꿈(\\n)으로 구분되고, 각 건의 필드는 ^로 구분됩니다.

        응답데이터 컬럼 (26개):
        CUST_ID | ACNT_NO | ODER_NO | ODER_QTY | SELN_BYOV_CLS | RCTF_CLS |
        ODER_KIND | ODER_COND | STCK_SHRN_ISCD | CNTG_QTY | CNTG_UNPR |
        STCK_CNTG_HOUR | RFUS_YN | CNTG_YN | ACPT_YN | BRNC_NO | ACNT_NO2 |
        ACNT_NAME | ORD_COND_PRC | ORD_EXG_GB | POPUP_YN | FILLER | CRDT_CLS |
        CRDT_LOAN_DATE | CNTG_ISNM40 | ODER_PRC

        Args:
            decrypted_data: 복호화된 메시지 (^로 구분된 필드들, 여러 건은 \\n으로 구분)
            tr_id: TR ID
            data_count: 데이터 건수

        Returns:
            파싱된 체결통보 데이터 딕셔너리 리스트
        """
        # 컬럼명 정의 (MCP 공식 코드 기준 26개)
        columns = [
            "CUST_ID", "ACNT_NO", "ODER_NO", "ODER_QTY", "SELN_BYOV_CLS", "RCTF_CLS",
            "ODER_KIND", "ODER_COND", "STCK_SHRN_ISCD", "CNTG_QTY", "CNTG_UNPR",
            "STCK_CNTG_HOUR", "RFUS_YN", "CNTG_YN", "ACPT_YN", "BRNC_NO", "ACNT_NO2",
            "ACNT_NAME", "ORD_COND_PRC", "ORD_EXG_GB", "POPUP_YN", "FILLER", "CRDT_CLS",
            "CRDT_LOAN_DATE", "CNTG_ISNM40", "ODER_PRC"
        ]

        result_list = []

        # 여러 건의 데이터는 줄바꿈으로 구분 (MCP 공식 코드: pd.read_csv 사용)
        lines = decrypted_data.strip().split('\n') if '\n' in decrypted_data else [decrypted_data]

        for line in lines:
            if not line.strip():
                continue

            # ^로 구분된 필드들 파싱
            data_fields = line.split('^')

            # 딕셔너리로 변환
            parsed_data = {}
            for i, column in enumerate(columns):
                if i < len(data_fields):
                    parsed_data[column] = data_fields[i].strip()
                else:
                    parsed_data[column] = ""

            # 추가 메타데이터
            parsed_data["_meta"] = {
                "tr_id": tr_id,
                "data_count": data_count,
            }

            result_list.append(parsed_data)

        return result_list

    async def _handle_message(self, message: str) -> None:
        """메시지 처리 (체결통보 수신) - MCP 공식 코드 기준"""
        try:
            # 먼저 JSON 파싱 시도
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                # JSON이 아니면 실시간 데이터 메시지
                # MCP 공식 코드: if raw[0] in ["0", "1"]:
                if message[0] in ["0", "1"]:
                    # MCP 공식 코드: d1 = raw.split("|")
                    parts = message.split("|")
                    if len(parts) < 4:
                        logger.warning(
                            f"Invalid message format ({self.account_no}): expected at least 4 parts, got {len(parts)}"
                        )
                        return

                    is_encrypted = parts[0]  # "0" 또는 "1"
                    tr_id = parts[1]
                    data_count = parts[2]
                    encrypted_data = parts[3]  # 암호화된 데이터 부분

                    # MCP 공식 코드: if dm.get("encrypt", None) == "Y":
                    if is_encrypted == "1":
                        # 암호화된 메시지 복호화
                        if not self._decrypt_key or not self._decrypt_iv:
                            logger.warning(
                                f"Encrypted message received but no decryption key available ({self.account_no})"
                            )
                            return

                        try:
                            # MCP 공식 코드: d = aes_cbc_base64_dec(dm["key"], dm["iv"], d)
                            decrypted_data = self._decrypt_message(encrypted_data)
                            parsed_data_list = self._parse_execution_notice(decrypted_data, tr_id, data_count)

                            logger.info(
                                f"📨 복호화된 체결통보 ({self.account_no}): "
                                f"tr_id={tr_id}, "
                                f"건수={len(parsed_data_list)}"
                            )

                            # 파싱된 데이터로 체결통보 처리 (여러 건 처리)
                            for parsed_data in parsed_data_list:
                                logger.info(
                                    f"  → 종목코드={parsed_data.get('STCK_SHRN_ISCD')}, "
                                    f"주문번호={parsed_data.get('ODER_NO')}, "
                                    f"체결수량={parsed_data.get('CNTG_QTY')}, "
                                    f"체결단가={parsed_data.get('CNTG_UNPR')}"
                                )
                                await self._process_execution_notice(parsed_data)
                            return
                        except Exception as e:
                            logger.error(
                                f"Failed to decrypt/parse message ({self.account_no}): {e}, "
                                f"message_preview={message[:100]}"
                            )
                            return
                    else:
                        # 암호화되지 않은 메시지 (is_encrypted == "0")
                        parsed_data_list = self._parse_execution_notice(encrypted_data, tr_id, data_count)
                        for parsed_data in parsed_data_list:
                            await self._process_execution_notice(parsed_data)
                        return
                else:
                    # 알 수 없는 형식
                    logger.warning(
                        f"Unknown message format ({self.account_no}): {message[:100]}"
                    )
                    return
            
            # JSON 형식 메시지 처리 (기존 로직)
            
            # 체결통보 메시지 구조 확인
            # KIS API 체결통보 형식:
            # - header: tr_id, tr_key 등
            # - body: 체결 정보 (종목코드, 주문번호, 체결가격, 체결수량 등)
            
            header = data.get("header", {})
            body = data.get("body", {})
            
            # body에 암호화된 데이터가 있는 경우 복호화
            if isinstance(body, dict) and "output" in body:
                output = body.get("output", {})
                encrypted_data = output.get("data") or output.get("encrypted_data")
                if encrypted_data and self._decrypt_key and self._decrypt_iv:
                    try:
                        decrypted_data = self._decrypt_message(encrypted_data)
                        parsed_data_list = self._parse_execution_notice(decrypted_data)
                        for parsed_data in parsed_data_list:
                            await self._process_execution_notice(parsed_data)
                        return
                    except Exception as e:
                        logger.error(
                            f"Failed to decrypt body data ({self.account_no}): {e}"
                        )
            
            tr_id = header.get("tr_id", "")
            tr_key = header.get("tr_key", "")
            
            # 체결통보 확인 (H0STCNI0: 실전, H0STCNI9: 모의)
            if tr_id in ("H0STCNI0", "H0STCNI9") or "체결" in str(body):
                logger.info(
                    f"📨 체결통보 수신 (JSON 형식) ({self.account_no}): "
                    f"tr_id={tr_id}, "
                    f"tr_key={tr_key}, "
                    f"body={body}"
                )
                
                # JSON 형식의 body를 파싱된 형식으로 변환
                if isinstance(body, dict):
                    # JSON 형식의 body를 KIS API 표준 형식으로 변환
                    parsed_data = {
                        "STCK_SHRN_ISCD": body.get("pdno") or body.get("종목코드") or body.get("stock_code") or "",
                        "ODER_NO": body.get("odno") or body.get("주문번호") or body.get("order_no") or "",
                        "CNTG_QTY": str(body.get("exec_qty") or body.get("체결수량") or body.get("CNTG_QTY") or ""),
                        "CNTG_UNPR": str(body.get("exec_price") or body.get("체결가격") or body.get("체결단가") or body.get("CNTG_UNPR") or ""),
                        "SELN_BYOV_CLS": body.get("ord_psbl_cd") or body.get("주문가능코드") or body.get("SELN_BYOV_CLS") or "",
                        "CNTG_YN": body.get("exec_type") or body.get("체결구분") or body.get("CNTG_YN") or "2",  # 기본값: 체결통보
                        "ACNT_NO": body.get("account_no") or self.account_no,
                        "_meta": {
                            "tr_id": tr_id,
                            "tr_key": tr_key,
                        }
                    }
                    await self._process_execution_notice(parsed_data)
            else:
                # 기타 메시지 (PING, PONG 등)
                logger.debug(
                    f"Received account message ({self.account_no}): "
                    f"tr_id={tr_id}, "
                    f"data={data}"
                )

        except json.JSONDecodeError as e:
            raise WebSocketMessageError(
                f"Failed to parse JSON message: {str(e)}",
                details={"account_no": self.account_no, "message_preview": message[:100]}
            )

    async def _process_execution_notice(self, parsed_data: dict) -> None:
        """
        파싱된 체결통보 데이터 처리
        
        Args:
            parsed_data: _parse_execution_notice에서 파싱된 데이터 또는 JSON 형식의 body를 변환한 데이터
        """
        try:
            # 컬럼명 매핑 (영문 -> 한글/영문 혼용)
            stock_code = parsed_data.get("STCK_SHRN_ISCD", "").strip()
            order_no = parsed_data.get("ODER_NO", "").strip()
            exec_qty_str = parsed_data.get("CNTG_QTY", "").strip()
            exec_price_str = parsed_data.get("CNTG_UNPR", "").strip()
            order_type = parsed_data.get("SELN_BYOV_CLS", "").strip()  # "01": 매도, "02": 매수
            exec_type = parsed_data.get("CNTG_YN", "").strip()  # "1": 접수통보, "2": 체결통보
            account_no = parsed_data.get("ACNT_NO", "").strip() or self.account_no
            
            # 체결통보인지 확인 (CNTG_YN이 "2"인 경우만 체결통보, 또는 값이 없으면 기본값으로 체결통보로 간주)
            if exec_type and exec_type != "2":
                logger.debug(
                    f"접수통보 수신 (체결 아님) ({self.account_no}): "
                    f"종목={stock_code}, 주문번호={order_no}, CNTG_YN={exec_type}"
                )
                return
            
            logger.info(
                f"📨 체결통보 처리 시작 ({self.account_no}): "
                f"종목={stock_code}, "
                f"주문번호={order_no}, "
                f"체결수량={exec_qty_str}, "
                f"체결단가={exec_price_str}, "
                f"매도매수구분={order_type}"
            )
            
            # 체결 정보 파싱
            if stock_code and order_no and exec_price_str and exec_qty_str:
                try:
                    exec_price = float(exec_price_str)
                    exec_qty = int(exec_qty_str)
                    # "02": 매수, "01": 매도 (KIS API 표준)
                    # 또는 "00": 매수, "01": 매도 (레거시)
                    is_buy = order_type in ["02", "00"] or (not order_type and exec_type not in ["매도", "SELL"])
                    
                    # 1. order_no로 Order 조회
                    order_data = self._redis_manager.get_order_by_order_no(order_no)
                    
                    if order_data:
                        # Order가 있는 경우 (새 로직)
                        daily_strategy_id = order_data.get("daily_strategy_id")
                        user_strategy_id = order_data.get("user_strategy_id")
                        order_quantity = order_data.get("order_quantity", 0)
                        order_id = order_data.get("order_id")
                        
                        # 2. Position 업데이트
                        if is_buy:
                            self._redis_manager.update_position_buy(
                                daily_strategy_id=daily_strategy_id,
                                stock_code=stock_code,
                                exec_qty=exec_qty,
                                exec_price=exec_price
                            )
                        else:
                            self._redis_manager.update_position_sell(
                                daily_strategy_id=daily_strategy_id,
                                stock_code=stock_code,
                                exec_qty=exec_qty,
                                exec_price=exec_price
                            )
                        
                        # 3. Order 상태 업데이트
                        prev_exec_qty = order_data.get("executed_quantity", 0)
                        total_exec_qty = prev_exec_qty + exec_qty
                        is_fully_executed = total_exec_qty >= order_quantity
                        
                        self._redis_manager.update_order_status(
                            order_id=order_id,
                            status="filled" if is_fully_executed else "partial",
                            executed_quantity=total_exec_qty,
                            executed_price=exec_price
                        )
                        
                        # 4. 업데이트된 Position 조회
                        updated_position = self._redis_manager.get_position(daily_strategy_id, stock_code)
                        
                        # 5. Kafka 메시지 발행
                        execution_message = {
                            "timestamp": datetime.now().isoformat(),
                            "user_strategy_id": user_strategy_id,
                            "daily_strategy_id": daily_strategy_id,
                            "stock_name": updated_position.get("stock_name", "") if updated_position else "",
                            "order_type": "BUY" if is_buy else "SELL",
                            "stock_code": stock_code,
                            "order_no": order_no,
                            "order_quantity": order_quantity,
                            "order_price": order_data.get("order_price", 0.0),
                            "order_dvsn": order_data.get("order_dvsn", ""),
                            "account_no": self.account_no,
                            "is_mock": self.is_mock,
                            "status": "executed" if is_fully_executed else "partially_executed",
                            "executed_quantity": exec_qty,
                            "executed_price": exec_price,
                            "total_executed_quantity": total_exec_qty,
                            "total_executed_price": exec_price,
                            "remaining_quantity": order_quantity - total_exec_qty,
                            "is_fully_executed": is_fully_executed,
                            "position": {
                                "holding_quantity": updated_position.get("holding_quantity", 0) if updated_position else 0,
                                "average_price": updated_position.get("average_price", 0.0) if updated_position else 0.0,
                                "total_buy_quantity": updated_position.get("total_buy_quantity", 0) if updated_position else 0,
                                "total_sell_quantity": updated_position.get("total_sell_quantity", 0) if updated_position else 0,
                                "realized_pnl": updated_position.get("realized_pnl", 0.0) if updated_position else 0.0,
                            } if updated_position else None,
                        }
                        await self._order_signal_producer.send_order_result(execution_message)
                        
                        logger.info(
                            f"✅ 체결통보 처리 완료: "
                            f"daily_strategy_id={daily_strategy_id}, "
                            f"종목={stock_code}, "
                            f"주문번호={order_no}, "
                            f"{'매수' if is_buy else '매도'} "
                            f"체결수량={exec_qty}, "
                            f"체결가격={exec_price:,.0f}, "
                            f"누적체결수량={total_exec_qty}, "
                            f"전량체결={is_fully_executed}"
                        )
                    
                    # 기존 로직 (호환성 유지) - Order가 없는 경우
                    for user_strategy_id in self.user_strategy_ids:
                        target_data = self._redis_manager.get_strategy_target(user_strategy_id, stock_code)
                        if target_data:
                            buy_order_no = target_data.get("buy_order_no")
                            sell_order_no = target_data.get("sell_order_no")
                            
                            if (is_buy and buy_order_no == order_no) or (not is_buy and sell_order_no == order_no):
                                order_quantity = target_data.get(f"{'buy' if is_buy else 'sell'}_quantity", 0)
                                
                                # 부분 체결 처리
                                self._redis_manager.update_strategy_target_execution(
                                    user_strategy_id=user_strategy_id,
                                    stock_code=stock_code,
                                    exec_quantity=exec_qty,
                                    exec_price=exec_price,
                                    order_no=order_no,
                                    is_buy=is_buy
                                )
                                
                                # Order가 없었던 경우에만 Kafka 발행
                                if not order_data:
                                    updated_target = self._redis_manager.get_strategy_target(user_strategy_id, stock_code)
                                    if updated_target:
                                        total_exec_quantity = updated_target.get(f"{'buy' if is_buy else 'sell'}_executed_quantity", 0)
                                        total_exec_price = updated_target.get(f"{'buy' if is_buy else 'sell'}_executed_price", 0.0)
                                        is_fully_executed = updated_target.get(f"{'buy' if is_buy else 'sell'}_executed", False)
                                        
                                        execution_message = {
                                            "timestamp": datetime.now().isoformat(),
                                            "user_strategy_id": user_strategy_id,
                                            "daily_strategy_id": None,
                                            "stock_name": "",
                                            "order_type": "BUY" if is_buy else "SELL",
                                            "stock_code": stock_code,
                                            "order_no": order_no,
                                            "order_quantity": order_quantity,
                                            "order_price": target_data.get(f"{'buy' if is_buy else 'sell'}_price", 0.0),
                                            "order_dvsn": target_data.get("order_dvsn", ""),
                                            "account_no": self.account_no,
                                            "is_mock": self.is_mock,
                                            "status": "executed" if is_fully_executed else "partially_executed",
                                            "executed_quantity": exec_qty,
                                            "executed_price": exec_price,
                                            "total_executed_quantity": total_exec_quantity,
                                            "total_executed_price": total_exec_price,
                                            "remaining_quantity": order_quantity - total_exec_quantity,
                                            "is_fully_executed": is_fully_executed,
                                            "position": None,
                                        }
                                        await self._order_signal_producer.send_order_result(execution_message)
                                    
                                    logger.info(
                                        f"✅ 체결통보 처리 완료 (레거시): "
                                        f"전략={user_strategy_id}, "
                                        f"종목={stock_code}, "
                                        f"{'매수' if is_buy else '매도'} "
                                        f"체결수량={exec_qty}"
                                    )
                                    
                except (ValueError, TypeError) as e:
                    logger.warning(
                        f"체결 정보 파싱 실패: "
                        f"stock_code={stock_code}, "
                        f"exec_price_str={exec_price_str}, "
                        f"exec_qty_str={exec_qty_str}, "
                        f"error={e}"
                    )
            else:
                logger.warning(
                    f"체결 정보 누락: "
                    f"stock_code={stock_code}, "
                    f"order_no={order_no}, "
                    f"exec_price={exec_price_str}, "
                    f"exec_qty={exec_qty_str}"
                )
        except Exception as e:
            logger.error(
                f"Error processing execution notice ({self.account_no}): {e}",
                exc_info=True
            )

    async def _reconnect(self) -> None:
        """재연결 시도"""
        self._reconnect_attempts += 1
        if self._reconnect_attempts >= self._max_reconnect_attempts:
            logger.error(
                f"Max reconnect attempts reached for account {self.account_no}: "
                f"{self._max_reconnect_attempts}"
            )
            self._error_stats.record_error(
                WebSocketConnectionError(
                    "Max reconnect attempts reached",
                    details={
                        "account_no": self.account_no,
                        "max_attempts": self._max_reconnect_attempts,
                        "attempts": self._reconnect_attempts
                    }
                )
            )
            self._running = False
            self._connection_failed = True
            # Redis 상태 업데이트
            self._redis_manager.update_connection_status(
                "account",
                account_no=self.account_no,
                status="failed",
                reconnect_attempts=self._reconnect_attempts
            )
            return

        # Redis 상태 업데이트
        self._redis_manager.update_connection_status(
            "account",
            account_no=self.account_no,
            status="reconnecting",
            reconnect_attempts=self._reconnect_attempts
        )

        wait_time = min(2 ** self._reconnect_attempts, 60)  # 지수 백오프
        logger.info(
            f"Reconnecting account websocket ({self.account_no}) in {wait_time} seconds... "
            f"(attempt {self._reconnect_attempts})"
        )
        await asyncio.sleep(wait_time)

    async def _load_from_redis(self) -> None:
        """Redis에서 연결 정보 로드 (재연결 시)"""
        try:
            data = self._redis_manager.get_account_connection(self.account_no)
            if data:
                logger.info(f"Found account websocket connection info in Redis: {self.account_no}")
                # Redis에서 access_token 복원 (없으면 유지)
                if data.get("access_token") and not self.access_token:
                    self.access_token = data.get("access_token")
                    logger.debug(f"Restored access_token from Redis for {self.account_no}")
                # Redis에서 hts_id 복원 (없으면 유지)
                if data.get("hts_id") and not self.hts_id:
                    self.hts_id = data.get("hts_id")
                    logger.debug(f"Restored hts_id from Redis for {self.account_no}")
                # Redis에서 복호화 키 및 IV 복원 (문자열 그대로 저장됨)
                decrypt_key_str = data.get("decrypt_key")
                decrypt_iv_str = data.get("decrypt_iv")
                if decrypt_key_str and decrypt_iv_str:
                    self._decrypt_key = decrypt_key_str
                    self._decrypt_iv = decrypt_iv_str
                    logger.info(
                        f"Restored decryption key and IV from Redis ({self.account_no}): "
                        f"key_length={len(self._decrypt_key)}, "
                        f"iv_length={len(self._decrypt_iv)}"
                    )
        except Exception as e:
            logger.warning(f"Failed to load connection info from Redis ({self.account_no}): {e}")

    async def disconnect(self) -> None:
        """연결 종료"""
        self._running = False
        if self._websocket:
            try:
                # 구독 해제 (실제 KIS API 형식에 맞게 수정 필요)
                unsubscribe_message = {
                    "header": {
                        "approval_key": self.ws_token,
                        "custtype": "P",
                        "tr_type": "2",  # 해제
                        "content-type": "utf-8",
                    },
                    "body": {
                        "input": {
                            "tr_id": "HDFSCNT0",
                            "tr_key": f"{self.account_no};{self.account_product_code}",
                        }
                    }
                }
                await self._websocket.send(json.dumps(unsubscribe_message))
                await asyncio.sleep(0.5)  # 해제 메시지 전송 대기
            except Exception as e:
                logger.error(f"Error unsubscribing account websocket ({self.account_no}): {e}")

            try:
                await self._websocket.close()
                logger.info(f"Account websocket disconnected: {self.account_no}")
            except Exception as e:
                logger.warning(f"Error closing account websocket ({self.account_no}, non-critical): {e}")

            self._websocket = None

        # Redis에서 연결 정보 삭제
        self._redis_manager.delete_account_connection(self.account_no)

    @property
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        return self._websocket is not None and not self._connection_failed

    @property
    def reconnect_attempts(self) -> int:
        """재연결 시도 횟수"""
        return self._reconnect_attempts
