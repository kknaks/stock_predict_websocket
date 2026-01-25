"""
KIS 가격 WebSocket Client

실시간 가격 정보 수신 및 Kafka로 프로듀싱
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

import websockets
from websockets.client import WebSocketClientProtocol

from app.kis.websocket.exceptions import (
    WebSocketConnectionError,
    WebSocketAuthError,
    WebSocketMessageError,
    WebSocketSubscriptionError,
    WebSocketTimeoutError,
)
from app.kis.websocket.error_stats import get_error_stats
from app.kis.websocket.redis_manager import get_redis_manager
from app.service.signal_execute import get_signal_executor
from app.utils.send_slack import send_slack
from app.kafka.price_producer import get_price_producer
from app.models.price import PriceMessage

logger = logging.getLogger(__name__)


class PriceWebSocketClient:
    """가격 웹소켓 클라이언트"""

    def __init__(
        self,
        ws_token: str,
        appkey: str,
        env_dv: str,
        stocks: List[str],
    ):
        """
        Args:
            ws_token: WebSocket 접속키
            appkey: 앱키
            env_dv: 환경구분 (real: 실전, demo: 모의)
            stocks: 모니터링 종목 코드 리스트
        """
        self.ws_token = ws_token
        self.appkey = appkey
        self.env_dv = env_dv
        self.stocks = stocks

        # WebSocket URL 설정
        if env_dv == "real":
            self.ws_url = "ws://ops.koreainvestment.com:21000"
        else:
            self.ws_url = "ws://ops.koreainvestment.com:31000"

        self._websocket: Optional[WebSocketClientProtocol] = None
        self._running = False
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._error_stats = get_error_stats()
        self._connection_failed = False
        self._redis_manager = get_redis_manager()

        # PING/PONG 추적 (자동 재연결용)
        self._last_ping_time: Optional[datetime] = None
        self._ping_timeout_seconds = 60  # 60초 동안 PING이 없으면 재연결

        # 시그널 실행기
        self._signal_executor = get_signal_executor()
        
        # Kafka Producer
        self._price_producer = get_price_producer()

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
                logger.warning("WebSocket connection closed")
                self._error_stats.record_error(
                    WebSocketConnectionError(
                        "WebSocket connection closed",
                        details={"close_code": e.code, "close_reason": e.reason}
                    )
                )
                # Redis 상태 업데이트
                self._redis_manager.update_connection_status(
                    "price",
                    status="disconnected",
                    reconnect_attempts=self._reconnect_attempts
                )
                if self._running:
                    await self._reconnect()
                else:
                    break
            except WebSocketAuthError as e:
                logger.error(f"WebSocket auth error: {e}")
                self._error_stats.record_error(e)
                self._connection_failed = True
                self._running = False
                # Redis에서 삭제 (인증 에러는 재연결 불가)
                self._redis_manager.delete_price_connection()
                break  # 인증 에러는 재시도 불가
            except WebSocketConnectionError as e:
                logger.error(f"WebSocket connection error: {e}")
                self._error_stats.record_error(e)
                # Redis 상태 업데이트
                self._redis_manager.update_connection_status(
                    "price",
                    status="reconnecting",
                    reconnect_attempts=self._reconnect_attempts
                )
                if self._running:
                    await self._reconnect()
                else:
                    break
            except Exception as e:
                logger.error(f"WebSocket error: {e}", exc_info=True)
                self._error_stats.record_error(e)
                if self._running:
                    await self._reconnect()
                else:
                    break

    async def _connect(self) -> None:
        """WebSocket 연결"""
        try:
            uri = f"{self.ws_url}?appkey={self.appkey}&appsecret={self.ws_token}&custtype=P"
            logger.info(f"Connecting to price websocket: {uri[:50]}...")

            try:
                self._websocket = await asyncio.wait_for(
                    websockets.connect(uri),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                raise WebSocketTimeoutError(
                    "WebSocket connection timeout",
                    details={"uri": uri[:50]}
                )
            except websockets.InvalidStatusCode as e:
                # 401, 403 등 인증 관련 에러
                if e.status_code in (401, 403):
                    raise WebSocketAuthError(
                        f"WebSocket authentication failed: {e.status_code}",
                        details={"status_code": e.status_code, "uri": uri[:50]}
                    )
                else:
                    raise WebSocketConnectionError(
                        f"WebSocket connection failed: {e.status_code}",
                        details={"status_code": e.status_code, "uri": uri[:50]}
                    )

            self._running = True
            self._reconnect_attempts = 0
            self._connection_failed = False
            self._last_ping_time = None  # 연결 시 PING 시간 초기화
            logger.info("Price websocket connected")

            # Redis에 연결 정보 저장
            self._redis_manager.save_price_connection(
                ws_token=self.ws_token,
                appkey=self.appkey,
                env_dv=self.env_dv,
                stocks=self.stocks,
                status="connected",
                reconnect_attempts=0,
            )

        except (WebSocketAuthError, WebSocketConnectionError, WebSocketTimeoutError):
            raise
        except Exception as e:
            logger.error(f"Failed to connect price websocket: {e}")
            raise WebSocketConnectionError(
                f"Unexpected connection error: {str(e)}",
                details={"error_type": type(e).__name__}
            ) from e

    async def _subscribe(self) -> None:
        """종목 구독 (체결가 + 호가) - 각 종목별로 개별 구독"""
        if not self._websocket:
            raise WebSocketConnectionError("WebSocket not connected")

        try:
            # 각 종목별로 개별 구독 (한 번에 여러 종목 구독이 안 될 수 있음)
            for stock in self.stocks:
                # 1. 실시간 체결가 구독 (H0STCNT0)
                subscribe_message_price = {
                    "header": {
                        "approval_key": self.ws_token,
                        "custtype": "P",
                        "tr_type": "1",  # 등록
                        "content-type": "utf-8",
                    },
                    "body": {
                        "input": {
                            "tr_id": "H0STCNT0",  # 실시간 주식 체결가 (KRX)
                            "tr_key": stock,  # 단일 종목코드
                        }
                    }
                }

                # 2. 실시간 호가 구독 (H0STASP0)
                subscribe_message_asking = {
                    "header": {
                        "approval_key": self.ws_token,
                        "custtype": "P",
                        "tr_type": "1",  # 등록
                        "content-type": "utf-8",
                    },
                    "body": {
                        "input": {
                            "tr_id": "H0STASP0",  # 실시간 주식 호가 (KRX)
                            "tr_key": stock,  # 단일 종목코드
                        }
                    }
                }

                try:
                    # 체결가 구독
                    await asyncio.wait_for(
                        self._websocket.send(json.dumps(subscribe_message_price)),
                        timeout=5.0
                    )
                    logger.info(f"체결가 구독 요청 전송: {stock}")
                    
                    # 호가 구독
                    await asyncio.wait_for(
                        self._websocket.send(json.dumps(subscribe_message_asking)),
                        timeout=5.0
                    )
                    logger.info(f"호가 구독 요청 전송: {stock}")
                    
                    # 각 종목 구독 요청 사이에 약간의 지연 (서버 처리 시간 확보)
                    await asyncio.sleep(0.1)
                    
                except asyncio.TimeoutError:
                    raise WebSocketTimeoutError(
                        "Subscription message send timeout",
                        details={"stock": stock}
                    )

            logger.info(f"Subscribed to {len(self.stocks)} stocks (체결가+호가): {self.stocks[:5]}...")

            # Slack 알림 전송
            await send_slack(f"[KIS WebSocket] 연결 완료 - 총 {len(self.stocks)}개 종목 구독 시작")

        except (WebSocketTimeoutError, WebSocketConnectionError):
            raise
        except Exception as e:
            logger.error(f"Failed to subscribe: {e}", exc_info=True)
            self._error_stats.record_error(
                WebSocketSubscriptionError(
                    f"Subscription failed: {str(e)}",
                    details={"stocks_count": len(self.stocks), "error_type": type(e).__name__}
                )
            )
            raise WebSocketSubscriptionError(
                f"Subscription failed: {str(e)}",
                details={"stocks_count": len(self.stocks)}
            ) from e

    async def _run(self) -> None:
        """메시지 수신 루프"""
        if not self._websocket:
            raise WebSocketConnectionError("WebSocket not connected")

        logger.info("Starting price websocket message loop...")
        
        # PING 타임아웃 체크 태스크 시작
        ping_check_task = asyncio.create_task(self._check_ping_timeout())
        
        message_count = 0
        last_message_time = datetime.now()

        try:
            async for message in self._websocket:
                if not self._running:
                    logger.info("WebSocket message loop stopped (running=False)")
                    break

                message_count += 1
                last_message_time = datetime.now()
                
                # 주기적으로 메시지 수신 상태 로깅 (100개마다)
                if message_count % 100 == 0:
                    logger.info(f"WebSocket 메시지 수신 중... (총 {message_count}개 메시지 수신)")

                try:
                    # 원본 메시지 로깅 (디버깅용 - 파이프 형식 데이터 확인)
                    # if "|" in message:
                    #     logger.info(f"[RAW DATA MESSAGE] {message[:200]}...")  # 처음 200자만
                    await self._handle_message(message)
                except WebSocketMessageError as e:
                    # 메시지 에러는 로깅 후 계속 진행
                    logger.warning(f"Message error (continuing): {e}")
                    self._error_stats.record_error(e)
                except Exception as e:
                    logger.error(f"Error handling message: {e}", exc_info=True)
                    self._error_stats.record_error(
                        WebSocketMessageError(
                            f"Unexpected message error: {str(e)}",
                            details={"error_type": type(e).__name__}
                        )
                    )

        except websockets.ConnectionClosed as e:
            logger.warning("Price websocket connection closed")
            self._error_stats.record_error(
                WebSocketConnectionError(
                    "WebSocket connection closed during message loop",
                    details={"close_code": e.code, "close_reason": e.reason}
                )
            )
            raise WebSocketConnectionError(
                "WebSocket connection closed",
                details={"close_code": e.code, "close_reason": e.reason}
            )
        except Exception as e:
            logger.error(f"Price websocket run error: {e}", exc_info=True)
            self._error_stats.record_error(e)
            raise WebSocketConnectionError(
                f"WebSocket run error: {str(e)}",
                details={"error_type": type(e).__name__}
            ) from e
        finally:
            # PING 체크 태스크 취소
            ping_check_task.cancel()
            try:
                await ping_check_task
            except asyncio.CancelledError:
                pass

    async def _handle_message(self, message: str) -> None:
        """메시지 처리"""
        try:
            # JSON 형식 메시지 (구독 성공, PING 등)
            if message.strip().startswith("{"):
                data = json.loads(message)
                header = data.get("header", {})
                body = data.get("body", {})
                tr_id = header.get("tr_id", "")
                
                # PING 메시지 처리
                if tr_id == "PINGPONG":
                    logger.info("✓ PING 메시지 수신 (연결 유지 중)")
                    await self._handle_ping(data)
                    return
                
                # 구독 성공 응답 처리
                if body.get("msg_cd") == "OPSP0000" and "SUBSCRIBE SUCCESS" in body.get("msg1", ""):
                    output = body.get("output", {})
                    self._encrypt_iv = output.get("iv")
                    self._encrypt_key = output.get("key")
                    logger.info("✓ 구독 성공 - 암호화 키 저장됨")
                    return
                
                # 구독/해제 실패 응답 처리
                msg_cd = body.get("msg_cd", "")
                msg1 = body.get("msg1", "")
                
                # 구독 실패 (invalid approval : NOT FOUND) - 심각한 에러
                if msg_cd == "OPSP0011" or ("invalid approval" in msg1.lower() and "not found" in msg1.lower()):
                    logger.error(
                        f"❌ 구독 실패: msg_cd={msg_cd}, msg1={msg1}, "
                        f"tr_id={tr_id}, tr_key={header.get('tr_key', '')}"
                    )
                    logger.error(
                        f"ws_token 확인 필요: "
                        f"길이={len(self.ws_token) if self.ws_token else 0}, "
                        f"시작={self.ws_token[:30] if self.ws_token else 'None'}..."
                    )
                    # 에러 기록만 하고 계속 진행 (재연결은 connect_and_run에서 처리)
                    self._error_stats.record_error(
                        WebSocketAuthError(
                            f"Subscription failed: {msg1}",
                            details={"msg_cd": msg_cd, "tr_id": tr_id, "tr_key": header.get("tr_key", "")}
                        )
                    )
                    # 연결 종료하여 재연결 트리거
                    if self._websocket:
                        try:
                            await self._websocket.close()
                        except Exception as e:
                            logger.warning(f"Error closing websocket: {e}")
                    return
                
                # 구독 해제 실패 (UNSUBSCRIBE ERROR) - 치명적이지 않음, 경고만
                if msg_cd == "OPSP0003" or ("unsubscribe error" in msg1.lower() and "not found" in msg1.lower()):
                    logger.warning(
                        f"⚠️ 구독 해제 경고 (무시 가능): msg_cd={msg_cd}, msg1={msg1}, "
                        f"tr_id={tr_id}, tr_key={header.get('tr_key', '')}"
                    )
                    # 구독 해제 실패는 치명적이지 않으므로 계속 진행
                    return
                
                logger.info(f"[JSON MESSAGE] {data}")
                return
            
            # 파이프(|)로 구분된 텍스트 형식 (실제 가격 데이터)
            # 형식: 0|H0STCNT0|004|005930^123929^...(체결데이터1)...^005930^123929^...(체결데이터2)...
            # - 첫 번째: 암호화 유무 (0=암호화 안됨, 1=암호화됨)
            # - 두 번째: tr_id (예: "H0STCNT0", "H0STASP0")
            # - 세 번째: 레코드 개수 (예: "001", "002", "004")
            # - 네 번째: 모든 데이터가 ^로 구분되어 연속으로 들어옴
            if "|" in message:
                records = message.split("|")
                
                if len(records) >= 4:
                    encrypt_flag = records[0]  # 암호화 유무
                    tr_id = records[1]  # TR_ID
                    record_count_str = records[2]  # 레코드 개수 문자열
                    all_fields_str = records[3]  # 모든 필드가 ^로 구분된 문자열
                    
                    # 체결가 데이터 처리 (H0STCNT0)
                    if tr_id == "H0STCNT0" and all_fields_str:
                        try:
                            record_count = int(record_count_str)
                        except ValueError:
                            logger.error(f"레코드 개수 파싱 실패: {record_count_str}")
                            return
                        
                        # 모든 필드를 ^로 분리
                        all_fields = all_fields_str.split("^")
                        
                        # 각 레코드는 46개 필드로 구성됨
                        FIELDS_PER_RECORD = 46
                        total_fields_needed = record_count * FIELDS_PER_RECORD
                        
                        # logger.info(
                        #     f"✓ 체결가 데이터 수신: tr_id={tr_id}, 레코드수={record_count}, "
                        #     f"총필드수={len(all_fields)}, 예상필드수={total_fields_needed}, 암호화={encrypt_flag}"
                        # )
                        
                        if len(all_fields) < total_fields_needed:
                            logger.warning(
                                f"필드 개수 부족: 예상={total_fields_needed}, 실제={len(all_fields)}"
                            )
                        
                        # 레코드별로 파싱 (각 레코드는 46개 필드)
                        for i in range(record_count):
                            start_idx = i * FIELDS_PER_RECORD
                            end_idx = start_idx + FIELDS_PER_RECORD
                            
                            if end_idx <= len(all_fields):
                                record_fields = all_fields[start_idx:end_idx]
                                
                                if len(record_fields) >= 3:
                                    # 필드 구조 파싱
                                    parsed_data = self._parse_price_data(record_fields)
                                    
                                    # Redis에 저장
                                    await self._save_price_to_redis(parsed_data)
                                    
                                    logger.debug(
                                        f"✓ 실시간 체결가 데이터 저장 [{i+1}/{record_count}]: "
                                        f"{parsed_data.get('MKSC_SHRN_ISCD')} - "
                                        f"{parsed_data.get('STCK_PRPR')}원 "
                                        f"({parsed_data.get('STCK_CNTG_HOUR')})"
                                    )

                                    # 매수 시그널 생성 (시가 매수 - strategy_id=1만)
                                    await self._signal_executor.check_and_generate_buy_signal(parsed_data)

                                    # 매도 시그널 생성 (목표가/손절가 도달 시)
                                    await self._signal_executor.check_and_generate_sell_signal(parsed_data)

                                    # Kafka Producer로 전송
                                    await self._send_to_kafka(parsed_data)
                            else:
                                logger.warning(
                                    f"레코드 {i+1}/{record_count} 필드 부족: "
                                    f"필요={FIELDS_PER_RECORD}, 실제={len(all_fields) - start_idx}"
                                )
                    
                    # 호가 데이터 처리 (H0STASP0)
                    elif tr_id == "H0STASP0" and all_fields_str:
                        try:
                            record_count = int(record_count_str)
                        except ValueError:
                            logger.error(f"레코드 개수 파싱 실패: {record_count_str}")
                            return
                        
                        # 모든 필드를 ^로 분리
                        all_fields = all_fields_str.split("^")
                        
                        # 각 레코드는 59개 필드로 구성됨 (GitHub 샘플 코드 기준)
                        FIELDS_PER_RECORD = 59
                        total_fields_needed = record_count * FIELDS_PER_RECORD
                        
                        # logger.info(
                        #     f"✓ 호가 데이터 수신: tr_id={tr_id}, 레코드수={record_count}, "
                        #     f"총필드수={len(all_fields)}, 예상필드수={total_fields_needed}, 암호화={encrypt_flag}"
                        # )
                        
                        if len(all_fields) < total_fields_needed:
                            logger.warning(
                                f"필드 개수 부족: 예상={total_fields_needed}, 실제={len(all_fields)}"
                            )
                        
                        # 레코드별로 파싱 (각 레코드는 47개 필드)
                        for i in range(record_count):
                            start_idx = i * FIELDS_PER_RECORD
                            end_idx = start_idx + FIELDS_PER_RECORD
                            
                            if end_idx <= len(all_fields):
                                record_fields = all_fields[start_idx:end_idx]
                                
                                if len(record_fields) >= 3:
                                    # 필드 구조 파싱
                                    parsed_data = self._parse_asking_price_data(record_fields)
                                    
                                    # Redis에 저장
                                    await self._save_asking_price_to_redis(parsed_data)
                                    
                                    logger.debug(
                                        f"✓ 실시간 호가 데이터 저장 [{i+1}/{record_count}]: "
                                        f"{parsed_data.get('MKSC_SHRN_ISCD')} - "
                                        f"매도1호가={parsed_data.get('ASKP1')}, "
                                        f"매수1호가={parsed_data.get('BIDP1')}"
                                    )
                                    # TODO: Kafka Producer로 전송
                                    # await self._send_to_kafka(parsed_data)
                            else:
                                logger.warning(
                                    f"레코드 {i+1}/{record_count} 필드 부족: "
                                    f"필요={FIELDS_PER_RECORD}, 실제={len(all_fields) - start_idx}"
                                )
                
                return
            
            # 기타 메시지 (파이프도 아니고 JSON도 아닌 경우)
            logger.debug(f"[OTHER MESSAGE] {message[:200]}...")
            
        except json.JSONDecodeError:
            # JSON이 아닌 경우는 이미 처리됨
            pass
        except Exception as e:
            logger.error(f"메시지 처리 오류: {e}", exc_info=True)
    
    def _parse_price_data(self, fields: List[str]) -> dict:
        """가격 데이터 필드 파싱"""
        # MCP에서 확인한 컬럼 순서
        columns = [
            "MKSC_SHRN_ISCD",      # 0: 종목코드
            "STCK_CNTG_HOUR",       # 1: 체결시간
            "STCK_PRPR",            # 2: 현재가
            "PRDY_VRSS_SIGN",       # 3: 전일대비부호
            "PRDY_VRSS",            # 4: 전일대비
            "PRDY_CTRT",            # 5: 등락률
            "WGHN_AVRG_STCK_PRC",   # 6: 가중평균주가
            "STCK_OPRC",            # 7: 시가
            "STCK_HGPR",            # 8: 고가
            "STCK_LWPR",            # 9: 저가
            "ASKP1",                # 10: 매도호가1
            "BIDP1",                # 11: 매수호가1
            "CNTG_VOL",             # 12: 체결량
            "ACML_VOL",             # 13: 누적거래량
            "ACML_TR_PBMN",         # 14: 누적거래대금
            "SELN_CNTG_CSNU",       # 15: 매도체결건수
            "SHNU_CNTG_CSNU",       # 16: 매수체결건수
            "NTBY_CNTG_CSNU",       # 17: 순매수체결건수
            "CTTR",                 # 18: 체결강도
            "SELN_CNTG_SMTN",       # 19: 총매도체결수량
            "SHNU_CNTG_SMTN",       # 20: 총매수체결수량
            "CCLD_DVSN",            # 21: 체결구분
            "SHNU_RATE",            # 22: 매수비율
            "PRDY_VOL_VRSS_ACML_VOL_RATE",  # 23: 전일거래량대비누적거래량비율
            "OPRC_HOUR",            # 24: 시가시간
            "OPRC_VRSS_PRPR_SIGN",  # 25: 시가대비현재가부호
            "OPRC_VRSS_PRPR",       # 26: 시가대비현재가
            "HGPR_HOUR",            # 27: 고가시간
            "HGPR_VRSS_PRPR_SIGN",  # 28: 고가대비현재가부호
            "HGPR_VRSS_PRPR",       # 29: 고가대비현재가
            "LWPR_HOUR",            # 30: 저가시간
            "LWPR_VRSS_PRPR_SIGN",  # 31: 저가대비현재가부호
            "LWPR_VRSS_PRPR",       # 32: 저가대비현재가
            "BSOP_DATE",            # 33: 영업일자
            "NEW_MKOP_CLS_CODE",    # 34: 신장전일구분코드
            "TRHT_YN",              # 35: 거래정지여부
            "ASKP_RSQN1",           # 36: 매도호가잔량1
            "BIDP_RSQN1",           # 37: 매수호가잔량1
            "TOTAL_ASKP_RSQN",      # 38: 총매도호가잔량
            "TOTAL_BIDP_RSQN",      # 39: 총매수호가잔량
            "VOL_TNRT",             # 40: 거래량회전율
            "PRDY_SMNS_HOUR_ACML_VOL",      # 41: 전일동시간누적거래량
            "PRDY_SMNS_HOUR_ACML_VOL_RATE", # 42: 전일동시간누적거래량비율
            "HOUR_CLS_CODE",        # 43: 시간구분코드
            "MRKT_TRTM_CLS_CODE",   # 44: 장구분코드
            "VI_STND_PRC",          # 45: VI적용기준가
        ]
        
        parsed = {}
        for i, field in enumerate(fields):
            if i < len(columns):
                column_name = columns[i]
                parsed[column_name] = field.strip()
        
        return parsed
    
    def _parse_asking_price_data(self, fields: List[str]) -> dict:
        """호가 데이터 필드 파싱 (H0STASP0)"""
        # GitHub 샘플 코드에서 확인한 컬럼 순서
        # https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/asking_price_krx/asking_price_krx.py
        columns = [
            "MKSC_SHRN_ISCD",      # 0: 종목코드
            "BSOP_HOUR",           # 1: 영업시간
            "HOUR_CLS_CODE",       # 2: 시간구분코드
            "ASKP1",               # 3: 매도호가1
            "ASKP2",               # 4: 매도호가2
            "ASKP3",               # 5: 매도호가3
            "ASKP4",               # 6: 매도호가4
            "ASKP5",               # 7: 매도호가5
            "ASKP6",               # 8: 매도호가6
            "ASKP7",               # 9: 매도호가7
            "ASKP8",               # 10: 매도호가8
            "ASKP9",               # 11: 매도호가9
            "ASKP10",              # 12: 매도호가10
            "BIDP1",               # 13: 매수호가1
            "BIDP2",               # 14: 매수호가2
            "BIDP3",               # 15: 매수호가3
            "BIDP4",               # 16: 매수호가4
            "BIDP5",               # 17: 매수호가5
            "BIDP6",               # 18: 매수호가6
            "BIDP7",               # 19: 매수호가7
            "BIDP8",               # 20: 매수호가8
            "BIDP9",               # 21: 매수호가9
            "BIDP10",              # 22: 매수호가10
            "ASKP_RSQN1",          # 23: 매도호가잔량1
            "ASKP_RSQN2",          # 24: 매도호가잔량2
            "ASKP_RSQN3",          # 25: 매도호가잔량3
            "ASKP_RSQN4",          # 26: 매도호가잔량4
            "ASKP_RSQN5",          # 27: 매도호가잔량5
            "ASKP_RSQN6",          # 28: 매도호가잔량6
            "ASKP_RSQN7",          # 29: 매도호가잔량7
            "ASKP_RSQN8",          # 30: 매도호가잔량8
            "ASKP_RSQN9",          # 31: 매도호가잔량9
            "ASKP_RSQN10",         # 32: 매도호가잔량10
            "BIDP_RSQN1",          # 33: 매수호가잔량1
            "BIDP_RSQN2",          # 34: 매수호가잔량2
            "BIDP_RSQN3",          # 35: 매수호가잔량3
            "BIDP_RSQN4",          # 36: 매수호가잔량4
            "BIDP_RSQN5",          # 37: 매수호가잔량5
            "BIDP_RSQN6",          # 38: 매수호가잔량6
            "BIDP_RSQN7",          # 39: 매수호가잔량7
            "BIDP_RSQN8",          # 40: 매수호가잔량8
            "BIDP_RSQN9",          # 41: 매수호가잔량9
            "BIDP_RSQN10",         # 42: 매수호가잔량10
            "TOTAL_ASKP_RSQN",     # 43: 총매도호가잔량
            "TOTAL_BIDP_RSQN",     # 44: 총매수호가잔량
            "OVTM_TOTAL_ASKP_RSQN", # 45: 시간외총매도호가잔량
            "OVTM_TOTAL_BIDP_RSQN", # 46: 시간외총매수호가잔량
            "ANTC_CNPR",           # 47: 예상체결가
            "ANTC_CNQN",           # 48: 예상체결량
            "ANTC_VOL",            # 49: 예상체결대금
            "ANTC_CNTG_VRSS",      # 50: 예상체결전일대비
            "ANTC_CNTG_VRSS_SIGN", # 51: 예상체결전일대비부호
            "ANTC_CNTG_PRDY_CTRT", # 52: 예상체결전일대비율
            "ACML_VOL",            # 53: 누적거래량
            "TOTAL_ASKP_RSQN_ICDC", # 54: 총매도호가잔량증감
            "TOTAL_BIDP_RSQN_ICDC", # 55: 총매수호가잔량증감
            "OVTM_TOTAL_ASKP_ICDC", # 56: 시간외총매도호가잔량증감
            "OVTM_TOTAL_BIDP_ICDC", # 57: 시간외총매수호가잔량증감
            "STCK_DEAL_CLS_CODE",  # 58: 주식거래구분코드
        ]
        
        parsed = {}
        for i, field in enumerate(fields):
            if i < len(columns):
                column_name = columns[i]
                parsed[column_name] = field.strip()
        
        return parsed
    
    async def _save_price_to_redis(self, price_data: dict) -> None:
        """실시간 가격 데이터를 Redis에 저장 (마지막 데이터만 유지)
        
        같은 종목코드에 대해 계속 덮어쓰기하여 최신 가격만 유지합니다.
        
        저장 형식:
        - 키: websocket:price_data:{종목코드} (예: websocket:price_data:005930)
        - 값: JSON 문자열 (모든 필드 + updated_at 타임스탬프)
        - TTL: 1시간 (3600초)
        - 동작: 같은 키에 대해 덮어쓰기 (SETEX 사용)
        """
        try:
            symbol = price_data.get("MKSC_SHRN_ISCD", "")
            if not symbol:
                logger.warning("종목코드가 없어 Redis 저장 불가")
                return
            
            # Redis 키: websocket:price_data:005930 (같은 종목은 같은 키 사용 → 덮어쓰기)
            redis_key = f"websocket:price_data:{symbol}"
            
            # 타임스탬프 추가
            price_data_with_ts = {
                **price_data,
                "updated_at": datetime.now().isoformat()
            }
            
            # Redis에 저장 (SETEX: 키가 있으면 덮어쓰기, 없으면 생성)
            if self._redis_manager._redis_client:
                self._redis_manager._redis_client.setex(
                    redis_key,
                    3600,  # 1시간
                    json.dumps(price_data_with_ts, ensure_ascii=False)
                )
                logger.debug(f"Redis 저장 완료 (덮어쓰기): {redis_key}")
            else:
                logger.warning("Redis 클라이언트가 연결되지 않음")
            
        except Exception as e:
            logger.warning(f"Redis 저장 실패: {e}")
    
    async def _save_asking_price_to_redis(self, asking_price_data: dict) -> None:
        """실시간 호가 데이터를 Redis에 저장 (마지막 데이터만 유지)
        
        같은 종목코드에 대해 계속 덮어쓰기하여 최신 호가만 유지합니다.
        
        저장 형식:
        - 키: websocket:asking_price_data:{종목코드} (예: websocket:asking_price_data:005930)
        - 값: JSON 문자열 (모든 필드 + updated_at 타임스탬프)
        - TTL: 1시간 (3600초)
        - 동작: 같은 키에 대해 덮어쓰기 (SETEX 사용)
        """
        try:
            symbol = asking_price_data.get("MKSC_SHRN_ISCD", "")
            if not symbol:
                logger.warning("종목코드가 없어 Redis 저장 불가")
                return
            
            # Redis 키: websocket:asking_price_data:005930 (같은 종목은 같은 키 사용 → 덮어쓰기)
            redis_key = f"websocket:asking_price_data:{symbol}"
            
            # 타임스탬프 추가
            asking_price_data_with_ts = {
                **asking_price_data,
                "updated_at": datetime.now().isoformat()
            }
            
            # Redis에 저장 (SETEX: 키가 있으면 덮어쓰기, 없으면 생성)
            if self._redis_manager._redis_client:
                self._redis_manager._redis_client.setex(
                    redis_key,
                    3600,  # 1시간
                    json.dumps(asking_price_data_with_ts, ensure_ascii=False)
                )
                logger.debug(f"Redis 저장 완료 (덮어쓰기): {redis_key}")
            else:
                logger.warning("Redis 클라이언트가 연결되지 않음")
            
        except Exception as e:
            logger.warning(f"Redis 저장 실패: {e}")


    async def _handle_ping(self, ping_data: dict) -> None:
        """PING 메시지에 대한 PONG 응답 및 연결 상태 업데이트"""
        try:
            pong_message = {
                "header": {
                    "tr_id": "PINGPONG",
                    "datetime": ping_data.get("header", {}).get("datetime", "")
                }
            }
            
            if self._websocket:
                await self._websocket.send(json.dumps(pong_message))
                logger.debug("Sent PONG response")
                
                # 마지막 PING 시간 업데이트
                self._last_ping_time = datetime.now()
                
                # PING/PONG으로 연결 상태 확인 - Redis에 마지막 활동 시간 업데이트
                self._redis_manager.update_connection_status(
                    "price",
                    status="connected",
                    reconnect_attempts=self._reconnect_attempts
                )
        except Exception as e:
            logger.warning(f"Failed to send PONG response: {e}")
    
    async def _check_ping_timeout(self) -> None:
        """PING 타임아웃 체크 - 일정 시간 동안 PING이 없으면 재연결"""
        try:
            while self._running:
                await asyncio.sleep(10)  # 10초마다 체크
                
                if self._last_ping_time:
                    elapsed = (datetime.now() - self._last_ping_time).total_seconds()
                    if elapsed > self._ping_timeout_seconds:
                        logger.warning(
                            f"PING timeout: {elapsed:.1f}초 동안 PING이 없음. 재연결 시도..."
                        )
                        # 연결 종료하여 재연결 트리거
                        if self._websocket:
                            await self._websocket.close()
                        break
                else:
                    # 첫 PING이 아직 오지 않았으면 계속 대기
                    pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"PING timeout check error: {e}", exc_info=True)

    async def _reconnect(self) -> None:
        """재연결 시도"""
        self._reconnect_attempts += 1
        if self._reconnect_attempts >= self._max_reconnect_attempts:
            logger.error(f"Max reconnect attempts reached: {self._max_reconnect_attempts}")
            self._error_stats.record_error(
                WebSocketConnectionError(
                    "Max reconnect attempts reached",
                    details={
                        "max_attempts": self._max_reconnect_attempts,
                        "attempts": self._reconnect_attempts
                    }
                )
            )
            self._running = False
            self._connection_failed = True
            # Redis 상태 업데이트
            self._redis_manager.update_connection_status(
                "price",
                status="failed",
                reconnect_attempts=self._reconnect_attempts
            )
            return

        # Redis 상태 업데이트
        self._redis_manager.update_connection_status(
            "price",
            status="reconnecting",
            reconnect_attempts=self._reconnect_attempts
        )

        wait_time = min(2 ** self._reconnect_attempts, 60)  # 지수 백오프
        logger.info(f"Reconnecting in {wait_time} seconds... (attempt {self._reconnect_attempts})")
        await asyncio.sleep(wait_time)

    async def _load_from_redis(self) -> None:
        """Redis에서 연결 정보 로드 (재연결 시)"""
        try:
            data = self._redis_manager.get_price_connection()
            if data:
                # 토큰이 만료되었을 수 있으므로 주의
                # 여기서는 기본 정보만 로드하고, 실제 연결은 기존 정보 사용
                logger.info("Found price websocket connection info in Redis")
                # 필요시 토큰 갱신 로직 추가 가능
        except Exception as e:
            logger.warning(f"Failed to load connection info from Redis: {e}")

    async def disconnect(self) -> None:
        """연결 종료"""
        self._running = False
        if self._websocket:
            try:
                # 1. 체결가 구독 해제
                unsubscribe_message_price = {
                    "header": {
                        "approval_key": self.ws_token,
                        "custtype": "P",
                        "tr_type": "2",  # 해제
                        "content-type": "utf-8",
                    },
                    "body": {
                        "input": {
                            "tr_id": "H0STCNT0",  # 체결가 구독 해제
                            "tr_key": ",".join(self.stocks),
                        }
                    }
                }
                await asyncio.wait_for(
                    self._websocket.send(json.dumps(unsubscribe_message_price)),
                    timeout=5.0
                )
                logger.info("체결가 구독 해제 요청 전송")
                
                # 2. 호가 구독 해제
                unsubscribe_message_asking = {
                    "header": {
                        "approval_key": self.ws_token,
                        "custtype": "P",
                        "tr_type": "2",  # 해제
                        "content-type": "utf-8",
                    },
                    "body": {
                        "input": {
                            "tr_id": "H0STASP0",  # 호가 구독 해제
                            "tr_key": ",".join(self.stocks),
                        }
                    }
                }
                await asyncio.wait_for(
                    self._websocket.send(json.dumps(unsubscribe_message_asking)),
                    timeout=5.0
                )
                logger.info("호가 구독 해제 요청 전송")
                
                await asyncio.sleep(0.5)  # 해제 메시지 전송 대기
            except Exception as e:
                logger.warning(f"Error unsubscribing (non-critical): {e}")

            try:
                await self._websocket.close()
                logger.info("Price websocket disconnected")
            except Exception as e:
                logger.warning(f"Error closing websocket (non-critical): {e}")

            self._websocket = None

        # Redis에서 연결 정보 삭제
        self._redis_manager.delete_price_connection()

        await send_slack(f"[KIS WebSocket] 종목 구독 종료 - 총 {len(self.stocks)}개 종목")

    @property
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        return self._websocket is not None and not self._connection_failed

    async def unsubscribe_stocks(self, stocks: List[str]) -> None:
        """종목 구독 해제 (체결가 + 호가 모두 해제) - 각 종목별로 개별 해제"""
        if not self._websocket:
            raise WebSocketConnectionError("WebSocket not connected")
        
        if not stocks:
            logger.warning("No stocks to unsubscribe")
            return

        try:
            # 각 종목별로 개별 해제 (한 번에 여러 종목 해제가 안 될 수 있음)
            for stock in stocks:
                # 1. 체결가 구독 해제 (H0STCNT0)
                unsubscribe_message_price = {
                    "header": {
                        "approval_key": self.ws_token,
                        "custtype": "P",
                        "tr_type": "2",  # 해제
                        "content-type": "utf-8",
                    },
                    "body": {
                        "input": {
                            "tr_id": "H0STCNT0",  # 체결가 구독 해제
                            "tr_key": stock,  # 단일 종목코드
                        }
                    }
                }

                await asyncio.wait_for(
                    self._websocket.send(json.dumps(unsubscribe_message_price)),
                    timeout=5.0
                )
                logger.debug(f"체결가 구독 해제 요청 전송: {stock}")

                # 2. 호가 구독 해제 (H0STASP0)
                unsubscribe_message_asking = {
                    "header": {
                        "approval_key": self.ws_token,
                        "custtype": "P",
                        "tr_type": "2",  # 해제
                        "content-type": "utf-8",
                    },
                    "body": {
                        "input": {
                            "tr_id": "H0STASP0",  # 호가 구독 해제
                            "tr_key": stock,  # 단일 종목코드
                        }
                    }
                }

                await asyncio.wait_for(
                    self._websocket.send(json.dumps(unsubscribe_message_asking)),
                    timeout=5.0
                )
                logger.debug(f"호가 구독 해제 요청 전송: {stock}")
                
                # 각 종목 해제 요청 사이에 약간의 지연 (서버 처리 시간 확보)
                await asyncio.sleep(0.1)

            # 구독 해제된 종목을 리스트에서 제거
            self.stocks = [s for s in self.stocks if s not in stocks]
            
            logger.info(f"Unsubscribed from {len(stocks)} stocks (체결가+호가): {stocks[:5]}...")

        except asyncio.TimeoutError:
            raise WebSocketTimeoutError(
                "Unsubscribe message send timeout",
                details={"stocks_count": len(stocks)}
            )
        except Exception as e:
            logger.error(f"Failed to unsubscribe stocks: {e}", exc_info=True)
            raise WebSocketSubscriptionError(
                f"Unsubscribe failed: {str(e)}",
                details={"stocks_count": len(stocks)}
            ) from e

    async def subscribe_stocks(self, stocks: List[str]) -> None:
        """종목 구독 추가 (체결가 + 호가 모두 구독) - 각 종목별로 개별 구독"""
        if not self._websocket:
            raise WebSocketConnectionError("WebSocket not connected")
        
        if not stocks:
            logger.warning("No stocks to subscribe")
            return

        try:
            # 각 종목별로 개별 구독 (한 번에 여러 종목 구독이 안 될 수 있음)
            for stock in stocks:
                # 1. 체결가 구독 (H0STCNT0)
                subscribe_message_price = {
                    "header": {
                        "approval_key": self.ws_token,
                        "custtype": "P",
                        "tr_type": "1",  # 등록
                        "content-type": "utf-8",
                    },
                    "body": {
                        "input": {
                            "tr_id": "H0STCNT0",  # 체결가 구독
                            "tr_key": stock,  # 단일 종목코드
                        }
                    }
                }

                await asyncio.wait_for(
                    self._websocket.send(json.dumps(subscribe_message_price)),
                    timeout=5.0
                )
                logger.debug(f"체결가 구독 요청 전송: {stock}")

                # 2. 호가 구독 (H0STASP0)
                subscribe_message_asking = {
                    "header": {
                        "approval_key": self.ws_token,
                        "custtype": "P",
                        "tr_type": "1",  # 등록
                        "content-type": "utf-8",
                    },
                    "body": {
                        "input": {
                            "tr_id": "H0STASP0",  # 호가 구독
                            "tr_key": stock,  # 단일 종목코드
                        }
                    }
                }

                await asyncio.wait_for(
                    self._websocket.send(json.dumps(subscribe_message_asking)),
                    timeout=5.0
                )
                logger.debug(f"호가 구독 요청 전송: {stock}")
                
                # 각 종목 구독 요청 사이에 약간의 지연 (서버 처리 시간 확보)
                await asyncio.sleep(0.1)

            # 새로 구독한 종목을 리스트에 추가 (중복 제거)
            new_stocks = [s for s in stocks if s not in self.stocks]
            self.stocks.extend(new_stocks)

            logger.info(f"Subscribed to {len(stocks)} stocks (체결가+호가): {stocks[:5]}...")

            # Slack 알림 전송
            await send_slack(f"[KIS WebSocket] 종목 구독 변경 - 총 {len(self.stocks)}개 종목")

        except asyncio.TimeoutError:
            raise WebSocketTimeoutError(
                "Subscribe message send timeout",
                details={"stocks_count": len(stocks)}
            )
        except Exception as e:
            logger.error(f"Failed to subscribe stocks: {e}", exc_info=True)
            raise WebSocketSubscriptionError(
                f"Subscribe failed: {str(e)}",
                details={"stocks_count": len(stocks)}
            ) from e

    async def update_stocks(self, new_stocks: List[str]) -> None:
        """종목 업데이트 (기존 종목 해제 후 새 종목 구독)"""
        if not self._websocket:
            raise WebSocketConnectionError("WebSocket not connected")
        
        old_stocks = self.stocks.copy()
        
        # 기존 종목 해제
        if old_stocks:
            try:
                await self.unsubscribe_stocks(old_stocks)
                logger.info(f"Unsubscribed from old stocks: {len(old_stocks)} stocks")
            except Exception as e:
                logger.warning(f"Failed to unsubscribe old stocks: {e}, continuing with new subscription")

        # 새 종목 구독
        if new_stocks:
            await self.subscribe_stocks(new_stocks)
            logger.info(f"Updated stocks: {len(old_stocks)} -> {len(new_stocks)} stocks")
        else:
            logger.warning("No new stocks to subscribe, all stocks unsubscribed")

    async def _send_to_kafka(self, parsed_data: dict) -> None:
        """파싱된 가격 데이터를 Kafka로 전송"""
        try:
            price_message = PriceMessage.from_parsed_data(parsed_data)
            await self._price_producer.send_price(price_message)
        except Exception as e:
            logger.error(f"Failed to send price data to Kafka: {e}", exc_info=True)

    @property
    def reconnect_attempts(self) -> int:
        """재연결 시도 횟수"""
        return self._reconnect_attempts
