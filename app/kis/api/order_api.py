"""
한국투자증권 주문 API

매도 시그널을 받아 실제 주문을 처리합니다.
- mock 모드: 주문 가정 후 Kafka 발행
- 실제 모드: KIS API로 실제 주문 전송
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any

import httpx

from app.kis.websocket.redis_manager import get_redis_manager
from app.kafka.order_signal_producer import get_order_signal_producer
from app.service.calculate_slippage import SignalResult, OrderType
from app.utils.send_slack import send_slack

logger = logging.getLogger(__name__)

# KIS API Base URLs
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"  # 실전
KIS_PAPER_URL = "https://openapivts.koreainvestment.com:29443"  # 모의


class OrderAPI:
    """주문 API 클라이언트"""

    def __init__(self):
        self._redis_manager = get_redis_manager()
        self._order_signal_producer = get_order_signal_producer()

    async def process_buy_order(
        self,
        user_strategy_id: int,
        signal: SignalResult,
        order_quantity: int = 1
    ) -> Dict[str, Any]:
        """
        매수 주문 처리

        Args:
            user_strategy_id: 사용자 전략 ID
            signal: 매수 시그널
            order_quantity: 주문 수량 (기본값: 1)

        Returns:
            주문 처리 결과
        """
        try:
            # 1. 전략 설정 조회
            strategy_config = self._redis_manager.get_strategy_config(user_strategy_id)
            if not strategy_config:
                error_msg = f"전략 설정을 찾을 수 없습니다: user_strategy_id={user_strategy_id}"
                logger.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg
                }

            is_mock = strategy_config.get("is_mock", False)
            user_id = strategy_config.get("user_id")

            # 3. 주문 정보 구성
            stock_code = signal.stock_code
            order_price = int(signal.recommended_order_price)
            
            # 주문 구분 코드 변환
            # 00: 지정가, 01: 시장가, 02: 조건부지정가, 03: 최유리지정가, 05: 장전시간외종가, 06: 장후시간외종가
            if signal.recommended_order_type == OrderType.LIMIT:
                ord_dvsn = "00"  # 지정가
            elif signal.recommended_order_type == OrderType.MARKET:
                ord_dvsn = "01"  # 시장가
            else:
                ord_dvsn = "00"  # 기본값: 지정가

            # 4. 주문 실행
            if is_mock:
                # Mock 모드: 계좌 정보 없이 주문 가정
                # Mock 모드에서 주문번호 생성 (타임스탬프 기반 고유 ID)
                mock_order_no = f"MOCK-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"
                
                logger.info(
                    f"[MOCK] 매수 주문 가정: "
                    f"전략={user_strategy_id}, "
                    f"종목={stock_code}, "
                    f"수량={order_quantity}, "
                    f"가격={order_price}, "
                    f"주문구분={ord_dvsn}, "
                    f"주문번호={mock_order_no}"
                )
                
                # Kafka로 주문 결과 발행 (체결되었다고 가정)
                mock_order_result = {
                    "timestamp": datetime.now().isoformat(),
                    "user_strategy_id": user_strategy_id,
                    "order_type": "BUY",
                    "stock_code": stock_code,
                    "order_no": mock_order_no,  # Mock 주문번호
                    "order_quantity": order_quantity,
                    "order_price": order_price,
                    "order_dvsn": ord_dvsn,
                    "account_no": "MOCK",  # Mock 모드에서는 계좌번호 불필요
                    "is_mock": True,
                    "status": "executed",  # 체결되었다고 가정
                    "executed_quantity": order_quantity,  # Mock 모드: 100% 체결
                    "executed_price": order_price,
                    # 부분 체결 정보 (Mock 모드: 100% 체결 가정)
                    "total_executed_quantity": order_quantity,
                    "total_executed_price": order_price,
                    "remaining_quantity": 0,
                    "is_fully_executed": True,
                }
                
                # 주문 결과를 Kafka로 발행 (주문번호 포함)
                await self._order_signal_producer.send_order_result(mock_order_result)
                
                # 전략 타겟에 매수 정보 업데이트 (Mock 모드: 100% 체결 가정)
                self._redis_manager.update_strategy_target_buy(
                    user_strategy_id=user_strategy_id,
                    stock_code=stock_code,
                    buy_quantity=order_quantity,
                    buy_price=order_price,
                    order_no=mock_order_no,  # Mock 주문번호 저장
                    executed=True  # Mock 모드: 100% 체결 가정
                )
                
                logger.info(f"[MOCK] 매수 주문 처리 완료: {stock_code}")

                # Slack 알림 전송
                await send_slack(
                    f"[MOCK 매수 체결] {stock_code} | "
                    f"{order_quantity}주 @ {order_price:,}원 | "
                    f"매수 사유 : {signal.reason}"
                )

                return {
                    "success": True,
                    "is_mock": True,
                    "result": mock_order_result
                }
            else:
                # 실제 모드: 계좌 정보 조회 필수
                account_connection = self._redis_manager.get_account_connection_by_strategy_id(user_strategy_id)
                if not account_connection:
                    error_msg = f"계좌 연결 정보를 찾을 수 없습니다: user_strategy_id={user_strategy_id}"
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "error": error_msg
                    }

                account_no = account_connection.get("account_no")
                account_product_code = account_connection.get("account_product_code")
                appkey = account_connection.get("appkey")
                env_dv = account_connection.get("env_dv", "demo")  # 기본값: demo
                access_token = account_connection.get("access_token")

                # 계좌번호 분리 (12345678-01 -> cano=12345678, acnt_prdt_cd=01)
                if "-" in account_no:
                    cano, acnt_prdt_cd = account_no.split("-")
                else:
                    cano = account_no
                    acnt_prdt_cd = account_product_code or "01"

                # 실제 모드: KIS API로 주문
                if not access_token:
                    error_msg = f"액세스 토큰이 없습니다: account_no={account_no}"
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "error": error_msg
                    }

                # 주문 API 호출
                order_result = await self._place_buy_order(
                    env_dv=env_dv,
                    cano=cano,
                    acnt_prdt_cd=acnt_prdt_cd,
                    appkey=appkey,
                    access_token=access_token,
                    stock_code=stock_code,
                    ord_dvsn=ord_dvsn,
                    ord_qty=str(order_quantity),
                    ord_unpr=str(order_price)
                )

                if order_result.get("success"):
                    order_no = order_result.get('order_no')
                    logger.info(
                        f"매수 주문 성공: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"주문번호={order_no}"
                    )
                    
                    # Kafka로 주문 결과 발행 (주문번호 포함)
                    order_result_message = {
                        "timestamp": datetime.now().isoformat(),
                        "user_strategy_id": user_strategy_id,
                        "order_type": "BUY",
                        "stock_code": stock_code,
                        "order_no": order_no,
                        "order_quantity": order_quantity,
                        "order_price": order_price,
                        "order_dvsn": ord_dvsn,
                        "account_no": account_no,
                        "is_mock": False,
                        "status": "ordered",  # 주문 성공 (체결 대기)
                        "executed_quantity": 0,  # 아직 체결 안 됨
                        "executed_price": 0.0,
                        # 부분 체결 정보 (주문 시점에는 0)
                        "total_executed_quantity": 0,
                        "total_executed_price": 0.0,
                        "remaining_quantity": order_quantity,
                        "is_fully_executed": False,
                    }
                    await self._order_signal_producer.send_order_result(order_result_message)
                    
                    # 전략 타겟에 매수 주문 정보 업데이트 (실제 모드: 주문 성공만, 체결은 체결 통보에서 처리)
                    self._redis_manager.update_strategy_target_buy(
                        user_strategy_id=user_strategy_id,
                        stock_code=stock_code,
                        buy_quantity=order_quantity,
                        buy_price=order_price,
                        order_no=order_no,
                        executed=False  # 실제 모드: 주문만 성공, 체결은 체결 통보에서 처리
                    )
                    
                    return {
                        "success": True,
                        "is_mock": False,
                        "result": order_result
                    }
                else:
                    logger.error(
                        f"매수 주문 실패: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"오류={order_result.get('error', 'N/A')}"
                    )
                    return {
                        "success": False,
                        "is_mock": False,
                        "error": order_result.get("error", "주문 실패")
                    }

        except Exception as e:
            logger.error(f"매수 주문 처리 오류: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    async def _place_buy_order(
        self,
        env_dv: str,
        cano: str,
        acnt_prdt_cd: str,
        appkey: str,
        access_token: str,
        stock_code: str,
        ord_dvsn: str,
        ord_qty: str,
        ord_unpr: str
    ) -> Dict[str, Any]:
        """
        KIS API로 매수 주문 전송

        Args:
            env_dv: 환경구분 (real: 실전, demo: 모의)
            cano: 종합계좌번호
            acnt_prdt_cd: 계좌상품코드
            appkey: 앱키
            access_token: 액세스 토큰
            stock_code: 종목코드
            ord_dvsn: 주문구분
            ord_qty: 주문수량
            ord_unpr: 주문단가

        Returns:
            주문 결과
        """
        try:
            # Base URL 설정
            base_url = KIS_BASE_URL if env_dv == "real" else KIS_PAPER_URL
            
            # TR_ID 설정
            if env_dv == "real":
                tr_id = "TTTC0002U"  # 실전 매수
            else:
                tr_id = "VTTC0002U"  # 모의 매수

            url = f"{base_url}/uapi/domestic-stock/v1/trading/order-cash"
            
            headers = {
                "authorization": f"Bearer {access_token}",
                "appkey": appkey,
                "appsecret": "",  # 주문 API에서는 appsecret 불필요
                "tr_id": tr_id,
                "custtype": "P",  # 개인
            }

            # 주문 파라미터 (대문자 필수)
            params = {
                "CANO": cano,  # 종합계좌번호
                "ACNT_PRDT_CD": acnt_prdt_cd,  # 계좌상품코드
                "PDNO": stock_code,  # 상품번호 (종목코드)
                "ORD_DVSN": ord_dvsn,  # 주문구분
                "ORD_QTY": ord_qty,  # 주문수량
                "ORD_UNPR": ord_unpr,  # 주문단가
                "EXCG_ID_DVSN_CD": "KRX",  # 거래소ID구분코드
                "CTAC_TLNO": "",  # 연락전화번호 (선택)
                "SLL_TYPE": "",  # 매도유형 (매수 시 빈값)
                "ALGO_NO": ""  # 알고리즘번호 (선택)
            }

            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, json=params)
                response.raise_for_status()
                data = response.json()

            # 응답 확인
            if data.get("rt_cd") == "0":  # 성공
                output = data.get("output", {})
                return {
                    "success": True,
                    "order_no": output.get("ODNO"),  # 주문번호
                    "order_time": output.get("ORD_TMD"),  # 주문시간
                    "raw_response": data
                }
            else:
                error_msg = data.get("msg1", "주문 실패")
                error_code = data.get("rt_cd", "N/A")
                logger.error(
                    f"KIS API 주문 실패: "
                    f"rt_cd={error_code}, "
                    f"msg1={error_msg}, "
                    f"stock_code={stock_code}"
                )
                return {
                    "success": False,
                    "error": error_msg,
                    "error_code": error_code,
                    "raw_response": data
                }

        except httpx.HTTPStatusError as e:
            logger.error(f"KIS API HTTP 오류: {e.response.status_code}, {e.response.text}")
            return {
                "success": False,
                "error": f"HTTP {e.response.status_code}: {str(e)}"
            }
        except Exception as e:
            logger.error(f"KIS API 주문 오류: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    async def process_sell_order(
        self,
        user_strategy_id: int,
        signal: SignalResult,
        order_quantity: int = 1
    ) -> Dict[str, Any]:
        """
        매도 주문 처리

        Args:
            user_strategy_id: 사용자 전략 ID
            signal: 매도 시그널
            order_quantity: 주문 수량 (기본값: 1)

        Returns:
            주문 처리 결과
        """
        try:
            # 1. 전략 설정 조회
            strategy_config = self._redis_manager.get_strategy_config(user_strategy_id)
            if not strategy_config:
                error_msg = f"전략 설정을 찾을 수 없습니다: user_strategy_id={user_strategy_id}"
                logger.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg
                }

            is_mock = strategy_config.get("is_mock", False)
            user_id = strategy_config.get("user_id")

            # 3. 주문 정보 구성
            stock_code = signal.stock_code
            order_price = int(signal.recommended_order_price)
            
            # 주문 구분 코드 변환
            # 00: 지정가, 01: 시장가, 02: 조건부지정가, 03: 최유리지정가, 05: 장전시간외종가, 06: 장후시간외종가
            if signal.recommended_order_type == OrderType.LIMIT:
                ord_dvsn = "00"  # 지정가
            elif signal.recommended_order_type == OrderType.MARKET:
                ord_dvsn = "01"  # 시장가
            else:
                ord_dvsn = "00"  # 기본값: 지정가

            # 4. 주문 실행
            if is_mock:
                # Mock 모드: 계좌 정보 없이 주문 가정
                # Mock 모드에서 주문번호 생성 (타임스탬프 기반 고유 ID)
                mock_order_no = f"MOCK-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"
                
                logger.info(
                    f"[MOCK] 매도 주문 가정: "
                    f"전략={user_strategy_id}, "
                    f"종목={stock_code}, "
                    f"수량={order_quantity}, "
                    f"가격={order_price}, "
                    f"주문구분={ord_dvsn}, "
                    f"주문번호={mock_order_no}"
                )
                
                # Kafka로 주문 결과 발행 (체결되었다고 가정)
                mock_order_result = {
                    "timestamp": datetime.now().isoformat(),
                    "user_strategy_id": user_strategy_id,
                    "order_type": "SELL",
                    "stock_code": stock_code,
                    "order_no": mock_order_no,  # Mock 주문번호
                    "order_quantity": order_quantity,
                    "order_price": order_price,
                    "order_dvsn": ord_dvsn,
                    "account_no": "MOCK",  # Mock 모드에서는 계좌번호 불필요
                    "is_mock": True,
                    "status": "executed",  # 체결되었다고 가정
                    "executed_quantity": order_quantity,  # Mock 모드: 100% 체결
                    "executed_price": order_price,
                    # 부분 체결 정보 (Mock 모드: 100% 체결 가정)
                    "total_executed_quantity": order_quantity,
                    "total_executed_price": order_price,
                    "remaining_quantity": 0,
                    "is_fully_executed": True,
                }
                
                # 주문 결과를 Kafka로 발행 (주문번호 포함)
                await self._order_signal_producer.send_order_result(mock_order_result)
                
                # 전략 타겟에 매도 정보 업데이트 (Mock 모드: 100% 체결 가정)
                self._redis_manager.update_strategy_target_sell(
                    user_strategy_id=user_strategy_id,
                    stock_code=stock_code,
                    sell_quantity=order_quantity,
                    sell_price=order_price,
                    order_no=mock_order_no,  # Mock 주문번호 저장
                    executed=True  # Mock 모드: 100% 체결 가정
                )
                
                logger.info(f"[MOCK] 매도 주문 처리 완료: {stock_code}")

                # Slack 알림 전송
                await send_slack(
                    f"[MOCK 매도 체결] {stock_code} | "
                    f"{order_quantity}주 @ {order_price:,}원 | "
                    f"매도 사유 : {signal.reason}"
                )

                return {
                    "success": True,
                    "is_mock": True,
                    "result": mock_order_result
                }
            else:
                # 실제 모드: 계좌 정보 조회 필수
                account_connection = self._redis_manager.get_account_connection_by_strategy_id(user_strategy_id)
                if not account_connection:
                    error_msg = f"계좌 연결 정보를 찾을 수 없습니다: user_strategy_id={user_strategy_id}"
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "error": error_msg
                    }

                account_no = account_connection.get("account_no")
                account_product_code = account_connection.get("account_product_code")
                appkey = account_connection.get("appkey")
                env_dv = account_connection.get("env_dv", "demo")  # 기본값: demo
                access_token = account_connection.get("access_token")

                # 계좌번호 분리 (12345678-01 -> cano=12345678, acnt_prdt_cd=01)
                if "-" in account_no:
                    cano, acnt_prdt_cd = account_no.split("-")
                else:
                    cano = account_no
                    acnt_prdt_cd = account_product_code or "01"

                # 실제 모드: KIS API로 주문
                if not access_token:
                    error_msg = f"액세스 토큰이 없습니다: account_no={account_no}"
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "error": error_msg
                    }

                # 주문 API 호출
                order_result = await self._place_sell_order(
                    env_dv=env_dv,
                    cano=cano,
                    acnt_prdt_cd=acnt_prdt_cd,
                    appkey=appkey,
                    access_token=access_token,
                    stock_code=stock_code,
                    ord_dvsn=ord_dvsn,
                    ord_qty=str(order_quantity),
                    ord_unpr=str(order_price)
                )

                if order_result.get("success"):
                    order_no = order_result.get('order_no')
                    logger.info(
                        f"매도 주문 성공: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"주문번호={order_no}"
                    )
                    
                    # Kafka로 주문 결과 발행 (주문번호 포함)
                    order_result_message = {
                        "timestamp": datetime.now().isoformat(),
                        "user_strategy_id": user_strategy_id,
                        "order_type": "SELL",
                        "stock_code": stock_code,
                        "order_no": order_no,
                        "order_quantity": order_quantity,
                        "order_price": order_price,
                        "order_dvsn": ord_dvsn,
                        "account_no": account_no,
                        "is_mock": False,
                        "status": "ordered",  # 주문 성공 (체결 대기)
                        "executed_quantity": 0,  # 아직 체결 안 됨
                        "executed_price": 0.0,
                        # 부분 체결 정보 (주문 시점에는 0)
                        "total_executed_quantity": 0,
                        "total_executed_price": 0.0,
                        "remaining_quantity": order_quantity,
                        "is_fully_executed": False,
                    }
                    await self._order_signal_producer.send_order_result(order_result_message)
                    
                    # 전략 타겟에 매도 주문 정보 업데이트 (실제 모드: 주문 성공만, 체결은 체결 통보에서 처리)
                    self._redis_manager.update_strategy_target_sell(
                        user_strategy_id=user_strategy_id,
                        stock_code=stock_code,
                        sell_quantity=order_quantity,
                        sell_price=order_price,
                        order_no=order_no,
                        executed=False  # 실제 모드: 주문만 성공, 체결은 체결 통보에서 처리
                    )
                    
                    return {
                        "success": True,
                        "is_mock": False,
                        "result": order_result
                    }
                else:
                    logger.error(
                        f"매도 주문 실패: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"오류={order_result.get('error', 'N/A')}"
                    )
                    return {
                        "success": False,
                        "is_mock": False,
                        "error": order_result.get("error", "주문 실패")
                    }

        except Exception as e:
            logger.error(f"매도 주문 처리 오류: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    async def _place_sell_order(
        self,
        env_dv: str,
        cano: str,
        acnt_prdt_cd: str,
        appkey: str,
        access_token: str,
        stock_code: str,
        ord_dvsn: str,
        ord_qty: str,
        ord_unpr: str
    ) -> Dict[str, Any]:
        """
        KIS API로 매도 주문 전송

        Args:
            env_dv: 환경구분 (real: 실전, demo: 모의)
            cano: 종합계좌번호
            acnt_prdt_cd: 계좌상품코드
            appkey: 앱키
            access_token: 액세스 토큰
            stock_code: 종목코드
            ord_dvsn: 주문구분
            ord_qty: 주문수량
            ord_unpr: 주문단가

        Returns:
            주문 결과
        """
        try:
            # Base URL 설정
            base_url = KIS_BASE_URL if env_dv == "real" else KIS_PAPER_URL
            
            # TR_ID 설정
            if env_dv == "real":
                tr_id = "TTTC0011U"  # 실전 매도
            else:
                tr_id = "VTTC0011U"  # 모의 매도

            url = f"{base_url}/uapi/domestic-stock/v1/trading/order-cash"
            
            headers = {
                "authorization": f"Bearer {access_token}",
                "appkey": appkey,
                "appsecret": "",  # 주문 API에서는 appsecret 불필요
                "tr_id": tr_id,
                "custtype": "P",  # 개인
            }

            # 주문 파라미터 (대문자 필수)
            params = {
                "CANO": cano,  # 종합계좌번호
                "ACNT_PRDT_CD": acnt_prdt_cd,  # 계좌상품코드
                "PDNO": stock_code,  # 상품번호 (종목코드)
                "ORD_DVSN": ord_dvsn,  # 주문구분
                "ORD_QTY": ord_qty,  # 주문수량
                "ORD_UNPR": ord_unpr,  # 주문단가
                "EXCG_ID_DVSN_CD": "KRX",  # 거래소ID구분코드
                "SLL_TYPE": "01",  # 매도유형 (01: 일반매도)
                "CNDT_PRIC": ""  # 조건가격 (스탑지정가호가 주문 시 사용)
            }

            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, json=params)
                response.raise_for_status()
                data = response.json()

            # 응답 확인
            if data.get("rt_cd") == "0":  # 성공
                output = data.get("output", {})
                return {
                    "success": True,
                    "order_no": output.get("ODNO"),  # 주문번호
                    "order_time": output.get("ORD_TMD"),  # 주문시간
                    "raw_response": data
                }
            else:
                error_msg = data.get("msg1", "주문 실패")
                error_code = data.get("rt_cd", "N/A")
                logger.error(
                    f"KIS API 주문 실패: "
                    f"rt_cd={error_code}, "
                    f"msg1={error_msg}, "
                    f"stock_code={stock_code}"
                )
                return {
                    "success": False,
                    "error": error_msg,
                    "error_code": error_code,
                    "raw_response": data
                }

        except httpx.HTTPStatusError as e:
            logger.error(f"KIS API HTTP 오류: {e.response.status_code}, {e.response.text}")
            return {
                "success": False,
                "error": f"HTTP {e.response.status_code}: {str(e)}"
            }
        except Exception as e:
            logger.error(f"KIS API 주문 오류: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    async def cancel_order(
        self,
        user_strategy_id: int,
        order_no: str,
        stock_code: str
    ) -> Dict[str, Any]:
        """
        주문 취소 처리

        Args:
            user_strategy_id: 사용자 전략 ID
            order_no: 주문번호
            stock_code: 종목코드

        Returns:
            취소 결과
        """
        try:
            # 1. 전략 설정 조회
            strategy_config = self._redis_manager.get_strategy_config(user_strategy_id)
            if not strategy_config:
                error_msg = f"전략 설정을 찾을 수 없습니다: user_strategy_id={user_strategy_id}"
                logger.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg
                }

            is_mock = strategy_config.get("is_mock", False)

            # 4. 주문 취소 실행
            if is_mock:
                # Mock 모드: 주문 취소 가정
                logger.info(
                    f"[MOCK] 주문 취소 가정: "
                    f"전략={user_strategy_id}, "
                    f"종목={stock_code}, "
                    f"주문번호={order_no}"
                )
                
                return {
                    "success": True,
                    "is_mock": True,
                    "result": {
                        "order_no": order_no,
                        "status": "cancelled"
                    }
                }
            else:
                # 실제 모드: 계좌 정보 조회 필수
                account_connection = self._redis_manager.get_account_connection_by_strategy_id(user_strategy_id)
                if not account_connection:
                    error_msg = f"계좌 연결 정보를 찾을 수 없습니다: user_strategy_id={user_strategy_id}"
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "error": error_msg
                    }

                account_no = account_connection.get("account_no")
                account_product_code = account_connection.get("account_product_code")
                appkey = account_connection.get("appkey")
                env_dv = account_connection.get("env_dv", "demo")  # 기본값: demo
                access_token = account_connection.get("access_token")

                # 계좌번호 분리 (12345678-01 -> cano=12345678, acnt_prdt_cd=01)
                if "-" in account_no:
                    cano, acnt_prdt_cd = account_no.split("-")
                else:
                    cano = account_no
                    acnt_prdt_cd = account_product_code or "01"

                # 실제 모드: KIS API로 주문 취소
                if not access_token:
                    error_msg = f"액세스 토큰이 없습니다: account_no={account_no}"
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "error": error_msg
                    }

                # 주문 취소 API 호출
                cancel_result = await self._place_cancel_order(
                    env_dv=env_dv,
                    cano=cano,
                    acnt_prdt_cd=acnt_prdt_cd,
                    appkey=appkey,
                    access_token=access_token,
                    stock_code=stock_code,
                    order_no=order_no
                )

                if cancel_result.get("success"):
                    logger.info(
                        f"주문 취소 성공: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"주문번호={order_no}"
                    )
                    
                    return {
                        "success": True,
                        "is_mock": False,
                        "result": cancel_result
                    }
                else:
                    logger.error(
                        f"주문 취소 실패: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"오류={cancel_result.get('error', 'N/A')}"
                    )
                    return {
                        "success": False,
                        "is_mock": False,
                        "error": cancel_result.get("error", "주문 취소 실패")
                    }

        except Exception as e:
            logger.error(f"주문 취소 처리 오류: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    async def _place_cancel_order(
        self,
        env_dv: str,
        cano: str,
        acnt_prdt_cd: str,
        appkey: str,
        access_token: str,
        stock_code: str,
        order_no: str
    ) -> Dict[str, Any]:
        """
        KIS API로 주문 취소 전송

        Args:
            env_dv: 환경구분 (real: 실전, demo: 모의)
            cano: 종합계좌번호
            acnt_prdt_cd: 계좌상품코드
            appkey: 앱키
            access_token: 액세스 토큰
            stock_code: 종목코드
            order_no: 주문번호

        Returns:
            취소 결과
        """
        try:
            # Base URL 설정
            base_url = KIS_BASE_URL if env_dv == "real" else KIS_PAPER_URL
            
            # TR_ID 설정 (주문 취소)
            if env_dv == "real":
                tr_id = "TTTC0003U"  # 실전 주문 취소
            else:
                tr_id = "VTTC0003U"  # 모의 주문 취소

            url = f"{base_url}/uapi/domestic-stock/v1/trading/order-rvsecncl"
            
            headers = {
                "authorization": f"Bearer {access_token}",
                "appkey": appkey,
                "appsecret": "",  # 주문 API에서는 appsecret 불필요
                "tr_id": tr_id,
                "custtype": "P",  # 개인
            }

            # 주문 취소 파라미터 (대문자 필수)
            params = {
                "CANO": cano,  # 종합계좌번호
                "ACNT_PRDT_CD": acnt_prdt_cd,  # 계좌상품코드
                "KRX_FWDG_ORD_ORGNO": "",  # KRX 선물 주문 원부번호 (취소 시 빈값)
                "ORGN_ODNO": order_no,  # 원주문번호 (취소할 주문번호)
                "ORD_DVSN": "00",  # 주문구분 (00: 지정가)
                "RVSE_CNCL_DVSN_CD": "02",  # 정정취소구분코드 (02: 취소)
                "ORD_QTY": "0",  # 주문수량 (취소 시 0)
                "ORD_UNPR": "0",  # 주문단가 (취소 시 0)
                "PDNO": stock_code,  # 상품번호 (종목코드)
            }

            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, json=params)
                response.raise_for_status()
                data = response.json()

            # 응답 확인
            if data.get("rt_cd") == "0":  # 성공
                output = data.get("output", {})
                return {
                    "success": True,
                    "order_no": output.get("ODNO"),  # 취소된 주문번호
                    "raw_response": data
                }
            else:
                error_msg = data.get("msg1", "주문 취소 실패")
                error_code = data.get("rt_cd", "N/A")
                logger.error(
                    f"KIS API 주문 취소 실패: "
                    f"rt_cd={error_code}, "
                    f"msg1={error_msg}, "
                    f"stock_code={stock_code}, "
                    f"order_no={order_no}"
                )
                return {
                    "success": False,
                    "error": error_msg,
                    "error_code": error_code,
                    "raw_response": data
                }

        except httpx.HTTPStatusError as e:
            logger.error(f"KIS API HTTP 오류: {e.response.status_code}, {e.response.text}")
            return {
                "success": False,
                "error": f"HTTP {e.response.status_code}: {str(e)}"
            }
        except Exception as e:
            logger.error(f"KIS API 주문 취소 오류: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }


# 싱글톤 인스턴스
_order_api_instance: Optional[OrderAPI] = None


def get_order_api() -> OrderAPI:
    """Order API 싱글톤 인스턴스 반환"""
    global _order_api_instance
    if _order_api_instance is None:
        _order_api_instance = OrderAPI()
    return _order_api_instance
