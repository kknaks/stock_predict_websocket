"""
한국투자증권 주문 API

매도 시그널을 받아 실제 주문을 처리합니다.
- mock 모드: 주문 가정 후 Kafka 발행
- 실제 모드: KIS API로 실제 주문 전송

Position 및 Order 관리:
- 매수 주문 시 Position 생성
- 체결 시 Position 업데이트
- Kafka 메시지에 daily_strategy_id, position 정보 포함
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

    def __init__(self, simulation_mode: bool = False):
        """
        Args:
            simulation_mode: True면 KIS API 호출 없이 mock 주문번호 생성 (테스트용)
        """
        self._redis_manager = get_redis_manager()
        self._order_signal_producer = get_order_signal_producer()
        self._simulation_mode = simulation_mode

    async def process_buy_order(
        self,
        user_strategy_id: int,
        signal: SignalResult,
        order_quantity: int = 1,
        stock_name: str = ""
    ) -> Dict[str, Any]:
        """
        매수 주문 처리

        Args:
            user_strategy_id: 사용자 전략 ID
            signal: 매수 시그널
            order_quantity: 주문 수량 (기본값: 1)
            stock_name: 종목명 (선택)

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
            account_type = strategy_config.get("account_type", "mock")  # real/paper/mock
            user_id = strategy_config.get("user_id")

            # 2. daily_strategy_id 조회 (없으면 생성)
            daily_strategy_id = self._redis_manager.get_daily_strategy_id(user_strategy_id)
            if not daily_strategy_id:
                daily_strategy_id = self._redis_manager.generate_daily_strategy_id()
                self._redis_manager.save_daily_strategy(
                    daily_strategy_id=daily_strategy_id,
                    user_strategy_id=user_strategy_id,
                    data={"is_mock": is_mock, "user_id": user_id}
                )
                logger.info(
                    f"New daily_strategy_id generated: {daily_strategy_id} "
                    f"for user_strategy_id={user_strategy_id}"
                )

            # 3. Position 생성 (없으면)
            stock_code = signal.stock_code
            position = self._redis_manager.create_position_if_not_exists(
                daily_strategy_id=daily_strategy_id,
                user_strategy_id=user_strategy_id,
                stock_code=stock_code,
                stock_name=stock_name
            )

            # 4. 주문 정보 구성
            order_price = int(signal.recommended_order_price)

            # 주문 구분 코드 변환
            # 00: 지정가, 01: 시장가, 02: 조건부지정가, 03: 최유리지정가, 05: 장전시간외종가, 06: 장후시간외종가
            if signal.recommended_order_type == OrderType.LIMIT:
                ord_dvsn = "00"  # 지정가
            elif signal.recommended_order_type == OrderType.MARKET:
                ord_dvsn = "01"  # 시장가
            else:
                ord_dvsn = "00"  # 기본값: 지정가

            # 5. Order 생성 및 저장
            order_id = str(uuid.uuid4())
            order_data = {
                "order_id": order_id,
                "daily_strategy_id": daily_strategy_id,
                "user_strategy_id": user_strategy_id,
                "stock_code": stock_code,
                "order_type": "BUY",
                "order_quantity": order_quantity,
                "order_price": order_price,
                "order_dvsn": ord_dvsn,
                "status": "pending",
                "executed_quantity": 0,
                "executed_price": 0.0,
                "remaining_quantity": order_quantity,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }
            self._redis_manager.save_order(order_data)

            # 6. 주문 실행 (account_type에 따라 분기)
            # mock: API 호출 없이 시뮬레이션
            # paper: 모의투자 API 호출
            # real: 실전투자 API 호출
            if account_type == "mock":
                # Mock 모드: 계좌 정보 없이 주문 가정 (테스트용)
                # Mock 모드에서 주문번호 생성 (타임스탬프 기반 고유 ID)
                mock_order_no = f"MOCK-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"

                logger.info(
                    f"[MOCK] 매수 주문 가정: "
                    f"전략={user_strategy_id}, "
                    f"daily_strategy_id={daily_strategy_id}, "
                    f"종목={stock_code}, "
                    f"수량={order_quantity}, "
                    f"가격={order_price}, "
                    f"주문구분={ord_dvsn}, "
                    f"주문번호={mock_order_no}"
                )

                # Order 상태 업데이트 (체결 완료)
                self._redis_manager.update_order_status(
                    order_id=order_id,
                    status="filled",
                    order_no=mock_order_no,
                    executed_quantity=order_quantity,
                    executed_price=order_price
                )

                # Position 업데이트 (매수 체결)
                self._redis_manager.update_position_buy(
                    daily_strategy_id=daily_strategy_id,
                    stock_code=stock_code,
                    exec_qty=order_quantity,
                    exec_price=order_price
                )

                # 업데이트된 Position 조회
                updated_position = self._redis_manager.get_position(daily_strategy_id, stock_code)

                # Kafka로 주문 결과 발행 (체결되었다고 가정)
                mock_order_result = {
                    "timestamp": datetime.now().isoformat(),
                    "user_strategy_id": user_strategy_id,
                    "daily_strategy_id": daily_strategy_id,  # 신규 추가
                    "stock_name": stock_name,  # 신규 추가
                    "order_type": "BUY",
                    "stock_code": stock_code,
                    "order_no": mock_order_no,
                    "order_quantity": order_quantity,
                    "order_price": order_price,
                    "order_dvsn": ord_dvsn,
                    "account_no": "MOCK",
                    "is_mock": True,
                    "status": "executed",
                    "executed_quantity": order_quantity,
                    "executed_price": order_price,
                    "total_executed_quantity": order_quantity,
                    "total_executed_price": order_price,
                    "remaining_quantity": 0,
                    "is_fully_executed": True,
                    # Position 정보 추가
                    "position": {
                        "holding_quantity": updated_position.get("holding_quantity", order_quantity) if updated_position else order_quantity,
                        "average_price": updated_position.get("average_price", order_price) if updated_position else order_price,
                        "total_buy_quantity": updated_position.get("total_buy_quantity", order_quantity) if updated_position else order_quantity,
                        "total_sell_quantity": updated_position.get("total_sell_quantity", 0) if updated_position else 0,
                        "realized_pnl": updated_position.get("realized_pnl", 0.0) if updated_position else 0.0,
                    } if updated_position else None,
                }

                # 주문 결과를 Kafka로 발행
                await self._order_signal_producer.send_order_result(mock_order_result)

                # 전략 타겟에 매수 정보 업데이트 (기존 호환성 유지)
                self._redis_manager.update_strategy_target_buy(
                    user_strategy_id=user_strategy_id,
                    stock_code=stock_code,
                    buy_quantity=order_quantity,
                    buy_price=order_price,
                    order_no=mock_order_no,
                    executed=True
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
                    "daily_strategy_id": daily_strategy_id,
                    "order_id": order_id,
                    "result": mock_order_result
                }
            else:
                # paper/real 모드: 계좌 정보 조회 필수
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
                appsecret = account_connection.get("appsecret")
                access_token = account_connection.get("access_token")

                # 계좌번호 분리 (12345678-01 -> cano=12345678, acnt_prdt_cd=01)
                if "-" in account_no:
                    cano, acnt_prdt_cd = account_no.split("-")
                else:
                    cano = account_no
                    acnt_prdt_cd = account_product_code or "01"

                # KIS API로 주문 (paper/real)
                if not access_token:
                    error_msg = f"액세스 토큰이 없습니다: account_no={account_no}"
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "error": error_msg
                    }

                # 주문 API 호출 (account_type에 따라 도메인/TR_ID 결정)
                order_result = await self._place_buy_order(
                    account_type=account_type,
                    cano=cano,
                    acnt_prdt_cd=acnt_prdt_cd,
                    appkey=appkey,
                    appsecret=appsecret,
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
                        f"daily_strategy_id={daily_strategy_id}, "
                        f"종목={stock_code}, "
                        f"주문번호={order_no}"
                    )

                    # Order 상태 업데이트 (주문 접수됨)
                    self._redis_manager.update_order_status(
                        order_id=order_id,
                        status="ordered",
                        order_no=order_no
                    )

                    # Kafka로 주문 결과 발행 (주문번호 포함)
                    order_result_message = {
                        "timestamp": datetime.now().isoformat(),
                        "user_strategy_id": user_strategy_id,
                        "daily_strategy_id": daily_strategy_id,  # 신규 추가
                        "stock_name": stock_name,  # 신규 추가
                        "order_type": "BUY",
                        "stock_code": stock_code,
                        "order_no": order_no,
                        "order_quantity": order_quantity,
                        "order_price": order_price,
                        "order_dvsn": ord_dvsn,
                        "account_no": account_no,
                        "is_mock": False,
                        "status": "ordered",
                        "executed_quantity": 0,
                        "executed_price": 0.0,
                        "total_executed_quantity": 0,
                        "total_executed_price": 0.0,
                        "remaining_quantity": order_quantity,
                        "is_fully_executed": False,
                        "position": None,  # 체결 시 업데이트됨
                    }
                    await self._order_signal_producer.send_order_result(order_result_message)

                    # 전략 타겟에 매수 주문 정보 업데이트 (기존 호환성 유지)
                    self._redis_manager.update_strategy_target_buy(
                        user_strategy_id=user_strategy_id,
                        stock_code=stock_code,
                        buy_quantity=order_quantity,
                        buy_price=order_price,
                        order_no=order_no,
                        executed=False
                    )

                    return {
                        "success": True,
                        "is_mock": False,
                        "daily_strategy_id": daily_strategy_id,
                        "order_id": order_id,
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
        account_type: str,
        cano: str,
        acnt_prdt_cd: str,
        appkey: str,
        appsecret: str,
        access_token: str,
        stock_code: str,
        ord_dvsn: str,
        ord_qty: str,
        ord_unpr: str
    ) -> Dict[str, Any]:
        """
        KIS API로 매수 주문 전송

        Args:
            account_type: 계좌유형 (real: 실전, paper: 모의)
            cano: 종합계좌번호
            acnt_prdt_cd: 계좌상품코드
            appkey: 앱키
            appsecret: 앱시크릿
            access_token: 액세스 토큰
            stock_code: 종목코드
            ord_dvsn: 주문구분
            ord_qty: 주문수량
            ord_unpr: 주문단가

        Returns:
            주문 결과
        """
        try:
            # Simulation 모드: API 호출 없이 mock 주문번호 반환 (테스트용)
            if self._simulation_mode:
                sim_order_no = f"SIM-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"
                logger.info(
                    f"[SIMULATION] 매수 주문 시뮬레이션: "
                    f"종목={stock_code}, 수량={ord_qty}, 가격={ord_unpr}, "
                    f"주문번호={sim_order_no}"
                )
                return {
                    "success": True,
                    "order_no": sim_order_no,
                    "order_time": datetime.now().strftime("%H%M%S"),
                    "raw_response": {"simulation": True}
                }

            # Base URL 및 TR_ID 설정 (account_type에 따라)
            # real: 실전투자 / paper: 모의투자
            if account_type == "real":
                base_url = KIS_BASE_URL
                tr_id = "TTTC0012U"  # 실전 매수
            else:
                # paper (모의투자)
                base_url = KIS_PAPER_URL
                tr_id = "VTTC0012U"  # 모의 매수

            url = f"{base_url}/uapi/domestic-stock/v1/trading/order-cash"
            
            headers = {
                "authorization": f"Bearer {access_token}",
                "appkey": appkey,
                "appsecret": appsecret,
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
                "EXCG_ID_DVSN_CD": "SOR" if account_type == "real" else "KRX",  # real: SOR(통합), paper: KRX
                "SLL_TYPE": "",  # 매도유형 (매수 시 빈값)
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
        order_quantity: int = 1,
        stock_name: str = ""
    ) -> Dict[str, Any]:
        """
        매도 주문 처리

        Args:
            user_strategy_id: 사용자 전략 ID
            signal: 매도 시그널
            order_quantity: 주문 수량 (기본값: 1)
            stock_name: 종목명 (선택)

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
            account_type = strategy_config.get("account_type", "mock")  # real/paper/mock
            user_id = strategy_config.get("user_id")

            # 2. daily_strategy_id 조회
            daily_strategy_id = self._redis_manager.get_daily_strategy_id(user_strategy_id)
            if not daily_strategy_id:
                error_msg = f"daily_strategy_id를 찾을 수 없습니다: user_strategy_id={user_strategy_id}"
                logger.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg
                }

            # 3. Position 확인 (보유 수량 검증)
            stock_code = signal.stock_code
            position = self._redis_manager.get_position(daily_strategy_id, stock_code)

            if not position:
                error_msg = f"Position을 찾을 수 없습니다: daily_strategy_id={daily_strategy_id}, stock_code={stock_code}"
                logger.warning(error_msg)
                # Position이 없어도 주문은 진행 (기존 호환성)

            holding_quantity = position.get("holding_quantity", 0) if position else 0

            # 보유 수량 검증 (Mock 모드가 아닌 경우만)
            if not is_mock and holding_quantity <= 0:
                error_msg = f"보유 수량이 없습니다: daily_strategy_id={daily_strategy_id}, stock_code={stock_code}"
                logger.warning(error_msg)
                return {
                    "success": False,
                    "error": error_msg
                }

            # 실제 매도 수량 결정 (보유 수량과 비교)
            if holding_quantity > 0:
                actual_sell_quantity = min(order_quantity, holding_quantity)
            else:
                actual_sell_quantity = order_quantity

            # 4. 주문 정보 구성
            order_price = int(signal.recommended_order_price)

            # 주문 구분 코드 변환
            # 00: 지정가, 01: 시장가, 02: 조건부지정가, 03: 최유리지정가, 05: 장전시간외종가, 06: 장후시간외종가
            if signal.recommended_order_type == OrderType.LIMIT:
                ord_dvsn = "00"  # 지정가
            elif signal.recommended_order_type == OrderType.MARKET:
                ord_dvsn = "01"  # 시장가
            else:
                ord_dvsn = "00"  # 기본값: 지정가

            # 시장가 주문의 경우 주문 단가는 "0"
            if ord_dvsn == "01":
                order_price = 0

            # 5. Order 생성 및 저장
            order_id = str(uuid.uuid4())
            order_data = {
                "order_id": order_id,
                "daily_strategy_id": daily_strategy_id,
                "user_strategy_id": user_strategy_id,
                "stock_code": stock_code,
                "order_type": "SELL",
                "order_quantity": actual_sell_quantity,
                "order_price": order_price,
                "order_dvsn": ord_dvsn,
                "status": "pending",
                "executed_quantity": 0,
                "executed_price": 0.0,
                "remaining_quantity": actual_sell_quantity,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }
            self._redis_manager.save_order(order_data)

            # 6. 주문 실행 (account_type에 따라 분기)
            # mock: API 호출 없이 시뮬레이션
            # paper: 모의투자 API 호출
            # real: 실전투자 API 호출
            if account_type == "mock":
                # Mock 모드: 계좌 정보 없이 주문 가정 (테스트용)
                # Mock 모드에서 주문번호 생성 (타임스탬프 기반 고유 ID)
                mock_order_no = f"MOCK-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"

                # 체결 가격 결정 (시장가인 경우 현재가 사용)
                exec_price = order_price
                if exec_price <= 0:
                    # 1순위: signal의 현재가
                    exec_price = int(signal.current_price) if signal.current_price > 0 else 0
                if exec_price <= 0:
                    # 2순위: Redis에서 실시간 현재가 조회
                    redis_price = self._redis_manager.get_current_price(stock_code)
                    if redis_price:
                        exec_price = int(redis_price)

                logger.info(
                    f"[MOCK] 매도 주문 가정: "
                    f"전략={user_strategy_id}, "
                    f"daily_strategy_id={daily_strategy_id}, "
                    f"종목={stock_code}, "
                    f"수량={actual_sell_quantity}, "
                    f"가격={exec_price}, "
                    f"주문구분={ord_dvsn}, "
                    f"주문번호={mock_order_no}"
                )

                # Order 상태 업데이트 (체결 완료)
                self._redis_manager.update_order_status(
                    order_id=order_id,
                    status="filled",
                    order_no=mock_order_no,
                    executed_quantity=actual_sell_quantity,
                    executed_price=exec_price
                )

                # Position 업데이트 (매도 체결)
                self._redis_manager.update_position_sell(
                    daily_strategy_id=daily_strategy_id,
                    stock_code=stock_code,
                    exec_qty=actual_sell_quantity,
                    exec_price=exec_price
                )

                # 업데이트된 Position 조회
                updated_position = self._redis_manager.get_position(daily_strategy_id, stock_code)

                # Kafka로 주문 결과 발행 (체결되었다고 가정)
                mock_order_result = {
                    "timestamp": datetime.now().isoformat(),
                    "user_strategy_id": user_strategy_id,
                    "daily_strategy_id": daily_strategy_id,  # 신규 추가
                    "stock_name": stock_name,  # 신규 추가
                    "order_type": "SELL",
                    "stock_code": stock_code,
                    "order_no": mock_order_no,
                    "order_quantity": actual_sell_quantity,
                    "order_price": exec_price,
                    "order_dvsn": ord_dvsn,
                    "account_no": "MOCK",
                    "is_mock": True,
                    "status": "executed",
                    "executed_quantity": actual_sell_quantity,
                    "executed_price": exec_price,
                    "total_executed_quantity": actual_sell_quantity,
                    "total_executed_price": exec_price,
                    "remaining_quantity": 0,
                    "is_fully_executed": True,
                    # Position 정보 추가
                    "position": {
                        "holding_quantity": updated_position.get("holding_quantity", 0) if updated_position else 0,
                        "average_price": updated_position.get("average_price", 0) if updated_position else 0,
                        "total_buy_quantity": updated_position.get("total_buy_quantity", 0) if updated_position else 0,
                        "total_sell_quantity": updated_position.get("total_sell_quantity", actual_sell_quantity) if updated_position else actual_sell_quantity,
                        "realized_pnl": updated_position.get("realized_pnl", 0.0) if updated_position else 0.0,
                    } if updated_position else None,
                }

                # 주문 결과를 Kafka로 발행
                await self._order_signal_producer.send_order_result(mock_order_result)

                # 전략 타겟에 매도 정보 업데이트 (기존 호환성 유지)
                self._redis_manager.update_strategy_target_sell(
                    user_strategy_id=user_strategy_id,
                    stock_code=stock_code,
                    sell_quantity=actual_sell_quantity,
                    sell_price=exec_price,
                    order_no=mock_order_no,
                    executed=True
                )

                logger.info(f"[MOCK] 매도 주문 처리 완료: {stock_code}")

                # Slack 알림 전송
                await send_slack(
                    f"[MOCK 매도 체결] {stock_code} | "
                    f"{actual_sell_quantity}주 @ {exec_price:,}원 | "
                    f"매도 사유 : {signal.reason}"
                )

                return {
                    "success": True,
                    "is_mock": True,
                    "daily_strategy_id": daily_strategy_id,
                    "order_id": order_id,
                    "result": mock_order_result
                }
            else:
                # paper/real 모드: 계좌 정보 조회 필수
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
                appsecret = account_connection.get("appsecret")
                access_token = account_connection.get("access_token")

                # 계좌번호 분리 (12345678-01 -> cano=12345678, acnt_prdt_cd=01)
                if "-" in account_no:
                    cano, acnt_prdt_cd = account_no.split("-")
                else:
                    cano = account_no
                    acnt_prdt_cd = account_product_code or "01"

                # KIS API로 주문 (paper/real)
                if not access_token:
                    error_msg = f"액세스 토큰이 없습니다: account_no={account_no}"
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "error": error_msg
                    }

                # 주문 API 호출 (account_type에 따라 도메인/TR_ID 결정)
                order_result = await self._place_sell_order(
                    account_type=account_type,
                    cano=cano,
                    acnt_prdt_cd=acnt_prdt_cd,
                    appkey=appkey,
                    appsecret=appsecret,
                    access_token=access_token,
                    stock_code=stock_code,
                    ord_dvsn=ord_dvsn,
                    ord_qty=str(actual_sell_quantity),
                    ord_unpr=str(order_price)
                )

                if order_result.get("success"):
                    order_no = order_result.get('order_no')
                    logger.info(
                        f"매도 주문 성공: "
                        f"전략={user_strategy_id}, "
                        f"daily_strategy_id={daily_strategy_id}, "
                        f"종목={stock_code}, "
                        f"주문번호={order_no}"
                    )

                    # Order 상태 업데이트 (주문 접수됨)
                    self._redis_manager.update_order_status(
                        order_id=order_id,
                        status="ordered",
                        order_no=order_no
                    )

                    # Kafka로 주문 결과 발행 (주문번호 포함)
                    order_result_message = {
                        "timestamp": datetime.now().isoformat(),
                        "user_strategy_id": user_strategy_id,
                        "daily_strategy_id": daily_strategy_id,  # 신규 추가
                        "stock_name": stock_name,  # 신규 추가
                        "order_type": "SELL",
                        "stock_code": stock_code,
                        "order_no": order_no,
                        "order_quantity": actual_sell_quantity,
                        "order_price": order_price,
                        "order_dvsn": ord_dvsn,
                        "account_no": account_no,
                        "is_mock": False,
                        "status": "ordered",
                        "executed_quantity": 0,
                        "executed_price": 0.0,
                        "total_executed_quantity": 0,
                        "total_executed_price": 0.0,
                        "remaining_quantity": actual_sell_quantity,
                        "is_fully_executed": False,
                        "position": None,  # 체결 시 업데이트됨
                    }
                    await self._order_signal_producer.send_order_result(order_result_message)

                    # 전략 타겟에 매도 주문 정보 업데이트 (기존 호환성 유지)
                    self._redis_manager.update_strategy_target_sell(
                        user_strategy_id=user_strategy_id,
                        stock_code=stock_code,
                        sell_quantity=actual_sell_quantity,
                        sell_price=order_price,
                        order_no=order_no,
                        executed=False
                    )

                    return {
                        "success": True,
                        "is_mock": False,
                        "daily_strategy_id": daily_strategy_id,
                        "order_id": order_id,
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
        account_type: str,
        cano: str,
        acnt_prdt_cd: str,
        appkey: str,
        appsecret: str,
        access_token: str,
        stock_code: str,
        ord_dvsn: str,
        ord_qty: str,
        ord_unpr: str
    ) -> Dict[str, Any]:
        """
        KIS API로 매도 주문 전송

        Args:
            account_type: 계좌유형 (real: 실전, paper: 모의)
            cano: 종합계좌번호
            acnt_prdt_cd: 계좌상품코드
            appkey: 앱키
            appsecret: 앱시크릿
            access_token: 액세스 토큰
            stock_code: 종목코드
            ord_dvsn: 주문구분
            ord_qty: 주문수량
            ord_unpr: 주문단가

        Returns:
            주문 결과
        """
        try:
            # Simulation 모드: API 호출 없이 mock 주문번호 반환 (테스트용)
            if self._simulation_mode:
                sim_order_no = f"SIM-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"
                logger.info(
                    f"[SIMULATION] 매도 주문 시뮬레이션: "
                    f"종목={stock_code}, 수량={ord_qty}, 가격={ord_unpr}, "
                    f"주문번호={sim_order_no}"
                )
                return {
                    "success": True,
                    "order_no": sim_order_no,
                    "order_time": datetime.now().strftime("%H%M%S"),
                    "raw_response": {"simulation": True}
                }

            # Base URL 및 TR_ID 설정 (account_type에 따라)
            # real: 실전투자 / paper: 모의투자
            if account_type == "real":
                base_url = KIS_BASE_URL
                tr_id = "TTTC0011U"  # 실전 매도
            else:
                # paper (모의투자)
                base_url = KIS_PAPER_URL
                tr_id = "VTTC0011U"  # 모의 매도

            url = f"{base_url}/uapi/domestic-stock/v1/trading/order-cash"

            headers = {
                "authorization": f"Bearer {access_token}",
                "appkey": appkey,
                "appsecret": appsecret,
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
                "EXCG_ID_DVSN_CD": "SOR" if account_type == "real" else "KRX",  # real: SOR(통합), paper: KRX
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

            account_type = strategy_config.get("account_type", "mock")  # real/paper/mock

            # 주문 취소 실행 (account_type에 따라 분기)
            if account_type == "mock":
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
                # paper/real 모드: 계좌 정보 조회 필수
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
                appsecret = account_connection.get("appsecret")
                access_token = account_connection.get("access_token")

                # 계좌번호 분리 (12345678-01 -> cano=12345678, acnt_prdt_cd=01)
                if "-" in account_no:
                    cano, acnt_prdt_cd = account_no.split("-")
                else:
                    cano = account_no
                    acnt_prdt_cd = account_product_code or "01"

                # KIS API로 주문 취소 (paper/real)
                if not access_token:
                    error_msg = f"액세스 토큰이 없습니다: account_no={account_no}"
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "error": error_msg
                    }

                # 주문 취소 API 호출 (account_type에 따라 도메인/TR_ID 결정)
                cancel_result = await self._place_cancel_order(
                    account_type=account_type,
                    cano=cano,
                    acnt_prdt_cd=acnt_prdt_cd,
                    appkey=appkey,
                    appsecret=appsecret,
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
        account_type: str,
        cano: str,
        acnt_prdt_cd: str,
        appkey: str,
        appsecret: str,
        access_token: str,
        stock_code: str,
        order_no: str
    ) -> Dict[str, Any]:
        """
        KIS API로 주문 취소 전송

        Args:
            account_type: 계좌유형 (real: 실전, paper: 모의)
            cano: 종합계좌번호
            acnt_prdt_cd: 계좌상품코드
            appkey: 앱키
            appsecret: 앱시크릿
            access_token: 액세스 토큰
            stock_code: 종목코드
            order_no: 주문번호

        Returns:
            취소 결과
        """
        try:
            # Base URL 및 TR_ID 설정 (account_type에 따라)
            # real: 실전투자 / paper: 모의투자
            if account_type == "real":
                base_url = KIS_BASE_URL
                tr_id = "TTTC0003U"  # 실전 주문 취소
            else:
                # paper (모의투자)
                base_url = KIS_PAPER_URL
                tr_id = "VTTC0003U"  # 모의 주문 취소

            url = f"{base_url}/uapi/domestic-stock/v1/trading/order-rvsecncl"
            
            headers = {
                "authorization": f"Bearer {access_token}",
                "appkey": appkey,
                "appsecret": appsecret,
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

    async def modify_order_to_market(
        self,
        user_strategy_id: int,
        order_no: str,
        stock_code: str,
        remaining_quantity: int
    ) -> Dict[str, Any]:
        """
        주문 정정 처리 (시장가로 변경)

        기존 지정가 주문을 시장가 주문으로 정정합니다.
        장 마감 시 미체결 매도 주문을 시장가로 변경할 때 사용합니다.

        Args:
            user_strategy_id: 사용자 전략 ID
            order_no: 원주문번호
            stock_code: 종목코드
            remaining_quantity: 미체결 수량

        Returns:
            정정 결과
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

            account_type = strategy_config.get("account_type", "mock")  # real/paper/mock

            # 주문 정정 실행 (account_type에 따라 분기)
            if account_type == "mock":
                # Mock 모드: 주문 정정 가정
                logger.info(
                    f"[MOCK] 주문 정정 가정 (시장가): "
                    f"전략={user_strategy_id}, "
                    f"종목={stock_code}, "
                    f"주문번호={order_no}, "
                    f"미체결수량={remaining_quantity}"
                )

                return {
                    "success": True,
                    "is_mock": True,
                    "result": {
                        "order_no": order_no,
                        "status": "modified",
                        "new_order_type": "MARKET"
                    }
                }
            else:
                # paper/real 모드: 계좌 정보 조회 필수
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
                appsecret = account_connection.get("appsecret")
                access_token = account_connection.get("access_token")

                # 계좌번호 분리 (12345678-01 -> cano=12345678, acnt_prdt_cd=01)
                if "-" in account_no:
                    cano, acnt_prdt_cd = account_no.split("-")
                else:
                    cano = account_no
                    acnt_prdt_cd = account_product_code or "01"

                # KIS API로 주문 정정 (paper/real)
                if not access_token:
                    error_msg = f"액세스 토큰이 없습니다: account_no={account_no}"
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "error": error_msg
                    }

                # 주문 정정 API 호출 (account_type에 따라 도메인/TR_ID 결정)
                modify_result = await self._place_modify_order(
                    account_type=account_type,
                    cano=cano,
                    acnt_prdt_cd=acnt_prdt_cd,
                    appkey=appkey,
                    appsecret=appsecret,
                    access_token=access_token,
                    stock_code=stock_code,
                    order_no=order_no,
                    ord_dvsn="01",  # 시장가
                    ord_qty=str(remaining_quantity),
                    ord_unpr="0"  # 시장가는 0
                )

                if modify_result.get("success"):
                    logger.info(
                        f"주문 정정 성공 (시장가): "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"주문번호={order_no}"
                    )

                    return {
                        "success": True,
                        "is_mock": False,
                        "result": modify_result
                    }
                else:
                    logger.error(
                        f"주문 정정 실패: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"오류={modify_result.get('error', 'N/A')}"
                    )
                    return {
                        "success": False,
                        "is_mock": False,
                        "error": modify_result.get("error", "주문 정정 실패")
                    }

        except Exception as e:
            logger.error(f"주문 정정 처리 오류: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    async def _place_modify_order(
        self,
        account_type: str,
        cano: str,
        acnt_prdt_cd: str,
        appkey: str,
        appsecret: str,
        access_token: str,
        stock_code: str,
        order_no: str,
        ord_dvsn: str,
        ord_qty: str,
        ord_unpr: str
    ) -> Dict[str, Any]:
        """
        KIS API로 주문 정정 전송

        Args:
            account_type: 계좌유형 (real: 실전, paper: 모의)
            cano: 종합계좌번호
            acnt_prdt_cd: 계좌상품코드
            appkey: 앱키
            appsecret: 앱시크릿
            access_token: 액세스 토큰
            stock_code: 종목코드
            order_no: 원주문번호
            ord_dvsn: 주문구분 (00: 지정가, 01: 시장가)
            ord_qty: 주문수량
            ord_unpr: 주문단가 (시장가는 0)

        Returns:
            정정 결과
        """
        try:
            # Simulation 모드: API 호출 없이 mock 주문번호 반환 (테스트용)
            if self._simulation_mode:
                sim_order_no = f"SIM-MOD-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"
                logger.info(
                    f"[SIMULATION] 주문 정정 시뮬레이션: "
                    f"종목={stock_code}, 수량={ord_qty}, 가격={ord_unpr}, "
                    f"원주문번호={order_no}, 새주문번호={sim_order_no}"
                )
                return {
                    "success": True,
                    "order_no": sim_order_no,
                    "order_time": datetime.now().strftime("%H%M%S"),
                    "raw_response": {"simulation": True}
                }

            # Base URL 및 TR_ID 설정 (account_type에 따라)
            # real: 실전투자 / paper: 모의투자
            if account_type == "real":
                base_url = KIS_BASE_URL
                tr_id = "TTTC0013U"  # 실전 주문 정정취소
            else:
                # paper (모의투자)
                base_url = KIS_PAPER_URL
                tr_id = "VTTC0013U"  # 모의 주문 정정취소

            url = f"{base_url}/uapi/domestic-stock/v1/trading/order-rvsecncl"

            headers = {
                "authorization": f"Bearer {access_token}",
                "appkey": appkey,
                "appsecret": appsecret,
                "tr_id": tr_id,
                "custtype": "P",  # 개인
            }

            # 주문 정정 파라미터 (대문자 필수)
            params = {
                "CANO": cano,  # 종합계좌번호
                "ACNT_PRDT_CD": acnt_prdt_cd,  # 계좌상품코드
                "KRX_FWDG_ORD_ORGNO": "",  # KRX 전송 주문 조직번호 (빈값)
                "ORGN_ODNO": order_no,  # 원주문번호 (정정할 주문번호)
                "ORD_DVSN": ord_dvsn,  # 주문구분 (00: 지정가, 01: 시장가)
                "RVSE_CNCL_DVSN_CD": "01",  # 정정취소구분코드 (01: 정정)
                "ORD_QTY": ord_qty,  # 주문수량
                "ORD_UNPR": ord_unpr,  # 주문단가 (시장가는 0)
                "QTY_ALL_ORD_YN": "Y",  # 잔량전부주문여부 (Y: 전량)
                "EXCG_ID_DVSN_CD": "SOR" if account_type == "real" else "KRX",  # real: SOR(통합), paper: KRX
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
                    "order_no": output.get("ODNO"),  # 새 주문번호
                    "order_time": output.get("ORD_TMD"),  # 주문시각
                    "raw_response": data
                }
            else:
                error_msg = data.get("msg1", "주문 정정 실패")
                error_code = data.get("rt_cd", "N/A")
                logger.error(
                    f"KIS API 주문 정정 실패: "
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
            logger.error(f"KIS API 주문 정정 오류: {e}", exc_info=True)
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
