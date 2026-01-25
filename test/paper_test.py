"""
Paper 모드 체결통보 테스트

주문은 Mock으로 하되, 체결통보는 실제 KIS 형식으로 시뮬레이션합니다.

흐름:
1. START 메시지 → 전략 초기화
2. Prediction 메시지 → 주문 생성 (status: ordered)
3. 체결통보 시뮬레이션 → KIS 형식 파싱 → Position/Order 업데이트
4. Kafka 발행

실행:
    python test/paper_test.py

필요 사항:
    - Redis: localhost:6379
    - Kafka(Redpanda): localhost:19092
"""

# 환경 변수 설정
import os
os.environ["REDIS_HOST"] = "localhost"
os.environ["KAFKA_USE_INTERNAL"] = "false"

import asyncio
import logging
import sys
import signal
import uuid
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config.settings import get_settings
get_settings.cache_clear()

from app.kafka import get_predict_consumer, get_websocket_consumer
from app.kafka.daily_strategy_producer import get_daily_strategy_producer
from app.kafka.order_signal_producer import get_order_signal_producer
from app.handler.prediction_handler import get_prediction_handler
from app.kis.websocket.redis_manager import get_redis_manager
from app.kis.api.order_api import get_order_api
from app.service.strategy_table import get_strategy_table
from app.service.calculate_slippage import SignalResult, OrderType

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PaperWebSocketHandler:
    """
    Paper 모드용 WebSocket 핸들러

    메시지의 is_mock, account_type 값을 그대로 사용합니다.
    """

    def __init__(self):
        self._redis_manager = get_redis_manager()
        self._strategy_table = get_strategy_table()

    async def handle_command(self, websocket_msg) -> None:
        """웹소켓 메시지 처리"""
        try:
            if websocket_msg.command == "START":
                await self._handle_start_command(websocket_msg)
            elif websocket_msg.command == "CLOSING":
                await self._handle_closing_command(websocket_msg)
            else:
                logger.warning(f"Unknown command: {websocket_msg.command}")
        except Exception as e:
            logger.error(f"Error handling command {websocket_msg.command}: {e}", exc_info=True)

    async def _handle_start_command(self, websocket_msg) -> None:
        """START 명령 처리"""
        try:
            start_command = websocket_msg.to_start_command()
            is_mock = start_command.config.is_mock
            env_dv = start_command.config.env_dv or "real"

            logger.info(
                f"[PAPER] Processing START: "
                f"is_mock={is_mock}, "
                f"users={len(start_command.config.users) if start_command.config.users else 0}"
            )

            all_strategies = []

            if start_command.config.users:
                for user in start_command.config.users:
                    account = user.account
                    account_type = account.account_type if account else "mock"

                    # 계좌 연결 정보 Redis에 저장 (order_api에서 사용)
                    if account:
                        user_strategy_ids = [
                            s.user_strategy_id if hasattr(s, 'user_strategy_id') else s.get('user_strategy_id')
                            for s in user.strategies
                        ] if user.strategies else []

                        self._redis_manager.save_account_connection(
                            account_no=account.account_no,
                            ws_token=account.ws_token or "",
                            appkey=account.app_key or "",
                            env_dv=env_dv,
                            account_product_code=account.account_product_code or "01",
                            is_mock=is_mock,
                            account_type=account_type.lower(),
                            access_token=account.access_token,
                            user_id=user.user_id,
                            user_strategy_ids=user_strategy_ids,
                            hts_id=account.hts_id,
                            status="connected",
                        )
                        logger.info(
                            f"[PAPER] Account connection saved: account_no={account.account_no}, "
                            f"user_strategy_ids={user_strategy_ids}"
                        )

                    if user.strategies:
                        for strategy in user.strategies:
                            strategy_dict = strategy.copy() if isinstance(strategy, dict) else dict(strategy)
                            strategy_dict["user_id"] = user.user_id
                            strategy_dict["is_mock"] = is_mock
                            strategy_dict["account_type"] = account_type.lower()  # PAPER -> paper
                            all_strategies.append(strategy_dict)

                            logger.info(
                                f"[PAPER] Strategy: user_strategy_id={strategy_dict.get('user_strategy_id')}, "
                                f"is_mock={is_mock}, account_type={account_type}"
                            )

            if all_strategies:
                await self._strategy_table.initialize_from_start_command(all_strategies)
                logger.info(f"[PAPER] Strategy table initialized: {len(all_strategies)} strategies")

                for strategy in all_strategies:
                    user_strategy_id = strategy.get("user_strategy_id")
                    if user_strategy_id:
                        existing_id = self._redis_manager.get_daily_strategy_id(user_strategy_id)
                        if not existing_id:
                            daily_strategy_id = self._redis_manager.generate_daily_strategy_id()
                            self._redis_manager.save_daily_strategy(
                                daily_strategy_id=daily_strategy_id,
                                user_strategy_id=user_strategy_id,
                                data={
                                    "is_mock": strategy.get("is_mock"),
                                    "account_type": strategy.get("account_type"),
                                    "user_id": strategy.get("user_id"),
                                }
                            )
                            logger.info(
                                f"[PAPER] Created daily_strategy_id={daily_strategy_id} "
                                f"for user_strategy_id={user_strategy_id}"
                            )
                        else:
                            logger.info(f"[PAPER] Using existing daily_strategy_id={existing_id}")

            logger.info("[PAPER] START command processed")

        except Exception as e:
            logger.error(f"[PAPER] Error processing START: {e}", exc_info=True)

    async def _handle_closing_command(self, websocket_msg) -> None:
        """CLOSING 명령 처리"""
        logger.info("[PAPER] Processing CLOSING command")
        self._strategy_table.clear_all_strategies()
        logger.info("[PAPER] CLOSING command processed")


class ExecutionNoticeSimulator:
    """
    KIS 체결통보 시뮬레이터

    실제 KIS WebSocket에서 받는 것과 동일한 형식의 체결통보를 생성하고 처리합니다.
    """

    def __init__(self):
        self._redis_manager = get_redis_manager()
        self._order_signal_producer = get_order_signal_producer()
        # 대기 중인 주문들 (order_no, stock_code, quantity, price, is_buy, user_strategy_id)
        self._pending_orders: List[Tuple[str, str, int, float, bool, int, str]] = []

    def create_execution_notice_raw(
        self,
        order_no: str,
        stock_code: str,
        exec_qty: int,
        exec_price: float,
        is_buy: bool,
        account_no: str = "test",
        tr_id: str = "H0STCNI9"  # 모의투자
    ) -> str:
        """
        KIS 체결통보 원시 메시지 생성 (암호화 안 됨)

        형식: 0|TR_ID|001|데이터(^로 구분, 26개 필드)
        """
        # 26개 필드
        fields = [
            "CUST001",          # 0: CUST_ID - 고객ID
            account_no,         # 1: ACNT_NO - 계좌번호
            order_no,           # 2: ODER_NO - 주문번호
            str(exec_qty),      # 3: ODER_QTY - 주문수량
            "02" if is_buy else "01",  # 4: SELN_BYOV_CLS - 매수/매도 ("01": 매도, "02": 매수)
            "",                 # 5: RCTF_CLS - 정정취소구분
            "00",               # 6: ODER_KIND - 주문종류
            "",                 # 7: ODER_COND - 주문조건
            stock_code,         # 8: STCK_SHRN_ISCD - 종목코드
            str(exec_qty),      # 9: CNTG_QTY - 체결수량
            str(int(exec_price)),  # 10: CNTG_UNPR - 체결단가
            datetime.now().strftime("%H%M%S"),  # 11: STCK_CNTG_HOUR - 체결시간
            "N",                # 12: RFUS_YN - 거부여부
            "2",                # 13: CNTG_YN - 체결구분 ("1": 접수, "2": 체결)
            "Y",                # 14: ACPT_YN - 접수여부
            "001",              # 15: BRNC_NO - 지점번호
            account_no,         # 16: ACNT_NO2 - 계좌번호2
            "테스트계좌",        # 17: ACNT_NAME - 계좌명
            str(int(exec_price)),  # 18: ORD_COND_PRC - 주문조건가격
            "KRX",              # 19: ORD_EXG_GB - 거래소구분
            "N",                # 20: POPUP_YN - 팝업여부
            "",                 # 21: FILLER - 필러
            "",                 # 22: CRDT_CLS - 신용구분
            "",                 # 23: CRDT_LOAN_DATE - 신용대출일자
            f"{'삼성전자' if stock_code == '005930' else 'SK하이닉스' if stock_code == '000660' else stock_code}",  # 24: CNTG_ISNM40 - 종목명
            str(int(exec_price)),  # 25: ODER_PRC - 주문가격
        ]

        # 원시 메시지 생성
        data = "^".join(fields)
        raw_message = f"0|{tr_id}|001|{data}"

        return raw_message

    def parse_execution_notice(self, raw_message: str) -> dict:
        """
        KIS 체결통보 메시지 파싱 (account_client.py와 동일한 로직)
        """
        parts = raw_message.split('|')

        if len(parts) < 4:
            raise ValueError(f"Invalid message format: expected at least 4 parts, got {len(parts)}")

        is_encrypted = parts[0].strip()
        tr_id = parts[1].strip()
        data_count = parts[2].strip()
        response_data = parts[3].strip() if len(parts) > 3 else ""

        if response_data:
            data_fields = response_data.split('^')

            columns = [
                "CUST_ID", "ACNT_NO", "ODER_NO", "ODER_QTY", "SELN_BYOV_CLS", "RCTF_CLS",
                "ODER_KIND", "ODER_COND", "STCK_SHRN_ISCD", "CNTG_QTY", "CNTG_UNPR",
                "STCK_CNTG_HOUR", "RFUS_YN", "CNTG_YN", "ACPT_YN", "BRNC_NO", "ACNT_NO2",
                "ACNT_NAME", "ORD_COND_PRC", "ORD_EXG_GB", "POPUP_YN", "FILLER", "CRDT_CLS",
                "CRDT_LOAN_DATE", "CNTG_ISNM40", "ODER_PRC"
            ]

            parsed_data = {}
            for i, column in enumerate(columns):
                if i < len(data_fields):
                    parsed_data[column] = data_fields[i].strip()
                else:
                    parsed_data[column] = ""

            parsed_data["_meta"] = {
                "is_encrypted": is_encrypted,
                "tr_id": tr_id,
                "data_count": data_count,
            }

            return parsed_data
        else:
            return {
                "_meta": {
                    "is_encrypted": is_encrypted,
                    "tr_id": tr_id,
                    "data_count": data_count,
                }
            }

    async def process_execution_notice(self, parsed_data: dict) -> bool:
        """
        체결통보 처리 (account_client.py의 _process_execution_notice와 동일한 로직)
        """
        try:
            stock_code = parsed_data.get("STCK_SHRN_ISCD", "").strip()
            order_no = parsed_data.get("ODER_NO", "").strip()
            exec_qty_str = parsed_data.get("CNTG_QTY", "").strip()
            exec_price_str = parsed_data.get("CNTG_UNPR", "").strip()
            order_type = parsed_data.get("SELN_BYOV_CLS", "").strip()
            exec_type = parsed_data.get("CNTG_YN", "").strip()
            account_no = parsed_data.get("ACNT_NO", "").strip()

            # 체결통보인지 확인
            if exec_type and exec_type != "2":
                logger.debug(f"접수통보 수신 (체결 아님): 종목={stock_code}, CNTG_YN={exec_type}")
                return False

            logger.info(
                f"📨 체결통보 처리 시작: "
                f"종목={stock_code}, "
                f"주문번호={order_no}, "
                f"체결수량={exec_qty_str}, "
                f"체결단가={exec_price_str}, "
                f"매수매도={'매수' if order_type == '02' else '매도'}"
            )

            if stock_code and order_no and exec_price_str and exec_qty_str:
                exec_price = float(exec_price_str)
                exec_qty = int(exec_qty_str)
                is_buy = order_type in ["02", "00"]

                # Order 조회
                order_data = self._redis_manager.get_order_by_order_no(order_no)

                if order_data:
                    daily_strategy_id = order_data.get("daily_strategy_id")
                    user_strategy_id = order_data.get("user_strategy_id")
                    order_quantity = order_data.get("order_quantity", 0)
                    order_id = order_data.get("order_id")

                    # Position 업데이트
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

                    # Order 상태 업데이트
                    prev_exec_qty = order_data.get("executed_quantity", 0)
                    total_exec_qty = prev_exec_qty + exec_qty
                    is_fully_executed = total_exec_qty >= order_quantity

                    self._redis_manager.update_order_status(
                        order_id=order_id,
                        status="filled" if is_fully_executed else "partial",
                        executed_quantity=total_exec_qty,
                        executed_price=exec_price
                    )

                    # Position 조회
                    updated_position = self._redis_manager.get_position(daily_strategy_id, stock_code)

                    # Kafka 발행
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
                        "account_no": account_no,
                        "is_mock": False,  # Paper 모드
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
                        f"전량체결={is_fully_executed}"
                    )
                    return True
                else:
                    logger.warning(f"Order not found for order_no={order_no}")
                    return False
            else:
                logger.warning(f"Missing required fields in execution notice")
                return False

        except Exception as e:
            logger.error(f"체결통보 처리 오류: {e}", exc_info=True)
            return False

    def add_pending_order(
        self,
        order_no: str,
        stock_code: str,
        quantity: int,
        price: float,
        is_buy: bool,
        user_strategy_id: int,
        stock_name: str = ""
    ):
        """대기 중인 주문 추가"""
        self._pending_orders.append(
            (order_no, stock_code, quantity, price, is_buy, user_strategy_id, stock_name)
        )
        logger.info(f"[PAPER] 주문 대기열 추가: {stock_code} {'매수' if is_buy else '매도'} {quantity}주 @ {price:,.0f}원")

    async def execute_pending_orders(self, price_change_pct: float = 0.02):
        """
        대기 중인 주문들을 체결 처리

        Args:
            price_change_pct: 체결 가격 변동률 (매도 시 적용)
        """
        for order_no, stock_code, quantity, price, is_buy, user_strategy_id, stock_name in self._pending_orders:
            # 체결 가격 (매도 시 약간 상승)
            exec_price = price if is_buy else price * (1 + price_change_pct)

            # 원시 체결통보 메시지 생성
            raw_message = self.create_execution_notice_raw(
                order_no=order_no,
                stock_code=stock_code,
                exec_qty=quantity,
                exec_price=exec_price,
                is_buy=is_buy
            )

            logger.info(f"\n[PAPER] 체결통보 시뮬레이션:")
            logger.info(f"  원시 메시지: {raw_message[:100]}...")

            # 파싱
            parsed_data = self.parse_execution_notice(raw_message)
            logger.info(f"  파싱 결과: 종목={parsed_data.get('STCK_SHRN_ISCD')}, "
                       f"주문번호={parsed_data.get('ODER_NO')}, "
                       f"체결수량={parsed_data.get('CNTG_QTY')}, "
                       f"체결단가={parsed_data.get('CNTG_UNPR')}")

            # 처리
            await self.process_execution_notice(parsed_data)

        self._pending_orders.clear()


class PaperModeHandler:
    """
    Paper 모드 핸들러

    app의 order_api를 그대로 사용하되, simulation_mode=True로 API 호출을 skip합니다.
    체결은 ExecutionNoticeSimulator를 통해 시뮬레이션합니다.
    """

    def __init__(self, execution_simulator: ExecutionNoticeSimulator):
        from app.kis.api.order_api import OrderAPI

        self._redis_manager = get_redis_manager()
        self._strategy_table = get_strategy_table()
        self._prediction_handler = get_prediction_handler()
        self._execution_simulator = execution_simulator
        # simulation_mode=True로 API 호출 없이 주문번호만 생성
        self._order_api = OrderAPI(simulation_mode=True)

    async def handle_prediction(self, prediction_msg) -> None:
        """
        Prediction 처리 - app 코드 그대로 사용

        1. prediction_handler.handle_prediction() → 목표가 계산 + daily_strategy Kafka 발행
        2. order_api.process_buy_order() → 주문 생성 + order_signal Kafka 발행 (status: ordered)
        """
        try:
            logger.info(f"[PAPER] Received prediction: {prediction_msg.total_count} stocks")

            # 1. 목표가 테이블 생성 (app 코드 그대로)
            await self._prediction_handler.handle_prediction(prediction_msg)

            # 2. 각 전략별로 주문 생성 (order_api 사용)
            all_strategies = self._strategy_table.get_all_strategies()

            for user_strategy_id in all_strategies:
                await self._create_orders_via_order_api(user_strategy_id, prediction_msg)

        except Exception as e:
            logger.error(f"[PAPER] Error handling prediction: {e}", exc_info=True)

    async def _create_orders_via_order_api(self, user_strategy_id: int, prediction_msg) -> None:
        """order_api를 사용한 주문 생성"""
        try:
            targets = self._redis_manager.get_strategy_all_targets(user_strategy_id)
            if not targets:
                logger.warning(f"[PAPER] No targets for user_strategy_id={user_strategy_id}")
                return

            for prediction in prediction_msg.predictions:
                stock_code = prediction.stock_code
                stock_name = prediction.stock_name

                target_data = targets.get(stock_code)
                if not target_data:
                    continue

                buy_price = float(prediction.stock_open)
                buy_quantity = int(target_data.get("target_quantity", 1))

                if buy_quantity <= 0:
                    continue

                # SignalResult 생성
                signal = SignalResult(
                    signal_type="BUY",
                    stock_code=stock_code,
                    current_price=buy_price,
                    target_price=float(target_data.get("target_price", buy_price)),
                    target_quantity=buy_quantity,
                    stop_loss_price=float(target_data.get("stop_loss_price", buy_price * 0.99)),
                    recommended_order_price=buy_price,
                    recommended_order_type=OrderType.LIMIT,
                    expected_slippage_pct=0.0,
                    urgency="NORMAL",
                    reason=f"Paper mode test - {stock_name}"
                )

                # order_api.process_buy_order() 호출 (simulation_mode=True)
                # 이 호출로:
                # - Order 생성 및 Redis 저장
                # - 주문번호 생성 (SIM-...)
                # - Order 상태 업데이트 (status: ordered)
                # - Kafka 발행 (order_signal, status: ordered)
                result = await self._order_api.process_buy_order(
                    user_strategy_id=user_strategy_id,
                    signal=signal,
                    order_quantity=buy_quantity,
                    stock_name=stock_name
                )

                if result.get("success"):
                    # 주문번호 추출
                    order_result = result.get("result", {})
                    order_no = order_result.get("order_no", "")

                    logger.info(
                        f"[PAPER] ✅ 매수 주문 완료 (order_api): {stock_name}({stock_code}) "
                        f"{buy_quantity}주 @ {buy_price:,.0f}원, "
                        f"주문번호={order_no}, status=ordered"
                    )

                    # 체결 대기열에 추가
                    self._execution_simulator.add_pending_order(
                        order_no=order_no,
                        stock_code=stock_code,
                        quantity=buy_quantity,
                        price=buy_price,
                        is_buy=True,
                        user_strategy_id=user_strategy_id,
                        stock_name=stock_name
                    )
                else:
                    logger.error(
                        f"[PAPER] ❌ 매수 주문 실패: {stock_name}({stock_code}), "
                        f"error={result.get('error', 'N/A')}"
                    )

        except Exception as e:
            logger.error(f"[PAPER] Error creating orders: {e}", exc_info=True)


# 전역 변수
shutdown_event = asyncio.Event()


def signal_handler(sig, frame):
    logger.info(f"Received signal {sig}, shutting down...")
    shutdown_event.set()


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Paper 모드 체결통보 테스트")
    parser.add_argument(
        "--exec-delay",
        type=float,
        default=3.0,
        help="주문 후 체결까지 대기 시간 (초)"
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Paper 모드 체결통보 테스트 서버 시작")
    logger.info("=" * 60)
    logger.info("")
    logger.info("설정:")
    logger.info(f"  - 체결 딜레이: {args.exec_delay}초")
    logger.info("")
    logger.info("흐름:")
    logger.info("  1. START → 전략 초기화")
    logger.info("  2. Prediction → 주문 생성 (status: ordered)")
    logger.info("  3. 체결통보 시뮬레이션 → KIS 형식 파싱 → Position 업데이트")
    logger.info("  4. Kafka 발행")
    logger.info("")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Kafka producers 시작
    daily_strategy_producer = get_daily_strategy_producer()
    order_signal_producer = get_order_signal_producer()

    await daily_strategy_producer.start()
    await order_signal_producer.start()
    logger.info("Kafka producers started")

    # 시뮬레이터 생성
    execution_simulator = ExecutionNoticeSimulator()
    paper_handler = PaperModeHandler(execution_simulator)

    # Paper WebSocket Handler (START 처리용)
    paper_ws_handler = PaperWebSocketHandler()

    # Kafka consumers 설정
    websocket_consumer = get_websocket_consumer()
    predict_consumer = get_predict_consumer()

    websocket_consumer.add_handler(paper_ws_handler.handle_command)
    predict_consumer.add_handler(paper_handler.handle_prediction)

    ws_connected = await websocket_consumer.start()
    pred_connected = await predict_consumer.start()

    if not ws_connected or not pred_connected:
        logger.error("Failed to connect Kafka consumers")
        return

    logger.info("Kafka consumers started")
    logger.info("")
    logger.info("서버 준비 완료. Kafka 메시지 대기 중...")
    logger.info("")

    ws_task = asyncio.create_task(websocket_consumer.consume())
    pred_task = asyncio.create_task(predict_consumer.consume())

    # 체결 시뮬레이션 루프
    async def execution_loop():
        while not shutdown_event.is_set():
            await asyncio.sleep(args.exec_delay)
            if execution_simulator._pending_orders:
                logger.info(f"\n[PAPER] {len(execution_simulator._pending_orders)}개 주문 체결 시뮬레이션 시작...")
                await execution_simulator.execute_pending_orders()

    exec_task = asyncio.create_task(execution_loop())

    try:
        await shutdown_event.wait()
    except asyncio.CancelledError:
        pass

    # 정리
    logger.info("Shutting down...")

    for task in [ws_task, pred_task, exec_task]:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await websocket_consumer.stop()
    await predict_consumer.stop()
    await daily_strategy_producer.stop()
    await order_signal_producer.stop()

    logger.info("Server shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
