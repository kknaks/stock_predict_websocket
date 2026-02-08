"""
KIS WebSocket Handler

Kafka에서 받은 웹소켓 메시지를 처리
"""

import logging
from typing import Optional, List

from app.models.websocket import WebSocketCommand, StartCommand, StopCommand, ClosingCommand
from app.kis.websocket.manager import get_websocket_manager
from app.kis.websocket.redis_manager import get_redis_manager
from app.kis.api.order_api import get_order_api
from app.service.strategy_table import get_strategy_table
from app.service.calculate_slippage import SignalResult, OrderType

logger = logging.getLogger(__name__)


class WebSocketHandler:
    """웹소켓 메시지 처리 핸들러"""

    def __init__(self):
        self._manager = get_websocket_manager()
        self._redis_manager = get_redis_manager()
        self._order_api = get_order_api()
        self._strategy_table = get_strategy_table()

    async def handle_command(self, websocket_msg: WebSocketCommand) -> None:
        """
        웹소켓 메시지 처리

        Args:
            websocket_msg: 웹소켓 메시지
        """
        try:
            if websocket_msg.command == "START":
                await self._handle_start_command(websocket_msg)
            elif websocket_msg.command == "CLOSING":
                await self._handle_close_command(websocket_msg)
            elif websocket_msg.command == "STOP":
                await self._handle_stop_command(websocket_msg)
            else:
                logger.warning(f"Unknown websocket command: {websocket_msg.command}")

        except Exception as e:
            logger.error(f"Error handling websocket message {websocket_msg.command}: {e}", exc_info=True)

    async def _handle_start_command(self, websocket_msg: WebSocketCommand) -> None:
        """START 웹소켓 메시지 처리"""
        try:
            start_command = websocket_msg.to_start_command()
            logger.info(
                f"Processing START websocket message: "
                f"target={start_command.target}, "
                f"exchange_type={start_command.exchange_type}, "
                f"stocks={len(start_command.config.stocks)}, "
                f"accounts={len(start_command.config.accounts)}"
            )

            # 전략 테이블 초기화 (config에 strategies가 있거나 users의 strategies가 있는 경우)
            strategy_table = get_strategy_table()
            all_strategies = []
            
            # config.strategies가 있으면 추가 (user_id가 없으면 account_id로 찾기)
            if start_command.config.strategies:
                for strategy in start_command.config.strategies:
                    strategy_dict = strategy.copy() if isinstance(strategy, dict) else dict(strategy)

                    # user_id가 없고 account_id가 있으면 users에서 찾기
                    if "user_id" not in strategy_dict and "account_id" in strategy_dict:
                        account_id = strategy_dict["account_id"]
                        # users 리스트에서 account_id로 user 찾기
                        for user in start_command.config.users:
                            if user.account.account_id == account_id:
                                strategy_dict["user_id"] = user.user_id
                                # account_type도 함께 설정
                                strategy_dict["account_type"] = user.account.account_type
                                break
                        else:
                            logger.warning(
                                f"Could not find user_id for account_id={account_id} in strategy: {strategy_dict}"
                            )

                    # config.is_mock을 항상 설정
                    strategy_dict["is_mock"] = start_command.config.is_mock
                    # account_type이 없으면 기본값 설정
                    if "account_type" not in strategy_dict:
                        strategy_dict["account_type"] = "mock" if start_command.config.is_mock else "real"

                    all_strategies.append(strategy_dict)
            
            # users의 strategies도 수집 (user_id, account_type 추가)
            if start_command.config.users:
                for user in start_command.config.users:
                    if user.strategies:
                        # 각 전략에 user_id, account_type 추가
                        for strategy in user.strategies:
                            strategy_with_user_id = strategy.copy() if isinstance(strategy, dict) else dict(strategy)
                            strategy_with_user_id["user_id"] = user.user_id
                            # config.is_mock을 항상 설정
                            strategy_with_user_id["is_mock"] = start_command.config.is_mock
                            # account_type 설정 (user의 account에서 가져옴)
                            strategy_with_user_id["account_type"] = user.account.account_type
                            all_strategies.append(strategy_with_user_id)
            
            if all_strategies:
                try:
                    await strategy_table.initialize_from_start_command(all_strategies)
                    logger.info(
                        f"Initialized strategy tables for {len(all_strategies)} strategies: "
                        f"{[s.get('user_strategy_id') for s in all_strategies]}"
                    )

                    # daily_strategy_id 생성 및 저장
                    for strategy in all_strategies:
                        user_strategy_id = strategy.get("user_strategy_id")
                        if user_strategy_id:
                            # 기존 daily_strategy_id 확인
                            existing_id = self._redis_manager.get_daily_strategy_id(user_strategy_id)
                            if not existing_id:
                                # 새 daily_strategy_id 생성
                                daily_strategy_id = self._redis_manager.generate_daily_strategy_id()
                                self._redis_manager.save_daily_strategy(
                                    daily_strategy_id=daily_strategy_id,
                                    user_strategy_id=user_strategy_id,
                                    data={
                                        "is_mock": strategy.get("is_mock", False),
                                        "account_type": strategy.get("account_type", "mock"),
                                        "user_id": strategy.get("user_id"),
                                        "strategy_id": strategy.get("strategy_id"),
                                    }
                                )
                                logger.info(
                                    f"Created daily_strategy_id={daily_strategy_id} "
                                    f"for user_strategy_id={user_strategy_id}"
                                )
                            else:
                                logger.info(
                                    f"Using existing daily_strategy_id={existing_id} "
                                    f"for user_strategy_id={user_strategy_id}"
                                )

                except Exception as e:
                    logger.error(f"Error initializing strategy tables: {e}", exc_info=True)
            else:
                logger.info("No strategies provided in START command, skipping strategy table initialization")

            success = await self._manager.start_all(start_command)
            if success:
                logger.info("START websocket message processed successfully")
            else:
                logger.error("Failed to process START websocket message")

        except ValueError as e:
            logger.error(f"Invalid START websocket message: {e}")
        except Exception as e:
            logger.error(f"Error processing START websocket message: {e}", exc_info=True)
            raise

    async def _handle_close_command(self, websocket_msg: WebSocketCommand) -> None:
        """CLOSING 웹소켓 메시지 처리 - 장 마감 시 주문 마감

        exchange_type이 있으면 해당 거래소 종목만 마감 처리.
        모든 price client가 종료되면 daily_strategy 삭제 및 전략 데이터 정리.
        """
        try:
            closing_command = websocket_msg.to_closing_command()
            exchange_type = closing_command.exchange_type
            logger.info(
                f"Processing CLOSING websocket message: "
                f"target={closing_command.target}, exchange_type={exchange_type}"
            )

            # 1. 모든 전략 ID 조회
            all_strategy_ids = self._strategy_table.get_all_strategies()
            logger.info(f"전체 전략 수: {len(all_strategy_ids)}, 전략 IDs: {all_strategy_ids}")

            if exchange_type:
                # exchange_type 기반 부분 마감
                # 1. 해당 exchange의 price websocket 종료
                await self._manager._stop_price_websocket(exchange_type)

                # 2. 해당 exchange의 종목만 마감 처리
                exchange_stocks = self._get_stocks_for_exchange(exchange_type)
                logger.info(
                    f"CLOSING {exchange_type}: 해당 종목 {len(exchange_stocks)}개 마감 처리"
                )

                for user_strategy_id in all_strategy_ids:
                    try:
                        await self._close_active_buy_orders(user_strategy_id, stock_codes=exchange_stocks)
                        await self._close_unsold_positions(user_strategy_id, stock_codes=exchange_stocks)
                    except Exception as e:
                        logger.error(
                            f"전략별 주문 마감 처리 오류 (user_strategy_id={user_strategy_id}): {e}",
                            exc_info=True
                        )

                # 3. 모든 price client가 종료되었는지 확인
                if not self._manager.has_running_price_clients():
                    # 마지막 마감 → daily_strategy 삭제 + 전략 데이터 정리
                    logger.info("모든 price client 종료됨. daily_strategy 삭제 및 전략 데이터 정리")
                    for user_strategy_id in all_strategy_ids:
                        self._redis_manager.delete_daily_strategy(user_strategy_id)
                    self._strategy_table.clear_all_strategies()
                else:
                    logger.info(
                        f"CLOSING {exchange_type} 완료. "
                        f"아직 실행 중인 price client 있음 - 전략 데이터 유지"
                    )
            else:
                # 하위호환: 기존 로직 (전체 마감)
                for user_strategy_id in all_strategy_ids:
                    try:
                        await self._close_active_buy_orders(user_strategy_id)
                        await self._close_unsold_positions(user_strategy_id)
                    except Exception as e:
                        logger.error(
                            f"전략별 주문 마감 처리 오류 (user_strategy_id={user_strategy_id}): {e}",
                            exc_info=True
                        )

                # daily_strategy 데이터 삭제
                for user_strategy_id in all_strategy_ids:
                    self._redis_manager.delete_daily_strategy(user_strategy_id)
                logger.info(f"Deleted daily_strategy for {len(all_strategy_ids)} strategies")

                # 메모리 및 Redis 전략 데이터 정리
                self._strategy_table.clear_all_strategies()

            logger.info("CLOSING websocket message processed successfully")

        except ValueError as e:
            logger.error(f"Invalid CLOSING websocket message: {e}")
        except Exception as e:
            logger.error(f"Error processing CLOSING websocket message: {e}", exc_info=True)

    async def _close_active_buy_orders(
        self, user_strategy_id: int, stock_codes: Optional[List[str]] = None
    ) -> None:
        """Order 기반 미체결 매수 주문 취소

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_codes: 처리할 종목 코드 리스트. None이면 전체.
        """
        try:
            daily_strategy_id = self._redis_manager.get_daily_strategy_id(user_strategy_id)
            if not daily_strategy_id:
                logger.debug(f"daily_strategy_id 없음: user_strategy_id={user_strategy_id}")
                return

            # strategy:targets에서 종목 목록 조회
            all_targets = self._redis_manager.get_strategy_all_targets(user_strategy_id)
            if not all_targets:
                logger.debug(f"전략 타겟 없음: user_strategy_id={user_strategy_id}")
                return

            # stock_codes 필터 적용
            target_stock_codes = list(all_targets.keys())
            if stock_codes is not None:
                target_stock_codes = [s for s in target_stock_codes if s in stock_codes]

            if not target_stock_codes:
                return

            logger.info(
                f"미체결 매수 주문 취소 처리: "
                f"user_strategy_id={user_strategy_id}, "
                f"daily_strategy_id={daily_strategy_id}, "
                f"종목수={len(target_stock_codes)}"
            )

            for stock_code in target_stock_codes:
                # 활성 매수 주문 조회
                active_buy = self._redis_manager.get_active_buy_order(daily_strategy_id, stock_code)
                if not active_buy:
                    continue

                order_no = active_buy.get("order_no")
                order_quantity = active_buy.get("order_quantity", 0)
                executed_quantity = active_buy.get("executed_quantity", 0)
                remaining_quantity = order_quantity - executed_quantity

                if not order_no or remaining_quantity <= 0:
                    continue

                logger.info(
                    f"매수 주문 취소: "
                    f"전략={user_strategy_id}, "
                    f"종목={stock_code}, "
                    f"주문번호={order_no}, "
                    f"주문수량={order_quantity}, "
                    f"체결수량={executed_quantity}, "
                    f"미체결수량={remaining_quantity}"
                )

                cancel_result = await self._order_api.cancel_order(
                    user_strategy_id=user_strategy_id,
                    order_no=order_no,
                    stock_code=stock_code
                )

                if cancel_result.get("success"):
                    logger.info(
                        f"✅ 매수 주문 취소 완료: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"주문번호={order_no}"
                    )
                else:
                    logger.error(
                        f"❌ 매수 주문 취소 실패: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"주문번호={order_no}, "
                        f"오류={cancel_result.get('error', 'N/A')}"
                    )

        except Exception as e:
            logger.error(f"미체결 매수 주문 취소 처리 오류: {e}", exc_info=True)

    async def _close_unsold_positions(
        self, user_strategy_id: int, stock_codes: Optional[List[str]] = None
    ) -> None:
        """Position 기반 미매도 종목 시장가 매도

        Case 1: 활성 매도 주문 없음 → 새 시장가 매도 주문 생성
        Case 2: 활성 매도 주문 있음 (미체결/부분체결) → 시장가로 정정 주문

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_codes: 처리할 종목 코드 리스트. None이면 전체.
        """
        try:
            # daily_strategy_id 조회
            daily_strategy_id = self._redis_manager.get_daily_strategy_id(user_strategy_id)
            if not daily_strategy_id:
                logger.debug(f"daily_strategy_id 없음: user_strategy_id={user_strategy_id}")
                return

            # 보유 수량이 있는 모든 Position 조회
            positions_with_holdings = self._redis_manager.get_positions_with_holdings(daily_strategy_id)

            # stock_codes 필터 적용
            if stock_codes is not None:
                positions_with_holdings = [
                    p for p in positions_with_holdings
                    if p.get("stock_code") in stock_codes
                ]

            if not positions_with_holdings:
                logger.info(f"미매도 Position 없음: user_strategy_id={user_strategy_id}")
                return

            logger.info(
                f"미매도 Position 마감 처리: "
                f"user_strategy_id={user_strategy_id}, "
                f"daily_strategy_id={daily_strategy_id}, "
                f"종목수={len(positions_with_holdings)}"
            )

            for position in positions_with_holdings:
                stock_code = position.get("stock_code")
                holding_quantity = position.get("holding_quantity", 0)
                stock_name = position.get("stock_name", "")

                if holding_quantity <= 0:
                    continue

                # 활성 매도 주문 확인
                active_sell = self._redis_manager.get_active_sell_order(daily_strategy_id, stock_code)

                if active_sell:
                    # Case 2: 이미 매도 주문 존재 → 시장가로 정정 주문
                    order_no = active_sell.get("order_no")
                    order_quantity = active_sell.get("order_quantity", 0)
                    executed_quantity = active_sell.get("executed_quantity", 0)
                    remaining_quantity = order_quantity - executed_quantity

                    if not order_no or remaining_quantity <= 0:
                        logger.debug(
                            f"정정 대상 없음 (주문번호 없거나 미체결 수량 없음): "
                            f"종목={stock_code}, order_no={order_no}, remaining={remaining_quantity}"
                        )
                        continue

                    logger.info(
                        f"매도 주문 정정 (시장가로 변경): "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"주문번호={order_no}, "
                        f"미체결수량={remaining_quantity}"
                    )

                    # 시장가로 정정 주문
                    modify_result = await self._order_api.modify_order_to_market(
                        user_strategy_id=user_strategy_id,
                        order_no=order_no,
                        stock_code=stock_code,
                        remaining_quantity=remaining_quantity
                    )

                    if modify_result.get("success"):
                        logger.info(
                            f"✅ 매도 주문 정정 완료 (시장가): "
                            f"전략={user_strategy_id}, "
                            f"종목={stock_code}, "
                            f"주문번호={order_no}"
                        )
                    else:
                        logger.error(
                            f"❌ 매도 주문 정정 실패: "
                            f"전략={user_strategy_id}, "
                            f"종목={stock_code}, "
                            f"오류={modify_result.get('error', 'N/A')}"
                        )
                else:
                    # Case 1: 매도 주문 없음 → 새 시장가 매도 주문 생성
                    logger.info(
                        f"신규 시장가 매도 (Position 기반): "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"보유수량={holding_quantity}"
                    )

                    # 시장가 매도 주문 생성
                    market_sell_signal = SignalResult(
                        signal_type="SELL",
                        stock_code=stock_code,
                        current_price=0.0,
                        target_price=None,
                        target_quantity=holding_quantity,
                        stop_loss_price=None,
                        recommended_order_price=0.0,  # 시장가
                        recommended_order_type=OrderType.MARKET,
                        expected_slippage_pct=0.0,
                        urgency="CRITICAL",
                        reason="장 마감 시 Position 기반 강제 매도 (시장가)"
                    )

                    # 시장가 매도 주문 실행
                    sell_result = await self._order_api.process_sell_order(
                        user_strategy_id=user_strategy_id,
                        signal=market_sell_signal,
                        order_quantity=holding_quantity,
                        stock_name=stock_name
                    )

                    if sell_result.get("success"):
                        logger.info(
                            f"✅ Position 기반 시장가 매도 완료: "
                            f"전략={user_strategy_id}, "
                            f"종목={stock_code}, "
                            f"수량={holding_quantity}"
                        )
                    else:
                        logger.error(
                            f"❌ Position 기반 시장가 매도 실패: "
                            f"전략={user_strategy_id}, "
                            f"종목={stock_code}, "
                            f"오류={sell_result.get('error', 'N/A')}"
                        )

        except Exception as e:
            logger.error(f"Position 기반 마감 처리 오류: {e}", exc_info=True)

    def _get_stocks_for_exchange(self, exchange_type: str) -> List[str]:
        """특정 exchange_type의 종목 코드 목록 조회

        manager의 price client 구독 종목 + strategy_table의 해당 exchange 종목을 합산.
        """
        stocks = set()

        # 1. manager의 price client에서 구독 중인 종목
        client_stocks = self._manager.get_price_client_stocks(exchange_type)
        stocks.update(client_stocks)

        # 2. strategy_table에서 해당 exchange 종목
        strategy_stocks = self._strategy_table.get_stocks_by_exchange_type(exchange_type)
        stocks.update(strategy_stocks)

        return list(stocks)

    async def _handle_stop_command(self, websocket_msg: WebSocketCommand) -> None:
        """STOP 웹소켓 메시지 처리"""
        try:
            stop_command = websocket_msg.to_stop_command()
            logger.info(
                f"Processing STOP websocket message: "
                f"target={stop_command.target}, exchange_type={stop_command.exchange_type}"
            )

            success = await self._manager.stop_all(stop_command)
            if success:
                logger.info("STOP websocket message processed successfully")
            else:
                logger.error("Failed to process STOP websocket message")

        except ValueError as e:
            logger.error(f"Invalid STOP websocket message: {e}")
        except Exception as e:
            logger.error(f"Error processing STOP websocket message: {e}", exc_info=True)
            raise


# 싱글톤 인스턴스
_handler_instance: Optional[WebSocketHandler] = None


def get_websocket_handler() -> WebSocketHandler:
    """WebSocket Handler 싱글톤 인스턴스 반환"""
    global _handler_instance
    if _handler_instance is None:
        _handler_instance = WebSocketHandler()
    return _handler_instance
