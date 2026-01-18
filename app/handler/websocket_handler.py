"""
KIS WebSocket Handler

Kafka에서 받은 웹소켓 메시지를 처리
"""

import logging
from typing import Optional

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
                                break
                        else:
                            logger.warning(
                                f"Could not find user_id for account_id={account_id} in strategy: {strategy_dict}"
                            )
                    
                    # config.is_mock을 항상 설정
                    strategy_dict["is_mock"] = start_command.config.is_mock
                    
                    all_strategies.append(strategy_dict)
            
            # users의 strategies도 수집 (user_id 추가)
            if start_command.config.users:
                for user in start_command.config.users:
                    if user.strategies:
                        # 각 전략에 user_id 추가
                        for strategy in user.strategies:
                            strategy_with_user_id = strategy.copy() if isinstance(strategy, dict) else dict(strategy)
                            strategy_with_user_id["user_id"] = user.user_id
                            # config.is_mock을 항상 설정
                            strategy_with_user_id["is_mock"] = start_command.config.is_mock
                            all_strategies.append(strategy_with_user_id)
            
            if all_strategies:
                try:
                    await strategy_table.initialize_from_start_command(all_strategies)
                    logger.info(
                        f"Initialized strategy tables for {len(all_strategies)} strategies: "
                        f"{[s.get('user_strategy_id') for s in all_strategies]}"
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
        """CLOSING 웹소켓 메시지 처리 - 장 마감 시 모든 주문 마감"""
        try:
            closing_command = websocket_msg.to_closing_command()
            logger.info(f"Processing CLOSING websocket message: target={closing_command.target}")

            # 1. 모든 전략 ID 조회
            all_strategy_ids = self._strategy_table.get_all_strategies()
            logger.info(f"전체 전략 수: {len(all_strategy_ids)}, 전략 IDs: {all_strategy_ids}")

            # 2. 각 전략의 모든 타겟 조회 및 처리
            for user_strategy_id in all_strategy_ids:
                try:
                    await self._close_strategy_orders(user_strategy_id)
                except Exception as e:
                    logger.error(
                        f"전략별 주문 마감 처리 오류 (user_strategy_id={user_strategy_id}): {e}",
                        exc_info=True
                    )

            logger.info("CLOSING websocket message processed successfully")

        except ValueError as e:
            logger.error(f"Invalid CLOSING websocket message: {e}")
        except Exception as e:
            logger.error(f"Error processing CLOSING websocket message: {e}", exc_info=True)

    async def _close_strategy_orders(self, user_strategy_id: int) -> None:
        """전략별 주문 마감 처리"""
        try:
            # 전략의 모든 타겟 조회
            all_targets = self._redis_manager.get_strategy_all_targets(user_strategy_id)
            logger.info(
                f"전략 {user_strategy_id}의 종목 수: {len(all_targets)}, "
                f"종목코드: {list(all_targets.keys())}"
            )

            for stock_code, target_data in all_targets.items():
                try:
                    # 매수 주문 처리
                    await self._close_buy_orders(user_strategy_id, stock_code, target_data)
                    
                    # 매도 주문 처리
                    await self._close_sell_orders(user_strategy_id, stock_code, target_data)
                    
                except Exception as e:
                    logger.error(
                        f"종목별 주문 마감 처리 오류 "
                        f"(user_strategy_id={user_strategy_id}, stock_code={stock_code}): {e}",
                        exc_info=True
                    )

        except Exception as e:
            logger.error(f"전략 주문 마감 처리 오류 (user_strategy_id={user_strategy_id}): {e}", exc_info=True)

    async def _close_buy_orders(self, user_strategy_id: int, stock_code: str, target_data: dict) -> None:
        """매수 주문 마감 처리"""
        buy_order_no = target_data.get("buy_order_no")
        buy_executed = target_data.get("buy_executed", False)
        buy_quantity = target_data.get("buy_quantity", 0)
        buy_executed_quantity = target_data.get("buy_executed_quantity", 0)
        
        # 매수 주문이 있고, 전량 체결되지 않은 경우
        if buy_order_no and not buy_executed:
            # 1. 주문이 나갔는데 매수 안된 경우 (buy_executed_quantity == 0)
            # 2. 일부 매수된 경우 (buy_executed_quantity > 0 but < buy_quantity)
            # 모두 취소 처리 (전체 주문 취소)
            logger.info(
                f"매수 주문 취소: "
                f"전략={user_strategy_id}, "
                f"종목={stock_code}, "
                f"주문번호={buy_order_no}, "
                f"주문수량={buy_quantity}, "
                f"체결수량={buy_executed_quantity}, "
                f"남은수량={buy_quantity - buy_executed_quantity}"
            )
            
            cancel_result = await self._order_api.cancel_order(
                user_strategy_id=user_strategy_id,
                order_no=buy_order_no,
                stock_code=stock_code
            )
            
            if cancel_result.get("success"):
                logger.info(
                    f"✅ 매수 주문 취소 완료: "
                    f"전략={user_strategy_id}, "
                    f"종목={stock_code}, "
                    f"주문번호={buy_order_no}"
                )
            else:
                logger.error(
                    f"❌ 매수 주문 취소 실패: "
                    f"전략={user_strategy_id}, "
                    f"종목={stock_code}, "
                    f"주문번호={buy_order_no}, "
                    f"오류={cancel_result.get('error', 'N/A')}"
                )

    async def _close_sell_orders(self, user_strategy_id: int, stock_code: str, target_data: dict) -> None:
        """매도 주문 마감 처리 - 매수된 종목 중 미매도 종목 시장가 매도"""
        buy_executed = target_data.get("buy_executed", False)
        buy_executed_quantity = target_data.get("buy_executed_quantity", 0)
        sell_executed = target_data.get("sell_executed", False)
        sell_executed_quantity = target_data.get("sell_executed_quantity", 0)
        sell_order_no = target_data.get("sell_order_no")
        
        # 매수된 종목이 있는 경우에만 처리
        if buy_executed and buy_executed_quantity > 0:
            # 남은 매도 수량 계산
            remaining_sell_quantity = buy_executed_quantity - sell_executed_quantity
            
            # 매도가 안 되었거나, 일부 매도가 된 경우
            if remaining_sell_quantity > 0:
                logger.info(
                    f"매도 주문 (시장가): "
                    f"전략={user_strategy_id}, "
                    f"종목={stock_code}, "
                    f"매수수량={buy_executed_quantity}, "
                    f"매도수량={sell_executed_quantity}, "
                    f"남은매도수량={remaining_sell_quantity}"
                )
                
                # 기존 매도 주문이 있고 미체결인 경우 취소
                if sell_order_no and not sell_executed:
                    logger.info(
                        f"기존 매도 주문 취소 후 재주문: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"주문번호={sell_order_no}"
                    )
                    cancel_result = await self._order_api.cancel_order(
                        user_strategy_id=user_strategy_id,
                        order_no=sell_order_no,
                        stock_code=stock_code
                    )
                    if not cancel_result.get("success"):
                        logger.warning(
                            f"기존 매도 주문 취소 실패 (무시하고 진행): "
                            f"전략={user_strategy_id}, "
                            f"종목={stock_code}, "
                            f"오류={cancel_result.get('error', 'N/A')}"
                        )
                
                # 시장가 매도 주문 생성 (SignalResult 생성)
                # 시장가 매도는 현재가로 주문 (가격은 0으로 설정하거나 시장가 코드 사용)
                market_sell_signal = SignalResult(
                    signal_type="SELL",
                    stock_code=stock_code,
                    current_price=0.0,  # 시장가이므로 가격 불필요
                    target_price=None,
                    target_quantity=remaining_sell_quantity,
                    stop_loss_price=None,
                    recommended_order_price=0.0,  # 시장가 (01)
                    recommended_order_type=OrderType.MARKET,  # 시장가
                    expected_slippage_pct=0.0,
                    urgency="CRITICAL",  # 장 마감이므로 긴급
                    reason="장 마감 시 강제 매도 (시장가)"
                )
                
                # 시장가 매도 주문 실행
                sell_result = await self._order_api.process_sell_order(
                    user_strategy_id=user_strategy_id,
                    signal=market_sell_signal,
                    order_quantity=remaining_sell_quantity
                )
                
                if sell_result.get("success"):
                    logger.info(
                        f"✅ 시장가 매도 주문 완료: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"수량={remaining_sell_quantity}"
                    )
                else:
                    logger.error(
                        f"❌ 시장가 매도 주문 실패: "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}, "
                        f"수량={remaining_sell_quantity}, "
                        f"오류={sell_result.get('error', 'N/A')}"
                    )

    async def _handle_stop_command(self, websocket_msg: WebSocketCommand) -> None:
        """STOP 웹소켓 메시지 처리"""
        try:
            stop_command = websocket_msg.to_stop_command()
            logger.info(f"Processing STOP websocket message: target={stop_command.target}")

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
