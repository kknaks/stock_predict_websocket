"""
KIS WebSocket Manager

웹소켓 생명주기 관리
"""

import asyncio
import logging
from typing import Optional, Dict, List
from enum import Enum

from app.models.websocket import StartCommand, StopCommand, TokenInfo, StartConfig
from app.kis.websocket.price_client import PriceWebSocketClient
from app.kis.websocket.account_client import AccountWebSocketClient
from app.kis.websocket.exceptions import (
    WebSocketConnectionError,
    WebSocketAuthError,
    WebSocketError,
)
from app.kis.websocket.error_stats import get_error_stats
from app.kis.websocket.redis_manager import get_redis_manager
from app.kafka.price_producer import get_price_producer
from app.kafka.asking_price_producer import get_asking_price_producer
from app.kafka.order_signal_producer import get_order_signal_producer
from app.kafka.daily_strategy_producer import get_daily_strategy_producer

logger = logging.getLogger(__name__)


class WebSocketStatus(str, Enum):
    """웹소켓 상태"""

    IDLE = "IDLE"  # 대기 중
    STARTING = "STARTING"  # 시작 중
    RUNNING = "RUNNING"  # 실행 중
    STOPPING = "STOPPING"  # 종료 중
    ERROR = "ERROR"  # 에러 상태


class WebSocketManager:
    """KIS 웹소켓 매니저"""

    def __init__(self):
        self._status = WebSocketStatus.IDLE
        self._price_client: Optional[PriceWebSocketClient] = None
        self._account_clients: Dict[str, AccountWebSocketClient] = {}
        self._tasks: List[asyncio.Task] = []
        self._error_stats = get_error_stats()
        self._failed_accounts: List[str] = []  # 시작 실패한 계좌들
        self._redis_manager = get_redis_manager()

    @property
    def status(self) -> WebSocketStatus:
        """현재 상태"""
        return self._status

    @property
    def is_running(self) -> bool:
        """실행 중인지 확인"""
        return self._status == WebSocketStatus.RUNNING

    async def start_all(self, command: StartCommand) -> bool:
        """
        모든 웹소켓 시작
        
        PRICE와 ACCOUNT는 독립적으로 관리됩니다:
        - PRICE 웹소켓: 1개만 존재 (이미 실행 중이면 스킵)
        - ACCOUNT 웹소켓: 여러 개 가능 (기존 계좌에 새로운 계좌 추가 가능)

        Args:
            command: START 명령

        Returns:
            성공 여부 (부분 성공도 True)
        """
        try:
            # 상태 업데이트 (IDLE이면 STARTING으로, RUNNING이면 그대로 유지)
            if self._status == WebSocketStatus.IDLE:
                self._status = WebSocketStatus.STARTING
            
            logger.info(f"Starting WebSocket connections... target={command.target}")

            # Kafka Producer 시작 (WebSocket과 생명주기 동일하게 관리)
            await self._start_kafka_producers()

            config = command.config
            tokens = command.tokens
            partial_failure = False
            price_websocket_started = False
            new_accounts_added = 0
            mock_accounts_skipped = 0  # account_type이 mock인 계좌는 개별 스킵됨

            # 가격 웹소켓 시작 (PRICE 타겟일 때만)
            if command.target in ("ALL", "PRICE"):
                # 이미 실행 중이면 스킵
                if self._price_client and self._price_client.is_connected:
                    logger.info("Price websocket is already running, skipping")
                elif not tokens:
                    logger.error("tokens is required for PRICE target")
                    if self._status == WebSocketStatus.STARTING:
                        self._status = WebSocketStatus.ERROR
                    return False
                elif config.stocks:
                    try:
                        await self._start_price_websocket(tokens, config)
                        price_websocket_started = True
                    except WebSocketAuthError as e:
                        logger.error(f"Price websocket auth error: {e}")
                        self._error_stats.record_error(e)
                        if self._status == WebSocketStatus.STARTING:
                            self._status = WebSocketStatus.ERROR
                        return False  # 인증 에러는 전체 실패
                    except Exception as e:
                        logger.error(f"Failed to start price websocket: {e}", exc_info=True)
                        self._error_stats.record_error(e)
                        partial_failure = True
                else:
                    logger.warning("No stocks specified for price websocket")

            # 계좌 웹소켓들 시작 (ACCOUNT 타겟일 때만)
            if command.target in ("ALL", "ACCOUNT"):
                if config.users:
                    # users가 있는 경우 (각 user의 account에서 ws_token과 app_key 사용)
                    try:
                        new_accounts_added, mock_accounts_skipped = await self._start_account_websockets_from_users(config)
                    except Exception as e:
                        logger.error(f"Failed to start some account websockets: {e}", exc_info=True)
                        self._error_stats.record_error(e)
                        partial_failure = True
                # elif config.accounts:
                #     # 기존 방식: accounts가 있고 tokens가 있는 경우
                #     if not tokens:
                #         logger.error("tokens is required when using accounts (not users)")
                #         partial_failure = True
                #     else:
                #         try:
                #             new_accounts = await self._start_account_websockets(tokens, config)
                #             new_accounts_added = new_accounts
                #         except Exception as e:
                #             logger.error(f"Failed to start some account websockets: {e}", exc_info=True)
                #             self._error_stats.record_error(e)
                #             partial_failure = True
                else:
                    logger.warning("No accounts or users specified for account websockets")

            # 성공 조건:
            # 1. 가격 웹소켓이 시작되었거나 (이미 실행 중이었거나)
            # 2. 계좌 웹소켓이 시작되었거나 (새로 추가되었거나, mock은 개별 스킵)
            success = False
            if command.target in ("ALL", "PRICE"):
                if price_websocket_started or (self._price_client and self._price_client.is_connected):
                    success = True
            if command.target in ("ALL", "ACCOUNT"):
                if new_accounts_added > 0 or mock_accounts_skipped > 0 or len(self._account_clients) > 0:
                    success = True

            if success:
                self._status = WebSocketStatus.RUNNING
                if partial_failure:
                    logger.warning(
                        f"WebSocket connections started with partial failures. "
                        f"Failed accounts: {self._failed_accounts}"
                    )
                else:
                    if mock_accounts_skipped > 0 and new_accounts_added == 0:
                        logger.info(f"WebSocket connections started (mock accounts skipped: {mock_accounts_skipped})")
                    elif new_accounts_added > 0:
                        logger.info(f"Successfully added {new_accounts_added} new account websocket(s)")
                    else:
                        logger.info("WebSocket connections started successfully")
                return True
            else:
                # 타겟별로 실패 처리
                if command.target == "PRICE" and not price_websocket_started:
                    if self._status == WebSocketStatus.STARTING:
                        self._status = WebSocketStatus.ERROR
                    logger.error("Price websocket failed to start")
                    return False
                elif command.target == "ACCOUNT" and new_accounts_added == 0 and mock_accounts_skipped == 0:
                    logger.warning("No account websockets were started")
                    return False
                else:
                    # ALL 타겟인 경우 부분 실패도 허용
                    self._status = WebSocketStatus.RUNNING
                    return True

        except Exception as e:
            logger.error(f"Failed to start WebSocket connections: {e}", exc_info=True)
            self._error_stats.record_error(e)
            if self._status == WebSocketStatus.STARTING:
                self._status = WebSocketStatus.ERROR
            return False

    async def stop_all(self, command: Optional[StopCommand] = None) -> bool:
        """
        모든 웹소켓 종료

        Args:
            command: STOP 명령 (선택)

        Returns:
            성공 여부
        """
        if self._status == WebSocketStatus.IDLE:
            logger.warning("WebSocket is already stopped")
            return True

        try:
            self._status = WebSocketStatus.STOPPING
            target = command.target if command else "ALL"
            logger.info(f"Stopping WebSocket connections... target={target}")

            stop_errors = []

            # 가격 웹소켓 종료
            if target in ("ALL", "PRICE"):
                try:
                    await self._stop_price_websocket()
                except Exception as e:
                    logger.error(f"Error stopping price websocket: {e}", exc_info=True)
                    self._error_stats.record_error(e, error_type=None, error_code=None, details={"websocket_type": "price"})
                    stop_errors.append(e)

            # 계좌 웹소켓들 종료
            if target in ("ALL", "ACCOUNT"):
                try:
                    await self._stop_account_websockets(target)
                except Exception as e:
                    logger.error(f"Error stopping account websockets: {e}", exc_info=True)
                    self._error_stats.record_error(e, error_type=None, error_code=None, details={"websocket_type": "account"})
                    stop_errors.append(e)

            # 모든 태스크 취소
            for task in self._tasks:
                if not task.done():
                    task.cancel()

            # 태스크 완료 대기 (에러 무시)
            if self._tasks:
                results = await asyncio.gather(*self._tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.warning(f"Task {i} ended with error: {result}")

            self._tasks.clear()

            # Kafka Producer 종료 (WebSocket과 생명주기 동일하게 관리)
            await self._stop_kafka_producers()

            self._status = WebSocketStatus.IDLE

            if stop_errors:
                logger.warning(
                    f"WebSocket connections stopped with some errors: {len(stop_errors)} errors"
                )
            else:
                logger.info("All WebSocket connections stopped successfully")

            return True

        except Exception as e:
            logger.error(f"Failed to stop WebSocket connections: {e}", exc_info=True)
            self._error_stats.record_error(e)
            self._status = WebSocketStatus.ERROR
            return False

    async def _start_price_websocket(self, tokens: TokenInfo, config: StartConfig) -> None:
        """가격 웹소켓 시작"""
        try:
            # Redis에서 기존 연결 정보 확인
            existing_connection = self._redis_manager.get_price_connection()
            if existing_connection:
                existing_appkey = existing_connection.get("appkey")
                existing_status = existing_connection.get("status", "unknown")
                
                # 같은 appkey가 이미 사용 중이면 에러 발생
                if existing_appkey == config.appkey and existing_status == "connected":
                    error_msg = (
                        f"Price websocket already in use with appkey: {config.appkey}. "
                        f"Please stop existing connection first."
                    )
                    logger.error(error_msg)
                    raise WebSocketConnectionError(
                        error_msg,
                        details={"appkey": config.appkey, "existing_status": existing_status}
                    )
                elif existing_appkey == config.appkey:
                    # 연결이 끊어진 상태면 Redis에서 삭제하고 계속 진행
                    logger.warning(
                        f"Found disconnected connection for appkey {config.appkey}, "
                        f"cleaning up Redis entry"
                    )
                    self._redis_manager.delete_price_connection()
            
            logger.info(f"Starting price websocket for {len(config.stocks)} stocks")
            self._price_client = PriceWebSocketClient(
                ws_token=tokens.ws_token,
                appkey=config.appkey,
                env_dv=config.env_dv,
                stocks=config.stocks,
            )

            task = asyncio.create_task(self._price_client.connect_and_run())
            self._tasks.append(task)
            logger.info("Price websocket started")

        except WebSocketAuthError:
            # 인증 에러는 그대로 전파 (전체 실패 처리)
            raise
        except Exception as e:
            logger.error(f"Failed to start price websocket: {e}", exc_info=True)
            self._error_stats.record_error(e, error_type=None, error_code=None, details={"websocket_type": "price"})
            raise

    # async def _start_account_websockets(self, tokens: TokenInfo, config: StartConfig) -> int:
    #     """
    #     계좌 웹소켓들 시작 (기존 방식: tokens 사용)
        
    #     Returns:
    #         새로 추가된 계좌 웹소켓 수
    #     """
    #     try:
    #         logger.info(f"Starting account websockets for {len(config.accounts)} accounts")
    #         success_count = 0
    #         failure_count = 0
    #         skipped_count = 0

    #         for account in config.accounts:
    #             # 이미 실행 중인 계좌는 스킵
    #             if account.account_no in self._account_clients:
    #                 existing_client = self._account_clients[account.account_no]
    #                 if existing_client.is_connected:
    #                     logger.info(f"Account websocket already running: {account.account_no}, skipping")
    #                     skipped_count += 1
    #                     continue
    #                 else:
    #                     # 연결이 끊어진 경우 제거하고 재시작
    #                     logger.info(f"Account websocket disconnected: {account.account_no}, restarting")
    #                     del self._account_clients[account.account_no]
                
    #             try:
    #                 logger.info(f"Starting account websocket: {account.account_no}")
    #                 account_client = AccountWebSocketClient(
    #                     ws_token=tokens.ws_token,
    #                     appkey=config.appkey,
    #                     env_dv=config.env_dv,
    #                     is_mock=config.is_mock,
    #                     account_no=account.account_no,
    #                     account_product_code=account.account_product_code,
    #                     access_token=tokens.access_token,
    #                 )

    #                 task = asyncio.create_task(account_client.connect_and_run())
    #                 self._tasks.append(task)
    #                 self._account_clients[account.account_no] = account_client
    #                 success_count += 1
    #                 logger.info(f"Account websocket started: {account.account_no}")

    #             except WebSocketAuthError as e:
    #                 # 인증 에러는 해당 계좌만 실패 처리
    #                 logger.error(
    #                     f"Account websocket auth error for {account.account_no}: {e}"
    #                 )
    #                 self._error_stats.record_error(e, error_type=None, error_code=None, details={"account_no": account.account_no})
    #                 self._failed_accounts.append(account.account_no)
    #                 failure_count += 1
    #             except Exception as e:
    #                 logger.error(
    #                     f"Failed to start account websocket {account.account_no}: {e}",
    #                     exc_info=True
    #                 )
    #                 self._error_stats.record_error(
    #                     e,
    #                     error_type=None,
    #                     error_code=None,
    #                     details={"account_no": account.account_no, "websocket_type": "account"}
    #                 )
    #                 self._failed_accounts.append(account.account_no)
    #                 failure_count += 1

    #         logger.info(
    #             f"Account websockets: {success_count} new, {skipped_count} skipped, {failure_count} failed"
    #         )

    #         if failure_count > 0:
    #             logger.warning(f"Failed to start {failure_count} account websockets")

    #         return success_count

    #     except Exception as e:
    #         logger.error(f"Failed to start account websockets: {e}", exc_info=True)
    #         self._error_stats.record_error(e, error_type=None, error_code=None, details={"websocket_type": "account"})
    #         raise

    async def _start_account_websockets_from_users(self, config: StartConfig) -> tuple[int, int]:
        """
        계좌 웹소켓들 시작 (users 방식: 각 user의 account에서 ws_token과 app_key 사용)

        Returns:
            (새로 추가된 계좌 웹소켓 수, mock으로 스킵된 계좌 수)
        """
        try:
            logger.info(f"Starting account websockets for {len(config.users)} users")
            success_count = 0
            failure_count = 0
            skipped_count = 0
            mock_skipped_count = 0

            for user in config.users:
                account = user.account

                # account_type이 mock인 경우 WebSocket 연결 스킵
                if account.account_type == "mock":
                    logger.info(f"Skipping account websocket for mock account: {account.account_no} (user_id: {user.user_id})")
                    mock_skipped_count += 1
                    continue

                # 이미 실행 중인 계좌는 스킵
                if account.account_no in self._account_clients:
                    existing_client = self._account_clients[account.account_no]
                    if existing_client.is_connected:
                        logger.info(f"Account websocket already running: {account.account_no} (user_id: {user.user_id}), skipping")
                        skipped_count += 1
                        continue
                    else:
                        # 연결이 끊어진 경우 제거하고 재시작
                        logger.info(f"Account websocket disconnected: {account.account_no} (user_id: {user.user_id}), restarting")
                        del self._account_clients[account.account_no]
                
                # Redis에서 기존 연결 정보 확인
                existing_connection = self._redis_manager.get_account_connection(account.account_no)
                if existing_connection:
                    existing_appkey = existing_connection.get("appkey")
                    existing_status = existing_connection.get("status", "unknown")
                    
                    # 같은 appkey가 이미 사용 중이면 에러 발생
                    if existing_appkey == account.app_key and existing_status == "connected":
                        error_msg = (
                            f"Account websocket already in use: account_no={account.account_no}, "
                            f"appkey={account.app_key}. Please stop existing connection first."
                        )
                        logger.error(error_msg)
                        self._error_stats.record_error(
                            WebSocketConnectionError(
                                error_msg,
                                details={"account_no": account.account_no, "appkey": account.app_key, "existing_status": existing_status}
                            ),
                            error_type=None,
                            error_code=None,
                            details={"account_no": account.account_no, "user_id": user.user_id}
                        )
                        self._failed_accounts.append(account.account_no)
                        failure_count += 1
                        continue
                    elif existing_appkey == account.app_key:
                        # 연결이 끊어진 상태면 Redis에서 삭제하고 계속 진행
                        logger.warning(
                            f"Found disconnected connection for account {account.account_no}, "
                            f"cleaning up Redis entry"
                        )
                        self._redis_manager.delete_account_connection(account.account_no)
                
                try:
                    # user의 strategies에서 user_strategy_id 리스트 추출
                    user_strategy_ids = [s.get("user_strategy_id") for s in user.strategies if s.get("user_strategy_id")]

                    logger.info(
                        f"Starting account websocket: {account.account_no} "
                        f"(user_id: {user.user_id}, hts_id: {account.hts_id}, account_type: {account.account_type})"
                    )
                    # is_mock은 account_type 기반으로 결정 (mock이면 True, 아니면 False)
                    is_mock = account.account_type == "mock"
                    account_client = AccountWebSocketClient(
                        ws_token=account.ws_token,
                        appkey=account.app_key,
                        env_dv=config.env_dv,
                        account_type=account.account_type,  # real/paper/mock
                        is_mock=is_mock,
                        account_no=account.account_no,
                        account_product_code=account.account_product_code,
                        access_token=account.access_token,
                        appsecret=account.app_secret,
                        user_id=user.user_id,
                        user_strategy_ids=user_strategy_ids,
                        hts_id=account.hts_id,
                    )

                    task = asyncio.create_task(account_client.connect_and_run())
                    self._tasks.append(task)
                    self._account_clients[account.account_no] = account_client
                    success_count += 1
                    logger.info(f"Account websocket started: {account.account_no} (user_id: {user.user_id})")

                except WebSocketAuthError as e:
                    # 인증 에러는 해당 계좌만 실패 처리
                    logger.error(
                        f"Account websocket auth error for {account.account_no} (user_id: {user.user_id}): {e}"
                    )
                    self._error_stats.record_error(e, error_type=None, error_code=None, details={"account_no": account.account_no, "user_id": user.user_id})
                    self._failed_accounts.append(account.account_no)
                    failure_count += 1
                except Exception as e:
                    logger.error(
                        f"Failed to start account websocket {account.account_no} (user_id: {user.user_id}): {e}",
                        exc_info=True
                    )
                    self._error_stats.record_error(
                        e,
                        error_type=None,
                        error_code=None,
                        details={"account_no": account.account_no, "user_id": user.user_id, "websocket_type": "account"}
                    )
                    self._failed_accounts.append(account.account_no)
                    failure_count += 1

            logger.info(
                f"Account websockets: {success_count} new, {skipped_count} skipped, "
                f"{mock_skipped_count} mock skipped, {failure_count} failed"
            )

            if failure_count > 0:
                logger.warning(f"Failed to start {failure_count} account websockets")

            return success_count, mock_skipped_count

        except Exception as e:
            logger.error(f"Failed to start account websockets: {e}", exc_info=True)
            self._error_stats.record_error(e, error_type=None, error_code=None, details={"websocket_type": "account"})
            raise

    async def _start_kafka_producers(self) -> None:
        """Kafka Producer들 시작"""
        try:
            price_producer = get_price_producer()
            if not price_producer._producer:
                await price_producer.start()
                logger.info("Price producer started")
        except Exception as e:
            logger.error(f"Failed to start price producer: {e}")

        try:
            asking_price_producer = get_asking_price_producer()
            if not asking_price_producer._producer:
                await asking_price_producer.start()
                logger.info("Asking price producer started")
        except Exception as e:
            logger.error(f"Failed to start asking price producer: {e}")

        try:
            order_signal_producer = get_order_signal_producer()
            if not order_signal_producer._producer:
                await order_signal_producer.start()
                logger.info("Order signal producer started")
        except Exception as e:
            logger.error(f"Failed to start order signal producer: {e}")

        try:
            daily_strategy_producer = get_daily_strategy_producer()
            if not daily_strategy_producer._producer:
                await daily_strategy_producer.start()
                logger.info("Daily strategy producer started")
        except Exception as e:
            logger.error(f"Failed to start daily strategy producer: {e}")

    async def _stop_kafka_producers(self) -> None:
        """Kafka Producer들 종료"""
        try:
            price_producer = get_price_producer()
            await price_producer.stop()
            logger.info("Price producer stopped")
        except Exception as e:
            logger.warning(f"Error stopping price producer: {e}")

        try:
            asking_price_producer = get_asking_price_producer()
            await asking_price_producer.stop()
            logger.info("Asking price producer stopped")
        except Exception as e:
            logger.warning(f"Error stopping asking price producer: {e}")

        try:
            order_signal_producer = get_order_signal_producer()
            await order_signal_producer.stop()
            logger.info("Order signal producer stopped")
        except Exception as e:
            logger.warning(f"Error stopping order signal producer: {e}")

        try:
            daily_strategy_producer = get_daily_strategy_producer()
            await daily_strategy_producer.stop()
            logger.info("Daily strategy producer stopped")
        except Exception as e:
            logger.warning(f"Error stopping daily strategy producer: {e}")

    async def _stop_price_websocket(self) -> None:
        """가격 웹소켓 종료"""
        if self._price_client:
            try:
                await self._price_client.disconnect()
                logger.info("Price websocket stopped")
            except Exception as e:
                logger.error(f"Error stopping price websocket: {e}", exc_info=True)
            finally:
                self._price_client = None
        
        # Redis에서 연결 정보 삭제 (항상 실행 - _price_client가 None이어도 Redis에 정보가 남아있을 수 있음)
        self._redis_manager.delete_price_connection()

    async def _stop_account_websockets(self, target: str) -> None:
        """계좌 웹소켓들 종료"""
        if target == "ALL":
            accounts_to_stop = list(self._account_clients.keys())
        else:
            # 특정 계좌만 종료
            accounts_to_stop = [target] if target in self._account_clients else []

        for account_no in accounts_to_stop:
            client = self._account_clients.get(account_no)
            if client:
                try:
                    await client.disconnect()
                    logger.info(f"Account websocket stopped: {account_no}")
                except Exception as e:
                    logger.error(f"Error stopping account websocket {account_no}: {e}", exc_info=True)
                finally:
                    del self._account_clients[account_no]
            
            # Redis에서 연결 정보 삭제 (항상 실행 - client가 None이어도 Redis에 정보가 남아있을 수 있음)
            self._redis_manager.delete_account_connection(account_no)

    async def update_price_stocks(self, new_stocks: List[str]) -> bool:
        """
        가격 웹소켓 종목 업데이트
        
        Args:
            new_stocks: 새로운 종목 리스트
            
        Returns:
            성공 여부
        """
        if not self._price_client:
            logger.warning("Price websocket is not running, cannot update stocks")
            return False
        
        if not self._price_client.is_connected:
            logger.warning("Price websocket is not connected, cannot update stocks")
            return False
        
        try:
            await self._price_client.update_stocks(new_stocks)
            logger.info(f"Price websocket stocks updated: {len(new_stocks)} stocks")
            
            # Redis에 업데이트된 종목 정보 저장
            if self._price_client.stocks:
                self._redis_manager.save_price_connection(
                    ws_token=self._price_client.ws_token,
                    appkey=self._price_client.appkey,
                    env_dv=self._price_client.env_dv,
                    stocks=self._price_client.stocks,
                    status="connected",
                    reconnect_attempts=self._price_client.reconnect_attempts,
                )
            
            return True
        except Exception as e:
            logger.error(f"Failed to update price websocket stocks: {e}", exc_info=True)
            self._error_stats.record_error(e, error_type=None, error_code=None, details={"websocket_type": "price"})
            return False

    async def add_price_stocks(self, new_stocks: List[str]) -> bool:
        """
        가격 웹소켓 종목 추가 (기존 종목 유지 + 새 종목만 추가)

        Args:
            new_stocks: 추가할 종목 리스트

        Returns:
            성공 여부
        """
        if not self._price_client:
            logger.warning("Price websocket is not running, cannot add stocks")
            return False

        if not self._price_client.is_connected:
            logger.warning("Price websocket is not connected, cannot add stocks")
            return False

        try:
            await self._price_client.add_stocks(new_stocks)
            logger.info(f"Price websocket stocks added: {len(new_stocks)} new stocks requested")

            # Redis에 업데이트된 종목 정보 저장
            if self._price_client.stocks:
                self._redis_manager.save_price_connection(
                    ws_token=self._price_client.ws_token,
                    appkey=self._price_client.appkey,
                    env_dv=self._price_client.env_dv,
                    stocks=self._price_client.stocks,
                    status="connected",
                    reconnect_attempts=self._price_client.reconnect_attempts,
                )

            return True
        except Exception as e:
            logger.error(f"Failed to add price websocket stocks: {e}", exc_info=True)
            self._error_stats.record_error(e, error_type=None, error_code=None, details={"websocket_type": "price"})
            return False

    def get_status(self) -> Dict:
        """상태 정보 반환"""
        # 가격 웹소켓 상태
        price_status = "stopped"
        price_redis_info = None
        if self._price_client:
            price_status = "connected" if self._price_client.is_connected else "disconnected"
            if self._price_client.reconnect_attempts > 0:
                price_status = f"{price_status} (reconnecting: {self._price_client.reconnect_attempts})"
        else:
            # Redis에서 확인
            price_redis_info = self._redis_manager.get_price_connection()
            if price_redis_info:
                price_status = f"redis_found ({price_redis_info.get('status', 'unknown')})"

        # 계좌 웹소켓 상태
        account_statuses = {}
        account_redis_info = {}
        for account_no, client in self._account_clients.items():
            status = "connected" if client.is_connected else "disconnected"
            if client.reconnect_attempts > 0:
                status = f"{status} (reconnecting: {client.reconnect_attempts})"
            account_statuses[account_no] = status

        # Redis에서 추가 계좌 정보 확인
        all_redis_accounts = self._redis_manager.get_all_account_connections()
        for account_no, redis_data in all_redis_accounts.items():
            if account_no not in account_statuses:
                account_statuses[account_no] = f"redis_found ({redis_data.get('status', 'unknown')})"
                account_redis_info[account_no] = redis_data

        return {
            "status": self._status.value,
            "price_websocket": price_status,
            "price_redis_info": price_redis_info,
            "account_websockets": account_statuses,
            "account_redis_info": account_redis_info,
            "total_accounts": len(self._account_clients),
            "failed_accounts": self._failed_accounts,
            "error_statistics": self._error_stats.get_statistics(),
            "redis_connected": self._redis_manager.is_connected(),
        }


# 싱글톤 인스턴스
_manager_instance: Optional[WebSocketManager] = None


def get_websocket_manager() -> WebSocketManager:
    """WebSocket Manager 싱글톤 인스턴스 반환"""
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = WebSocketManager()
    return _manager_instance
