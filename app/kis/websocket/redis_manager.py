"""
Redis WebSocket Connection State Manager

웹소켓 연결 상태를 Redis에 저장/조회/관리
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any

import redis
from redis.exceptions import RedisError

from app.config.settings import settings

logger = logging.getLogger(__name__)


class WebSocketRedisManager:
    """웹소켓 연결 상태 Redis 관리"""

    # Redis 키 접두사
    KEY_PREFIX = "websocket"
    PRICE_KEY = f"{KEY_PREFIX}:price"
    ACCOUNT_KEY_PREFIX = f"{KEY_PREFIX}:account"
    ACCOUNT_BY_USER_ID_KEY_PREFIX = f"{KEY_PREFIX}:account:user"
    ACCOUNT_BY_STRATEGY_KEY_PREFIX = f"{KEY_PREFIX}:account:strategy"
    
    # Strategy Table Redis 키 접두사
    STRATEGY_CONFIG_KEY_PREFIX = "strategy:config"
    STRATEGY_TARGET_KEY_PREFIX = "strategy:target"
    STRATEGY_TARGETS_SET_KEY_PREFIX = "strategy:targets"

    # TTL (초) - 24시간
    TTL = 24 * 60 * 60

    def __init__(self):
        """Redis 연결 초기화"""
        try:
            self._redis_client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password if settings.redis_password else None,
                decode_responses=settings.redis_decode_responses,
                socket_timeout=settings.redis_socket_timeout,
                socket_connect_timeout=settings.redis_socket_connect_timeout,
            )
            # 연결 테스트
            self._redis_client.ping()
            logger.info(f"Redis connected: {settings.redis_host}:{settings.redis_port}")
        except RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self._redis_client = None

    def _get_account_key(self, account_no: str) -> str:
        """계좌별 Redis 키 생성"""
        return f"{self.ACCOUNT_KEY_PREFIX}:{account_no}"

    def save_price_connection(
        self,
        ws_token: str,
        appkey: str,
        env_dv: str,
        stocks: list,
        status: str = "connected",
        reconnect_attempts: int = 0,
    ) -> bool:
        """
        가격 웹소켓 연결 정보 저장

        Args:
            ws_token: WebSocket 접속키
            appkey: 앱키
            env_dv: 환경구분
            stocks: 종목 코드 리스트
            status: 연결 상태
            reconnect_attempts: 재연결 시도 횟수

        Returns:
            성공 여부
        """
        if not self._redis_client:
            logger.warning("Redis not connected, skipping save")
            return False

        try:
            data = {
                "ws_token": ws_token,
                "appkey": appkey,
                "env_dv": env_dv,
                "stocks": stocks,
                "status": status,
                "reconnect_attempts": reconnect_attempts,
                "connected_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }

            self._redis_client.setex(
                self.PRICE_KEY,
                self.TTL,
                json.dumps(data, ensure_ascii=False)
            )
            logger.debug(f"Price websocket connection saved to Redis")
            return True

        except RedisError as e:
            logger.error(f"Failed to save price connection to Redis: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error saving price connection: {e}", exc_info=True)
            return False

    def get_price_connection(self) -> Optional[Dict[str, Any]]:
        """
        가격 웹소켓 연결 정보 조회

        Returns:
            연결 정보 딕셔너리 또는 None
        """
        if not self._redis_client:
            logger.warning("Redis not connected, cannot get connection info")
            return None

        try:
            data = self._redis_client.get(self.PRICE_KEY)
            if data:
                return json.loads(data)
            return None

        except RedisError as e:
            logger.error(f"Failed to get price connection from Redis: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse price connection data: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting price connection: {e}", exc_info=True)
            return None

    def save_account_connection(
        self,
        account_no: str,
        ws_token: str,
        appkey: str,
        env_dv: str,
        account_product_code: str,
        is_mock: bool = False,
        access_token: Optional[str] = None,
        user_id: Optional[int] = None,
        user_strategy_ids: Optional[list] = None,
        hts_id: Optional[str] = None,
        status: str = "connected",
        reconnect_attempts: int = 0,
    ) -> bool:
        """
        계좌 웹소켓 연결 정보 저장

        Args:
            account_no: 계좌번호
            ws_token: WebSocket 접속키
            appkey: 앱키
            env_dv: 환경구분
            account_product_code: 계좌상품코드
            access_token: OAuth 액세스 토큰 (API 주문용)
            user_id: 사용자 ID
            user_strategy_ids: 사용자 전략 ID 리스트
            hts_id: 고객ID (체결통보 구독용)
            status: 연결 상태
            reconnect_attempts: 재연결 시도 횟수

        Returns:
            성공 여부
        """
        if not self._redis_client:
            logger.warning("Redis not connected, skipping save")
            return False

        try:
            key = self._get_account_key(account_no)
            data = {
                "account_no": account_no,
                "ws_token": ws_token,
                "appkey": appkey,
                "env_dv": env_dv,
                "account_product_code": account_product_code,
                "is_mock": is_mock,
                "status": status,
                "reconnect_attempts": reconnect_attempts,
                "connected_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }
            
            # access_token이 있으면 추가
            if access_token:
                data["access_token"] = access_token
            
            # user_id가 있으면 추가
            if user_id is not None:
                data["user_id"] = user_id
            
            # user_strategy_ids가 있으면 추가
            if user_strategy_ids:
                data["user_strategy_ids"] = user_strategy_ids
            
            # hts_id가 있으면 추가
            if hts_id:
                data["hts_id"] = hts_id

            # 계좌번호 기준으로 저장
            self._redis_client.setex(
                key,
                self.TTL,
                json.dumps(data, ensure_ascii=False)
            )
            
            # user_id로도 인덱스 저장
            if user_id is not None:
                user_key = f"{self.ACCOUNT_BY_USER_ID_KEY_PREFIX}:{user_id}"
                self._redis_client.setex(
                    user_key,
                    self.TTL,
                    account_no
                )
            
            # user_strategy_id별로 인덱스 저장
            if user_strategy_ids:
                for strategy_id in user_strategy_ids:
                    strategy_key = f"{self.ACCOUNT_BY_STRATEGY_KEY_PREFIX}:{strategy_id}"
                    self._redis_client.setex(
                        strategy_key,
                        self.TTL,
                        account_no
                    )
            
            logger.debug(f"Account websocket connection saved to Redis: {account_no}")
            return True

        except RedisError as e:
            logger.error(f"Failed to save account connection to Redis ({account_no}): {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error saving account connection ({account_no}): {e}", exc_info=True)
            return False

    def get_account_connection(self, account_no: str) -> Optional[Dict[str, Any]]:
        """
        계좌 웹소켓 연결 정보 조회

        Args:
            account_no: 계좌번호

        Returns:
            연결 정보 딕셔너리 또는 None
        """
        if not self._redis_client:
            logger.warning("Redis not connected, cannot get connection info")
            return None

        try:
            key = self._get_account_key(account_no)
            data = self._redis_client.get(key)
            if data:
                return json.loads(data)
            return None

        except RedisError as e:
            logger.error(f"Failed to get account connection from Redis ({account_no}): {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse account connection data ({account_no}): {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting account connection ({account_no}): {e}", exc_info=True)
            return None

    def get_account_connection_by_user_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        user_id로 계좌 웹소켓 연결 정보 조회

        Args:
            user_id: 사용자 ID

        Returns:
            연결 정보 딕셔너리 또는 None
        """
        if not self._redis_client:
            logger.warning("Redis not connected, cannot get connection info")
            return None

        try:
            # user_id로 account_no 조회
            user_key = f"{self.ACCOUNT_BY_USER_ID_KEY_PREFIX}:{user_id}"
            account_no = self._redis_client.get(user_key)
            if account_no:
                return self.get_account_connection(account_no)
            return None

        except RedisError as e:
            logger.error(f"Failed to get account connection by user_id from Redis ({user_id}): {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting account connection by user_id ({user_id}): {e}", exc_info=True)
            return None

    def get_account_connection_by_strategy_id(self, user_strategy_id: int) -> Optional[Dict[str, Any]]:
        """
        user_strategy_id로 계좌 웹소켓 연결 정보 조회

        Args:
            user_strategy_id: 사용자 전략 ID

        Returns:
            연결 정보 딕셔너리 또는 None
        """
        if not self._redis_client:
            logger.warning("Redis not connected, cannot get connection info")
            return None

        try:
            # user_strategy_id로 account_no 조회
            strategy_key = f"{self.ACCOUNT_BY_STRATEGY_KEY_PREFIX}:{user_strategy_id}"
            account_no = self._redis_client.get(strategy_key)
            if account_no:
                return self.get_account_connection(account_no)
            return None

        except RedisError as e:
            logger.error(f"Failed to get account connection by strategy_id from Redis ({user_strategy_id}): {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting account connection by strategy_id ({user_strategy_id}): {e}", exc_info=True)
            return None

    def update_connection_status(
        self,
        connection_type: str,
        account_no: Optional[str] = None,
        status: str = "connected",
        reconnect_attempts: int = 0,
    ) -> bool:
        """
        연결 상태 업데이트

        Args:
            connection_type: "price" 또는 "account"
            account_no: 계좌번호 (account 타입인 경우)
            status: 연결 상태
            reconnect_attempts: 재연결 시도 횟수

        Returns:
            성공 여부
        """
        if not self._redis_client:
            return False

        try:
            if connection_type == "price":
                data = self.get_price_connection()
                if data:
                    data["status"] = status
                    data["reconnect_attempts"] = reconnect_attempts
                    data["updated_at"] = datetime.now().isoformat()
                    self._redis_client.setex(
                        self.PRICE_KEY,
                        self.TTL,
                        json.dumps(data, ensure_ascii=False)
                    )
                    return True
            elif connection_type == "account" and account_no:
                data = self.get_account_connection(account_no)
                if data:
                    data["status"] = status
                    data["reconnect_attempts"] = reconnect_attempts
                    data["updated_at"] = datetime.now().isoformat()
                    key = self._get_account_key(account_no)
                    self._redis_client.setex(
                        key,
                        self.TTL,
                        json.dumps(data, ensure_ascii=False)
                    )
                    return True

            return False

        except Exception as e:
            logger.error(f"Failed to update connection status: {e}", exc_info=True)
            return False

    def delete_price_connection(self) -> bool:
        """가격 웹소켓 연결 정보 삭제"""
        if not self._redis_client:
            return False

        try:
            self._redis_client.delete(self.PRICE_KEY)
            logger.debug("Price websocket connection deleted from Redis")
            return True
        except RedisError as e:
            logger.error(f"Failed to delete price connection from Redis: {e}")
            return False

    def delete_account_connection(self, account_no: str) -> bool:
        """계좌 웹소켓 연결 정보 삭제"""
        if not self._redis_client:
            return False

        try:
            key = self._get_account_key(account_no)
            self._redis_client.delete(key)
            logger.debug(f"Account websocket connection deleted from Redis: {account_no}")
            return True
        except RedisError as e:
            logger.error(f"Failed to delete account connection from Redis ({account_no}): {e}")
            return False

    def get_all_account_connections(self) -> Dict[str, Dict[str, Any]]:
        """
        모든 계좌 웹소켓 연결 정보 조회

        Returns:
            {account_no: connection_info} 딕셔너리
        """
        if not self._redis_client:
            return {}

        try:
            pattern = f"{self.ACCOUNT_KEY_PREFIX}:*"
            keys = self._redis_client.keys(pattern)
            connections = {}

            for key in keys:
                try:
                    data = self._redis_client.get(key)
                    if data:
                        conn_data = json.loads(data)
                        account_no = conn_data.get("account_no")
                        if account_no:
                            connections[account_no] = conn_data
                except Exception as e:
                    logger.warning(f"Failed to parse connection data for key {key}: {e}")

            return connections

        except RedisError as e:
            logger.error(f"Failed to get all account connections from Redis: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error getting all account connections: {e}", exc_info=True)
            return {}

    def is_connected(self) -> bool:
        """Redis 연결 상태 확인"""
        if not self._redis_client:
            return False

        try:
            self._redis_client.ping()
            return True
        except Exception:
            return False

    def _get_strategy_config_key(self, user_strategy_id: int) -> str:
        """전략 설정 Redis 키 생성"""
        return f"{self.STRATEGY_CONFIG_KEY_PREFIX}:{user_strategy_id}"

    def _get_strategy_target_key(self, user_strategy_id: int, stock_code: str) -> str:
        """종목별 목표가 Redis 키 생성"""
        return f"{self.STRATEGY_TARGET_KEY_PREFIX}:{user_strategy_id}:{stock_code}"

    def _get_strategy_targets_set_key(self, user_strategy_id: int) -> str:
        """전략의 종목 목록 Set Redis 키 생성"""
        return f"{self.STRATEGY_TARGETS_SET_KEY_PREFIX}:{user_strategy_id}"

    def save_strategy_config(self, user_strategy_id: int, config_data: Dict[str, Any]) -> bool:
        """
        전략 설정을 Redis에 저장

        Args:
            user_strategy_id: 사용자 전략 ID
            config_data: 전략 설정 데이터

        Returns:
            성공 여부
        """
        if not self._redis_client:
            logger.warning("Redis not connected, skipping save")
            return False

        try:
            key = self._get_strategy_config_key(user_strategy_id)
            self._redis_client.setex(
                key,
                self.TTL,
                json.dumps(config_data, ensure_ascii=False, default=str)
            )
            logger.debug(f"Strategy config saved to Redis: {key}")
            return True
        except Exception as e:
            logger.error(f"Failed to save strategy config to Redis: {e}", exc_info=True)
            return False

    def get_strategy_config(self, user_strategy_id: int) -> Optional[Dict[str, Any]]:
        """
        전략 설정을 Redis에서 조회

        Args:
            user_strategy_id: 사용자 전략 ID

        Returns:
            전략 설정 딕셔너리 또는 None
        """
        if not self._redis_client:
            return None

        try:
            key = self._get_strategy_config_key(user_strategy_id)
            data = self._redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Failed to get strategy config from Redis: {e}", exc_info=True)
            return None

    def save_strategy_target(self, user_strategy_id: int, stock_code: str, target_data: Dict[str, Any]) -> bool:
        """
        종목별 목표가를 Redis에 저장

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드
            target_data: 목표가 데이터

        Returns:
            성공 여부
        """
        if not self._redis_client:
            logger.warning("Redis not connected, skipping save")
            return False

        try:
            # 종목별 목표가 저장
            target_key = self._get_strategy_target_key(user_strategy_id, stock_code)
            self._redis_client.setex(
                target_key,
                self.TTL,
                json.dumps(target_data, ensure_ascii=False, default=str)
            )
            
            # 전략의 종목 목록에 추가 (Set 사용)
            targets_set_key = self._get_strategy_targets_set_key(user_strategy_id)
            self._redis_client.sadd(targets_set_key, stock_code)
            self._redis_client.expire(targets_set_key, self.TTL)
            
            logger.debug(f"Strategy target saved to Redis: {target_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to save strategy target to Redis: {e}", exc_info=True)
            return False

    def get_strategy_target(self, user_strategy_id: int, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        종목별 목표가를 Redis에서 조회

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드

        Returns:
            목표가 데이터 딕셔너리 또는 None
        """
        if not self._redis_client:
            return None

        try:
            key = self._get_strategy_target_key(user_strategy_id, stock_code)
            data = self._redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Failed to get strategy target from Redis: {e}", exc_info=True)
            return None

    def get_strategy_all_targets(self, user_strategy_id: int) -> Dict[str, Dict[str, Any]]:
        """
        전략의 모든 종목 목표가를 Redis에서 조회

        Args:
            user_strategy_id: 사용자 전략 ID

        Returns:
            {stock_code: target_data} 딕셔너리
        """
        if not self._redis_client:
            return {}

        try:
            targets_set_key = self._get_strategy_targets_set_key(user_strategy_id)
            stock_codes = self._redis_client.smembers(targets_set_key)
            
            targets = {}
            for stock_code in stock_codes:
                target_data = self.get_strategy_target(user_strategy_id, stock_code)
                if target_data:
                    targets[stock_code] = target_data
            
            return targets
        except Exception as e:
            logger.error(f"Failed to get all strategy targets from Redis: {e}", exc_info=True)
            return {}

    def delete_strategy_target(self, user_strategy_id: int, stock_code: str) -> bool:
        """
        종목별 목표가를 Redis에서 삭제

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드

        Returns:
            성공 여부
        """
        if not self._redis_client:
            return False

        try:
            target_key = self._get_strategy_target_key(user_strategy_id, stock_code)
            self._redis_client.delete(target_key)
            
            targets_set_key = self._get_strategy_targets_set_key(user_strategy_id)
            self._redis_client.srem(targets_set_key, stock_code)
            
            logger.debug(f"Strategy target deleted from Redis: {target_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete strategy target from Redis: {e}", exc_info=True)
            return False

    def update_strategy_target_buy(
        self,
        user_strategy_id: int,
        stock_code: str,
        buy_quantity: int,
        buy_price: float,
        order_no: Optional[str] = None,
        executed: bool = False
    ) -> bool:
        """
        매수 완료 정보를 전략 타겟에 업데이트

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드
            buy_quantity: 실제 매수 수량
            buy_price: 실제 매수 가격
            order_no: 주문번호 (선택)
            executed: 체결 여부 (True: 체결 완료, False: 주문만 성공)

        Returns:
            성공 여부
        """
        if not self._redis_client:
            logger.warning("Redis not connected, skipping update")
            return False

        try:
            # 기존 타겟 데이터 조회
            target_data = self.get_strategy_target(user_strategy_id, stock_code)
            if not target_data:
                logger.warning(
                    f"Strategy target not found for update: "
                    f"user_strategy_id={user_strategy_id}, stock_code={stock_code}"
                )
                return False

            # 매수 정보 업데이트
            target_data["buy_executed"] = executed
            target_data["buy_quantity"] = buy_quantity
            target_data["buy_price"] = buy_price
            target_data["buy_order_time"] = datetime.now().isoformat()
            if order_no:
                target_data["buy_order_no"] = order_no

            # Redis에 저장
            target_key = self._get_strategy_target_key(user_strategy_id, stock_code)
            self._redis_client.setex(
                target_key,
                self.TTL,
                json.dumps(target_data, ensure_ascii=False, default=str)
            )

            logger.info(
                f"Updated buy info for strategy target: "
                f"user_strategy_id={user_strategy_id}, "
                f"stock_code={stock_code}, "
                f"buy_quantity={buy_quantity}, "
                f"buy_price={buy_price}, "
                f"executed={executed}"
            )
            return True
        except Exception as e:
            logger.error(
                f"Failed to update buy info for strategy target: {e}",
                exc_info=True
            )
            return False

    def update_strategy_target_execution(
        self,
        user_strategy_id: int,
        stock_code: str,
        exec_quantity: int,
        exec_price: float,
        order_no: Optional[str] = None,
        is_buy: bool = True
    ) -> bool:
        """
        체결통보에 따라 전략 타겟 업데이트 (부분 체결 처리)

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드
            exec_quantity: 체결 수량 (이번 체결)
            exec_price: 체결 가격 (이번 체결)
            order_no: 주문번호
            is_buy: True면 매수, False면 매도

        Returns:
            성공 여부
        """
        if not self._redis_client:
            logger.warning("Redis not connected, skipping update")
            return False

        try:
            # 기존 타겟 데이터 조회
            target_data = self.get_strategy_target(user_strategy_id, stock_code)
            if not target_data:
                logger.warning(
                    f"Strategy target not found for execution update: "
                    f"user_strategy_id={user_strategy_id}, stock_code={stock_code}"
                )
                return False

            # 체결 정보 필드명 결정
            if is_buy:
                order_quantity_key = "buy_quantity"
                exec_quantity_key = "buy_executed_quantity"  # 누적 체결 수량
                exec_price_key = "buy_executed_price"  # 가중평균 가격
                order_quantity = target_data.get("buy_quantity", 0)
                order_no_key = "buy_order_no"
                executed_key = "buy_executed"
            else:
                order_quantity_key = "sell_quantity"
                exec_quantity_key = "sell_executed_quantity"
                exec_price_key = "sell_executed_price"
                order_quantity = target_data.get("sell_quantity", 0)
                order_no_key = "sell_order_no"
                executed_key = "sell_executed"

            # 주문번호 확인 (주문번호가 있고 일치하지 않으면 무시)
            if order_no and target_data.get(order_no_key):
                if target_data.get(order_no_key) != order_no:
                    logger.warning(
                        f"Order number mismatch: "
                        f"expected={target_data.get(order_no_key)}, "
                        f"received={order_no}"
                    )
                    return False

            # 기존 누적 체결 수량 및 가격 조회
            current_exec_quantity = target_data.get(exec_quantity_key, 0)
            current_exec_price = target_data.get(exec_price_key, 0.0)

            # 누적 체결 수량 업데이트
            new_exec_quantity = current_exec_quantity + exec_quantity

            # 가중평균 가격 계산
            if new_exec_quantity > 0:
                total_value = (current_exec_quantity * current_exec_price) + (exec_quantity * exec_price)
                new_exec_price = total_value / new_exec_quantity
            else:
                new_exec_price = exec_price

            # 체결 정보 업데이트
            target_data[exec_quantity_key] = new_exec_quantity
            target_data[exec_price_key] = new_exec_price

            # 전량 체결 여부 확인
            if new_exec_quantity >= order_quantity:
                target_data[executed_key] = True
                logger.info(
                    f"✅ 전량 체결 완료 ({'매수' if is_buy else '매도'}): "
                    f"user_strategy_id={user_strategy_id}, "
                    f"stock_code={stock_code}, "
                    f"주문수량={order_quantity}, "
                    f"누적체결수량={new_exec_quantity}, "
                    f"가중평균가격={new_exec_price:.2f}"
                )
            else:
                target_data[executed_key] = False
                logger.info(
                    f"⚠️ 부분 체결 ({'매수' if is_buy else '매도'}): "
                    f"user_strategy_id={user_strategy_id}, "
                    f"stock_code={stock_code}, "
                    f"주문수량={order_quantity}, "
                    f"누적체결수량={new_exec_quantity}, "
                    f"남은수량={order_quantity - new_exec_quantity}, "
                    f"가중평균가격={new_exec_price:.2f}"
                )

            # 체결 시간 업데이트
            exec_time_key = f"{'buy' if is_buy else 'sell'}_execution_time"
            target_data[exec_time_key] = datetime.now().isoformat()

            # Redis에 저장
            target_key = self._get_strategy_target_key(user_strategy_id, stock_code)
            self._redis_client.setex(
                target_key,
                self.TTL,
                json.dumps(target_data, ensure_ascii=False, default=str)
            )

            return True
        except Exception as e:
            logger.error(
                f"Failed to update execution info for strategy target: {e}",
                exc_info=True
            )
            return False

    def update_strategy_target_sell(
        self,
        user_strategy_id: int,
        stock_code: str,
        sell_quantity: int,
        sell_price: float,
        order_no: Optional[str] = None,
        executed: bool = False
    ) -> bool:
        """
        매도 완료 정보를 전략 타겟에 업데이트

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드
            sell_quantity: 실제 매도 수량
            sell_price: 실제 매도 가격
            order_no: 주문번호 (선택)
            executed: 체결 여부 (True: 체결 완료, False: 주문만 성공)

        Returns:
            성공 여부
        """
        if not self._redis_client:
            logger.warning("Redis not connected, skipping update")
            return False

        try:
            # 기존 타겟 데이터 조회
            target_data = self.get_strategy_target(user_strategy_id, stock_code)
            if not target_data:
                logger.warning(
                    f"Strategy target not found for update: "
                    f"user_strategy_id={user_strategy_id}, stock_code={stock_code}"
                )
                return False

            # 매도 정보 업데이트
            target_data["sell_executed"] = executed
            target_data["sell_quantity"] = sell_quantity
            target_data["sell_price"] = sell_price
            target_data["sell_order_time"] = datetime.now().isoformat()
            if order_no:
                target_data["sell_order_no"] = order_no

            # Redis에 저장
            target_key = self._get_strategy_target_key(user_strategy_id, stock_code)
            self._redis_client.setex(
                target_key,
                self.TTL,
                json.dumps(target_data, ensure_ascii=False, default=str)
            )

            logger.info(
                f"Updated sell info for strategy target: "
                f"user_strategy_id={user_strategy_id}, "
                f"stock_code={stock_code}, "
                f"sell_quantity={sell_quantity}, "
                f"sell_price={sell_price}, "
                f"executed={executed}"
            )
            return True
        except Exception as e:
            logger.error(
                f"Failed to update sell info for strategy target: {e}",
                exc_info=True
            )
            return False

    def delete_strategy_all_targets(self, user_strategy_id: int) -> bool:
        """
        전략의 모든 종목 목표가를 Redis에서 삭제

        Args:
            user_strategy_id: 사용자 전략 ID

        Returns:
            성공 여부
        """
        if not self._redis_client:
            return False

        try:
            # 모든 종목 코드 조회
            targets_set_key = self._get_strategy_targets_set_key(user_strategy_id)
            stock_codes = self._redis_client.smembers(targets_set_key)
            
            # 각 종목의 목표가 삭제
            for stock_code in stock_codes:
                target_key = self._get_strategy_target_key(user_strategy_id, stock_code)
                self._redis_client.delete(target_key)
            
            # Set 삭제
            self._redis_client.delete(targets_set_key)
            
            # 전략 설정도 삭제
            config_key = self._get_strategy_config_key(user_strategy_id)
            self._redis_client.delete(config_key)
            
            logger.debug(f"All strategy targets deleted from Redis for strategy: {user_strategy_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete all strategy targets from Redis: {e}", exc_info=True)
            return False


# 싱글톤 인스턴스
_redis_manager_instance: Optional[WebSocketRedisManager] = None


def get_redis_manager() -> WebSocketRedisManager:
    """WebSocketRedisManager 싱글톤 인스턴스 반환"""
    global _redis_manager_instance
    if _redis_manager_instance is None:
        _redis_manager_instance = WebSocketRedisManager()
    return _redis_manager_instance
