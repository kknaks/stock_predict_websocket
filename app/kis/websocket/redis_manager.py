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
        account_type: str = "mock",
        access_token: Optional[str] = None,
        user_id: Optional[int] = None,
        user_strategy_ids: Optional[list] = None,
        hts_id: Optional[str] = None,
        status: str = "connected",
        reconnect_attempts: int = 0,
        decrypt_key: Optional[str] = None,
        decrypt_iv: Optional[str] = None,
    ) -> bool:
        """
        계좌 웹소켓 연결 정보 저장

        Args:
            account_no: 계좌번호
            ws_token: WebSocket 접속키
            appkey: 앱키
            env_dv: 환경구분
            account_product_code: 계좌상품코드
            is_mock: 모의 여부
            account_type: 계좌 유형 (real/paper/mock)
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
                "account_type": account_type,
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
            
            # 복호화 키 및 IV가 있으면 추가 (Base64 인코딩된 문자열로 저장)
            if decrypt_key:
                data["decrypt_key"] = decrypt_key
            if decrypt_iv:
                data["decrypt_iv"] = decrypt_iv

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

    # ==========================================================================
    # Position 관리 메서드
    # ==========================================================================

    # Position Redis 키 접두사
    POSITION_KEY_PREFIX = "position"
    POSITION_LIST_KEY_PREFIX = "position:list"
    POSITION_BY_USER_KEY_PREFIX = "position:by_user"

    # Order Redis 키 접두사
    ORDER_KEY_PREFIX = "order"
    ORDER_BY_NO_KEY_PREFIX = "order:by_no"
    ORDER_ACTIVE_KEY_PREFIX = "order:active"

    # Position/Order TTL (25시간)
    POSITION_TTL = 90000

    def _get_position_key(self, daily_strategy_id: int, stock_code: str) -> str:
        """Position Redis 키 생성"""
        return f"{self.POSITION_KEY_PREFIX}:{daily_strategy_id}:{stock_code}"

    def _get_position_list_key(self, daily_strategy_id: int) -> str:
        """Position 목록 Redis 키 생성"""
        return f"{self.POSITION_LIST_KEY_PREFIX}:{daily_strategy_id}"

    def _get_position_by_user_key(self, user_strategy_id: int, stock_code: str) -> str:
        """user_strategy_id로 Position 조회용 인덱스 키"""
        return f"{self.POSITION_BY_USER_KEY_PREFIX}:{user_strategy_id}:{stock_code}"

    def _get_order_key(self, order_id: str) -> str:
        """Order Redis 키 생성"""
        return f"{self.ORDER_KEY_PREFIX}:{order_id}"

    def _get_order_by_no_key(self, order_no: str) -> str:
        """order_no로 Order 조회용 인덱스 키"""
        return f"{self.ORDER_BY_NO_KEY_PREFIX}:{order_no}"

    def _get_order_active_key(self, daily_strategy_id: int, stock_code: str) -> str:
        """활성 주문 목록 Redis 키 (Set)"""
        return f"{self.ORDER_ACTIVE_KEY_PREFIX}:{daily_strategy_id}:{stock_code}"

    def save_position(self, daily_strategy_id: int, stock_code: str, position_data: dict) -> bool:
        """
        Position을 Redis에 저장

        Args:
            daily_strategy_id: 일일 전략 ID
            stock_code: 종목 코드
            position_data: Position 데이터 (dict)

        Returns:
            성공 여부
        """
        if not self._redis_client:
            logger.warning("Redis not connected, skipping save position")
            return False

        try:
            # Position 저장
            key = self._get_position_key(daily_strategy_id, stock_code)
            self._redis_client.setex(
                key,
                self.POSITION_TTL,
                json.dumps(position_data, ensure_ascii=False, default=str)
            )

            # 인덱스 업데이트: position:list:{daily_strategy_id} (Set)
            list_key = self._get_position_list_key(daily_strategy_id)
            self._redis_client.sadd(list_key, stock_code)
            self._redis_client.expire(list_key, self.POSITION_TTL)

            # user_strategy_id 인덱스: position:by_user:{user_strategy_id}:{stock_code} → daily_strategy_id
            user_strategy_id = position_data.get("user_strategy_id")
            if user_strategy_id:
                user_key = self._get_position_by_user_key(user_strategy_id, stock_code)
                self._redis_client.setex(user_key, self.POSITION_TTL, str(daily_strategy_id))

            logger.debug(
                f"Position saved: daily_strategy_id={daily_strategy_id}, "
                f"stock_code={stock_code}, "
                f"holding_quantity={position_data.get('holding_quantity', 0)}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to save position to Redis: {e}", exc_info=True)
            return False

    def get_position(self, daily_strategy_id: int, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        Position 조회

        Args:
            daily_strategy_id: 일일 전략 ID
            stock_code: 종목 코드

        Returns:
            Position 데이터 또는 None
        """
        if not self._redis_client:
            return None

        try:
            key = self._get_position_key(daily_strategy_id, stock_code)
            data = self._redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Failed to get position from Redis: {e}", exc_info=True)
            return None

    def get_position_by_user(self, user_strategy_id: int, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        user_strategy_id로 Position 조회

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드

        Returns:
            Position 데이터 또는 None
        """
        if not self._redis_client:
            return None

        try:
            # user_strategy_id → daily_strategy_id 인덱스 조회
            user_key = self._get_position_by_user_key(user_strategy_id, stock_code)
            daily_strategy_id_str = self._redis_client.get(user_key)

            if daily_strategy_id_str:
                daily_strategy_id = int(daily_strategy_id_str)
                return self.get_position(daily_strategy_id, stock_code)
            return None
        except Exception as e:
            logger.error(f"Failed to get position by user from Redis: {e}", exc_info=True)
            return None

    def get_all_positions(self, daily_strategy_id: int) -> list:
        """
        daily_strategy_id의 모든 Position 조회

        Args:
            daily_strategy_id: 일일 전략 ID

        Returns:
            Position 목록 (list of dict)
        """
        if not self._redis_client:
            return []

        try:
            list_key = self._get_position_list_key(daily_strategy_id)
            stock_codes = self._redis_client.smembers(list_key)

            positions = []
            for stock_code in stock_codes:
                position = self.get_position(daily_strategy_id, stock_code)
                if position:
                    positions.append(position)
            return positions
        except Exception as e:
            logger.error(f"Failed to get all positions from Redis: {e}", exc_info=True)
            return []

    def get_positions_with_holdings(self, daily_strategy_id: int) -> list:
        """
        보유 수량이 있는 모든 Position 조회 (holding_quantity > 0)

        Args:
            daily_strategy_id: 일일 전략 ID

        Returns:
            보유 중인 Position 목록
        """
        positions = self.get_all_positions(daily_strategy_id)
        return [p for p in positions if p.get("holding_quantity", 0) > 0]

    def update_position_buy(
        self,
        daily_strategy_id: int,
        stock_code: str,
        exec_qty: int,
        exec_price: float
    ) -> bool:
        """
        매수 체결 시 Position 업데이트

        - holding_quantity += 체결수량
        - average_price 가중평균 재계산
        - total_buy_quantity, total_buy_amount 업데이트

        Args:
            daily_strategy_id: 일일 전략 ID
            stock_code: 종목 코드
            exec_qty: 체결 수량
            exec_price: 체결 가격

        Returns:
            성공 여부
        """
        if not self._redis_client:
            return False

        try:
            position = self.get_position(daily_strategy_id, stock_code)
            if not position:
                logger.warning(
                    f"Position not found for buy update: "
                    f"daily_strategy_id={daily_strategy_id}, stock_code={stock_code}"
                )
                return False

            # 가중평균 가격 계산
            current_qty = position.get("holding_quantity", 0)
            current_avg = position.get("average_price", 0.0)
            total_qty = current_qty + exec_qty

            if total_qty > 0:
                new_avg = (
                    (current_qty * current_avg) + (exec_qty * exec_price)
                ) / total_qty
            else:
                new_avg = exec_price

            # Position 업데이트
            position["holding_quantity"] = total_qty
            position["average_price"] = round(new_avg, 2)
            position["total_investment"] = round(total_qty * new_avg, 2)
            position["total_buy_quantity"] = position.get("total_buy_quantity", 0) + exec_qty
            position["total_buy_amount"] = position.get("total_buy_amount", 0.0) + (exec_qty * exec_price)
            position["updated_at"] = datetime.now().isoformat()

            # 저장
            success = self.save_position(daily_strategy_id, stock_code, position)

            if success:
                logger.info(
                    f"✅ Position 매수 업데이트: "
                    f"종목={stock_code}, "
                    f"체결수량={exec_qty}, "
                    f"체결가격={exec_price:,.0f}, "
                    f"보유수량={total_qty}, "
                    f"평균가={new_avg:,.0f}"
                )
            return success

        except Exception as e:
            logger.error(f"Failed to update position buy: {e}", exc_info=True)
            return False

    def update_position_sell(
        self,
        daily_strategy_id: int,
        stock_code: str,
        exec_qty: int,
        exec_price: float
    ) -> bool:
        """
        매도 체결 시 Position 업데이트

        - holding_quantity -= 체결수량
        - realized_pnl 계산
        - total_sell_quantity, total_sell_amount 업데이트

        Args:
            daily_strategy_id: 일일 전략 ID
            stock_code: 종목 코드
            exec_qty: 체결 수량
            exec_price: 체결 가격

        Returns:
            성공 여부
        """
        if not self._redis_client:
            return False

        try:
            position = self.get_position(daily_strategy_id, stock_code)
            if not position:
                logger.warning(
                    f"Position not found for sell update: "
                    f"daily_strategy_id={daily_strategy_id}, stock_code={stock_code}"
                )
                return False

            avg_price = position.get("average_price", 0.0)

            # 실현 손익 계산
            pnl = (exec_price - avg_price) * exec_qty

            # Position 업데이트
            new_holding_qty = position.get("holding_quantity", 0) - exec_qty
            position["holding_quantity"] = max(0, new_holding_qty)
            position["total_sell_quantity"] = position.get("total_sell_quantity", 0) + exec_qty
            position["total_sell_amount"] = position.get("total_sell_amount", 0.0) + (exec_qty * exec_price)
            position["realized_pnl"] = position.get("realized_pnl", 0.0) + pnl
            position["updated_at"] = datetime.now().isoformat()

            # 보유 수량이 0이면 투자금액도 0
            if position["holding_quantity"] <= 0:
                position["holding_quantity"] = 0
                position["total_investment"] = 0
            else:
                position["total_investment"] = round(position["holding_quantity"] * avg_price, 2)

            # 저장
            success = self.save_position(daily_strategy_id, stock_code, position)

            if success:
                logger.info(
                    f"✅ Position 매도 업데이트: "
                    f"종목={stock_code}, "
                    f"체결수량={exec_qty}, "
                    f"체결가격={exec_price:,.0f}, "
                    f"잔여수량={position['holding_quantity']}, "
                    f"실현손익={pnl:,.0f}"
                )
            return success

        except Exception as e:
            logger.error(f"Failed to update position sell: {e}", exc_info=True)
            return False

    def create_position_if_not_exists(
        self,
        daily_strategy_id: int,
        user_strategy_id: int,
        stock_code: str,
        stock_name: str = ""
    ) -> Optional[Dict[str, Any]]:
        """
        Position이 없으면 새로 생성

        Args:
            daily_strategy_id: 일일 전략 ID
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드
            stock_name: 종목명

        Returns:
            Position 데이터
        """
        position = self.get_position(daily_strategy_id, stock_code)
        if position:
            return position

        # 새 Position 생성
        now = datetime.now().isoformat()
        new_position = {
            "daily_strategy_id": daily_strategy_id,
            "user_strategy_id": user_strategy_id,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "holding_quantity": 0,
            "average_price": 0.0,
            "total_investment": 0.0,
            "total_buy_quantity": 0,
            "total_buy_amount": 0.0,
            "total_sell_quantity": 0,
            "total_sell_amount": 0.0,
            "realized_pnl": 0.0,
            "created_at": now,
            "updated_at": now,
        }

        if self.save_position(daily_strategy_id, stock_code, new_position):
            logger.info(
                f"New position created: "
                f"daily_strategy_id={daily_strategy_id}, "
                f"user_strategy_id={user_strategy_id}, "
                f"stock_code={stock_code}"
            )
            return new_position
        return None

    # ==========================================================================
    # Order 관리 메서드
    # ==========================================================================

    def save_order(self, order_data: dict) -> bool:
        """
        Order를 Redis에 저장

        Args:
            order_data: Order 데이터 (dict)

        Returns:
            성공 여부
        """
        if not self._redis_client:
            logger.warning("Redis not connected, skipping save order")
            return False

        try:
            order_id = order_data.get("order_id")
            if not order_id:
                logger.error("order_id is required to save order")
                return False

            # Order 저장
            key = self._get_order_key(order_id)
            self._redis_client.setex(
                key,
                self.POSITION_TTL,
                json.dumps(order_data, ensure_ascii=False, default=str)
            )

            # order_no 인덱스 (order_no가 있는 경우만)
            order_no = order_data.get("order_no")
            if order_no:
                order_no_key = self._get_order_by_no_key(order_no)
                self._redis_client.setex(order_no_key, self.POSITION_TTL, order_id)

            # 활성 주문 인덱스 (미체결 상태인 경우)
            status = order_data.get("status", "pending")
            if status in ["pending", "submitted", "ordered", "partial"]:
                daily_strategy_id = order_data.get("daily_strategy_id")
                stock_code = order_data.get("stock_code")
                if daily_strategy_id and stock_code:
                    active_key = self._get_order_active_key(daily_strategy_id, stock_code)
                    self._redis_client.sadd(active_key, order_id)
                    self._redis_client.expire(active_key, self.POSITION_TTL)

            logger.debug(
                f"Order saved: order_id={order_id}, "
                f"order_no={order_no}, "
                f"status={status}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to save order to Redis: {e}", exc_info=True)
            return False

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """
        Order 조회

        Args:
            order_id: 주문 ID

        Returns:
            Order 데이터 또는 None
        """
        if not self._redis_client:
            return None

        try:
            key = self._get_order_key(order_id)
            data = self._redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Failed to get order from Redis: {e}", exc_info=True)
            return None

    def get_order_by_order_no(self, order_no: str) -> Optional[Dict[str, Any]]:
        """
        order_no로 Order 조회

        Args:
            order_no: KIS API 주문번호

        Returns:
            Order 데이터 또는 None
        """
        if not self._redis_client:
            return None

        try:
            order_no_key = self._get_order_by_no_key(order_no)
            order_id = self._redis_client.get(order_no_key)

            if order_id:
                return self.get_order(order_id)
            return None
        except Exception as e:
            logger.error(f"Failed to get order by order_no from Redis: {e}", exc_info=True)
            return None

    def get_active_orders(self, daily_strategy_id: int, stock_code: str) -> list:
        """
        활성 주문 목록 조회

        Args:
            daily_strategy_id: 일일 전략 ID
            stock_code: 종목 코드

        Returns:
            활성 Order 목록
        """
        if not self._redis_client:
            return []

        try:
            active_key = self._get_order_active_key(daily_strategy_id, stock_code)
            order_ids = self._redis_client.smembers(active_key)

            orders = []
            for order_id in order_ids:
                order = self.get_order(order_id)
                if order and order.get("status") in ["pending", "submitted", "ordered", "partial"]:
                    orders.append(order)
            return orders
        except Exception as e:
            logger.error(f"Failed to get active orders from Redis: {e}", exc_info=True)
            return []

    def get_active_sell_order(self, daily_strategy_id: int, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        활성 매도 주문 조회 (하나만)

        Args:
            daily_strategy_id: 일일 전략 ID
            stock_code: 종목 코드

        Returns:
            활성 매도 Order 또는 None
        """
        active_orders = self.get_active_orders(daily_strategy_id, stock_code)
        for order in active_orders:
            if order.get("order_type") == "SELL":
                return order
        return None

    def get_active_buy_order(self, daily_strategy_id: int, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        활성 매수 주문 조회 (하나만)

        Args:
            daily_strategy_id: 일일 전략 ID
            stock_code: 종목 코드

        Returns:
            활성 매수 Order 또는 None
        """
        active_orders = self.get_active_orders(daily_strategy_id, stock_code)
        for order in active_orders:
            if order.get("order_type") == "BUY":
                return order
        return None

    def update_order_status(
        self,
        order_id: str,
        status: str,
        order_no: Optional[str] = None,
        executed_quantity: Optional[int] = None,
        executed_price: Optional[float] = None
    ) -> bool:
        """
        Order 상태 업데이트

        Args:
            order_id: 주문 ID
            status: 새 상태
            order_no: KIS API 주문번호 (선택)
            executed_quantity: 체결 수량 (선택)
            executed_price: 체결 가격 (선택)

        Returns:
            성공 여부
        """
        if not self._redis_client:
            return False

        try:
            order = self.get_order(order_id)
            if not order:
                logger.warning(f"Order not found for status update: order_id={order_id}")
                return False

            # 상태 업데이트
            old_status = order.get("status")
            order["status"] = status
            order["updated_at"] = datetime.now().isoformat()

            if order_no:
                order["order_no"] = order_no

            if executed_quantity is not None:
                order["executed_quantity"] = executed_quantity
                order["remaining_quantity"] = order.get("order_quantity", 0) - executed_quantity

            if executed_price is not None:
                order["executed_price"] = executed_price

            # 저장
            success = self.save_order(order)

            # 체결 완료 또는 취소 시 활성 주문 목록에서 제거
            if status in ["filled", "cancelled", "rejected"]:
                daily_strategy_id = order.get("daily_strategy_id")
                stock_code = order.get("stock_code")
                if daily_strategy_id and stock_code:
                    active_key = self._get_order_active_key(daily_strategy_id, stock_code)
                    self._redis_client.srem(active_key, order_id)

            if success:
                logger.info(
                    f"Order status updated: order_id={order_id}, "
                    f"{old_status} → {status}, "
                    f"executed_qty={executed_quantity}"
                )
            return success

        except Exception as e:
            logger.error(f"Failed to update order status: {e}", exc_info=True)
            return False

    def remove_order_from_active(self, order_id: str) -> bool:
        """
        활성 주문 목록에서 제거

        Args:
            order_id: 주문 ID

        Returns:
            성공 여부
        """
        if not self._redis_client:
            return False

        try:
            order = self.get_order(order_id)
            if not order:
                return False

            daily_strategy_id = order.get("daily_strategy_id")
            stock_code = order.get("stock_code")

            if daily_strategy_id and stock_code:
                active_key = self._get_order_active_key(daily_strategy_id, stock_code)
                self._redis_client.srem(active_key, order_id)
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to remove order from active: {e}", exc_info=True)
            return False

    # ==========================================================================
    # Daily Strategy ID 관리
    # ==========================================================================

    DAILY_STRATEGY_KEY_PREFIX = "daily_strategy"
    DAILY_STRATEGY_BY_USER_KEY_PREFIX = "daily_strategy:by_user"

    def _get_daily_strategy_key(self, daily_strategy_id: int) -> str:
        """Daily Strategy Redis 키 생성"""
        return f"{self.DAILY_STRATEGY_KEY_PREFIX}:{daily_strategy_id}"

    def _get_daily_strategy_by_user_key(self, user_strategy_id: int) -> str:
        """user_strategy_id로 daily_strategy_id 조회용 인덱스 키"""
        return f"{self.DAILY_STRATEGY_BY_USER_KEY_PREFIX}:{user_strategy_id}"

    def save_daily_strategy(self, daily_strategy_id: int, user_strategy_id: int, data: dict) -> bool:
        """
        Daily Strategy 정보 저장

        Args:
            daily_strategy_id: 일일 전략 ID
            user_strategy_id: 사용자 전략 ID
            data: 추가 데이터

        Returns:
            성공 여부
        """
        if not self._redis_client:
            return False

        try:
            # Daily Strategy 저장
            key = self._get_daily_strategy_key(daily_strategy_id)
            strategy_data = {
                "daily_strategy_id": daily_strategy_id,
                "user_strategy_id": user_strategy_id,
                "created_at": datetime.now().isoformat(),
                **data
            }
            self._redis_client.setex(
                key,
                self.POSITION_TTL,
                json.dumps(strategy_data, ensure_ascii=False, default=str)
            )

            # user_strategy_id → daily_strategy_id 인덱스
            user_key = self._get_daily_strategy_by_user_key(user_strategy_id)
            self._redis_client.setex(user_key, self.POSITION_TTL, str(daily_strategy_id))

            logger.info(
                f"Daily strategy saved: "
                f"daily_strategy_id={daily_strategy_id}, "
                f"user_strategy_id={user_strategy_id}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to save daily strategy: {e}", exc_info=True)
            return False

    def get_daily_strategy_id(self, user_strategy_id: int) -> Optional[int]:
        """
        user_strategy_id로 daily_strategy_id 조회

        Args:
            user_strategy_id: 사용자 전략 ID

        Returns:
            daily_strategy_id 또는 None
        """
        if not self._redis_client:
            return None

        try:
            user_key = self._get_daily_strategy_by_user_key(user_strategy_id)
            daily_strategy_id_str = self._redis_client.get(user_key)

            if daily_strategy_id_str:
                return int(daily_strategy_id_str)
            return None
        except Exception as e:
            logger.error(f"Failed to get daily_strategy_id: {e}", exc_info=True)
            return None

    def get_daily_strategy(self, daily_strategy_id: int) -> Optional[Dict[str, Any]]:
        """
        Daily Strategy 조회

        Args:
            daily_strategy_id: 일일 전략 ID

        Returns:
            Daily Strategy 데이터 또는 None
        """
        if not self._redis_client:
            return None

        try:
            key = self._get_daily_strategy_key(daily_strategy_id)
            data = self._redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Failed to get daily strategy: {e}", exc_info=True)
            return None

    def generate_daily_strategy_id(self) -> int:
        """
        새로운 daily_strategy_id 생성 (Redis INCR 사용)

        Returns:
            새 daily_strategy_id
        """
        if not self._redis_client:
            # Redis 없으면 타임스탬프 기반 ID
            return int(datetime.now().strftime("%Y%m%d%H%M%S"))

        try:
            counter_key = "daily_strategy:counter"
            new_id = self._redis_client.incr(counter_key)
            # 하루 후 만료 (다음 날 새로 시작)
            self._redis_client.expire(counter_key, 86400)
            return new_id
        except Exception as e:
            logger.error(f"Failed to generate daily_strategy_id: {e}", exc_info=True)
            return int(datetime.now().strftime("%Y%m%d%H%M%S"))


# 싱글톤 인스턴스
_redis_manager_instance: Optional[WebSocketRedisManager] = None


def get_redis_manager() -> WebSocketRedisManager:
    """WebSocketRedisManager 싱글톤 인스턴스 반환"""
    global _redis_manager_instance
    if _redis_manager_instance is None:
        _redis_manager_instance = WebSocketRedisManager()
    return _redis_manager_instance
