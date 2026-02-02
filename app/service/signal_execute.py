"""
시그널 실행 모듈

실시간 가격 데이터를 받아 매수/매도 시그널을 생성하고 실행합니다.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

from app.service.signal_generator import get_signal_generator
from app.service.calculate_slippage import SignalResult
from app.service.strategy_table import get_strategy_table
from app.kis.websocket.redis_manager import get_redis_manager
from app.kis.api.order_api import get_order_api

logger = logging.getLogger(__name__)


class SignalExecutor:
    """시그널 생성 및 실행 클래스"""

    def __init__(self):
        self._signal_generator = get_signal_generator()
        self._strategy_table = get_strategy_table()
        self._redis_manager = get_redis_manager()
        self._order_api = get_order_api()

        # 시그널 체크 최적화
        self._last_signal_check: Dict[str, datetime] = {}  # {종목코드: 마지막체크시간}
        self._signal_check_interval = 0.5  # 같은 종목 체크 간격 (초)
        self._generated_signals: set = set()  # 이미 생성된 시그널 (전략ID_종목코드)
        self._last_prices: Dict[str, float] = {}  # {종목코드: 마지막가격} - 가격 변동 없으면 스킵

    async def check_and_generate_buy_signal(self, price_data: dict) -> None:
        """
        매수 시그널 생성 체크 (시가 매수)

        조건:
        - strategy_id == 1인 전략만
        - 시가 감지 (STCK_OPRC > 0)
        - 현재가와 시가 차이 1% 미만
        - 장 시작 후 10분 이내
        - 중복 방지

        최적화:
        1. 쓰로틀링: 같은 종목은 0.5초 간격으로만 체크
        2. 가격 변동 체크: 가격이 변하지 않으면 스킵
        3. 중복 방지: 이미 BUY 시그널 생성된 전략-종목은 스킵
        """
        try:
            stock_code = price_data.get('MKSC_SHRN_ISCD', '')
            if not stock_code:
                return

            # 시가 확인
            opening_price = self._signal_generator.slippage_calculator._parse_float(price_data.get('STCK_OPRC', 0))
            current_price = self._signal_generator.slippage_calculator._parse_float(price_data.get('STCK_PRPR', 0))
            opening_hour = price_data.get('OPRC_HOUR', '')

            # 시가가 없으면 스킵
            if opening_price <= 0:
                return

            if current_price <= 0:
                return

            # 현재가와 시가 차이 계산 (%)
            price_diff_pct = abs((current_price - opening_price) / opening_price * 100) if opening_price > 0 else 999

            # 1% 미만 차이만 허용
            if price_diff_pct >= 1.0:
                logger.debug(
                    f"시가 매수 조건 불만족 (가격 차이 초과): "
                    f"종목={stock_code}, "
                    f"시가={opening_price:,.0f}, "
                    f"현재가={current_price:,.0f}, "
                    f"차이={price_diff_pct:.2f}%"
                )
                return

            # 장 시작 후 10분 이내 확인
            if opening_hour:
                try:
                    # OPRC_HOUR 형식: "HHMMSS" (예: "090000")
                    if len(opening_hour) >= 6:
                        hour_str = opening_hour[:2]
                        min_str = opening_hour[2:4]
                        sec_str = opening_hour[4:6]
                        
                        today = datetime.now().date()
                        opening_time = datetime.strptime(
                            f"{today} {hour_str}:{min_str}:{sec_str}",
                            "%Y-%m-%d %H:%M:%S"
                        )
                        current_time = datetime.now()
                        time_diff = current_time - opening_time

                        # 10분 초과 시 스킵
                        if time_diff > timedelta(minutes=10):
                            logger.debug(
                                f"시가 매수 조건 불만족 (10분 초과): "
                                f"종목={stock_code}, "
                                f"시가시간={opening_hour}, "
                                f"경과시간={time_diff.total_seconds() / 60:.1f}분"
                            )
                            return
                except Exception as e:
                    logger.warning(f"시가시간 파싱 오류: {e}, opening_hour={opening_hour}")

            # 최적화 1: 가격 변동 없으면 스킵
            last_price = self._last_prices.get(stock_code, 0)
            if current_price == last_price:
                return
            self._last_prices[stock_code] = current_price

            # 최적화 2: 쓰로틀링 - 같은 종목 0.5초 내 재체크 방지
            now = datetime.now()
            last_check = self._last_signal_check.get(stock_code)
            if last_check:
                elapsed = (now - last_check).total_seconds()
                if elapsed < self._signal_check_interval:
                    return
            self._last_signal_check[stock_code] = now

            # Redis에서 호가 데이터 가져오기
            asking_price_data = await self._get_asking_price_from_redis(stock_code)
            if not asking_price_data:
                logger.debug(f"호가 데이터 없음: {stock_code}")
                return

            # 모든 전략에 대해 시그널 체크
            strategy_ids = self._strategy_table.get_all_strategies()

            for user_strategy_id in strategy_ids:
                # strategy_id == 1만 필터링
                strategy_info = self._strategy_table.get_strategy_info(user_strategy_id)
                if not strategy_info or strategy_info.strategy_id != 1:
                    continue

                # paper 계좌는 KRX 장 운영시간(09:00~15:30)에만 주문 가능
                if strategy_info.account_type == "paper":
                    now_time = datetime.now().strftime("%H%M%S")
                    if now_time < "090000" or now_time > "153000":
                        logger.debug(
                            f"paper 계좌 매수 스킵 (KRX 장 운영시간 외): "
                            f"전략={user_strategy_id}, 종목={stock_code}"
                        )
                        continue

                # 최적화 3: 이미 BUY 시그널 생성된 조합은 스킵
                signal_key = f"{user_strategy_id}_{stock_code}_BUY"
                if signal_key in self._generated_signals:
                    continue

                # 해당 전략의 종목 목표가 확인 (예측 데이터가 있어야 함)
                target = self._strategy_table.get_target_for_comparison(
                    user_strategy_id, stock_code
                )

                if target is None:
                    logger.debug(
                        f"시가 매수 스킵 (목표가 없음): "
                        f"전략={user_strategy_id}, "
                        f"종목={stock_code}"
                    )
                    continue

                # 매수 시그널 생성
                signal = self._signal_generator.generate_buy_signal(
                    stock_code=stock_code,
                    price_data=price_data,
                    asking_price_data=asking_price_data,
                    order_quantity=target.target_quantity
                )

                if signal:
                    # BUY 시그널은 한 번만 생성 (중복 방지)
                    self._generated_signals.add(signal_key)
                    await self.handle_signal(user_strategy_id, signal)

        except Exception as e:
            logger.error(f"매수 시그널 체크 오류: {e}", exc_info=True)

    async def check_and_generate_sell_signal(self, price_data: dict) -> None:
        """
        매도 시그널 생성 체크 (Position 기반)

        최적화:
        1. 쓰로틀링: 같은 종목은 0.5초 간격으로만 체크
        2. 가격 변동 체크: 가격이 변하지 않으면 스킵
        3. 중복 방지: 이미 SELL 시그널 생성된 전략-종목은 스킵
        4. Position 기반: holding_quantity > 0 인 경우에만 매도 시그널 생성
        """
        try:
            stock_code = price_data.get('MKSC_SHRN_ISCD', '')
            if not stock_code:
                return

            current_price = float(price_data.get('STCK_PRPR') or 0)
            if current_price <= 0:
                return

            # 최적화 1: 가격 변동 없으면 스킵
            last_price = self._last_prices.get(stock_code, 0)
            if current_price == last_price:
                return
            self._last_prices[stock_code] = current_price

            # 최적화 2: 쓰로틀링 - 같은 종목 0.5초 내 재체크 방지
            now = datetime.now()
            last_check = self._last_signal_check.get(stock_code)
            if last_check:
                elapsed = (now - last_check).total_seconds()
                if elapsed < self._signal_check_interval:
                    return
            self._last_signal_check[stock_code] = now

            # Redis에서 호가 데이터 가져오기
            asking_price_data = await self._get_asking_price_from_redis(stock_code)
            if not asking_price_data:
                logger.debug(f"호가 데이터 없음: {stock_code}")
                return

            # 수동 매도 타겟 체크 (Mock 모드용)
            await self._check_manual_sell_targets(
                stock_code=stock_code,
                current_price=current_price,
                price_data=price_data,
                asking_price_data=asking_price_data
            )

            # 모든 전략에 대해 시그널 체크 (자동 매도)
            strategy_ids = self._strategy_table.get_all_strategies()

            for user_strategy_id in strategy_ids:
                # 최적화 3: 이미 SELL 시그널 생성된 조합은 스킵
                signal_key = f"{user_strategy_id}_{stock_code}_SELL"
                if signal_key in self._generated_signals:
                    continue

                # paper 계좌는 KRX 장 운영시간(09:00~15:30)에만 주문 가능
                strategy_info = self._strategy_table.get_strategy_info(user_strategy_id)
                if strategy_info and strategy_info.account_type == "paper":
                    now_time = datetime.now().strftime("%H%M%S")
                    if now_time < "090000" or now_time > "153000":
                        logger.debug(
                            f"paper 계좌 매도 스킵 (KRX 장 운영시간 외): "
                            f"전략={user_strategy_id}, 종목={stock_code}"
                        )
                        continue

                # Position 조회 (보유 수량 확인)
                position = self._redis_manager.get_position_by_user(user_strategy_id, stock_code)
                holding_quantity = position.get("holding_quantity", 0) if position else 0

                # 보유 수량 없으면 스킵
                if holding_quantity <= 0:
                    continue

                # 활성 매도 주문 확인 (중복 주문 방지)
                if position:
                    daily_strategy_id = position.get("daily_strategy_id")
                    if daily_strategy_id:
                        active_sell = self._redis_manager.get_active_sell_order(
                            daily_strategy_id, stock_code
                        )
                        if active_sell:
                            logger.debug(
                                f"활성 매도 주문 있음, 스킵: "
                                f"전략={user_strategy_id}, 종목={stock_code}"
                            )
                            continue

                target = self._strategy_table.get_target_for_comparison(
                    user_strategy_id, stock_code
                )

                if target is None:
                    continue

                # 매도 시그널 생성 (Position의 holding_quantity 사용)
                signal = self._signal_generator.generate_sell_signal(
                    stock_code=stock_code,
                    price_data=price_data,
                    asking_price_data=asking_price_data,
                    target_price=target.target_sell_price,
                    stop_loss_price=target.stop_loss_price,
                    order_quantity=holding_quantity  # Position의 보유 수량 사용
                )

                if signal:
                    if signal.signal_type == "SELL":
                        # SELL 시그널은 한 번만 생성 (중복 방지)
                        self._generated_signals.add(signal_key)
                        await self.handle_signal(user_strategy_id, signal)
                    elif signal.signal_type == "HOLD":
                        # HOLD 경고는 계속 허용 (단, 쓰로틀링 적용됨)
                        await self.handle_signal(user_strategy_id, signal)

        except Exception as e:
            logger.error(f"매도 시그널 체크 오류: {e}", exc_info=True)

    async def handle_signal(self, user_strategy_id: int, signal: SignalResult) -> None:
        """
        생성된 시그널 처리

        Args:
            user_strategy_id: 전략 ID
            signal: 생성된 시그널
        """
        try:
            if signal.signal_type == "BUY":
                # 매수 시그널 로깅
                logger.warning(
                    f"🟢 매수 시그널 생성! "
                    f"[전략={user_strategy_id}] "
                    f"종목={signal.stock_code}, "
                    f"수량={signal.target_quantity}, "
                    f"현재가={signal.current_price:,.0f}, "
                    f"추천가={signal.recommended_order_price:,.0f}, "
                    f"주문유형={signal.recommended_order_type.value}, "
                    f"예상슬리피지={signal.expected_slippage_pct:.3f}%, "
                    f"긴급도={signal.urgency}, "
                    f"사유={signal.reason}"
                )

                # 주문 처리 (mock 여부에 따라 자동 분기)
                order_result = await self._order_api.process_buy_order(
                    user_strategy_id=user_strategy_id,
                    signal=signal,
                    order_quantity=signal.target_quantity
                )

                if order_result.get("success"):
                    logger.info(
                        f"✅ 매수 주문 처리 완료: "
                        f"[전략={user_strategy_id}] "
                        f"종목={signal.stock_code}, "
                        f"결과={order_result}"
                    )
                else:
                    # 주문 실패 시 1회 재시도
                    logger.warning(
                        f"⚠️ 매수 주문 실패, 재시도 중: "
                        f"[전략={user_strategy_id}] "
                        f"종목={signal.stock_code}, "
                        f"오류={order_result.get('error', 'N/A')}"
                    )

                    # 재시도
                    retry_result = await self._order_api.process_buy_order(
                        user_strategy_id=user_strategy_id,
                        signal=signal,
                        order_quantity=signal.target_quantity
                    )

                    if retry_result.get("success"):
                        logger.info(
                            f"✅ 매수 주문 재시도 성공: "
                            f"[전략={user_strategy_id}] "
                            f"종목={signal.stock_code}"
                        )
                    else:
                        logger.error(
                            f"❌ 매수 주문 재시도 실패: "
                            f"[전략={user_strategy_id}] "
                            f"종목={signal.stock_code}, "
                            f"오류={retry_result.get('error', 'N/A')}"
                        )
                        # 재시도도 실패하면 시그널 초기화 (다음 시그널 허용)
                        self.clear_generated_signal(user_strategy_id, signal.stock_code, "BUY")

                # Redis에 시그널 저장 (백업용)
                await self._save_signal_to_redis(user_strategy_id, signal)

            elif signal.signal_type == "SELL":
                # 매도 시그널 로깅
                logger.warning(
                    f"🔴 매도 시그널 생성! "
                    f"[전략={user_strategy_id}] "
                    f"종목={signal.stock_code}, "
                    f"수량={signal.target_quantity}, "
                    f"현재가={signal.current_price:,.0f}, "
                    f"추천가={signal.recommended_order_price:,.0f}, "
                    f"주문유형={signal.recommended_order_type.value}, "
                    f"예상슬리피지={signal.expected_slippage_pct:.3f}%, "
                    f"긴급도={signal.urgency}, "
                    f"사유={signal.reason}"
                )

                # 주문 처리 (mock 여부에 따라 자동 분기)
                order_result = await self._order_api.process_sell_order(
                    user_strategy_id=user_strategy_id,
                    signal=signal,
                    order_quantity=signal.target_quantity  # Position의 holding_quantity
                )

                if order_result.get("success"):
                    logger.info(
                        f"✅ 매도 주문 처리 완료: "
                        f"[전략={user_strategy_id}] "
                        f"종목={signal.stock_code}, "
                        f"결과={order_result}"
                    )
                else:
                    # 주문 실패 시 1회 재시도
                    logger.warning(
                        f"⚠️ 매도 주문 실패, 재시도 중: "
                        f"[전략={user_strategy_id}] "
                        f"종목={signal.stock_code}, "
                        f"오류={order_result.get('error', 'N/A')}"
                    )

                    # 재시도
                    retry_result = await self._order_api.process_sell_order(
                        user_strategy_id=user_strategy_id,
                        signal=signal,
                        order_quantity=signal.target_quantity
                    )

                    if retry_result.get("success"):
                        logger.info(
                            f"✅ 매도 주문 재시도 성공: "
                            f"[전략={user_strategy_id}] "
                            f"종목={signal.stock_code}"
                        )
                    else:
                        logger.error(
                            f"❌ 매도 주문 재시도 실패: "
                            f"[전략={user_strategy_id}] "
                            f"종목={signal.stock_code}, "
                            f"오류={retry_result.get('error', 'N/A')}"
                        )
                        # 재시도도 실패하면 시그널 초기화 (다음 시그널 허용)
                        self.clear_generated_signal(user_strategy_id, signal.stock_code, "SELL")

                # Redis에 시그널 저장 (백업용)
                await self._save_signal_to_redis(user_strategy_id, signal)

            elif signal.signal_type == "HOLD":
                # 손절가 접근 경고 로깅
                logger.info(
                    f"⚠️ 손절가 접근 경고! "
                    f"[전략={user_strategy_id}] "
                    f"종목={signal.stock_code}, "
                    f"현재가={signal.current_price:,.0f}, "
                    f"손절가={signal.stop_loss_price:,.0f}, "
                    f"사유={signal.reason}"
                )

        except Exception as e:
            logger.error(f"시그널 처리 오류: {e}", exc_info=True)

    async def _get_asking_price_from_redis(self, stock_code: str) -> Optional[dict]:
        """Redis에서 호가 데이터 가져오기"""
        try:
            if not self._redis_manager._redis_client:
                return None

            redis_key = f"websocket:asking_price_data:{stock_code}"
            data = self._redis_manager._redis_client.get(redis_key)

            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.warning(f"Redis에서 호가 데이터 조회 실패: {e}")
            return None

    async def _save_signal_to_redis(self, user_strategy_id: int, signal: SignalResult) -> None:
        """매수/매도 시그널을 Redis에 저장"""
        try:
            if not self._redis_manager._redis_client:
                logger.warning("Redis 클라이언트가 연결되지 않음")
                return

            signal_type_lower = signal.signal_type.lower()
            redis_key = f"signal:{signal_type_lower}:{user_strategy_id}:{signal.stock_code}"

            signal_data = {
                "signal_type": signal.signal_type,
                "stock_code": signal.stock_code,
                "current_price": signal.current_price,
                "target_price": signal.target_price,
                "stop_loss_price": signal.stop_loss_price,
                "recommended_order_price": signal.recommended_order_price,
                "recommended_order_type": signal.recommended_order_type.value,
                "expected_slippage_pct": signal.expected_slippage_pct,
                "urgency": signal.urgency,
                "reason": signal.reason,
                "created_at": datetime.now().isoformat(),
                "user_strategy_id": user_strategy_id
            }

            # 30분 TTL로 저장
            self._redis_manager._redis_client.setex(
                redis_key,
                1800,
                json.dumps(signal_data, ensure_ascii=False)
            )

            logger.debug(f"시그널 Redis 저장 완료: {redis_key}")

        except Exception as e:
            logger.error(f"시그널 Redis 저장 실패: {e}", exc_info=True)

    async def _check_manual_sell_targets(
        self,
        stock_code: str,
        current_price: float,
        price_data: dict,
        asking_price_data: dict
    ) -> None:
        """
        수동 매도 타겟 체크 (Mock 모드용)

        Redis에 등록된 수동 매도 타겟을 확인하고 가격 조건이 충족되면 주문 생성

        Args:
            stock_code: 종목 코드
            current_price: 현재가
            price_data: 가격 데이터
            asking_price_data: 호가 데이터
        """
        try:
            # 모든 전략에 대해 수동 매도 타겟 체크
            strategy_ids = self._strategy_table.get_all_strategies()

            for user_strategy_id in strategy_ids:
                # 수동 매도 타겟 조회
                target = self._redis_manager.get_manual_sell_target(user_strategy_id, stock_code)
                if not target:
                    continue

                order_type = target.get("order_type", "LIMIT")
                order_price = float(target.get("order_price", 0))
                order_quantity = int(target.get("order_quantity", 0))

                if order_quantity <= 0:
                    logger.warning(f"Invalid manual sell quantity: {order_quantity}")
                    self._redis_manager.delete_manual_sell_target(user_strategy_id, stock_code)
                    continue

                # 가격 조건 체크
                should_execute = False

                if order_type == "MARKET":
                    # 시장가: 바로 실행
                    should_execute = True
                    logger.info(
                        f"[MANUAL SELL] 시장가 조건 충족: "
                        f"전략={user_strategy_id}, 종목={stock_code}, 현재가={current_price:,.0f}"
                    )
                else:
                    # 지정가: 지정가 <= 현재가 일 때 실행 (매도)
                    if order_price <= current_price:
                        should_execute = True
                        logger.info(
                            f"[MANUAL SELL] 지정가 조건 충족: "
                            f"전략={user_strategy_id}, 종목={stock_code}, "
                            f"지정가={order_price:,.0f} <= 현재가={current_price:,.0f}"
                        )
                    else:
                        logger.debug(
                            f"[MANUAL SELL] 지정가 조건 미충족: "
                            f"전략={user_strategy_id}, 종목={stock_code}, "
                            f"지정가={order_price:,.0f} > 현재가={current_price:,.0f}"
                        )

                if should_execute:
                    # SlippageCalculator를 통해 최적 가격 계산
                    signal = self._signal_generator.generate_manual_sell_signal(
                        stock_code=stock_code,
                        price_data=price_data,
                        asking_price_data=asking_price_data,
                        order_type=order_type,
                        order_price=order_price,
                        order_quantity=order_quantity
                    )

                    if signal:
                        logger.warning(
                            f"🔵 수동 매도 시그널 생성! "
                            f"[전략={user_strategy_id}] "
                            f"종목={stock_code}, "
                            f"수량={order_quantity}, "
                            f"현재가={current_price:,.0f}, "
                            f"추천가={signal.recommended_order_price:,.0f}"
                        )

                        # 주문 처리
                        order_result = await self._order_api.process_sell_order(
                            user_strategy_id=user_strategy_id,
                            signal=signal,
                            order_quantity=order_quantity
                        )

                        if order_result.get("success"):
                            logger.info(
                                f"✅ 수동 매도 주문 처리 완료: "
                                f"[전략={user_strategy_id}] 종목={stock_code}"
                            )
                            # 타겟 삭제
                            self._redis_manager.delete_manual_sell_target(user_strategy_id, stock_code)
                        else:
                            logger.error(
                                f"❌ 수동 매도 주문 실패: "
                                f"[전략={user_strategy_id}] 종목={stock_code}, "
                                f"오류={order_result.get('error', 'N/A')}"
                            )

        except Exception as e:
            logger.error(f"수동 매도 타겟 체크 오류: {e}", exc_info=True)

    def clear_generated_signal(self, user_strategy_id: int, stock_code: str, signal_type: str = None) -> None:
        """
        생성된 시그널 초기화 (주문 체결 후 호출)

        Args:
            user_strategy_id: 사용자 전략 ID
            stock_code: 종목 코드
            signal_type: 시그널 유형 (BUY, SELL 또는 None - None이면 둘 다 초기화)
        """
        if signal_type:
            # 특정 시그널 유형만 초기화
            signal_key = f"{user_strategy_id}_{stock_code}_{signal_type}"
            self._generated_signals.discard(signal_key)
            logger.info(f"시그널 초기화: {signal_key}")
        else:
            # 모든 시그널 유형 초기화
            buy_key = f"{user_strategy_id}_{stock_code}_BUY"
            sell_key = f"{user_strategy_id}_{stock_code}_SELL"
            self._generated_signals.discard(buy_key)
            self._generated_signals.discard(sell_key)
            logger.info(f"시그널 초기화: {buy_key}, {sell_key}")


# 싱글톤 인스턴스
_signal_executor_instance: Optional[SignalExecutor] = None


def get_signal_executor() -> SignalExecutor:
    """SignalExecutor 싱글톤 인스턴스 반환"""
    global _signal_executor_instance
    if _signal_executor_instance is None:
        _signal_executor_instance = SignalExecutor()
    return _signal_executor_instance
