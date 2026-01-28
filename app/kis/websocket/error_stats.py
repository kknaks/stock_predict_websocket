"""
KIS WebSocket Error Statistics

에러 통계 수집 및 관리
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict
from dataclasses import dataclass, field

from app.kis.websocket.exceptions import WebSocketError

logger = logging.getLogger(__name__)


@dataclass
class ErrorRecord:
    """에러 기록"""

    timestamp: datetime
    error_type: str
    error_code: str
    message: str
    details: Dict = field(default_factory=dict)


class ErrorStats:
    """에러 통계 수집 클래스"""

    def __init__(self, max_records: int = 1000, retention_hours: int = 24):
        """
        Args:
            max_records: 최대 보관 에러 기록 수
            retention_hours: 에러 기록 보관 시간 (시간)
        """
        self.max_records = max_records
        self.retention_hours = retention_hours

        # 에러 기록 리스트
        self._error_records: List[ErrorRecord] = []

        # 에러 타입별 카운트
        self._error_counts: Dict[str, int] = defaultdict(int)

        # 에러 코드별 카운트
        self._error_code_counts: Dict[str, int] = defaultdict(int)

        # 최근 에러 (최대 100개)
        self._recent_errors: List[ErrorRecord] = []

    def record_error(
        self,
        error: Exception,
        error_type: str = None,
        error_code: str = None,
        details: Dict = None,
    ) -> None:
        """
        에러 기록

        Args:
            error: 발생한 예외
            error_type: 에러 타입 (WebSocketError인 경우 자동 추출)
            error_code: 에러 코드 (WebSocketError인 경우 자동 추출)
            details: 추가 상세 정보
        """
        now = datetime.now()

        # WebSocketError인 경우 자동 추출
        if isinstance(error, WebSocketError):
            error_type = error_type or error.__class__.__name__
            error_code = error_code or error.error_code
            details = details or error.details
            message = str(error)
        else:
            error_type = error_type or error.__class__.__name__
            error_code = error_code or "UNKNOWN"
            details = details or {}
            message = str(error)

        # 에러 기록 생성
        record = ErrorRecord(
            timestamp=now,
            error_type=error_type,
            error_code=error_code,
            message=message,
            details=details,
        )

        # 기록 추가
        self._error_records.append(record)
        self._error_counts[error_type] += 1
        self._error_code_counts[error_code] += 1

        # 최근 에러에 추가
        self._recent_errors.append(record)
        if len(self._recent_errors) > 100:
            self._recent_errors.pop(0)

        # 오래된 기록 정리
        self._cleanup_old_records()

        # 최대 기록 수 초과 시 정리
        if len(self._error_records) > self.max_records:
            self._error_records = self._error_records[-self.max_records:]

        logger.debug(
            f"Error recorded: type={error_type}, code={error_code}, "
            f"total_count={self._error_counts[error_type]}"
        )

    def _cleanup_old_records(self) -> None:
        """오래된 에러 기록 정리"""
        cutoff_time = datetime.now() - timedelta(hours=self.retention_hours)
        self._error_records = [r for r in self._error_records if r.timestamp >= cutoff_time]

    def get_error_counts(self) -> Dict[str, int]:
        """에러 타입별 카운트 반환"""
        return dict(self._error_counts)

    def get_error_code_counts(self) -> Dict[str, int]:
        """에러 코드별 카운트 반환"""
        return dict(self._error_code_counts)

    def get_recent_errors(self, limit: int = 10) -> List[ErrorRecord]:
        """최근 에러 기록 반환"""
        return self._recent_errors[-limit:]

    def get_errors_by_type(self, error_type: str) -> List[ErrorRecord]:
        """특정 타입의 에러 기록 반환"""
        return [r for r in self._error_records if r.error_type == error_type]

    def get_errors_in_timeframe(
        self, start_time: datetime, end_time: datetime
    ) -> List[ErrorRecord]:
        """특정 시간대의 에러 기록 반환"""
        return [
            r
            for r in self._error_records
            if start_time <= r.timestamp <= end_time
        ]

    def get_total_error_count(self) -> int:
        """전체 에러 수 반환"""
        return sum(self._error_counts.values())

    def get_error_rate(self, minutes: int = 60) -> float:
        """
        에러 발생률 계산 (에러/분)

        Args:
            minutes: 계산할 시간 범위 (분)

        Returns:
            분당 에러 발생률
        """
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        recent_errors = [
            r for r in self._error_records if r.timestamp >= cutoff_time
        ]
        return len(recent_errors) / minutes if minutes > 0 else 0.0

    def get_statistics(self) -> Dict:
        """전체 통계 반환"""
        return {
            "total_errors": self.get_total_error_count(),
            "error_counts_by_type": self.get_error_counts(),
            "error_counts_by_code": self.get_error_code_counts(),
            "error_rate_per_minute": self.get_error_rate(60),
            "recent_errors_count": len(self._recent_errors),
            "total_records": len(self._error_records),
        }

    def clear(self) -> None:
        """통계 초기화"""
        self._error_records.clear()
        self._error_counts.clear()
        self._error_code_counts.clear()
        self._recent_errors.clear()
        logger.info("Error statistics cleared")


# 싱글톤 인스턴스
_error_stats_instance: Optional[ErrorStats] = None


def get_error_stats() -> ErrorStats:
    """ErrorStats 싱글톤 인스턴스 반환"""
    global _error_stats_instance
    if _error_stats_instance is None:
        _error_stats_instance = ErrorStats()
    return _error_stats_instance
