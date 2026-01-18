"""
KIS WebSocket Custom Exceptions
"""


class WebSocketError(Exception):
    """웹소켓 기본 에러"""

    def __init__(self, message: str, error_code: str = None, details: dict = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}


class WebSocketConnectionError(WebSocketError):
    """웹소켓 연결 에러"""

    def __init__(self, message: str, error_code: str = "CONNECTION_ERROR", details: dict = None):
        super().__init__(message, error_code, details)


class WebSocketAuthError(WebSocketError):
    """웹소켓 인증 에러 (토큰 만료, 무효 등)"""

    def __init__(self, message: str, error_code: str = "AUTH_ERROR", details: dict = None):
        super().__init__(message, error_code, details)


class WebSocketMessageError(WebSocketError):
    """웹소켓 메시지 처리 에러 (파싱, 검증 등)"""

    def __init__(self, message: str, error_code: str = "MESSAGE_ERROR", details: dict = None):
        super().__init__(message, error_code, details)


class WebSocketSubscriptionError(WebSocketError):
    """웹소켓 구독 에러"""

    def __init__(self, message: str, error_code: str = "SUBSCRIPTION_ERROR", details: dict = None):
        super().__init__(message, error_code, details)


class WebSocketKafkaError(WebSocketError):
    """Kafka 전송 에러"""

    def __init__(self, message: str, error_code: str = "KAFKA_ERROR", details: dict = None):
        super().__init__(message, error_code, details)


class WebSocketTimeoutError(WebSocketError):
    """웹소켓 타임아웃 에러"""

    def __init__(self, message: str, error_code: str = "TIMEOUT_ERROR", details: dict = None):
        super().__init__(message, error_code, details)
