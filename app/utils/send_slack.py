import httpx

from app.config.settings import settings


async def send_slack(message: str) -> bool:
    """
    Slack으로 메시지를 비동기로 전송합니다.

    Args:
        message: 전송할 메시지 내용

    Returns:
        전송 성공 여부
    """
    if not settings.slack_webhook_url:
        return False

    payload = {"text": message}

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(settings.slack_webhook_url, json=payload)
            return response.status_code == 200
        except httpx.RequestError:
            return False


def send_slack_sync(message: str) -> bool:
    """
    Slack으로 메시지를 동기로 전송합니다.

    Args:
        message: 전송할 메시지 내용

    Returns:
        전송 성공 여부
    """
    if not settings.slack_webhook_url:
        return False

    payload = {"text": message}

    try:
        response = httpx.post(settings.slack_webhook_url, json=payload)
        return response.status_code == 200
    except httpx.RequestError:
        return False
