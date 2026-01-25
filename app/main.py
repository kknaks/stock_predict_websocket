"""
Stock Predict WebSocket Server

Kafka에서 AI 예측 결과를 수신하여 WebSocket으로 클라이언트에 전달
Kafka에서 KIS 웹소켓 명령을 수신하여 웹소켓 연결 관리
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Set, Optional

import websockets
from websockets.server import WebSocketServerProtocol as WebSocketServerProtocol

from app.config.settings import settings
from app.kafka import get_predict_consumer, get_websocket_consumer
from app.handler.prediction_handler import get_prediction_handler
from app.handler.websocket_handler import get_websocket_handler

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 연결된 웹소켓 클라이언트들
connected_clients: Set[WebSocketServerProtocol] = set()


async def broadcast(message: str) -> None:
    """모든 연결된 클라이언트에게 메시지 전송"""
    if not connected_clients:
        return

    disconnected = set()
    for client in connected_clients:
        try:
            await client.send(message)
        except websockets.ConnectionClosed:
            disconnected.add(client)

    # 끊어진 연결 제거
    connected_clients.difference_update(disconnected)


async def handle_prediction_broadcast(prediction_msg) -> None:
    """예측 결과를 웹소켓으로 브로드캐스트"""
    # datetime 객체를 JSON 직렬화 가능한 형태로 변환
    predictions_json = []
    for p in prediction_msg.predictions:
        pred_dict = p.model_dump()
        # datetime 객체를 ISO 형식 문자열로 변환
        if isinstance(pred_dict.get('timestamp'), datetime):
            pred_dict['timestamp'] = pred_dict['timestamp'].isoformat()
        predictions_json.append(pred_dict)
    
    message = json.dumps({
        "type": "prediction",
        "timestamp": prediction_msg.timestamp.isoformat(),
        "total_count": prediction_msg.total_count,
        "predictions": predictions_json,
    }, ensure_ascii=False)

    await broadcast(message)
    logger.info(f"Broadcast prediction to {len(connected_clients)} clients")


async def websocket_handler(websocket: WebSocketServerProtocol) -> None:
    """웹소켓 연결 핸들러"""
    connected_clients.add(websocket)
    client_info = f"{websocket.remote_address}"
    logger.info(f"Client connected: {client_info} (total: {len(connected_clients)})")

    try:
        # 연결 시 최신 예측 결과 전송
        handler = get_prediction_handler()
        if handler.latest_predictions:
            # datetime 객체를 JSON 직렬화 가능한 형태로 변환
            predictions_json = []
            for p in handler.latest_predictions.predictions:
                pred_dict = p.model_dump()
                # datetime 객체를 ISO 형식 문자열로 변환
                if isinstance(pred_dict.get('timestamp'), datetime):
                    pred_dict['timestamp'] = pred_dict['timestamp'].isoformat()
                predictions_json.append(pred_dict)
            
            await websocket.send(json.dumps({
                "type": "initial",
                "timestamp": handler.latest_predictions.timestamp.isoformat(),
                "total_count": handler.latest_predictions.total_count,
                "predictions": predictions_json,
            }, ensure_ascii=False))

        # 클라이언트 메시지 대기 (연결 유지)
        async for message in websocket:
            # 클라이언트로부터 메시지 수신 시 처리 (필요 시 확장)
            logger.debug(f"Received from {client_info}: {message}")

    except websockets.ConnectionClosed:
        logger.info(f"Client disconnected: {client_info}")
    finally:
        connected_clients.discard(websocket)
        logger.info(f"Client removed: {client_info} (total: {len(connected_clients)})")


async def main():
    """메인 실행 함수"""
    logger.info("Starting WebSocket server...")

    # 예측 결과 Kafka consumer 설정
    prediction_consumer = get_predict_consumer()
    prediction_handler = get_prediction_handler()

    # 예측 결과 핸들러 등록
    prediction_consumer.add_handler(prediction_handler.handle_prediction)
    prediction_consumer.add_handler(handle_prediction_broadcast)

    # 웹소켓 Kafka consumer 설정
    websocket_consumer = get_websocket_consumer()
    websocket_handler = get_websocket_handler()

    # 웹소켓 핸들러 등록
    websocket_consumer.add_handler(websocket_handler.handle_command)

    # 예측 결과 Kafka consumer 시작
    prediction_connected = await prediction_consumer.start()
    prediction_task: Optional[asyncio.Task] = None
    if prediction_connected:
        logger.info("Prediction Kafka connection established")
        prediction_task = asyncio.create_task(prediction_consumer.consume())
    else:
        logger.warning("Failed to connect to Prediction Kafka")

    # 웹소켓 Kafka consumer 시작
    websocket_connected = await websocket_consumer.start()
    websocket_task: Optional[asyncio.Task] = None
    if websocket_connected:
        logger.info("WebSocket Kafka connection established")
        websocket_task = asyncio.create_task(websocket_consumer.consume())
    else:
        logger.warning("Failed to connect to WebSocket Kafka")

    # WebSocket 서버 시작
    async with websockets.serve(
        websocket_handler,
        settings.host,
        settings.port,
    ):
        logger.info(f"WebSocket server running on ws://{settings.host}:{settings.port}")

        # 종료 시그널 대기
        try:
            await asyncio.Future()  # 무한 대기
        except asyncio.CancelledError:
            pass

    # 정리
    logger.info("Shutting down server...")

    # 예측 결과 Consumer 정리
    if prediction_task:
        prediction_task.cancel()
        try:
            await prediction_task
        except asyncio.CancelledError:
            pass
    await prediction_consumer.stop()

    # 웹소켓 Consumer 정리
    if websocket_task:
        websocket_task.cancel()
        try:
            await websocket_task
        except asyncio.CancelledError:
            pass
    await websocket_consumer.stop()

    logger.info("Server shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
