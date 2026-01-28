FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Poetry 설치
RUN pip install --no-cache-dir poetry==1.8.3

# Poetry 설정 (가상환경 비활성화)
RUN poetry config virtualenvs.create false

# 의존성 파일 복사
COPY pyproject.toml poetry.lock* ./

# Lock 파일 생성 및 의존성 설치
RUN poetry lock --no-update || poetry lock
RUN poetry install --only main --no-root --no-interaction --no-ansi

# 애플리케이션 코드 복사
COPY app/ ./app/

# 환경 변수
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 웹소켓 포트
EXPOSE 8001

CMD ["python", "-m", "app.main"]
