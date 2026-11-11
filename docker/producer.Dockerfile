FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 TZ=UTC
WORKDIR /app

RUN pip install --no-cache-dir "confluent-kafka==2.12.2" "psycopg[binary]==3.2.12" "pyyaml==6.0.2"

COPY config ./config
COPY producer ./producer

RUN useradd --create-home --uid 10001 app
USER app

CMD ["python", "-m", "producer.simulator"]
