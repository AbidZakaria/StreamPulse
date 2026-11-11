FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 TZ=UTC
WORKDIR /app

RUN pip install --no-cache-dir "fastapi==0.120.4" "uvicorn[standard]==0.38.0" \
    "psycopg[binary]==3.2.12" "psycopg-pool==3.2.7" "pyyaml==6.0.2"

COPY config ./config
COPY api ./api

RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --retries=6 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
