FROM python:3.10-slim

LABEL maintainer="Crypto Advisor Team"
LABEL description="Binance WebSocket → Kafka Producer"

WORKDIR /app

# System deps for kafka-python + websocket
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy whole app to /app
COPY . /app

# Prometheus metrics port 
# this container will use port 8000 
EXPOSE 8000

# Non-root user for security
RUN adduser --disabled-password --gecos "" appuser
USER appuser


CMD ["python", "-u", "producer.py"]
