# Build context: month_end_trader/ (see docker-compose.yml). .dockerignore keeps secrets/state out of the image.
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY met/ /app/met/
COPY tests/fixtures/ /app/tests/fixtures/
CMD ["python", "-m", "met.cli", "run", "--config", "/app/config.yaml"]
