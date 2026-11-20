FROM python:3.12-slim
WORKDIR /app
COPY month_end_trader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY month_end_trader/ /app/
# In the container, set ib.host to "ib-gateway" and ib.port to 4004 (paper) / 4003 (live) in config.yaml
CMD ["python", "-m", "met.cli", "run", "--config", "/app/config.yaml"]
