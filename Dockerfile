FROM python:3.12-slim

WORKDIR /app

ENV PYTHONPATH=/app/src 

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc pkg-config libmariadb-dev-compat libmariadb-dev \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install  -r requirements.txt

COPY src/ ./src/
COPY config.yaml ./config.yaml

RUN mkdir -p /app/models 

EXPOSE 8000

CMD ["uvicorn", "src.api.api:api", "--host", "0.0.0.0", "--port", "8000"]