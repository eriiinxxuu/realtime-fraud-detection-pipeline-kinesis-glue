FROM python:3.10.16-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

    
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    gcc g++ build-essential \
    python3-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY producer/requirements.txt /app/requirements.txt
COPY producer/main.py /app/main.py

RUN pip install --no-cache-dir -r /app/requirements.txt

ENTRYPOINT ["python3", "main.py"]