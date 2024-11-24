FROM python:3.12.4-alpine

ENV PYTHONUNBUFFERED 1
WORKDIR /app

COPY requirements.txt .
RUN apk update && \
    apk add --no-cache bash postgresql-dev && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .