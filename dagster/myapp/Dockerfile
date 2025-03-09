FROM python:3.12.4-alpine

ENV PYTHONUNBUFFERED 1

RUN apk update && \
    apk add --no-cache busybox-extras bash && \
    pip install --no-cache-dir --upgrade pip

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["dagster", "dev", "--host", "0.0.0.0", "-p", "3000"]
