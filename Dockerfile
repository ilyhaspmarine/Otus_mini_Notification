FROM python:3.11-slim-bookworm

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

RUN groupadd -r -g 1000 basicgroup && \
    useradd -r -u 1000 -g basicgroup basicuser

WORKDIR /notification-app

# Установка librdkafka
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /notification-app

USER basicuser

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]