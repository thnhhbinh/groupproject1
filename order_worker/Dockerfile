FROM python:3.9-slim

# Cài đặt các thư viện hệ thống cần thiết cho thư viện psycopg2 (kết nối Postgres)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

# Cài đặt các thư viện (Pika, MySQL Connector, Psycopg2)
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Lệnh khởi chạy Worker (chạy như một script Python bình thường)
CMD ["python", "worker.py"]