# Sử dụng Python phiên bản nhẹ (slim) để giảm dung lượng image
FROM python:3.9-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Copy file requirements vào trước để tận dụng cache của Docker
COPY requirements.txt .

# Cài đặt các thư viện (FastAPI, Pika, MySQL Connector)
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn của folder order_api vào container
COPY . .

# Mở cổng 5000 (cổng nội bộ của ứng dụng)
EXPOSE 5000

# Lệnh khởi chạy API sử dụng Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]