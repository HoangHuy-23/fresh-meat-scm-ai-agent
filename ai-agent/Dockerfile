# Sử dụng một image Python gọn nhẹ
FROM python:3.9-slim

# Thiết lập thư mục làm việc bên trong container
WORKDIR /app

# Sao chép file requirements vào trước để tận dụng cache của Docker
COPY requirements.txt .

# Cài đặt các thư viện
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ code của agent vào
COPY . .

# Expose cổng 5001 để bên ngoài có thể gọi vào
EXPOSE 5001

# Lệnh mặc định khi container chạy (chúng ta sẽ ghi đè nó sau)
CMD ["python", "agent_brain.py"]