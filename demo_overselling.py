import concurrent.futures
import requests
import redis
import time

# 1. Kết nối trực tiếp vào Redis để nạp tồn kho = 1
print("[*] Đang ép tồn kho của sản phẩm 101 về đúng 1 để demo...")
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
r.set("product:101:stock", 1)
print(f"[V] Tồn kho hiện tại: {r.get('product:101:stock')}")

# 2. Gửi 5 request mua hàng đồng thời
url = "http://localhost:8000/orders"
headers = {"Content-Type": "application/json", "apikey": "noah-secret-key"}
payload = {"user_id": 1, "product_id": 101, "quantity": 1}

def send_request(req_id):
    response = requests.post(url, headers=headers, json=payload)
    return response.status_code, response.json()

print("\n[*] Đang gửi 5 request mua hàng cùng một lúc...")
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(send_request, i) for i in range(5)]
    for future in concurrent.futures.as_completed(futures):
        print(future.result())

# 3. Kiểm tra lại tồn kho
print(f"\n[*] Tồn kho cuối cùng sau khi bị tranh mua: {r.get('product:101:stock')}")
