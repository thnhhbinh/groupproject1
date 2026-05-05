import concurrent.futures
import requests
import redis

# Thiết lập sẵn tồn kho = 1 ngầm để video demo chạy hoàn hảo
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
r.set("product:101:stock", 1)

url = "http://localhost:8000/orders"
headers = {"Content-Type": "application/json", "apikey": "noah-secret-key"}
payload = {"user_id": 1, "product_id": 101, "quantity": 1}

def send_request():
    response = requests.post(url, headers=headers, json=payload)
    return response.status_code, response.json()

print("[*] Starting concurrent stress test (5 requests)...")
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(send_request) for _ in range(5)]
    for future in concurrent.futures.as_completed(futures):
        print(future.result())

print(f"[*] Final stock in Redis: {r.get('product:101:stock')}")
