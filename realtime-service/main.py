from fastapi import FastAPI, WebSocket
from typing import List
from pydantic import BaseModel
import json
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import redis.asyncio as redis

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

clients: List[WebSocket] = []
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(redis_listener())

async def redis_listener():
    pubsub = redis_client.pubsub()
    await pubsub.subscribe('new_orders')
    try:
        async for message in pubsub.listen():
            if message['type'] == 'message':
                await broadcast(message['data'])
    except Exception as e:
        print(f"Redis listener error: {e}")

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.append(ws)
    try:
        while True:
            await ws.receive_text()
    except:
        clients.remove(ws)

async def broadcast(message: str):
    for client in clients:
        await client.send_text(message)

# ✅ MODEL CHUẨN
class Order(BaseModel):
    user_id: int
    product_id: int
    quantity: int

# 🔥 API CHUẨN ĐỀ
@app.post("/orders")
async def create_order(order: Order):
    data = {
        "event": "new_order",
        "data": f"User {order.user_id} ordered product {order.product_id}"
    }
    await broadcast(json.dumps(data))
    return {"status": "order emitted"}