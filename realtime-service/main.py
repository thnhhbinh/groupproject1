from fastapi import FastAPI, WebSocket
from typing import List
from pydantic import BaseModel
import json
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

clients: List[WebSocket] = []

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