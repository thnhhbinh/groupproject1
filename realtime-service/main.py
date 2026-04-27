from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import List
from pydantic import BaseModel

app = FastAPI()

clients: List[WebSocket] = []


# ======================
# WEBSOCKET
# ======================
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.append(ws)
    print("[INFO] Client connected")

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.remove(ws)
        print("[INFO] Client disconnected")


async def broadcast(message: str):
    for client in clients:
        await client.send_text(message)


# ======================
# EVENT MODEL
# ======================
class Event(BaseModel):
    event: str
    data: str


# ======================
# EMIT EVENT
# ======================
@app.post("/emit")
async def emit_event(body: Event):
    await broadcast(body.json())
    return {"status": "sent"}


# ======================
# ORDERS API
# ======================
@app.get("/orders")
def get_orders():
    return {"orders": ["order1", "order2"]}


# ======================
# REPORT API
# ======================
@app.get("/report")
def get_report():
    return {"report": "ok"}