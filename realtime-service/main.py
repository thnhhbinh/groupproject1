from fastapi import FastAPI, WebSocket
from typing import List
from pydantic import BaseModel

app = FastAPI(root_path="/realtime")

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
    

class Event(BaseModel):
    event: str
    data: str

@app.post("/emit")
async def emit_event(body: Event):
    await broadcast(body.json())
    return {"status": "sent"}        