import asyncio
import websockets

async def listen():
    try:
        async with websockets.connect("ws://localhost:8000/ws?apikey=noah-secret-key") as ws:
            print("Connected to WS")
            while True:
                msg = await ws.recv()
                print("Received:", msg)
    except Exception as e:
        print("Error:", e)

asyncio.run(listen())
