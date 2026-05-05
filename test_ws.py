import asyncio
import websockets

async def listen():
    try:
        async with websockets.connect("ws://localhost:8000/ws?apikey=noah-secret-key") as websocket:
            print("[+] Connected to WebSocket via Kong!")
            while True:
                message = await websocket.recv()
                print(f"[+] Received message: {message}")
    except Exception as e:
        print(f"[-] WebSocket error: {e}")

asyncio.run(listen())
