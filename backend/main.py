from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import random

from events import normalize_event
from processor import process_event, timeline

app = FastAPI()

# ---------------- BASIC API ----------------
@app.get("/")
def root():
    return {"status": "Backend running"}

# ---------------- CORS ----------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# ---------------- WEBSOCKET CLIENTS ----------------
clients: list[WebSocket] = []

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)
    try:
        while True:
            await asyncio.sleep(1)  # keep connection alive
    except WebSocketDisconnect:
        clients.remove(websocket)

# ---------------- MOCK VISION AI EVENTS ----------------
async def generate_mock_events():
    cameras = ["CAM_01", "CAM_02", "CAM_03"]
    event_types = ["PERSON_DETECTED", "DWELL_UPDATE", "ENGAGEMENT", "HANDOFF"]

    while True:
        await asyncio.sleep(random.uniform(0.5, 2))

        raw_event = {
            "cam": random.choice(cameras),
            "time": datetime.now().isoformat(),
            "type": random.choice(event_types),
            "track_id": random.randint(100, 105),
            "details": {"dwell_time": random.randint(1, 10)}
        }

        event = normalize_event(raw_event)
        processed = process_event(event)

        if processed:
            for client in clients[:]:
                try:
                    await client.send_json(processed)  # ✅ SINGLE EVENT
                except:
                    clients.remove(client)

# ---------------- STARTUP ----------------
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(generate_mock_events())

# ---------------- RUN ----------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=True
    )
