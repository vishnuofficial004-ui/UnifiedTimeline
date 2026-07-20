# backend/main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import random
from events import normalize_event
from processor import process_event, timeline, get_metrics

app = FastAPI()

# ---------------- BASIC API ----------------
@app.get("/")
def root():
    return {"status": "Backend running"}

@app.get("/metrics")
def metrics():
    return get_metrics()

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
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        clients.remove(websocket)

# ---------------- MOCK VISION AI EVENTS ----------------
# NEW: configurable feed count
NUM_CAMERA_FEEDS = 20  # within your claimed 15-25 range

event_types = ["PERSON_DETECTED", "DWELL_UPDATE", "ENGAGEMENT", "HANDOFF"]

async def broadcast(processed):
    """NEW: extracted broadcast so each camera task can reuse it"""
    for client in clients[:]:
        try:
            await client.send_json(processed)
        except:
            clients.remove(client)

async def camera_feed_task(cam_id: str):
    """
    NEW: Each camera now runs as its own independent async task,
    generating events on its own timer — this is what makes the
    feeds genuinely concurrent instead of one shared loop.
    """
    while True:
        await asyncio.sleep(random.uniform(0.5, 2))
        raw_event = {
            "cam": cam_id,
            "time": datetime.now().isoformat(),
            "type": random.choice(event_types),
            "track_id": random.randint(100, 105),
            "details": {"dwell_time": random.randint(1, 10)}
        }
        event = normalize_event(raw_event)
        processed = process_event(event)
        if processed:
            await broadcast(processed)

# ---------------- STARTUP ----------------
@app.on_event("startup")
async def startup_event():
    # NEW: spin up one independent task per camera feed
    for i in range(1, NUM_CAMERA_FEEDS + 1):
        cam_id = f"CAM_{i:02d}"
        asyncio.create_task(camera_feed_task(cam_id))

# ---------------- RUN ----------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=True
    )