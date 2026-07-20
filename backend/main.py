# backend/main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import random
import time  # NEW
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

# NEW: separate latency metrics endpoint
@app.get("/latency")
def latency_stats():
    if not latency_samples:
        return {"samples": 0, "avg_ms": 0, "max_ms": 0, "min_ms": 0}
    return {
        "samples": len(latency_samples),
        "avg_ms": round(sum(latency_samples) / len(latency_samples), 2),
        "max_ms": round(max(latency_samples), 2),
        "min_ms": round(min(latency_samples), 2)
    }

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
NUM_CAMERA_FEEDS = 20
event_types = ["PERSON_DETECTED", "DWELL_UPDATE", "ENGAGEMENT", "HANDOFF"]

# NEW: rolling latency samples (capped so memory doesn't grow forever)
latency_samples = []
MAX_LATENCY_SAMPLES = 500

async def broadcast(processed):
    """Send event to all clients and record delivery latency per send."""
    gen_time = processed.pop("_gen_time_monotonic", None)  # NEW: pull internal timing field
    for client in clients[:]:
        try:
            await client.send_json(processed)
            if gen_time is not None:  # NEW
                elapsed_ms = (time.monotonic() - gen_time) * 1000
                latency_samples.append(elapsed_ms)
                if len(latency_samples) > MAX_LATENCY_SAMPLES:
                    latency_samples.pop(0)
        except:
            clients.remove(client)

async def camera_feed_task(cam_id: str):
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
            processed["_gen_time_monotonic"] = time.monotonic()  # NEW: stamp right before send
            await broadcast(processed)

# ---------------- STARTUP ----------------
@app.on_event("startup")
async def startup_event():
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