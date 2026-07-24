from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import random
import time
import json  # NEW: needed to parse incoming ack messages from clients
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

# NEW: true end-to-end latency stats, based on client acknowledgments
@app.get("/latency/e2e")
def e2e_latency_stats():
    if not e2e_latency_samples:
        return {"samples": 0, "avg_ms": 0, "max_ms": 0, "min_ms": 0}
    return {
        "samples": len(e2e_latency_samples),
        "avg_ms": round(sum(e2e_latency_samples) / len(e2e_latency_samples), 2),
        "max_ms": round(max(e2e_latency_samples), 2),
        "min_ms": round(min(e2e_latency_samples), 2)
    }

@app.post("/simulate/burst")
async def simulate_burst(events_per_camera: int = 5):
    start_index = len(latency_samples)
    tasks = []
    for i in range(1, NUM_CAMERA_FEEDS + 1):
        cam_id = f"CAM_{i:02d}"
        for _ in range(events_per_camera):
            tasks.append(fire_single_event(cam_id))
    await asyncio.gather(*tasks, return_exceptions=True)

    burst_samples = latency_samples[start_index:]
    if not burst_samples:
        return {"events_fired": len(tasks), "burst_samples": 0, "avg_ms": 0, "max_ms": 0}
    return {
        "events_fired": len(tasks),
        "burst_samples": len(burst_samples),
        "avg_ms": round(sum(burst_samples) / len(burst_samples), 2),
        "max_ms": round(max(burst_samples), 2),
        "min_ms": round(min(burst_samples), 2)
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

# NEW: track send time per event_id so we can compute round-trip time when an ack arrives
pending_acks = {}  # key: event_id, value: time.monotonic() at send

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)
    try:
        while True:
            # CHANGED: instead of just sleeping, actually listen for ack messages from the client
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "ack":
                    event_id = msg.get("event_id")
                    sent_at = pending_acks.pop(event_id, None)  # NEW
                    if sent_at is not None:
                        elapsed_ms = (time.monotonic() - sent_at) * 1000
                        e2e_latency_samples.append(elapsed_ms)
                        if len(e2e_latency_samples) > MAX_LATENCY_SAMPLES:
                            e2e_latency_samples.pop(0)
            except (json.JSONDecodeError, TypeError):
                pass  # NEW: ignore malformed ack messages rather than crashing the connection
    except WebSocketDisconnect:
        clients.remove(websocket)

# ---------------- MOCK VISION AI EVENTS ----------------
NUM_CAMERA_FEEDS = 20
event_types = ["PERSON_DETECTED", "DWELL_UPDATE", "ENGAGEMENT", "HANDOFF"]

latency_samples = []
e2e_latency_samples = []  # NEW: round-trip (client-acked) latency samples
MAX_LATENCY_SAMPLES = 2000

async def send_to_client(client: WebSocket, payload: dict, gen_time):
    try:
        await client.send_json(payload)
        if gen_time is not None:
            elapsed_ms = (time.monotonic() - gen_time) * 1000
            latency_samples.append(elapsed_ms)
            if len(latency_samples) > MAX_LATENCY_SAMPLES:
                latency_samples.pop(0)
    except Exception:
        if client in clients:
            clients.remove(client)

async def broadcast(payload):
    gen_time = payload.pop("_gen_time_monotonic", None)
    if not clients:
        return
    # NEW: record send time for this event_id so an incoming ack can be matched to it
    if payload.get("event_id"):
        pending_acks[payload["event_id"]] = time.monotonic()
    await asyncio.gather(
        *(send_to_client(client, payload, gen_time) for client in clients[:]),
        return_exceptions=True
    )

async def fire_single_event(cam_id: str):
    raw_event = {
        "cam": cam_id,
        "time": datetime.now().isoformat(),
        "type": random.choice(event_types),
        "track_id": random.randint(100, 105),
        "details": {"dwell_time": random.randint(1, 10)}
    }
    event = normalize_event(raw_event)
    processed, msg_type = process_event(event)
    if processed:
        payload = {**processed, "message_type": msg_type}
        payload["_gen_time_monotonic"] = time.monotonic()
        await broadcast(payload)

async def camera_feed_task(cam_id: str):
    while True:
        await asyncio.sleep(random.uniform(0.5, 2))
        await fire_single_event(cam_id)

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