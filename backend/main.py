# backend/main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import random
import time
from events import normalize_event
from processor import process_event, timeline, get_metrics

app = FastAPI()

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

# NEW: trigger a burst of simultaneous events across all cameras
@app.post("/simulate/burst")
async def simulate_burst(events_per_camera: int = 5):
    """
    Fire events_per_camera events from EVERY camera feed all at once,
    to simulate peak traffic instead of steady trickle load.
    Returns latency stats measured only during this burst.
    """
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

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

NUM_CAMERA_FEEDS = 20
event_types = ["PERSON_DETECTED", "DWELL_UPDATE", "ENGAGEMENT", "HANDOFF"]

latency_samples = []
MAX_LATENCY_SAMPLES = 2000  # NEW: raised cap since bursts generate many samples fast

async def send_to_client(client: WebSocket, processed: dict, gen_time):
    try:
        await client.send_json(processed)
        if gen_time is not None:
            elapsed_ms = (time.monotonic() - gen_time) * 1000
            latency_samples.append(elapsed_ms)
            if len(latency_samples) > MAX_LATENCY_SAMPLES:
                latency_samples.pop(0)
    except Exception:
        if client in clients:
            clients.remove(client)

async def broadcast(processed):
    gen_time = processed.pop("_gen_time_monotonic", None)
    if not clients:
        return
    await asyncio.gather(
        *(send_to_client(client, processed, gen_time) for client in clients[:]),
        return_exceptions=True
    )

async def fire_single_event(cam_id: str):
    """NEW: extracted single-event generation so both the steady loop
    and the burst simulator can reuse the exact same event path."""
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
        processed["_gen_time_monotonic"] = time.monotonic()
        await broadcast(processed)

async def camera_feed_task(cam_id: str):
    while True:
        await asyncio.sleep(random.uniform(0.5, 2))
        await fire_single_event(cam_id)  # CHANGED: reuse shared function

@app.on_event("startup")
async def startup_event():
    for i in range(1, NUM_CAMERA_FEEDS + 1):
        cam_id = f"CAM_{i:02d}"
        asyncio.create_task(camera_feed_task(cam_id))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=True
    )