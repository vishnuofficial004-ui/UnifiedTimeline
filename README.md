# Unified Timeline

Backend for collecting events from multiple store cameras and showing them on one live dashboard. Built this to figure out how a real-time multi-camera event pipeline would actually work — dedup, aggregation, WebSocket streaming, all of it.

Note: the camera feeds are simulated. There's a script that generates fake detection/dwell/engagement/handoff events to mimic what 20 cameras would be sending, since I didn't have access to real camera hardware/footage. The focus here was the backend pipeline, not computer vision.

## What's in here

- Events come in from (simulated) cameras, get normalized into one schema
- Duplicate events within a short time window get merged instead of just spamming the dashboard — e.g. if someone's dwell time updates 5 times in 2 seconds, that becomes one event with the total
- Everything streams out over WebSocket to whoever's connected, live
- Added some basic metrics so I could actually measure how well the dedup works instead of guessing
- Also measured end-to-end latency (client acks back when it gets an event) to see how fast delivery actually is

## Folder structure

```
backend/
  main.py        -> FastAPI app, websocket endpoint, the fake camera feed generators
  events.py      -> turns raw event into a normalized schema + assigns severity
  processor.py   -> dedup + aggregation logic, metrics
frontend/
  index.html     -> the live dashboard
  test.html      -> quick page I used to just watch raw messages come in while debugging
```

## Running it

```
cd backend
pip install "uvicorn[standard]" fastapi
uvicorn main:app --reload
```

Then just open `frontend/index.html` in a browser, it'll connect and you'll see events start showing up.

## Endpoints

- `GET /` - health check
- `WS /ws` - the actual event stream
- `GET /metrics` - shows how many events got deduped/aggregated, and the reduction %
- `GET /latency` - server side latency (how long send_json takes)
- `GET /latency/e2e` - real round trip latency, client acks every event it receives
- `POST /simulate/burst?events_per_camera=N` - fires a burst of events across all cameras at once, used this to stress test

## Numbers I actually measured

Ran this locally a bunch of times:

- Dedup/aggregation cut down forwarded events by ~50% (got 49.5-49.7% across different runs, over 5000+ events)
- End to end latency averaged under 2ms, maxed out at 16ms across 2000 samples (including a burst test)

Worth mentioning — this was all tested on localhost, client and server on the same machine. So no real network involved, which is obviously why the latency numbers are this low. Would need to test across actual devices/network to get numbers that mean something for a real deployment.

## What's not done / what I'd fix next

Being honest about where this stands:

- State is all in-memory right now, resets if the server restarts, won't work if you run multiple instances of this
- No auth on anything, endpoints are wide open (fine for local testing, not fine for anything real)
- Severity field exists on events but isn't actually used for routing yet — every client gets every event regardless of severity
- No logic linking a person across different cameras, each camera's person_id is independent
- Dedup windows and camera count are just hardcoded constants, should be config driven
- Latency testing needs to happen over a real network, not just localhost

## If I keep working on this

- Move shared state to Redis so it can scale across instances
- Build out the severity-based routing
- Add some kind of re-identification logic to link people across cameras
- Add auth before this touches anything real
- Test latency properly across a network
