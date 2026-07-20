# backend/processor.py
from datetime import datetime, timedelta

# In-memory timeline
timeline = []

# Last seen dict for chatter-free deduplication
last_seen = {}  # key: (person_id, event_type), value: timestamp

# --- NEW: per-event-type dedup windows (seconds) ---
DEDUP_WINDOWS = {
    "PERSON_DETECTED": 5,   # low-frequency, stays suppressed longer
    "DWELL_UPDATE": 2,      # high-frequency chatter, short window
    "ENGAGEMENT": 3,
    "HANDOFF": 1            # rare + important, barely suppressed
}
DEFAULT_DEDUP_SECONDS = 3  # fallback for unknown event types

metrics = {
    "total_events_received": 0,
    "events_deduplicated": 0,
    "events_forwarded": 0
}

def process_event(event, dedup_seconds=None):
    """
    Deduplicate and add event to timeline.
    Dedup window is now determined per event_type unless overridden.
    """
    metrics["total_events_received"] += 1

    event_type = event["event_type"]
    key = (event["person_id"], event_type)
    ts = datetime.fromisoformat(event["timestamp"])

    # NEW: resolve window — explicit override > per-type config > default
    window = dedup_seconds if dedup_seconds is not None else DEDUP_WINDOWS.get(event_type, DEFAULT_DEDUP_SECONDS)

    if key in last_seen:
        if ts - last_seen[key] < timedelta(seconds=window):
            metrics["events_deduplicated"] += 1
            return None

    last_seen[key] = ts
    timeline.append(event)
    timeline.sort(key=lambda x: x["timestamp"])

    metrics["events_forwarded"] += 1
    return event


def get_metrics():
    total = metrics["total_events_received"]
    deduped = metrics["events_deduplicated"]
    reduction_pct = round((deduped / total) * 100, 2) if total > 0 else 0
    return {
        **metrics,
        "reduction_percentage": reduction_pct
    }

