# backend/processor.py
from datetime import datetime, timedelta

# In-memory timeline
timeline = []

# Last seen dict for chatter-free deduplication
last_seen = {}  # key: (person_id, event_type), value: timestamp

# --- NEW: metrics tracking ---
metrics = {
    "total_events_received": 0,
    "events_deduplicated": 0,
    "events_forwarded": 0
}

def process_event(event, dedup_seconds=3):
    """
    Deduplicate and add event to timeline
    """
    metrics["total_events_received"] += 1  # NEW

    key = (event["person_id"], event["event_type"])
    ts = datetime.fromisoformat(event["timestamp"])

    if key in last_seen:
        if ts - last_seen[key] < timedelta(seconds=dedup_seconds):
            # Duplicate / chatter, ignore
            metrics["events_deduplicated"] += 1  # NEW
            return None

    # Update last seen
    last_seen[key] = ts

    # Add to timeline
    timeline.append(event)
    timeline.sort(key=lambda x: x["timestamp"])

    metrics["events_forwarded"] += 1  # NEW
    return event


def get_metrics():
    """NEW: Return current dedup stats with computed reduction %"""
    total = metrics["total_events_received"]
    deduped = metrics["events_deduplicated"]
    reduction_pct = round((deduped / total) * 100, 2) if total > 0 else 0
    return {
        **metrics,
        "reduction_percentage": reduction_pct
    }