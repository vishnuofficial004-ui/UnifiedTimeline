from datetime import datetime, timedelta

# In-memory timeline
timeline = []

# Last seen dict for dedup window tracking
last_seen = {}  # key: (person_id, event_type), value: timestamp

# Track the "active" event object per key so repeats can be merged into it
active_events = {}  # key: (person_id, event_type), value: event dict

# Per-event-type dedup windows (seconds)
DEDUP_WINDOWS = {
    "PERSON_DETECTED": 5,
    "DWELL_UPDATE": 2,
    "ENGAGEMENT": 3,
    "HANDOFF": 1
}
DEFAULT_DEDUP_SECONDS = 3

metrics = {
    "total_events_received": 0,
    "events_deduplicated": 0,
    "events_forwarded": 0,
    "events_aggregated": 0
}


def process_event(event, dedup_seconds=None):
    """
    Deduplicate + aggregate events within the dedup window.

    Returns a tuple: (payload, message_type)
      - ("event dict", "NEW")    -> a fresh event, first of its kind in this window
      - ("event dict", "UPDATE") -> an aggregated merge of a duplicate into the active event
      - (None, None)             -> nothing to send (only happens if there's no active
                                     event to merge into, which shouldn't normally occur)
    """
    metrics["total_events_received"] += 1

    event_type = event["event_type"]
    key = (event["person_id"], event_type)
    ts = datetime.fromisoformat(event["timestamp"])

    window = dedup_seconds if dedup_seconds is not None else DEDUP_WINDOWS.get(event_type, DEFAULT_DEDUP_SECONDS)

    if key in last_seen and ts - last_seen[key] < timedelta(seconds=window):
        # Within dedup window -> aggregate instead of dropping
        metrics["events_deduplicated"] += 1
        metrics["events_aggregated"] += 1

        active = active_events.get(key)
        if active:
            incoming_dwell = event.get("details", {}).get("dwell_time", 0)
            active["details"]["dwell_time"] = active["details"].get("dwell_time", 0) + incoming_dwell
            active["details"]["merged_count"] = active["details"].get("merged_count", 1) + 1
            active["timestamp"] = event["timestamp"]
            return active, "UPDATE"
        return None, None

    # New window starts: this event becomes the "active" one for this key
    last_seen[key] = ts
    event["details"]["merged_count"] = 1
    active_events[key] = event

    timeline.append(event)
    timeline.sort(key=lambda x: x["timestamp"])

    metrics["events_forwarded"] += 1
    return event, "NEW"


def get_metrics():
    total = metrics["total_events_received"]
    deduped = metrics["events_deduplicated"]
    reduction_pct = round((deduped / total) * 100, 2) if total > 0 else 0
    return {
        **metrics,
        "reduction_percentage": reduction_pct
    }