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

# NEW: per-event-type merge strategy for aggregation
# "sum"  -> add incoming value to existing (e.g. accumulating dwell time)
# "max"  -> keep the higher of the two (e.g. peak engagement score)
# "none" -> don't merge at all; treat every occurrence as significant and forward it as NEW
MERGE_STRATEGY = {
    "PERSON_DETECTED": "sum",
    "DWELL_UPDATE": "sum",
    "ENGAGEMENT": "max",
    "HANDOFF": "none"
}
DEFAULT_MERGE_STRATEGY = "sum"

metrics = {
    "total_events_received": 0,
    "events_deduplicated": 0,
    "events_forwarded": 0,
    "events_aggregated": 0
}


def _merge_dwell_time(active, event, strategy):
    """NEW: apply the configured merge strategy to dwell_time."""
    incoming = event.get("details", {}).get("dwell_time", 0)
    current = active["details"].get("dwell_time", 0)
    if strategy == "max":
        active["details"]["dwell_time"] = max(current, incoming)
    else:  # "sum" and any unrecognized strategy falls back to sum
        active["details"]["dwell_time"] = current + incoming


def process_event(event, dedup_seconds=None):
    """
    Deduplicate + aggregate events within the dedup window, using a
    per-event-type merge strategy.

    Returns a tuple: (payload, message_type)
      - (event dict, "NEW")    -> fresh event, or a "none"-strategy type that
                                   is never merged (always forwarded as new)
      - (event dict, "UPDATE") -> aggregated merge of a duplicate into the active event
      - (None, None)           -> nothing to send
    """
    metrics["total_events_received"] += 1

    event_type = event["event_type"]
    key = (event["person_id"], event_type)
    ts = datetime.fromisoformat(event["timestamp"])

    window = dedup_seconds if dedup_seconds is not None else DEDUP_WINDOWS.get(event_type, DEFAULT_DEDUP_SECONDS)
    strategy = MERGE_STRATEGY.get(event_type, DEFAULT_MERGE_STRATEGY)  # NEW

    # NEW: "none" strategy means this event type is never aggregated —
    # every occurrence is treated as individually significant (e.g. HANDOFF)
    if strategy == "none":
        last_seen[key] = ts
        event["details"]["merged_count"] = 1
        active_events[key] = event
        timeline.append(event)
        timeline.sort(key=lambda x: x["timestamp"])
        metrics["events_forwarded"] += 1
        return event, "NEW"

    if key in last_seen and ts - last_seen[key] < timedelta(seconds=window):
        metrics["events_deduplicated"] += 1
        metrics["events_aggregated"] += 1

        active = active_events.get(key)
        if active:
            _merge_dwell_time(active, event, strategy)  # CHANGED: use configured strategy
            active["details"]["merged_count"] = active["details"].get("merged_count", 1) + 1
            active["timestamp"] = event["timestamp"]
            return active, "UPDATE"
        return None, None

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