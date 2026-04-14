# backend/processor.py
from datetime import datetime, timedelta

# In-memory timeline
timeline = []

# Last seen dict for chatter-free deduplication
last_seen = {}  # key: (person_id, event_type), value: timestamp

def process_event(event, dedup_seconds=3):
    """
    Deduplicate and add event to timeline
    """
    key = (event["person_id"], event["event_type"])
    ts = datetime.fromisoformat(event["timestamp"])

    if key in last_seen:
        if ts - last_seen[key] < timedelta(seconds=dedup_seconds):
            # Duplicate / chatter, ignore
            return None

    # Update last seen
    last_seen[key] = ts

    # Add to timeline
    timeline.append(event)
    # Sort timeline by timestamp
    timeline.sort(key=lambda x: x["timestamp"])
    return event
