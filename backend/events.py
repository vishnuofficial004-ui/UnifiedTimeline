
import uuid
from datetime import datetime

def normalize_event(raw_event):
    """
    Convert raw event into standard schema
    """
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.fromisoformat(raw_event["time"]).isoformat(),
        "camera_id": raw_event["cam"],
        "person_id": str(raw_event.get("track_id", "unknown")),
        "event_type": raw_event["type"],
        "details": raw_event.get("details", {})
    }
