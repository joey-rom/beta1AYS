# /awsays/data/bid_status_store.py
from __future__ import annotations

import json
import os
import fcntl
from datetime import datetime
from contextlib import contextmanager
from typing import Any, Dict, Optional

# You said you already have a data directory.
# This default matches that.
BID_STATUS_PATH = os.environ.get("AYS_BID_STATUS_PATH", "/awsays/data/bid_status.json")
os.makedirs(os.path.dirname(BID_STATUS_PATH), exist_ok=True)

@contextmanager
def _locked_json(path: str) -> Dict[str, Any]:
    """
    Exclusive-lock the JSON file for the duration of read-modify-write.
    Returns a dict-like object you can edit in-place.
    """
    fd = os.open(path, os.O_CREAT | os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        with os.fdopen(fd, "r+") as f:
            try:
                f.seek(0)
                raw = f.read().strip()
                data = json.loads(raw) if raw else {}
                if not isinstance(data, dict):
                    data = {}
            except Exception:
                data = {}

            yield data

            f.seek(0)
            f.truncate()
            json.dump(data, f, indent=2)
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except Exception:
            pass

def get_bid_status(job_id: str) -> Optional[Dict[str, Any]]:
    """Return the stored record for job_id, or None."""
    if not job_id:
        return None
    if not os.path.exists(BID_STATUS_PATH):
        return None
    with _locked_json(BID_STATUS_PATH) as data:
        rec = data.get(str(job_id))
        return rec if isinstance(rec, dict) else None

def set_bid_status(job_id: str, project_name: str, bid_status: str, updated_by: str) -> Dict[str, Any]:
    """
    Set bid status for a job_id. Returns the stored record.
    Last-write-wins, with updated_by + updated_at.
    """
    job_id = str(job_id).strip()
    if not job_id:
        raise ValueError("job_id is required")

    record = {
        "project_name": (project_name or "").strip(),
        "bid_status": (bid_status or "").strip(),      # e.g. "Bid" / "No Bid" / ""
        "updated_by": (updated_by or "unknown").strip(),
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    with _locked_json(BID_STATUS_PATH) as data:
        data[job_id] = record

    return record

def get_all_bid_status() -> Dict[str, Dict[str, Any]]:
    """Return the full mapping {job_id: record}."""
    if not os.path.exists(BID_STATUS_PATH):
        return {}
    with _locked_json(BID_STATUS_PATH) as data:
        # Copy so callers don't accidentally mutate the live structure
        return {str(k): (v if isinstance(v, dict) else {}) for k, v in data.items()}


