"""
Betty Sentinel integration for little-brother-v4.

BettySentinel manages a background thread that sends signed heartbeat and
service-state telemetry to Betty Sentinel every 60 seconds. State is read
directly from the orchestrator — no HTTP round-trip needed.
"""

import hashlib
import hmac
import json
import logging
import os
import socket
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger("betty")

ROOT = Path(__file__).resolve().parent.parent
SEQ_FILE = ROOT / "data" / "reports" / "betty_seq.json"
STALE_MINUTES = 10
LOOP_INTERVAL = 60


def _ts_utc() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"


def _canonical(payload: dict) -> bytes:
    body = {k: v for k, v in payload.items() if k != "signature"}
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")


class BettyAgent:
    def __init__(self, betty_url: str, agent_id: str, secret_hex: str):
        self._url = betty_url.rstrip("/")
        self._agent_id = agent_id
        self._secret_bytes = bytes.fromhex(secret_hex)
        self._session = requests.Session()
        self._session.headers["Content-Type"] = "application/json"

    def _next_sequence(self) -> int:
        try:
            SEQ_FILE.parent.mkdir(parents=True, exist_ok=True)
            if SEQ_FILE.exists():
                data = json.loads(SEQ_FILE.read_text())
                seq = int(data["seq"]) + 1
            else:
                seq = int(time.time())
            tmp = SEQ_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps({"seq": seq}))
            os.replace(tmp, SEQ_FILE)
            return seq
        except Exception as exc:
            log.warning("seq file error (%s), falling back to time()", exc)
            return int(time.time())

    def _sign(self, payload: dict) -> dict:
        sig = hmac.new(self._secret_bytes, _canonical(payload), hashlib.sha256).hexdigest()
        return {**payload, "signature": sig}

    def send_heartbeat(self) -> bool:
        payload = self._sign({
            "event_type": "agent_heartbeat",
            "schema_version": "1.0",
            "agent_id": self._agent_id,
            "host_id": socket.gethostname(),
            "environment": "production",
            "bridge_version": "1.0.0",
            "ts_utc": _ts_utc(),
            "sequence_number": self._next_sequence(),
            "services_summary": {},
            "system_summary": {},
        })
        return self._post("/ingest/heartbeat", payload)

    def send_service_state(self, last_data_utc: str, status: str, metrics: dict) -> bool:
        payload = self._sign({
            "event_type": "service_state",
            "schema_version": "1.0",
            "agent_id": self._agent_id,
            "service_name": "little-brother",
            "status": status,
            "last_data_utc": last_data_utc,
            "metrics_summary": metrics,
            "ts_utc": _ts_utc(),
            "sequence_number": self._next_sequence(),
        })
        return self._post("/ingest/service-state", payload)

    def _post(self, path: str, payload: dict) -> bool:
        url = self._url + path
        try:
            resp = self._session.post(url, json=payload, timeout=10)
            if resp.status_code == 202:
                log.info("%s → 202", path)
                return True
            log.warning("%s → %s %s", path, resp.status_code, resp.text[:200])
            return False
        except Exception as exc:
            log.warning("%s failed: %s", path, exc)
            return False

    def close(self):
        self._session.close()


def _last_active_window_ts(db_path: str) -> str | None:
    """Return the most recent active_window_events timestamp, or None."""
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=3)
        row = conn.execute("SELECT MAX(timestamp) FROM active_window_events").fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def _collect_state(orchestrator) -> tuple[str, str, dict]:
    """Read health state directly from the running orchestrator."""
    monitors = orchestrator.monitor_map
    total = len(monitors)
    active = sum(1 for m in monitors.values() if m.is_running)
    queue_depth = orchestrator.db.event_queue.qsize() if orchestrator.db else 0
    uptime_seconds = orchestrator.uptime_seconds

    last_ts_raw = _last_active_window_ts(orchestrator.db.db_path) if orchestrator.db else None
    if last_ts_raw:
        last_dt = datetime.fromisoformat(last_ts_raw).replace(tzinfo=timezone.utc)
        last_data_utc = last_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{last_dt.microsecond:06d}Z"
        age_minutes = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60
    else:
        last_data_utc = _ts_utc()
        age_minutes = 0

    if active < total:
        status = "degraded"
    elif age_minutes > STALE_MINUTES:
        status = "stale"
    else:
        status = "ok"

    metrics = {
        "active_monitors": active,
        "total_monitors": total,
        "queue_depth": queue_depth,
        "uptime_seconds": uptime_seconds,
    }
    return last_data_utc, status, metrics


class BettySentinel:
    """Lifecycle wrapper — start/stop the Betty telemetry loop as a daemon thread."""

    def __init__(self):
        self._agent: BettyAgent | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, orchestrator) -> bool:
        cfg = orchestrator.config.get("betty", {})
        if not cfg.get("enabled", False):
            log.info("betty.enabled=false — skipping")
            return False
        secret_hex = cfg.get("secret_hex", "")
        if not secret_hex:
            log.info("betty.secret_hex is empty — skipping")
            return False

        self._agent = BettyAgent(
            betty_url=cfg["url"],
            agent_id=cfg["agent_id"],
            secret_hex=secret_hex,
        )
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop,
            args=(orchestrator,),
            name="betty-sentinel",
            daemon=True,
        )
        self._thread.start()
        log.info("Betty Sentinel started (agent_id=%s)", cfg["agent_id"])
        return True

    def stop(self):
        if self._thread is None:
            return
        self._stop.set()
        self._thread.join(timeout=5)
        if self._agent:
            self._agent.close()
        log.info("Betty Sentinel stopped")

    def _loop(self, orchestrator):
        while not self._stop.is_set():
            try:
                last_data_utc, status, metrics = _collect_state(orchestrator)
                self._agent.send_heartbeat()
                self._agent.send_service_state(last_data_utc, status, metrics)
            except Exception as exc:
                log.warning("Betty loop error: %s", exc)
            self._stop.wait(timeout=LOOP_INTERVAL)
