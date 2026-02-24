import os
import sqlite3
import threading
from datetime import datetime, timedelta

from flask import Flask, jsonify, request, send_from_directory
from werkzeug.serving import make_server


DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "little_brother.db")


def get_db():
    """Open a read-only connection to the database with timeout."""
    path = os.path.abspath(DB_PATH)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    return conn


def hours_ago(hours):
    """Return ISO timestamp for N hours ago."""
    dt = datetime.utcnow() - timedelta(hours=hours)
    return dt.isoformat()


# --- Flask app ---

app = Flask(__name__, static_folder="static")


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/summary")
def api_summary():
    conn = get_db()
    try:
        result = {}
        for table in ["active_window_events", "mouse_click_events", "browser_tab_events", "file_events"]:
            row = conn.execute(
                f"SELECT COUNT(*) as cnt, MIN(timestamp) as first_ts, MAX(timestamp) as last_ts FROM {table}"
            ).fetchone()
            result[table] = {
                "count": row["cnt"],
                "first": row["first_ts"],
                "last": row["last_ts"],
            }

        db_path = os.path.abspath(DB_PATH)
        result["db_size_kb"] = round(os.path.getsize(db_path) / 1024, 1) if os.path.exists(db_path) else 0
        return jsonify(result)
    finally:
        conn.close()


@app.route("/api/active-windows")
def api_active_windows():
    hours = float(request.args.get("hours", 24))
    since = hours_ago(hours)
    conn = get_db()
    try:
        # Top apps by switch count
        top_apps = conn.execute("""
            SELECT process_name, COUNT(*) as switches,
                   MIN(timestamp) as first_seen, MAX(timestamp) as last_seen
            FROM active_window_events
            WHERE timestamp >= ? AND process_name != ''
            GROUP BY process_name
            ORDER BY switches DESC
            LIMIT 20
        """, (since,)).fetchall()

        # Recent window switches
        recent = conn.execute("""
            SELECT timestamp, window_title, process_name, hwnd
            FROM active_window_events
            WHERE timestamp >= ?
            ORDER BY id DESC
            LIMIT 50
        """, (since,)).fetchall()

        return jsonify({
            "top_apps": [dict(r) for r in top_apps],
            "recent": [dict(r) for r in recent],
        })
    finally:
        conn.close()


@app.route("/api/mouse-clicks")
def api_mouse_clicks():
    hours = float(request.args.get("hours", 24))
    since = hours_ago(hours)
    conn = get_db()
    try:
        # Clicks by button
        by_button = conn.execute("""
            SELECT button, COUNT(*) as cnt
            FROM mouse_click_events
            WHERE timestamp >= ?
            GROUP BY button
            ORDER BY cnt DESC
        """, (since,)).fetchall()

        # Clicks by window
        by_window = conn.execute("""
            SELECT window_title, COUNT(*) as cnt
            FROM mouse_click_events
            WHERE timestamp >= ? AND window_title != ''
            GROUP BY window_title
            ORDER BY cnt DESC
            LIMIT 15
        """, (since,)).fetchall()

        # Click positions for heatmap
        positions = conn.execute("""
            SELECT x, y FROM mouse_click_events
            WHERE timestamp >= ?
        """, (since,)).fetchall()

        return jsonify({
            "by_button": [dict(r) for r in by_button],
            "by_window": [{"title": r["window_title"][:80], "count": r["cnt"]} for r in by_window],
            "positions": [dict(r) for r in positions],
        })
    finally:
        conn.close()


@app.route("/api/file-events")
def api_file_events():
    hours = float(request.args.get("hours", 24))
    since = hours_ago(hours)
    conn = get_db()
    try:
        # Events by type
        by_type = conn.execute("""
            SELECT event_type, COUNT(*) as cnt
            FROM file_events
            WHERE timestamp >= ?
            GROUP BY event_type
            ORDER BY cnt DESC
        """, (since,)).fetchall()

        # Top directories (extract parent directory from src_path)
        top_dirs = conn.execute("""
            SELECT
                CASE
                    WHEN INSTR(REPLACE(src_path, '\\', '/'), '/') > 0
                    THEN SUBSTR(REPLACE(src_path, '\\', '/'), 1,
                         LENGTH(REPLACE(src_path, '\\', '/'))
                         - LENGTH(REPLACE(REPLACE(src_path, '\\', '/'),
                           SUBSTR(REPLACE(src_path, '\\', '/'),
                             INSTR(REPLACE(
                               REPLACE(REPLACE(src_path, '\\', '/'), '/', CHAR(0)),
                               CHAR(0), '/'
                             ), '/') + 0), '')))
                    ELSE src_path
                END as dir_path,
                COUNT(*) as cnt
            FROM file_events
            WHERE timestamp >= ?
            GROUP BY dir_path
            ORDER BY cnt DESC
            LIMIT 15
        """, (since,)).fetchall()

        # Simpler approach - just get raw paths and group in Python
        raw = conn.execute("""
            SELECT src_path, COUNT(*) as cnt
            FROM file_events
            WHERE timestamp >= ?
            GROUP BY src_path
            ORDER BY cnt DESC
        """, (since,)).fetchall()

        # Group by parent directory in Python
        dir_counts = {}
        for r in raw:
            path = r["src_path"].replace("\\", "/")
            parent = "/".join(path.split("/")[:-1]) if "/" in path else path
            dir_counts[parent] = dir_counts.get(parent, 0) + r["cnt"]

        top_dirs_clean = sorted(dir_counts.items(), key=lambda x: -x[1])[:15]

        return jsonify({
            "by_type": [dict(r) for r in by_type],
            "top_dirs": [{"path": d[0], "count": d[1]} for d in top_dirs_clean],
        })
    finally:
        conn.close()


@app.route("/api/browser-tabs")
def api_browser_tabs():
    hours = float(request.args.get("hours", 24))
    since = hours_ago(hours)
    conn = get_db()
    try:
        by_type = conn.execute("""
            SELECT event_type, COUNT(*) as cnt
            FROM browser_tab_events
            WHERE timestamp >= ?
            GROUP BY event_type
            ORDER BY cnt DESC
        """, (since,)).fetchall()

        recent = conn.execute("""
            SELECT timestamp, browser, event_type, title, url
            FROM browser_tab_events
            WHERE timestamp >= ?
            ORDER BY id DESC
            LIMIT 30
        """, (since,)).fetchall()

        return jsonify({
            "by_type": [dict(r) for r in by_type],
            "recent": [dict(r) for r in recent],
        })
    finally:
        conn.close()


@app.route("/api/timeline")
def api_timeline():
    hours = float(request.args.get("hours", 24))
    since = hours_ago(hours)
    conn = get_db()
    try:
        # Bucket events by minute for each table
        tables = {
            "windows": "active_window_events",
            "clicks": "mouse_click_events",
            "tabs": "browser_tab_events",
            "files": "file_events",
        }
        result = {}
        for key, table in tables.items():
            rows = conn.execute(f"""
                SELECT SUBSTR(timestamp, 1, 16) as minute, COUNT(*) as cnt
                FROM {table}
                WHERE timestamp >= ?
                GROUP BY minute
                ORDER BY minute
            """, (since,)).fetchall()
            result[key] = [{"minute": r["minute"], "count": r["cnt"]} for r in rows]

        return jsonify(result)
    finally:
        conn.close()


# --- Server wrapper ---

class DashboardServer:
    """Flask dashboard server that runs in a background thread."""

    def __init__(self, config):
        self.port = config.get("dashboard_port", 5000)
        self._server = None
        self._thread = None

    def start(self):
        self._server = make_server("0.0.0.0", self.port, app)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        print(f"[Dashboard] Running at http://localhost:{self.port}")

    def stop(self):
        if self._server:
            self._server.shutdown()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        print("[Dashboard] Stopped")
