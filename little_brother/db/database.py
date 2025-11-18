import sqlite3
import queue
import threading
import time
import os
import json
import datetime


def load_config():
    """Load configuration from config.json."""
    config_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
    with open(config_path, "r") as f:
        return json.load(f)


class Database:
    """Thread-safe database manager for Little Brother events."""

    def __init__(self, db_path="little_brother.db"):
        """Initialize database connection and start writer thread.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.event_queue = queue.Queue()
        self.running = True

        # Create database connection
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        # Load schema
        self.load_schema()

        # Start writer thread
        self.start_writer_thread()

    def load_schema(self):
        """Load and execute schema.sql to create tables."""
        schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
        with open(schema_path, "r") as f:
            schema_sql = f.read()

        cursor = self.conn.cursor()
        cursor.executescript(schema_sql)
        self.conn.commit()
        print(f"Schema loaded from {schema_path}")

    def write_event(self, table, data_dict):
        """Queue an event to be written to the database.

        Args:
            table: Table name (e.g., 'active_window_events')
            data_dict: Dictionary of column names to values
        """
        self.event_queue.put((table, data_dict))

    def start_writer_thread(self):
        """Start the background writer thread."""
        self.writer_thread = threading.Thread(target=self.writer_loop, daemon=True)
        self.writer_thread.start()
        print("Database writer thread started")

    def writer_loop(self):
        """Main loop for writer thread - processes queued events."""
        while self.running:
            try:
                # Get event from queue with timeout to allow checking self.running
                try:
                    table, data_dict = self.event_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                # Build INSERT statement
                columns = ", ".join(data_dict.keys())
                placeholders = ", ".join(["?" for _ in data_dict])
                sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

                # Execute and commit
                cursor = self.conn.cursor()
                cursor.execute(sql, list(data_dict.values()))
                self.conn.commit()

                # Mark task as done
                self.event_queue.task_done()

                # Sleep to reduce CPU load
                time.sleep(0.01)

            except Exception as e:
                print(f"Error in writer loop: {e}")
                time.sleep(0.01)

    def stop(self):
        """Gracefully shut down the database writer."""
        print("Stopping database writer...")
        self.running = False

        # Wait for queue to be empty
        self.event_queue.join()

        # Wait for writer thread to finish
        if self.writer_thread.is_alive():
            self.writer_thread.join(timeout=2.0)

        # Close database connection
        self.conn.close()
        print("Database stopped cleanly")

    # Insert wrapper methods

    def log_active_window(self, timestamp, window_title, process_name, process_path, hwnd):
        """Log an active window event.

        Args:
            timestamp: ISO format timestamp string
            window_title: Title of the active window
            process_name: Name of the process
            process_path: Full path to the process executable
            hwnd: Windows handle identifier
        """
        self.write_event("active_window_events", {
            "timestamp": timestamp,
            "window_title": window_title,
            "process_name": process_name,
            "process_path": process_path,
            "hwnd": hwnd
        })

    def log_mouse_click(self, timestamp, button, x, y, window_title):
        """Log a mouse click event.

        Args:
            timestamp: ISO format timestamp string
            button: Mouse button ('left', 'right', 'middle')
            x: X coordinate of click
            y: Y coordinate of click
            window_title: Title of window where click occurred
        """
        self.write_event("mouse_click_events", {
            "timestamp": timestamp,
            "button": button,
            "x": x,
            "y": y,
            "window_title": window_title
        })

    def log_browser_tab(self, timestamp, browser, event_type, title, url):
        """Log a browser tab event.

        Args:
            timestamp: ISO format timestamp string
            browser: Browser name (e.g., 'chrome', 'firefox')
            event_type: Type of event ('created', 'updated', 'activated', 'removed')
            title: Page title
            url: Page URL
        """
        self.write_event("browser_tab_events", {
            "timestamp": timestamp,
            "browser": browser,
            "event_type": event_type,
            "title": title,
            "url": url
        })

    def log_file_event(self, timestamp, event_type, src_path, is_directory):
        """Log a filesystem event.

        Args:
            timestamp: ISO format timestamp string
            event_type: Type of event ('created', 'modified', 'deleted', 'moved')
            src_path: Path to the file or directory
            is_directory: 1 if directory, 0 if file
        """
        self.write_event("file_events", {
            "timestamp": timestamp,
            "event_type": event_type,
            "src_path": src_path,
            "is_directory": is_directory
        })


if __name__ == "__main__":
    print("Testing database module...")

    # Create database instance
    db = Database("test_little_brother.db")

    # Insert a dummy active window event
    timestamp = datetime.datetime.utcnow().isoformat()
    db.log_active_window(
        timestamp=timestamp,
        window_title="Test Window - Notepad",
        process_name="notepad.exe",
        process_path="C:\\Windows\\System32\\notepad.exe",
        hwnd=123456
    )

    print(f"Logged dummy active window event at {timestamp}")

    # Wait for event to be processed
    time.sleep(0.5)

    # Stop database
    db.stop()

    print("Database test completed successfully!")
