import threading


class Event:
    """A monitoring event published by the EventBus."""

    __slots__ = ("event_type", "table", "data", "timestamp")

    def __init__(self, event_type, table, data, timestamp):
        self.event_type = event_type
        self.table = table
        self.data = data
        self.timestamp = timestamp

    def to_dict(self):
        return {
            "event_type": self.event_type,
            "table": self.table,
            "data": self.data,
            "timestamp": self.timestamp,
        }


TABLE_TO_EVENT_TYPE = {
    "active_window_events": "active_window",
    "mouse_click_events": "mouse_click",
    "browser_tab_events": "browser_tab",
    "file_events": "file_event",
}


class EventBus:
    """Thread-safe publish/subscribe event bus."""

    def __init__(self):
        self._subscribers = []
        self._lock = threading.Lock()

    def subscribe(self, callback):
        with self._lock:
            self._subscribers.append(callback)

    def unsubscribe(self, callback):
        with self._lock:
            self._subscribers = [s for s in self._subscribers if s is not callback]

    def publish(self, event):
        with self._lock:
            subs = list(self._subscribers)
        for cb in subs:
            try:
                cb(event)
            except Exception as e:
                print(f"[EventBus] Subscriber error: {e}")
