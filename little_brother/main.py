import time
from db.database import Database

if __name__ == "__main__":
    print("Testing database init...")
    db = Database()
    print("Database initialized. Writer thread running.")
    time.sleep(1)
    db.stop()
    print("Database stopped cleanly.")
