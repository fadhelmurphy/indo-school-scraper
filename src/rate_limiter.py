import time
from datetime import datetime


class RateLimiter:
    def __init__(self, request_limit=150, cooldown_minutes=3):
        self.request_count = 0
        self.request_limit = request_limit
        self.cooldown_minutes = cooldown_minutes
        self.last_reset = datetime.now()

    def check_and_wait(self):
        current_time = datetime.now()
        self.request_count += 1

        if self.request_count >= self.request_limit:
            cooldown_seconds = self.cooldown_minutes * 60
            print(
                f"\nReached {self.request_limit} requests. Cooling down for {self.cooldown_minutes} minutes..."
            )
            time.sleep(cooldown_seconds)
            self.request_count = 0
            self.last_reset = current_time
