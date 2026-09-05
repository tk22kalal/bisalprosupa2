"""
Rate limiter for download requests to prevent Telegram timeout errors
"""
import time
from collections import defaultdict
from typing import Tuple

class RateLimiter:
    def __init__(self):
        # Timestamps of requests in the sliding window (for rate limiting)
        self.ip_requests = defaultdict(list)
        # Active request count per IP (for concurrency limiting)
        self.ip_active = defaultdict(int)

        # Rate limit: max requests per time window
        self.max_requests_per_window = 5   # e.g., 5 requests per 60 seconds
        self.time_window = 60              # 60 seconds

        # Concurrency limit: max simultaneous active requests per IP
        self.max_concurrent = 2            # 2 concurrent downloads

        # Cooldown: minimum seconds between requests from same IP
        self.min_delay_between_requests = 2

    def clean_old_entries(self, ip: str):
        """Remove timestamps older than time_window"""
        current_time = time.time()
        if ip in self.ip_requests:
            self.ip_requests[ip] = [
                t for t in self.ip_requests[ip]
                if current_time - t < self.time_window
            ]

    def can_proceed(self, ip: str) -> Tuple[bool, str]:
        """
        Check if a new request can start.
        Returns: (can_proceed, message)
        """
        current_time = time.time()
        self.clean_old_entries(ip)

        # 1. Enforce cooldown (min delay between requests)
        if self.ip_requests[ip]:
            last_time = self.ip_requests[ip][-1]
            elapsed = current_time - last_time
            if elapsed < self.min_delay_between_requests:
                wait = int(self.min_delay_between_requests - elapsed)
                return False, f"Please wait {wait}s before next request"

        # 2. Enforce rate limit (max requests in time window)
        recent_count = len(self.ip_requests[ip])
        if recent_count >= self.max_requests_per_window:
            return False, "Too many requests. Please wait before trying again."

        # 3. Enforce concurrency limit (max active downloads)
        if self.ip_active[ip] >= self.max_concurrent:
            return False, "Too many active downloads. Wait for one to finish."

        return True, "OK"

    def add_request(self, ip: str):
        """Record a new request (called when download starts)"""
        self.ip_requests[ip].append(time.time())
        self.ip_active[ip] += 1

    def remove_request(self, ip: str):
        """Decrement active count (called when download finishes)"""
        if self.ip_active[ip] > 0:
            self.ip_active[ip] -= 1

# Global instance
rate_limiter = RateLimiter()
