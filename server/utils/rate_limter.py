# utils/rate_limiter.py
import time
from collections import defaultdict, deque
from fastapi import HTTPException, Request
from functools import wraps

class RateLimiter:
    def __init__(self, max_requests: int = 15, window_seconds: int = 60):
        """
        in-memory rate limiter
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        # Store request timestamps per IP
        self.requests = defaultdict(deque)

    def is_allowed(self, client_ip: str) -> bool:
        """Check if request is allowed for this IP"""
        current_time = time.time()

        # Clean old requests outside the window
        while (self.requests[client_ip] and
               current_time - self.requests[client_ip][0] > self.window_seconds):
            self.requests[client_ip].popleft()

        # Check if under limit
        if len(self.requests[client_ip]) < self.max_requests:
            self.requests[client_ip].append(current_time)
            return True

        return False

    def get_reset_time(self, client_ip: str) -> int:
        """Get seconds until rate limit resets for this IP"""
        if not self.requests[client_ip]:
            return 0

        oldest_request = self.requests[client_ip][0]
        reset_time = oldest_request + self.window_seconds
        return max(0, int(reset_time - time.time()))

    def get_remaining_requests(self, client_ip: str) -> int:
        """Get number of requests remaining in current window"""
        current_time = time.time()

        # Clean old requests
        while (self.requests[client_ip] and
               current_time - self.requests[client_ip][0] > self.window_seconds):
            self.requests[client_ip].popleft()

        return max(0, self.max_requests - len(self.requests[client_ip]))

    def cleanup_old_ips(self):
        """Remove IPs that haven't made requests recently (cleanup memory)"""
        current_time = time.time()
        cleanup_threshold = self.window_seconds * 2  # Clean IPs inactive for 2x window

        ips_to_remove = []
        for ip, requests in self.requests.items():
            if not requests or current_time - requests[-1] > cleanup_threshold:
                ips_to_remove.append(ip)

        for ip in ips_to_remove:
            del self.requests[ip]

        if ips_to_remove:
            print(f"Rate limiter: Cleaned up {len(ips_to_remove)} inactive IPs")

# Global rate limiter instance
rate_limiter = RateLimiter(max_requests=15, window_seconds=60)

def rate_limit(func):
    """Decorator to apply rate limiting to FastAPI endpoints"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Find the request object in the arguments
        request = None
        for arg in args:
            if isinstance(arg, Request):
                request = arg
                break

        if not request:
            # If no request found, allow (shouldn't happen in normal FastAPI usage)
            return await func(*args, **kwargs)

        client_ip = request.client.host

        if not rate_limiter.is_allowed(client_ip):
            reset_time = rate_limiter.get_reset_time(client_ip)
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "Rate limit exceeded",
                    "message": f"Maximum {rate_limiter.max_requests} requests per minute allowed",
                    "retry_after_seconds": reset_time,
                    "retry_after": f"{reset_time} seconds"
                },
                headers={
                    "Retry-After": str(reset_time),
                    "X-RateLimit-Limit": str(rate_limiter.max_requests),
                    "X-RateLimit-Remaining": str(rate_limiter.get_remaining_requests(client_ip)),
                    "X-RateLimit-Reset": str(int(time.time()) + reset_time)
                }
            )

        # Add rate limit headers to successful responses
        response = await func(*args, **kwargs)

        # If response is a dict, we can't add headers directly
        # Headers will be added by FastAPI middleware if needed

        return response

    return wrapper

#Cleanup function that can be called periodically
def cleanup_rate_limiter():
    """Cleanup old IP addresses from rate limiter memory"""
    rate_limiter.cleanup_old_ips()