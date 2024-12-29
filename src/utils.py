import json
import urllib.request
import urllib.parse
import urllib.error
from rate_limiter import RateLimiter

rate_limiter = RateLimiter(request_limit=150, cooldown_minutes=3)


def make_request(url, params=None):
    rate_limiter.check_and_wait()

    if params:
        query_string = urllib.parse.urlencode(params)
        url = f"{url}?{query_string}"

    try:
        with urllib.request.urlopen(url) as response:
            return json.loads(response.read().decode())
    except urllib.error.URLError as e:
        print(f"Error fetching data: {e}")
        return None
