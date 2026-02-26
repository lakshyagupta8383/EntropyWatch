import time
import requests
from source.config import API_URL

def collect_latency():
    start = time.time()
    try:
        response = requests.get(API_URL, timeout=2)
        latency = (time.time() - start) * 1000
        return latency, response.status_code
    except Exception:
        return None, 500
