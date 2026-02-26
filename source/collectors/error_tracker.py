from collections import deque
from config import WINDOW_SIZE

_request_history = deque(maxlen=WINDOW_SIZE)

def update_error_rate(status_code):
    success = status_code == 200
    _request_history.append(success)

def get_error_rate():
    if not _request_history:
        return 0.0
    
    failures = _request_history.count(False)
    return failures / len(_request_history)