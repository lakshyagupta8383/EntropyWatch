import psutil
import time

_prev = psutil.net_io_counters()
_prev_time = time.time()

def collect_network_io():
    global _prev, _prev_time

    current = psutil.net_io_counters()
    now = time.time()
    interval = max(now - _prev_time, 0.0001)
    sent_mb_s = (current.bytes_sent - _prev.bytes_sent) / (1024 * 1024) / interval
    recv_mb_s = (current.bytes_recv - _prev.bytes_recv) / (1024 * 1024) / interval

    _prev = current
    _prev_time = now

    return sent_mb_s, recv_mb_s