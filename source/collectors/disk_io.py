import psutil
import time

_prev = psutil.disk_io_counters()
_prev_time = time.time()

def collect_disk_io():
    global _prev, _prev_time

    current = psutil.disk_io_counters()
    now = time.time()

    interval = now - _prev_time

    read_mb_s = (current.read_bytes - _prev.read_bytes) / (1024 * 1024) / interval
    write_mb_s = (current.write_bytes - _prev.write_bytes) / (1024 * 1024) / interval

    _prev = current
    _prev_time = now

    return read_mb_s, write_mb_s