import signal
import socket
import time

from source.collectors.cpu_memory import collect_cpu_memory
from source.collectors.disk_io import collect_disk_io
from source.collectors.network_io import collect_network_io
from source.collectors.latency_monitor import collect_latency
from source.collectors.error_tracker import update_error_rate, get_error_rate
from source.config import COLLECTION_INTERVAL, KAFKA_TOPIC
from source.producer.kafka_producer import MetricsProducer

class SystemMonitor:
    def __init__(self):
        self.host = socket.gethostname()

    def collect(self):
        cpu, memory = collect_cpu_memory()
        disk_r, disk_w = collect_disk_io()
        net_s, net_r = collect_network_io()

        latency, status = collect_latency()
        update_error_rate(status)
        error_rate = get_error_rate()
        latency_value = latency if latency is not None else 0.0

        return {
            "timestamp": int(time.time()),
            "host": self.host,

            "cpu": cpu,
            "memory": memory,

            "disk_read_mb_s": disk_r,
            "disk_write_mb_s": disk_w,

            "net_sent_mb_s": net_s,
            "net_recv_mb_s": net_r,

            "latency": latency_value,
            "error_rate": error_rate
        }


def main():
    monitor = SystemMonitor()
    producer = MetricsProducer()

    def shutdown(_sig=None, _frame=None):
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        metric = monitor.collect()
        producer.send(KAFKA_TOPIC, metric)
        time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
    main()
