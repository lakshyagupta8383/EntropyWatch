import json
import threading
import time
from collections import deque

from kafka import KafkaConsumer

from source.config import KAFKA_BROKER, KAFKA_TOPIC
from processor.spark.config import OUTPUT_TOPIC


class SharedState:
    def __init__(self):
        self.lock = threading.Lock()
        self.latest_metrics = None
        self.latest_alert = None
        self.entropy_history = deque(maxlen=20)
        self.alerts_feed = deque(maxlen=10)

    def update_metrics(self, data):
        with self.lock:
            self.latest_metrics = data

    def update_alert(self, data):
        with self.lock:
            self.latest_alert = data
            self.entropy_history.append(data)
            if data.get("alert"):
                self.alerts_feed.appendleft(data)

    def snapshot(self):
        with self.lock:
            return (
                self.latest_metrics,
                self.latest_alert,
                list(self.entropy_history),
                list(self.alerts_feed),
            )


def consume_topic(topic, group_id, handler):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    for message in consumer:
        handler(message.value)


def format_panel_title(title):
    return f"=== {title} ==="


def render_dashboard(state):
    metrics, alert, entropy_history, alerts_feed = state.snapshot()

    print("\033[2J\033[H", end="")  # clear screen
    print(format_panel_title("Global System Status"))
    if alert:
        print(f"Status: {alert.get('severity', 'Unknown')}")
        print(f"Entropy: {alert.get('entropy_overall', 0):.4f}")
        print(f"Threshold: {alert.get('threshold', 0):.4f}")
        print(f"Deviation: {alert.get('deviation_percent', 0):.2%}")
        print(f"Alert: {alert.get('alert', False)}")
    else:
        print("No entropy data yet")

    print("\n" + format_panel_title("Entropy Over Time (latest)"))
    if entropy_history:
        for item in entropy_history[-10:]:
            w_end = item.get("window_end")
            ent = item.get("entropy_overall")
            sev = item.get("severity")
            print(f"{w_end} | entropy={ent:.4f} | {sev}")
    else:
        print("No history yet")

    print("\n" + format_panel_title("Entropy Breakdown by Metric"))
    if alert:
        print(f"CPU: {alert.get('entropy_cpu', 0):.4f}")
        print(f"Memory: {alert.get('entropy_memory', 0):.4f}")
        print(f"Latency: {alert.get('entropy_latency', 0):.4f}")
        print(f"Error Rate: {alert.get('entropy_error_rate', 0):.4f}")
    else:
        print("No entropy data yet")

    print("\n" + format_panel_title("Raw System Metrics"))
    if metrics:
        print(f"CPU: {metrics.get('cpu', 0):.2f}%")
        print(f"Memory: {metrics.get('memory', 0):.2f}%")
        print(f"Latency: {metrics.get('latency', 0):.2f} ms")
        print(f"Error Rate: {metrics.get('error_rate', 0):.4f}")
        print(f"Disk R/W: {metrics.get('disk_read_mb_s', 0):.2f}/{metrics.get('disk_write_mb_s', 0):.2f} MB/s")
        print(f"Net S/R: {metrics.get('net_sent_mb_s', 0):.2f}/{metrics.get('net_recv_mb_s', 0):.2f} MB/s")
    else:
        print("No metrics yet")

    print("\n" + format_panel_title("Early Warning Alerts Feed"))
    if alerts_feed:
        for item in alerts_feed:
            w_end = item.get("window_end")
            ent = item.get("entropy_overall")
            thr = item.get("threshold")
            sev = item.get("severity")
            print(f"{w_end} | entropy={ent:.4f} | threshold={thr:.4f} | {sev}")
    else:
        print("No alerts yet")

    print("\n" + format_panel_title("Baseline & Learning Window"))
    if alert:
        print(f"Baseline mean: {alert.get('baseline_mean', 0):.4f}")
        print(f"Baseline std: {alert.get('baseline_std', 0):.4f}")
    else:
        print("No baseline yet")


def main():
    state = SharedState()

    threads = [
        threading.Thread(
            target=consume_topic,
            args=(KAFKA_TOPIC, "entropywatch-metrics-ui", state.update_metrics),
            daemon=True,
        ),
        threading.Thread(
            target=consume_topic,
            args=(OUTPUT_TOPIC, "entropywatch-alerts-ui", state.update_alert),
            daemon=True,
        ),
    ]

    for t in threads:
        t.start()

    while True:
        render_dashboard(state)
        time.sleep(1)


if __name__ == "__main__":
    main()
