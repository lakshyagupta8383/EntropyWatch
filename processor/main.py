import json
import signal
import sys
from datetime import datetime

from source.config import KAFKA_BROKER, KAFKA_TOPIC
from processor.consumer import MetricsConsumer
from processor.detector import AnomalyDetector


def format_event(metric):
    ts = metric.get("timestamp")
    if ts is None:
        return json.dumps(metric)
    try:
        iso = datetime.fromtimestamp(ts).isoformat()
        return f"{iso} {json.dumps(metric)}"
    except Exception:
        return json.dumps(metric)


def main():
    consumer = MetricsConsumer(KAFKA_BROKER, KAFKA_TOPIC)
    detector = AnomalyDetector()

    def shutdown(_sig=None, _frame=None):
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for metric in consumer:
        anomalies = detector.detect(metric)
        if anomalies:
            print("ANOMALY", format_event(metric))
            for message in anomalies:
                print(f"  - {message}")


if __name__ == "__main__":
    main()
