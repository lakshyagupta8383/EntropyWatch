import json
import random
import time

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

base = {
    "window_start": int(time.time()) - 60,
    "window_end": int(time.time()),
    "entropy_cpu": 0.5,
    "entropy_memory": 0.4,
    "entropy_latency": 0.6,
    "entropy_error_rate": 0.2,
    "entropy_overall": 0.55,
    "baseline_mean": 0.3,
    "baseline_std": 0.05,
    "threshold": 0.4,
    "deviation_percent": 0.375,
}

for i in range(3):
    msg = base.copy()
    msg.update(
        {
            "window_start": msg["window_start"] + i * 30,
            "window_end": msg["window_end"] + i * 30,
            "entropy_overall": round(0.55 + random.uniform(-0.1, 0.1), 4),
            "severity": random.choice(["Warning", "Critical"]),
            "alert": True,
        }
    )
    producer.send("system-alerts", msg)
    producer.flush()
    time.sleep(0.5)

producer.close()
print("demo alerts sent")
