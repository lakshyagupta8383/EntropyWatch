from collections import deque
from math import sqrt


class MovingStats:
    def __init__(self, window_size=50):
        self.window_size = window_size
        self.values = deque(maxlen=window_size)

    def update(self, value):
        self.values.append(float(value))

    def mean(self):
        if not self.values:
            return 0.0
        return sum(self.values) / len(self.values)

    def std(self):
        if len(self.values) < 2:
            return 0.0
        m = self.mean()
        variance = sum((v - m) ** 2 for v in self.values) / (len(self.values) - 1)
        return sqrt(variance)


class AnomalyDetector:
    def __init__(self, window_size=50, z_threshold=3.0):
        self.window_size = window_size
        self.z_threshold = z_threshold
        self.stats = {
            "cpu": MovingStats(window_size),
            "memory": MovingStats(window_size),
            "latency": MovingStats(window_size),
            "error_rate": MovingStats(window_size),
        }

    def detect(self, metric):
        """Return a list of anomaly messages for a single metric event."""
        anomalies = []
        for key in self.stats:
            value = metric.get(key)
            if value is None:
                continue

            stats = self.stats[key]
            stats.update(value)
            std = stats.std()
            mean = stats.mean()

            if std > 0:
                z = abs(value - mean) / std
                if z >= self.z_threshold and len(stats.values) >= 5:
                    anomalies.append(
                        f"{key} z-score {z:.2f} (value={value}, mean={mean:.2f}, std={std:.2f})"
                    )

        # Simple absolute thresholds (acts as a safety net).
        if metric.get("cpu", 0) >= 90:
            anomalies.append(f"cpu high: {metric['cpu']}")
        if metric.get("memory", 0) >= 90:
            anomalies.append(f"memory high: {metric['memory']}")
        if metric.get("latency", 0) >= 500:
            anomalies.append(f"latency high: {metric['latency']}ms")
        if metric.get("error_rate", 0) >= 0.2:
            anomalies.append(f"error_rate high: {metric['error_rate']}")

        return anomalies
