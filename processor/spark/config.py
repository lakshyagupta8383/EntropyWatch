import os

RAW_TOPIC = "system-metrics"
OUTPUT_TOPIC = "system-alerts"
KAFKA_BROKER = "kafka:9092"

WINDOW_DURATION = "30 seconds"
WINDOW_SLIDE = "10 seconds"
BASELINE_WINDOW = "1 minute"
BASELINE_SLIDE = "10 seconds"
WATERMARK = "30 seconds"

THRESHOLD_STD_MULT = 2.0

CPU_SPLITS = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
MEMORY_SPLITS = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
LATENCY_SPLITS = [0, 50, 100, 200, 300, 500, 800, 1200, 2000, 5000]
ERROR_RATE_SPLITS = [0.0, 0.01, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0]

CPU_WEIGHT = 0.25
MEMORY_WEIGHT = 0.25
LATENCY_WEIGHT = 0.25
ERROR_RATE_WEIGHT = 0.25

DEMO_MODE = os.getenv("ENTROPY_DEMO_MODE", "true").lower() in {"1", "true", "yes"}

CHECKPOINT_DIR = "/tmp/entropywatch-checkpoints"
