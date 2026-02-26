from kafka import KafkaProducer
import json
from source.config import KAFKA_BROKER

class MetricsProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def send(self, topic, data):
        self.producer.send(topic, value=data)
        self.producer.flush()
