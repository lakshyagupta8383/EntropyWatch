import json
from kafka import KafkaConsumer


class MetricsConsumer:
    def __init__(self, broker, topic, group_id="entropywatch-processor"):
        self.topic = topic
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=broker,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    def __iter__(self):
        return self

    def __next__(self):
        message = next(self.consumer)
        return message.value

    def close(self):
        self.consumer.close()
