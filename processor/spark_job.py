from processor.spark.config import (
    KAFKA_BROKER,
    RAW_TOPIC,
    OUTPUT_TOPIC,
    CHECKPOINT_DIR,
)
from processor.spark.pipeline import build_metrics_stream, build_entropy_stream
from processor.spark.session import build_session
from processor.spark.sinks import write_to_kafka, write_to_console


def main():
    spark = build_session("EntropyWatch")

    metrics = build_metrics_stream(spark, KAFKA_BROKER, RAW_TOPIC)
    entropy = build_entropy_stream(metrics)

    kafka_query = write_to_kafka(entropy, OUTPUT_TOPIC, KAFKA_BROKER, CHECKPOINT_DIR)
    console_query = write_to_console(entropy)

    kafka_query.awaitTermination()
    console_query.awaitTermination()


if __name__ == "__main__":
    main()
