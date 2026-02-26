from pyspark.sql.functions import col, to_json, struct


def write_to_kafka(df, topic, broker, checkpoint_dir):
    payload = df.select(to_json(struct([col(c) for c in df.columns])).alias("value"))
    return (
        payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("topic", topic)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .start()
    )


def write_to_console(df):
    return df.writeStream.format("console").outputMode("append").start()
