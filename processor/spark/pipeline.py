from pyspark.sql.functions import (
    col,
    lit,
    to_timestamp,
    from_unixtime,
    from_json,
    avg,
    stddev_samp,
    coalesce,
    when,
    window,
)

from processor.spark.config import (
    BASELINE_SLIDE,
    BASELINE_WINDOW,
    CPU_SPLITS,
    CPU_WEIGHT,
    DEMO_MODE,
    ERROR_RATE_SPLITS,
    ERROR_RATE_WEIGHT,
    LATENCY_SPLITS,
    LATENCY_WEIGHT,
    MEMORY_SPLITS,
    MEMORY_WEIGHT,
    WINDOW_DURATION,
    WINDOW_SLIDE,
    WATERMARK,
    THRESHOLD_STD_MULT,
)
from processor.spark.entropy import compute_entropy
from processor.spark.schema import metrics_schema


def build_metrics_stream(spark, kafka_broker, topic):
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = df.select(from_json(col("value").cast("string"), metrics_schema()).alias("data"))
    parsed = parsed.select("data.*")

    with_time = parsed.withColumn(
        "event_time", to_timestamp(from_unixtime(col("timestamp")))
    )

    cleaned = (
        with_time.fillna({"latency": 0.0, "error_rate": 0.0})
        .withColumn("cpu", col("cpu").cast("double"))
        .withColumn("memory", col("memory").cast("double"))
        .withColumn("latency", col("latency").cast("double"))
        .withColumn("error_rate", col("error_rate").cast("double"))
        .withWatermark("event_time", WATERMARK)
    )

    return cleaned


def build_entropy_stream(metrics_df):
    entropy_cpu = compute_entropy(
        metrics_df,
        "event_time",
        "cpu",
        CPU_SPLITS,
        WINDOW_DURATION,
        WINDOW_SLIDE,
        "cpu",
    )
    entropy_mem = compute_entropy(
        metrics_df,
        "event_time",
        "memory",
        MEMORY_SPLITS,
        WINDOW_DURATION,
        WINDOW_SLIDE,
        "memory",
    )
    entropy_lat = compute_entropy(
        metrics_df,
        "event_time",
        "latency",
        LATENCY_SPLITS,
        WINDOW_DURATION,
        WINDOW_SLIDE,
        "latency",
    )
    entropy_err = compute_entropy(
        metrics_df,
        "event_time",
        "error_rate",
        ERROR_RATE_SPLITS,
        WINDOW_DURATION,
        WINDOW_SLIDE,
        "error_rate",
    )

    entropies = (
        entropy_cpu.join(entropy_mem, on="window")
        .join(entropy_lat, on="window")
        .join(entropy_err, on="window")
        .select(
            col("window").start.alias("window_start"),
            col("window").end.alias("window_end"),
            col("entropy_cpu"),
            col("entropy_memory"),
            col("entropy_latency"),
            col("entropy_error_rate"),
        )
    )

    entropy_overall = (
        entropies.withColumn(
            "entropy_overall",
            col("entropy_cpu") * lit(CPU_WEIGHT)
            + col("entropy_memory") * lit(MEMORY_WEIGHT)
            + col("entropy_latency") * lit(LATENCY_WEIGHT)
            + col("entropy_error_rate") * lit(ERROR_RATE_WEIGHT),
        )
    )

    baseline_keyed = entropy_overall.withColumn(
        "baseline_window", window(col("window_end"), BASELINE_WINDOW, BASELINE_SLIDE)
    )

    baseline_stats = baseline_keyed.groupBy("baseline_window").agg(
        avg("entropy_overall").alias("baseline_mean"),
        stddev_samp("entropy_overall").alias("baseline_std"),
    )

    with_baseline = (
        baseline_keyed.join(baseline_stats, on="baseline_window")
        .withColumn("baseline_mean", coalesce(col("baseline_mean"), lit(0.0)))
        .withColumn("baseline_std", coalesce(col("baseline_std"), lit(0.0)))
    )

    thresholded = (
        with_baseline.withColumn(
            "threshold", col("baseline_mean") + lit(THRESHOLD_STD_MULT) * col("baseline_std")
        )
        .withColumn(
            "deviation_percent",
            when(col("threshold") > 0, (col("entropy_overall") - col("threshold")) / col("threshold")).otherwise(lit(0.0)),
        )
        .withColumn(
            "severity",
            when(col("entropy_overall") >= col("threshold"),
                 when(col("deviation_percent") >= 0.5, lit("Critical")).otherwise(lit("Warning"))
            ).otherwise(lit("Healthy"))
        )
        .withColumn("alert", col("entropy_overall") >= col("threshold"))
    )

    if DEMO_MODE:
        thresholded = thresholded.withColumn("severity", lit("Demo")).withColumn("alert", lit(True))

    return thresholded.select(
        "window_start",
        "window_end",
        "entropy_cpu",
        "entropy_memory",
        "entropy_latency",
        "entropy_error_rate",
        "entropy_overall",
        "baseline_mean",
        "baseline_std",
        "threshold",
        "deviation_percent",
        "severity",
        "alert",
    )
