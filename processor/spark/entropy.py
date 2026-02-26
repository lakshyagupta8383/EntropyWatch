from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import col, count, lit, log, sum as spark_sum, window


def _log2(value_col):
    return log(value_col) / log(lit(2.0))


def compute_entropy(df, time_col, value_col, splits, window_duration, window_slide, metric_name):
    bucketizer = Bucketizer(
        splits=splits,
        inputCol=value_col,
        outputCol=f"{metric_name}_bin",
        handleInvalid="keep",
    )
    bucketed = bucketizer.transform(df)

    windowed = bucketed.groupBy(
        window(col(time_col), window_duration, window_slide).alias("window"),
        col(f"{metric_name}_bin"),
    ).agg(count(lit(1)).alias("bin_count"))

    totals = windowed.groupBy("window").agg(spark_sum("bin_count").alias("total_count"))

    joined = windowed.join(totals, on="window")
    prob = (col("bin_count") / col("total_count")).alias("p")

    entropy = (
        joined.select("window", prob)
        .withColumn("entropy_part", -col("p") * _log2(col("p")))
        .groupBy("window")
        .agg(spark_sum("entropy_part").alias(f"entropy_{metric_name}"))
    )

    return entropy
