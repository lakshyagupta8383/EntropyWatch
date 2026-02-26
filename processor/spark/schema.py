from pyspark.sql.types import DoubleType, LongType, StringType, StructType


def metrics_schema():
    return (
        StructType()
        .add("timestamp", LongType())
        .add("host", StringType())
        .add("cpu", DoubleType())
        .add("memory", DoubleType())
        .add("disk_read_mb_s", DoubleType())
        .add("disk_write_mb_s", DoubleType())
        .add("net_sent_mb_s", DoubleType())
        .add("net_recv_mb_s", DoubleType())
        .add("latency", DoubleType())
        .add("error_rate", DoubleType())
    )
