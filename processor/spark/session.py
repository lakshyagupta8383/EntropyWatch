from pyspark.sql import SparkSession


def build_session(app_name="EntropyWatch"):
    return SparkSession.builder.appName(app_name).getOrCreate()
