import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import FloatType, StringType, StructField, StructType


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 scripts/main.py
def create_spark_session():
    """
    Creates a SparkSession with the necessary configuration for Kafka streaming.

    Returns:
        SparkSession: A SparkSession object ready for use with Kafka streaming.
    """
    return (
        SparkSession.builder.appName("kafka_streaming")
        .config(
            "spark.mongodb.output.uri",
            os.environ.get("MONGODB_URI"),
        )
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .getOrCreate()
    )


def create_streaming_dataframe(spark_session: SparkSession) -> DataFrame:
    """
    Creates a streaming DataFrame from a Kafka topic.

    Args:
        spark_session (SparkSession): The SparkSession object.

    Returns:
        DataFrame: A streaming DataFrame containing data from the Kafka topic.
    """
    return (
        spark_session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:19092")
        .option("subscribe", "default")
        .option("startingOffsets", "latest")
        .load()
    )


def mongodb_sink(dataframe: DataFrame, _) -> None:
    dataframe = dataframe.cache()
    dataframe.write.format("mongo").mode("append").option(
        "database", "data-engineer"
    ).option("collection", "users").save()


def format_data(dataframe):
    """
    Formats the streaming DataFrame by:
        1. Casting the value column to a string.
        2. Selecting the data after parsing the JSON with the provided schema.
        3. Selecting all columns from the 'data' alias.

    Args:
        dataframe (DataFrame): The streaming DataFrame to be formatted.

    Returns:
        DataFrame: The formatted streaming DataFrame.
    """
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("title", StringType(), False),
            StructField("email", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("nat", StringType(), False),
            StructField("cellphone", StringType(), False),
            StructField("picture", StringType(), False),
            StructField("birthday", StringType(), False),
            StructField("registered", FloatType(), False),
        ]
    )

    return (
        dataframe.selectExpr("CAST(value as STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )


if __name__ == "__main__":
    session = create_spark_session()
    session.sparkContext.setLogLevel("ERROR")
    streaming_df = create_streaming_dataframe(session)
    df = format_data(streaming_df)

    # Start writing the formatted data to MongoDB
    streaming = df.writeStream.foreachBatch(mongodb_sink).option(
        "checkpointLocation", "checkpoint"
    )
    query = streaming.start()
    query.awaitTermination()
