import datetime
import uuid
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    from_json,
    to_timestamp,
    avg,
    current_timestamp,
    to_json,
    struct,
)
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# Налаштування для роботи з Kafka через PySpark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 pyspark-shell"
)

# Конфігурація Kafka
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC_INPUT = "building_sensors"
KAFKA_TOPIC_OUTPUT = "alert_topic"
SECURITY_PROTOCOL = "PLAINTEXT"

# Ініціалізація Spark Session
spark = (
    SparkSession.builder.appName("KafkaStreaming")
    .master("local[*]")
    .config("spark.sql.debug.maxToStringFields", "200")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.streaming.backpressure.enabled", "true")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# Завантаження CSV-файлу з умовами алертів
alerts_df = spark.read.option("header", True).csv("./alerts_conditions.csv")

# Схема JSON-повідомлень Kafka
json_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
])

# Визначення шлюзу для чекпоінтів
checkpoint_location = "/tmp/checkpoint"

# Читання струменю з Kafka
raw_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("kafka.security.protocol", SECURITY_PROTOCOL)
    .option("subscribe", KAFKA_TOPIC_INPUT)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "500")
    .load()
)

# Обробка JSON-повідомлень
sensor_data_df = (
    raw_stream_df.selectExpr("CAST(value AS STRING) AS json_string")
    .withColumn("data", from_json(col("json_string"), json_schema))
    .select("data.*")
    .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
)

# Аналіз даних з часовими вікнами
windowed_df = (
    sensor_data_df.withWatermark("timestamp", "10 seconds")
    .groupBy(expr("window(timestamp, '1 minute', '30 seconds')"))
    .agg(avg("temperature").alias("avg_temp"), avg("humidity").alias("avg_humidity"))
)

# Фільтрація алертів
alerts_filtered = (
    windowed_df.crossJoin(alerts_df)
    .filter(
        (
            ((col("avg_temp") > col("temperature_min")) & (col("temperature_min") != -999))
            | ((col("avg_temp") < col("temperature_max")) & (col("temperature_max") != -999))
            | ((col("avg_humidity") > col("humidity_min")) & (col("humidity_min") != -999))
            | ((col("avg_humidity") < col("humidity_max")) & (col("humidity_max") != -999))
        )
    )
    .withColumn("alert_id", expr("uuid()"))
    .withColumn("alert_time", current_timestamp())
    .select(col("alert_id").alias("key"), to_json(struct("window.start", "window.end", "avg_temp", "avg_humidity", "code", "message", "alert_time")).alias("value"))
)

# Виведення у Kafka
kafka_output = (
    alerts_filtered.writeStream.outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", KAFKA_TOPIC_OUTPUT)
    .option("checkpointLocation", checkpoint_location)
    .trigger(processingTime="10 seconds")
    .start()
)

# Чекання завершення
kafka_output.awaitTermination()
