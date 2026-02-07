from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, to_timestamp, abs, avg
import os

db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

spark = SparkSession.builder.appName("DetectTimingAnomalies").getOrCreate()

push_events = spark.read.json("/data/raw/github-archive/2024-12-01-0.json")
push_events = push_events.filter(col("type") == "PushEvent")

push_events = push_events.select(
    col("actor.login").alias("developer"),
    col("created_at").alias("timestamp"),
    col("payload.size").alias("num_commits")
)

push_events = push_events.withColumn("timestamp", to_timestamp(col("timestamp")))

push_events = push_events.withColumn("hour_of_day", hour(col("timestamp")))

developer_baselines = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "developer_baselines").option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver").load()

joined = push_events.join(developer_baselines, on="developer", how="inner")

timing_anomalies = joined.withColumn("hour_deviation", abs(col("hour_of_day") - col("avg_hour_of_day")))

timing_anomalies = timing_anomalies.filter(col("hour_deviation") > 6)

timing_anomalies.show(10, truncate=False)
print(f"\nTotal timing anomalies detected: {timing_anomalies.count()}")

connection_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

timing_anomalies.write.mode("overwrite").jdbc(
        url=jdbc_url,
        table="timing_anomalies",
        properties=connection_properties
    )

spark.stop()