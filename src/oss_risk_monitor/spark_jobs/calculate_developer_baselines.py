from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, count, hour, dayofweek, to_timestamp
import os

db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")

spark = SparkSession.builder.appName("CalculateDeveloperBaselines").getOrCreate()

push_events = spark.read.json("/data/raw/github-archive/2024-12-01-0.json")
push_events = push_events.filter(col("type") == "PushEvent")
push_events = push_events.select(
    col("actor.login").alias("developer"),
    col("created_at").alias("timestamp"),
    col("payload.size").alias("num_commits")
)

push_events = push_events.withColumn("timestamp", to_timestamp(col("timestamp")))
push_events = push_events.withColumn("hour_of_day", hour(col("timestamp")))
push_events = push_events.withColumn("day_of_week", dayofweek(col("timestamp")))

developer_baselines = push_events.groupBy("developer").agg(
    avg("num_commits").alias("avg_commits_per_push"),
    stddev("num_commits").alias("stddev_commits_per_push"),
    count("*").alias("total_pushes"),
    avg("hour_of_day").alias("avg_hour_of_day")
)

developer_baselines.show(10, truncate=False)
print(f"\nTotal developers analyzed: {developer_baselines.count()}")

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
connection_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

developer_baselines.write.mode("overwrite").jdbc(
        url=jdbc_url,
        table="developer_baselines",
        properties=connection_properties)

spark.stop()