from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp

spark = SparkSession.builder.appName("ExtractPushEvents").getOrCreate()

df = spark.read.json("/data/raw/github-archive/2024-12-01-0.json")

push_events = df.filter(col("type") == "PushEvent")

simplified = push_events.select(
    col("id"), col("actor.login").alias("actor"), 
    col("repo.name").alias("repo"), col("created_at"),
    col("payload.size").alias("num_commits")
)

simplified = simplified.withColumn("hour_of_day", hour(to_timestamp("created_at")))

print("Simplified PushEvents")
simplified.show(10, truncate=False)

print(f"\nTotal PushEvents: {simplified.count()}")

spark.stop()