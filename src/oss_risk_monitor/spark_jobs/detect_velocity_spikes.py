from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, when, abs
import os

db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

spark = SparkSession.builder.appName("DetectVelocitySpikes").getOrCreate()

push_events = spark.read.json("/data/raw/github-archive/2024-12-01-0.json")
push_events = push_events.filter(col("type") == "PushEvent")

current_activity = push_events.select(
    col("repo.name").alias("repo"),
    col("payload.size").alias("num_commits")
)

repo_baselines = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "repo_baselines").option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver").load()

joined = current_activity.join(repo_baselines, on="repo", how="inner")

# Calculate z-score
anomalies = joined.withColumn("z_score", (col("num_commits") - col("avg_commits")) / col("stddev_commits"))
anomalies = anomalies.filter(abs(col("z_score")) > 3)

anomalies.show(10, truncate=False)
print(f"\nTotal anomalies detected: {anomalies.count()}")

connection_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

anomalies.write.mode("overwrite").jdbc(
        url=jdbc_url,
        table="velocity_anomalies",
        properties=connection_properties)

spark.stop()